use std::{
    collections::{HashMap, HashSet, VecDeque},
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::{Error, Result, anyhow};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int16Array, Int64Array, ListArray, RecordBatch, UInt32Array,
    builder::BinaryBuilder,
};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef,
};
use arrow_select::take::take;
use async_trait::async_trait;
use beam_model_rs::v1::{
    Coder, Components, Environment, FunctionSpec, PCollection, PTransform, WindowingStrategy,
    merge_status,
};
use datafusion::{
    catalog::streaming::StreamingTable,
    error::DataFusionError,
    execution::{TaskContext, memory_pool::FairSpillPool, runtime_env::RuntimeEnvBuilder},
    physical_plan::{
        SendableRecordBatchStream, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
    },
    prelude::{SessionConfig, SessionContext, col},
};
use log::{info, warn};
use paimon::table::ArrowRecordBatchStream;
use tempfile::TempDir;
use tokio_stream::StreamExt;

use crate::{
    coders::primitives::{BeamWindow, WindowedValue},
    engine::{
        trigger::{
            AccumulationMode, OutputTime, Trigger, TriggerContext, TriggerRunner, TriggerSpec,
            pane_info, pane_timing,
        },
        window::{MergedWindows, WindowMergeService},
    },
    jobservice::urns::beam_urns,
    store::{
        KEY_COLUMN, VALUE_COLUMN,
        element_store::{
            FlareElementStore, PCollectionWriter, WINDOW_COUNT_COLUMN, WINDOW_EXTRA_KEYS_COLUMN,
            WINDOW_KEY_COLUMN, WINDOW_TIMESTAMP_COLUMN,
        },
        record::{
            BeamKV, BeamRecord, IterableValue, PrimitiveValue, RecordTableSchema,
            primitive_value_from_array_row, record_batch_to_beamrecords,
        },
    },
    transforms::{ExecutionContext, FlareTransform},
};

const GROUP_KEY_COLUMN: &str = "flare_gbk_group_key";
const DEFAULT_SORT_MEMORY_LIMIT_BYTES: usize = 1024 * 1024 * 1024;
const DEFAULT_SORT_SPILL_LIMIT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_SORT_SPILL_RESERVATION_BYTES: usize = 10 * 1024 * 1024;
// Match the normal Arrow batch scale instead of paying Paimon/Arrow setup costs
// for every 1,024 generated groups.
const OUTPUT_BATCH_SIZE: usize = 8 * 1024;

/// DataFusion resources used to externally sort one bundle's input.
///
/// These are deliberately process-level knobs for now. Durable streaming state
/// will have its own configuration; this only controls the bounded external sort
/// used by the current runner-native GroupByKey.
#[derive(Debug, Clone, Copy)]
struct GbkSortConfig {
    memory_limit_bytes: usize,
    spill_limit_bytes: u64,
    spill_reservation_bytes: usize,
}

impl GbkSortConfig {
    fn from_env() -> Self {
        Self {
            memory_limit_bytes: parse_env_bytes(
                "FLAREDB_GBK_MEMORY_LIMIT_BYTES",
                DEFAULT_SORT_MEMORY_LIMIT_BYTES,
            ),
            spill_limit_bytes: parse_env_u64(
                "FLAREDB_GBK_SPILL_LIMIT_BYTES",
                DEFAULT_SORT_SPILL_LIMIT_BYTES,
            ),
            spill_reservation_bytes: parse_env_bytes(
                "FLAREDB_GBK_SORT_SPILL_RESERVATION_BYTES",
                DEFAULT_SORT_SPILL_RESERVATION_BYTES,
            ),
        }
    }
}

fn parse_env_bytes(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn parse_env_u64(name: &str, default: u64) -> u64 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

/// Accumulated state for one `(key, window)` group, carried across runs.
///
/// Holds the values seen so far, the trigger state machine for this group, and
/// the pane bookkeeping needed to describe the next firing.
struct WindowState {
    key: PrimitiveValue,
    window: BeamWindow,
    elements: Vec<PaneElement>,
    pane_timestamps: Vec<i64>,
    trigger: TriggerRunner,
    pane_index: i64,
    /// A firing has happened for this window.
    fired: bool,
    /// The closing pane has been emitted; no further firings.
    closed: bool,
    /// This window has already been evaluated during the current GBK execution.
    evaluated_this_run: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct GroupIdentity {
    key: Vec<u8>,
    window: Vec<u8>,
}

impl WindowState {
    fn new(key: PrimitiveValue, window: BeamWindow, trigger: TriggerRunner) -> Self {
        Self {
            key,
            window,
            elements: Vec::new(),
            pane_timestamps: Vec::new(),
            trigger,
            pane_index: 0,
            fired: false,
            closed: false,
            evaluated_this_run: false,
        }
    }
}

#[derive(Debug, Clone)]
struct PaneElement {
    value: BeamRecord,
    timestamp_millis: i64,
}

/// Storage boundary for accumulated GroupByKey state.
///
/// The current implementation is intentionally in-memory. This boundary keeps
/// sorting/group formation separate from state retention so a future Paimon +
/// SlateDB implementation can replace it without changing the bounded sort loop.
#[async_trait]
trait GbkStateBackend: Send + Sync {
    async fn begin_run(&self);

    async fn last_evaluated_watermark(&self) -> Option<i64>;

    async fn mark_watermark_evaluated(&self, watermark: i64);

    async fn apply_group(
        &self,
        key: PrimitiveValue,
        window: BeamWindow,
        elements: Vec<PaneElement>,
        spec: &TriggerSpec,
        input_watermark: i64,
        processing_time: i64,
    ) -> Result<Option<WindowedValue>>;

    async fn take_ready(
        &self,
        spec: &TriggerSpec,
        input_watermark: i64,
        processing_time: i64,
        limit: usize,
    ) -> Result<Vec<WindowedValue>>;
}

#[derive(Default)]
struct InMemoryGbkStateInner {
    states: HashMap<GroupIdentity, WindowState>,
    ready_scan: Option<VecDeque<GroupIdentity>>,
    last_evaluated_watermark: Option<i64>,
}

#[derive(Clone, Default)]
struct InMemoryGbkState {
    inner: Arc<Mutex<InMemoryGbkStateInner>>,
}

#[async_trait]
impl GbkStateBackend for InMemoryGbkState {
    async fn begin_run(&self) {
        let mut inner = self.inner.lock().expect("gbk state lock not poisoned");
        inner.ready_scan = None;
        for state in inner.states.values_mut() {
            state.evaluated_this_run = false;
        }
    }

    async fn last_evaluated_watermark(&self) -> Option<i64> {
        self.inner
            .lock()
            .expect("gbk state lock not poisoned")
            .last_evaluated_watermark
    }

    async fn mark_watermark_evaluated(&self, watermark: i64) {
        self.inner
            .lock()
            .expect("gbk state lock not poisoned")
            .last_evaluated_watermark = Some(watermark);
    }

    async fn apply_group(
        &self,
        key: PrimitiveValue,
        window: BeamWindow,
        elements: Vec<PaneElement>,
        spec: &TriggerSpec,
        input_watermark: i64,
        processing_time: i64,
    ) -> Result<Option<WindowedValue>> {
        let end = window.max_timestamp_millis();
        let expired = spec.earliest_completion(end) < input_watermark;
        let mut inner = self.inner.lock().expect("gbk state lock not poisoned");
        if inner.states.is_empty()
            && expired
            && spec.trigger == Trigger::Default
            && spec.accumulation == AccumulationMode::Discarding
        {
            let timestamp_millis = output_timestamp(
                spec.output_time,
                &window,
                &elements
                    .iter()
                    .map(|element| element.timestamp_millis)
                    .collect::<Vec<_>>(),
            );
            return Ok(Some(WindowedValue {
                value: BeamRecord::KV(BeamKV {
                    key,
                    value: Box::new(BeamRecord::ITERABLE(IterableValue::from_records(
                        elements.into_iter().map(|element| element.value).collect(),
                    ))),
                }),
                timestamp_millis,
                windows: vec![window],
                pane: pane_info(crate::coders::primitives::PaneTiming::OnTime, 0, true, true),
            }));
        }

        let identity = group_identity(&key, &window);
        let state = inner.states.entry(identity.clone()).or_insert_with(|| {
            WindowState::new(
                key,
                window.clone(),
                TriggerRunner::new(spec.trigger.clone()),
            )
        });
        for _ in 0..elements.len() {
            state.trigger.on_element(processing_time);
        }
        state
            .pane_timestamps
            .extend(elements.iter().map(|element| element.timestamp_millis));
        state.elements.extend(elements);

        let output = fire_state(state, spec, input_watermark, processing_time);
        if state.closed {
            inner.states.remove(&identity);
        }
        Ok(output)
    }

    async fn take_ready(
        &self,
        spec: &TriggerSpec,
        input_watermark: i64,
        processing_time: i64,
        limit: usize,
    ) -> Result<Vec<WindowedValue>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut inner = self.inner.lock().expect("gbk state lock not poisoned");
        if inner.ready_scan.is_none() {
            inner.ready_scan = Some(inner.states.keys().cloned().collect());
        }
        let mut output = Vec::new();
        while output.len() < limit {
            let Some(identity) = inner.ready_scan.as_mut().and_then(VecDeque::pop_front) else {
                break;
            };
            let Some(state) = inner.states.get_mut(&identity) else {
                continue;
            };
            if state.evaluated_this_run {
                continue;
            }
            if let Some(value) = fire_state(state, spec, input_watermark, processing_time) {
                output.push(value);
            }
            if state.closed {
                inner.states.remove(&identity);
            }
        }
        if inner.ready_scan.as_ref().is_some_and(VecDeque::is_empty) {
            inner.ready_scan = None;
        }
        Ok(output)
    }
}

fn output_timestamp(output_time: OutputTime, window: &BeamWindow, pane_timestamps: &[i64]) -> i64 {
    match output_time {
        OutputTime::LatestInPane => pane_timestamps
            .iter()
            .copied()
            .max()
            .unwrap_or_else(|| window.max_timestamp_millis()),
        OutputTime::EarliestInPane => pane_timestamps
            .iter()
            .copied()
            .min()
            .unwrap_or_else(|| window.max_timestamp_millis()),
        OutputTime::EndOfWindow => window.max_timestamp_millis(),
    }
}

/// Identity of a `(key, window)` group, used to key the state map.
fn group_identity(key: &PrimitiveValue, window: &BeamWindow) -> GroupIdentity {
    GroupIdentity {
        key: encoded_primitive_sort_key(key),
        window: window.to_binary_key(),
    }
}

fn window_prune_cutoff(
    previous_watermark: Option<i64>,
    allowed_lateness: i64,
    needs_merge: bool,
    is_global_window: bool,
    input_is_bounded: bool,
) -> Option<i64> {
    if needs_merge || is_global_window || input_is_bounded {
        return None;
    }
    previous_watermark.map(|watermark| watermark.saturating_sub(allowed_lateness))
}

/// Runner-native implementation of Beam's `GroupByKey`.
///
/// Groups input `KV<K, V>` elements by `(key, window)` and emits
/// `KV<K, Iterable<V>>` whenever the window's trigger fires — Beam's
/// "ungrouping" step. The trigger comes from the input's windowing strategy, so a
/// single window can emit an early pane, an on-time pane, and late panes, subject
/// to its accumulation mode.
///
/// It consumes its input incrementally: each run reads only the rows appended
/// since its last run (via this stage's read cursor) and folds them into
/// per-group state. DataFusion externally sorts the incremental input by an
/// encoded key and canonical window, then the runner consumes that globally
/// ordered stream one group at a time. The accumulated state (values, trigger,
/// pane) is kept here across runs.
#[derive(Clone)]
pub struct GroupByKey {
    name: String,
    id: String,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
    state: InMemoryGbkState,
}

#[async_trait]
impl FlareTransform for GroupByKey {
    fn urn() -> &'static str
    where
        Self: Sized,
    {
        beam_urns::GROUP_BY_KEY_TRANSFORM
    }

    fn with(
        id: String,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        name: String,
    ) -> Self {
        Self {
            id,
            inputs,
            outputs,
            name,
            state: InMemoryGbkState::default(),
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> Result<(), Error> {
        // GroupByKey consumes exactly one input PCollection.
        let input_pcollection_id = ctx
            .input_pcollection_ids
            .first()
            .cloned()
            .expect("GroupByKey expects exactly one input PCollection");

        let spec = TriggerSpec::from_windowing_strategy(ctx.windowing_strategy.as_ref());
        let needs_merge = ctx
            .windowing_strategy
            .as_ref()
            .is_some_and(|strategy| strategy.merge_status == merge_status::Enum::NeedsMerge as i32);
        let is_global_window = ctx
            .windowing_strategy
            .as_ref()
            .and_then(|strategy| strategy.window_fn.as_ref())
            .is_none_or(|window_fn| window_fn.urn == beam_urns::GLOBAL_WINDOWS_FN);
        let previous_watermark = self.state.last_evaluated_watermark().await;
        let window_prune_before = window_prune_cutoff(
            previous_watermark,
            spec.allowed_lateness,
            needs_merge,
            is_global_window,
            ctx.input_is_bounded,
        );
        if let Some(cutoff) = window_prune_before {
            info!(
                "GroupByKey: pruning finite windows ending at or before {cutoff} based on previously evaluated watermark {previous_watermark:?}"
            );
        } else if ctx.input_is_bounded && previous_watermark.is_some() {
            info!("GroupByKey: input pruning disabled for a bounded PCollection");
        } else if is_global_window && previous_watermark.is_some() {
            info!("GroupByKey: input pruning disabled for the global window");
        } else if needs_merge && previous_watermark.is_some() {
            info!(
                "GroupByKey: input pruning disabled because the windowing strategy requires merging"
            );
        }
        self.state.begin_run().await;

        // Read only the input appended since this stage's last run. The cursor is
        // this stage's position in the input PCollection's changelog.
        let cursor = ctx
            .store
            .cursor(&ctx.stage_id, &input_pcollection_id)
            .await?;
        let dropped_due_to_lateness = if let Some(cutoff) = window_prune_before {
            let dropped = ctx
                .store
                .count_incremental_arrow_pruned_before(&input_pcollection_id, cursor, cutoff)
                .await?;
            if dropped > 0 {
                warn!(
                    "GroupByKey: dropped {dropped} input row(s) due to lateness at cutoff {cutoff}; \
                     bounded global-window pipelines must not reach this path"
                );
            } else {
                info!("GroupByKey: lateness pruning dropped 0 input row(s) at cutoff {cutoff}");
            }
            dropped
        } else {
            0
        };
        let config = GbkSortConfig::from_env();
        let (sorted_inputs, latest) = open_sorted_input_streams(
            &ctx.store,
            &input_pcollection_id,
            cursor,
            window_prune_before,
            &config,
        )
        .await?;
        let mut writer =
            PCollectionWriter::new(ctx.store.clone(), ctx.output_pcollection_id.clone());
        let mut output = Vec::with_capacity(OUTPUT_BATCH_SIZE);
        let mut fired = 0usize;
        let mut decoded_rows = 0usize;

        let sort_started = Instant::now();
        let mut next_progress = 1_000_000usize;
        let mut decode_time = std::time::Duration::ZERO;
        let mut group_time = std::time::Duration::ZERO;
        let mut write_time = std::time::Duration::ZERO;
        for (bucket, mut sorted_input) in sorted_inputs {
            info!("GroupByKey: processing bucket {bucket} for {input_pcollection_id}");
            let sort_result: Result<()> = async {
                let mut decoder = SortedGroupDecoder::default();
                while let Some(batch) = sorted_input.stream.next().await {
                    let batch = batch?;
                    decoded_rows += batch.num_rows();
                    let decode_started = Instant::now();
                    let groups = decoder.push_batch(&batch, &sorted_input.table_schema)?;
                    decode_time += decode_started.elapsed();
                    let group_started = Instant::now();
                    fired += process_key_groups_batch(
                        groups,
                        needs_merge,
                        ctx.window_merger.as_deref(),
                        &input_pcollection_id,
                        &spec,
                        ctx.input_watermark,
                        ctx.processing_time,
                        &self.state,
                        &mut output,
                    )
                    .await?;
                    group_time += group_started.elapsed();
                    let write_started = Instant::now();
                    flush_output_if_full(&mut writer, &mut output).await?;
                    write_time += write_started.elapsed();
                    if decoded_rows >= next_progress {
                        info!(
                            "GroupByKey: decoded {decoded_rows} rows, fired {fired} panes in {:?} (decode={decode_time:?}, group/merge={group_time:?}, output_write={write_time:?})",
                            sort_started.elapsed(),
                        );
                        next_progress = (decoded_rows / 1_000_000 + 1) * 1_000_000;
                    }
                }
                if let Some(key_group) = decoder.finish() {
                    fired += process_key_groups_batch(
                        vec![key_group],
                        needs_merge,
                        ctx.window_merger.as_deref(),
                        &input_pcollection_id,
                        &spec,
                        ctx.input_watermark,
                        ctx.processing_time,
                        &self.state,
                        &mut output,
                    )
                    .await?;
                }
                Ok(())
            }
            .await;
            if let Err(error) = sort_result {
                let actual_spill_bytes = directory_size_bytes(sorted_input._temp_dir.path());
                let configured_spill_limit = GbkSortConfig::from_env().spill_limit_bytes;
                warn!(
                    "GroupByKey: external sort failed for bucket {bucket} \
                     (configured_spill_limit={configured_spill_limit} bytes, \
                     actual_spill_bytes={actual_spill_bytes}): {error:#}"
                );
                return Err(anyhow!(
                    "GroupByKey external sort failed for bucket {bucket} \
                     (configured_spill_limit={configured_spill_limit} bytes, \
                     actual_spill_bytes={actual_spill_bytes}): {error:#}"
                ));
            }
        }

        // A watermark advance can make groups from an earlier run ready without
        // any new input. Drain those in bounded chunks as well.
        loop {
            let ready = self
                .state
                .take_ready(
                    &spec,
                    ctx.input_watermark,
                    ctx.processing_time,
                    OUTPUT_BATCH_SIZE,
                )
                .await?;
            if ready.is_empty() {
                break;
            }
            fired += ready.len();
            writer.write(ready).await?;
        }
        flush_output_if_full(&mut writer, &mut output).await?;
        if !output.is_empty() {
            writer.write(std::mem::take(&mut output)).await?;
        }

        // Advance the input cursor only after all emitted panes are durable.
        // Otherwise a failed output commit could make a retry skip its input.
        writer.commit().await?;
        if let Some(latest) = latest {
            ctx.store
                .set_cursor(&ctx.stage_id, &input_pcollection_id, latest)
                .await?;
        }
        // A runner stage can be scheduled once with no input at the bounded
        // sentinel watermark. Recording that watermark would make rows produced
        // afterward look late. Only arm pruning after input has been observed.
        if decoded_rows > 0 || previous_watermark.is_some() {
            self.state
                .mark_watermark_evaluated(ctx.input_watermark)
                .await;
        }

        if fired == 0 {
            info!(
                "GroupByKey: no window fired at input watermark {} (dropped_due_to_lateness={})",
                ctx.input_watermark, dropped_due_to_lateness
            );
        } else {
            info!(
                "Executed GroupByKey: {} pane(s) fired at input watermark {} \
                 (dropped_due_to_lateness={})",
                fired, ctx.input_watermark, dropped_due_to_lateness
            );
        }

        Ok(())
    }

    fn output_pcol_ids(&self) -> HashSet<String> {
        self.outputs.clone().into_values().collect()
    }

    fn unique_name(&self) -> String {
        self.name.clone()
    }

    fn windowing_strategies(&self) -> HashMap<String, WindowingStrategy> {
        HashMap::new()
    }

    fn coders(&self) -> HashMap<String, Coder> {
        HashMap::new()
    }

    fn environments(&self) -> HashMap<String, Environment> {
        HashMap::new()
    }

    fn transfrom_spec(&self) -> HashMap<String, PTransform> {
        let mut transforms = HashMap::new();
        transforms.insert(
            self.id.clone(),
            PTransform {
                spec: Some(FunctionSpec {
                    urn: Self::urn().to_string(),
                    payload: Vec::new(),
                }),
                inputs: self.inputs.clone(),
                outputs: self.outputs.clone(),
                unique_name: self.name.clone(),
                subtransforms: Vec::new(),
                environment_id: String::new(),
                display_data: Vec::new(),
                annotations: HashMap::new(),
            },
        );
        transforms
    }

    fn pcollections(&self, components: &Components) -> HashMap<String, PCollection> {
        self.inputs
            .values()
            .chain(self.outputs.values())
            .filter_map(|id| {
                components
                    .pcollections
                    .get(id)
                    .cloned()
                    .map(|pcollection| (id.clone(), pcollection))
            })
            .collect()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

struct SortedInputStream {
    stream: SendableRecordBatchStream,
    table_schema: Arc<RecordTableSchema>,
    /// DataFusion removes its spill files when the runtime is dropped, but the
    /// directory itself is owned here so it also disappears on error paths.
    _temp_dir: TempDir,
}

struct OneShotPaimonPartition {
    schema: SchemaRef,
    stream: Mutex<Option<ArrowRecordBatchStream>>,
    rows_seen: Arc<AtomicU64>,
    started: Instant,
}

impl std::fmt::Debug for OneShotPaimonPartition {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OneShotPaimonPartition")
            .field("schema", &self.schema)
            .finish_non_exhaustive()
    }
}

impl PartitionStream for OneShotPaimonPartition {
    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
        let stream = self
            .stream
            .lock()
            .expect("gbk paimon stream lock not poisoned")
            .take()
            .expect("GroupByKey input stream may only be executed once");
        let rows_seen = Arc::clone(&self.rows_seen);
        let started = self.started;
        let stream = futures::StreamExt::flat_map(stream, move |batch| {
            let batches = match batch {
                Ok(batch) => {
                    let previous = rows_seen.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
                    let current = previous + batch.num_rows() as u64;
                    if previous / 1_000_000 != current / 1_000_000 {
                        info!(
                            "GroupByKey: sorted {current} input rows in {:?}",
                            started.elapsed()
                        );
                    }
                    match add_encoded_key_column(batch).and_then(flatten_window_batch) {
                        Ok(batches) => batches.into_iter().map(Ok).collect(),
                        Err(error) => {
                            vec![Err(DataFusionError::Execution(error.to_string()))]
                        }
                    }
                }
                Err(error) => vec![Err(DataFusionError::External(Box::new(error)))],
            };
            tokio_stream::iter(batches)
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

/// Open lazy Paimon incremental reads grouped by bucket and feed each bucket
/// directly into its own DataFusion sort.
///
/// The sort key is a deterministic binary encoding of the Beam key followed by
/// the canonical window key. `value` is carried through the sort but never
/// participates in comparison, because Beam does not define an order for a
/// GroupByKey iterable. Bucketing by the original Beam key guarantees that a
/// key's windows are not split across these sorts.
async fn open_sorted_input_streams(
    store: &Arc<FlareElementStore>,
    pcollection_id: &str,
    cursor: Option<i64>,
    window_prune_before: Option<i64>,
    config: &GbkSortConfig,
) -> Result<(Vec<(i32, SortedInputStream)>, Option<i64>)> {
    let projection = [
        KEY_COLUMN,
        VALUE_COLUMN,
        WINDOW_TIMESTAMP_COLUMN,
        WINDOW_KEY_COLUMN,
        WINDOW_EXTRA_KEYS_COLUMN,
        WINDOW_COUNT_COLUMN,
    ];
    let (streams, latest) = store
        .open_incremental_arrow_by_bucket(
            pcollection_id,
            cursor,
            Some(&projection),
            window_prune_before,
        )
        .await?;
    if streams.is_empty() {
        return Ok((Vec::new(), latest));
    }

    let table_schema = store.registry.get(pcollection_id).ok_or_else(|| {
        anyhow!("GroupByKey input PCollection {pcollection_id} has no registered schema")
    })?;
    let input_schema = gbk_input_schema(&table_schema.arrow_schema)?;
    let schema = gbk_flattened_schema(&input_schema);
    let spill_parent = Path::new(store.catalog.warehouse()).join(".gbk-spill");
    std::fs::create_dir_all(&spill_parent)?;
    let mut sorted_streams = Vec::with_capacity(streams.len());
    for (bucket, stream) in streams {
        let temp_dir = tempfile::Builder::new()
            .prefix(&format!("group-by-key-bucket-{bucket}-"))
            .tempdir_in(&spill_parent)?;
        info!(
            "GroupByKey: starting external sort for {pcollection_id} bucket {bucket} \
             (memory={} bytes, spill_limit={} bytes, spill_reservation={} bytes)",
            config.memory_limit_bytes, config.spill_limit_bytes, config.spill_reservation_bytes
        );

        let runtime = Arc::new(
            RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(FairSpillPool::new(config.memory_limit_bytes)))
                .with_temp_file_path(temp_dir.path())
                .with_max_temp_directory_size(config.spill_limit_bytes)
                .build()?,
        );
        let session = SessionContext::new_with_config_rt(
            SessionConfig::new()
                .with_target_partitions(1)
                .with_sort_spill_reservation_bytes(config.spill_reservation_bytes),
            runtime,
        );
        let partition: Arc<dyn PartitionStream> = Arc::new(OneShotPaimonPartition {
            schema: Arc::clone(&schema),
            stream: Mutex::new(Some(stream)),
            rows_seen: Arc::new(AtomicU64::new(0)),
            started: Instant::now(),
        });
        session.register_table(
            "gbk_input",
            Arc::new(StreamingTable::try_new(
                Arc::clone(&schema),
                vec![partition],
            )?),
        )?;

        let df = session.table("gbk_input").await?;
        let df = df.sort(vec![
            col(GROUP_KEY_COLUMN).sort(true, false),
            col(WINDOW_KEY_COLUMN).sort(true, false),
        ])?;
        let stream = df.execute_stream().await?;
        sorted_streams.push((
            bucket,
            SortedInputStream {
                stream,
                table_schema: table_schema.clone(),
                _temp_dir: temp_dir,
            },
        ));
    }

    Ok((sorted_streams, latest))
}

fn directory_size_bytes(path: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(path) else {
        return 0;
    };
    entries
        .flatten()
        .map(|entry| {
            let path = entry.path();
            entry
                .metadata()
                .ok()
                .map(|metadata| {
                    if metadata.is_dir() {
                        directory_size_bytes(&path)
                    } else {
                        metadata.len()
                    }
                })
                .unwrap_or(0)
        })
        .sum()
}

fn gbk_input_schema(table_schema: &ArrowSchema) -> Result<Arc<ArrowSchema>> {
    let fields = [
        KEY_COLUMN,
        VALUE_COLUMN,
        WINDOW_TIMESTAMP_COLUMN,
        WINDOW_KEY_COLUMN,
        WINDOW_EXTRA_KEYS_COLUMN,
        WINDOW_COUNT_COLUMN,
    ]
    .into_iter()
    .map(|name| {
        table_schema
            .field_with_name(name)
            .map(|field| {
                Arc::new(ArrowField::new(
                    field.name(),
                    normalize_paimon_list_data_type(field.data_type()),
                    field.is_nullable(),
                ))
            })
            .map_err(|_| anyhow!("GroupByKey input is missing the '{name}' column"))
    })
    .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(ArrowSchema::new_with_metadata(
        fields,
        table_schema.metadata().clone(),
    )))
}

/// Paimon materializes Arrow list children as `element`; the writer-side schema
/// cached in FlareDB names them `item`. DataFusion compares the full list field
/// identity, so normalize the transient GBK sort schema to match Paimon's reader.
fn normalize_paimon_list_data_type(data_type: &ArrowDataType) -> ArrowDataType {
    match data_type {
        ArrowDataType::List(child) => ArrowDataType::List(Arc::new(ArrowField::new(
            "element",
            normalize_paimon_list_data_type(child.data_type()),
            child.is_nullable(),
        ))),
        ArrowDataType::LargeList(child) => ArrowDataType::LargeList(Arc::new(ArrowField::new(
            "element",
            normalize_paimon_list_data_type(child.data_type()),
            child.is_nullable(),
        ))),
        ArrowDataType::FixedSizeList(child, size) => ArrowDataType::FixedSizeList(
            Arc::new(ArrowField::new(
                "element",
                normalize_paimon_list_data_type(child.data_type()),
                child.is_nullable(),
            )),
            *size,
        ),
        other => other.clone(),
    }
}

fn gbk_flattened_schema(input_schema: &ArrowSchema) -> SchemaRef {
    let mut fields = [
        KEY_COLUMN,
        VALUE_COLUMN,
        WINDOW_TIMESTAMP_COLUMN,
        WINDOW_KEY_COLUMN,
    ]
    .into_iter()
    .map(|name| {
        input_schema
            .field_with_name(name)
            .expect("GBK input schema was validated")
            .clone()
    })
    .map(Arc::new)
    .collect::<Vec<_>>();
    fields.push(Arc::new(ArrowField::new(
        GROUP_KEY_COLUMN,
        ArrowDataType::Binary,
        false,
    )));
    Arc::new(ArrowSchema::new_with_metadata(
        fields,
        input_schema.metadata().clone(),
    ))
}

/// Expands scalar-plus-extra window storage into one row per `(element, window)`.
///
/// The common one-window case takes the scalar path without materializing a
/// generic list. Multi-window rows append only their extra windows. This keeps
/// the Paimon stream single-pass while preserving Beam's per-window grouping.
fn flatten_window_batch(batch: RecordBatch) -> Result<Vec<RecordBatch>> {
    let counts = batch
        .column_by_name(WINDOW_COUNT_COLUMN)
        .ok_or_else(|| anyhow!("GBK input is missing '{WINDOW_COUNT_COLUMN}'"))?
        .as_any()
        .downcast_ref::<Int16Array>()
        .ok_or_else(|| anyhow!("'{WINDOW_COUNT_COLUMN}' must be Int16"))?;
    let first_windows = batch
        .column_by_name(WINDOW_KEY_COLUMN)
        .ok_or_else(|| anyhow!("GBK input is missing '{WINDOW_KEY_COLUMN}'"))?
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| anyhow!("'{WINDOW_KEY_COLUMN}' must be Binary"))?;
    let extra_windows = batch
        .column_by_name(WINDOW_EXTRA_KEYS_COLUMN)
        .ok_or_else(|| anyhow!("GBK input is missing '{WINDOW_EXTRA_KEYS_COLUMN}'"))?
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow!("'{WINDOW_EXTRA_KEYS_COLUMN}' must be a List"))?;
    let extra_window_values = extra_windows
        .values()
        .as_any()
        .downcast_ref::<BinaryArray>()
        .ok_or_else(|| anyhow!("'{WINDOW_EXTRA_KEYS_COLUMN}' must contain Binary keys"))?;
    let offsets = extra_windows.value_offsets();

    let mut scalar_rows = Vec::with_capacity(batch.num_rows());
    let mut extra_rows = Vec::new();
    let mut extra_window_indices = Vec::new();
    for row in 0..batch.num_rows() {
        let declared = usize::try_from(counts.value(row))
            .map_err(|_| anyhow!("negative window count at GBK input row {row}"))?;
        let start = usize::try_from(offsets[row])
            .map_err(|_| anyhow!("negative extra-window offset at GBK input row {row}"))?;
        let end = usize::try_from(offsets[row + 1])
            .map_err(|_| anyhow!("negative extra-window offset at GBK input row {}", row + 1))?;
        let extra_count = end.saturating_sub(start);
        let first_present = !first_windows.is_null(row);
        if declared != extra_count + usize::from(first_present) {
            return Err(anyhow!(
                "window count does not match scalar and extra keys at GBK input row {row}"
            ));
        }
        if first_present {
            scalar_rows.push(u32::try_from(row).map_err(|_| anyhow!("GBK row index exceeds u32"))?);
        }
        for window_index in start..end {
            if extra_window_values.is_null(window_index) {
                return Err(anyhow!("null extra window key at GBK input row {row}"));
            }
            extra_rows.push(u32::try_from(row).map_err(|_| anyhow!("GBK row index exceeds u32"))?);
            extra_window_indices.push(
                u32::try_from(window_index).map_err(|_| anyhow!("GBK window index exceeds u32"))?,
            );
        }
    }

    let mut batches = Vec::with_capacity(2);
    if !scalar_rows.is_empty() {
        batches.push(take_flattened_window_batch(
            &batch,
            &scalar_rows,
            first_windows,
            &scalar_rows,
        )?);
    }
    if !extra_rows.is_empty() {
        batches.push(take_flattened_window_batch(
            &batch,
            &extra_rows,
            extra_window_values,
            &extra_window_indices,
        )?);
    }
    Ok(batches)
}

fn take_flattened_window_batch(
    batch: &RecordBatch,
    row_indices: &[u32],
    window_values: &BinaryArray,
    window_indices: &[u32],
) -> Result<RecordBatch> {
    let indices = UInt32Array::from(row_indices.to_vec());
    let window_indices = UInt32Array::from(window_indices.to_vec());
    let key = batch
        .column_by_name(KEY_COLUMN)
        .ok_or_else(|| anyhow!("GBK input is missing '{KEY_COLUMN}'"))?;
    let value = batch
        .column_by_name(VALUE_COLUMN)
        .ok_or_else(|| anyhow!("GBK input is missing '{VALUE_COLUMN}'"))?;
    let timestamp = batch
        .column_by_name(WINDOW_TIMESTAMP_COLUMN)
        .ok_or_else(|| anyhow!("GBK input is missing '{WINDOW_TIMESTAMP_COLUMN}'"))?;
    let group_key = batch
        .column_by_name(GROUP_KEY_COLUMN)
        .ok_or_else(|| anyhow!("GBK input is missing '{GROUP_KEY_COLUMN}'"))?;
    let columns = vec![
        take(key.as_ref(), &indices, None)?,
        take(value.as_ref(), &indices, None)?,
        take(timestamp.as_ref(), &indices, None)?,
        take(window_values, &window_indices, None)?,
        take(group_key.as_ref(), &indices, None)?,
    ];
    let source_schema = batch.schema();
    let fields = vec![
        Arc::new(source_schema.field_with_name(KEY_COLUMN)?.clone()),
        Arc::new(source_schema.field_with_name(VALUE_COLUMN)?.clone()),
        Arc::new(
            source_schema
                .field_with_name(WINDOW_TIMESTAMP_COLUMN)?
                .clone(),
        ),
        Arc::new(ArrowField::new(
            WINDOW_KEY_COLUMN,
            window_values.data_type().clone(),
            true,
        )),
        Arc::new(source_schema.field_with_name(GROUP_KEY_COLUMN)?.clone()),
    ];
    let schema = Arc::new(ArrowSchema::new_with_metadata(
        fields,
        source_schema.metadata().clone(),
    ));
    Ok(RecordBatch::try_new(schema, columns)?)
}

fn schema_with_group_key(schema: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<Arc<ArrowField>> = schema.fields().iter().cloned().collect();
    fields.push(Arc::new(ArrowField::new(
        GROUP_KEY_COLUMN,
        ArrowDataType::Binary,
        false,
    )));
    Arc::new(ArrowSchema::new_with_metadata(
        fields,
        schema.metadata().clone(),
    ))
}

fn add_encoded_key_column(batch: RecordBatch) -> Result<RecordBatch> {
    let key_column = batch
        .column_by_name(KEY_COLUMN)
        .ok_or_else(|| anyhow!("GroupByKey input is missing the '{KEY_COLUMN}' column"))?;
    let mut keys = BinaryBuilder::new();
    for row in 0..batch.num_rows() {
        let key = primitive_value_from_array_row(key_column.as_ref(), key_column.data_type(), row)?;
        keys.append_value(encoded_primitive_sort_key(&key));
    }
    let keys: ArrayRef = Arc::new(keys.finish());

    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns.push(keys);
    let schema = schema_with_group_key(&batch.schema());
    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Encode a key with exactly the equality semantics used by [`PrimitiveValue`].
///
/// Typed Arrow ordering is insufficient for floating-point keys because FlareDB
/// treats distinct NaN payloads and signed zeroes as distinct keys. Encoding the
/// variant and float bits ensures sort equality and byte equality are identical.
fn encoded_primitive_sort_key(value: &PrimitiveValue) -> Vec<u8> {
    let mut encoded = Vec::new();
    match value {
        PrimitiveValue::String(value) => {
            encoded.push(0);
            encoded.extend_from_slice(value.as_bytes());
        }
        PrimitiveValue::Bytes(value) => {
            encoded.push(1);
            encoded.extend_from_slice(value);
        }
        PrimitiveValue::Int64(value) => {
            encoded.push(2);
            encoded.extend_from_slice(&value.to_be_bytes());
        }
        PrimitiveValue::Float64(value) => {
            encoded.push(3);
            encoded.extend_from_slice(&value.to_bits().to_be_bytes());
        }
        PrimitiveValue::Bool(value) => {
            encoded.push(4);
            encoded.push(u8::from(*value));
        }
        PrimitiveValue::Void => encoded.push(5),
    }
    encoded
}

struct DecodedKeyGroup {
    key: PrimitiveValue,
    windows: WindowValues,
}

struct InProgressKeyGroup {
    encoded_key: Vec<u8>,
    key: PrimitiveValue,
    windows: WindowValues,
}

#[derive(Default)]
struct SortedGroupDecoder {
    current: Option<InProgressKeyGroup>,
}

impl SortedGroupDecoder {
    fn push_batch(
        &mut self,
        batch: &RecordBatch,
        table_schema: &RecordTableSchema,
    ) -> Result<Vec<DecodedKeyGroup>> {
        let window_column = batch.column_by_name(WINDOW_KEY_COLUMN).ok_or_else(|| {
            anyhow!("GroupByKey result is missing the '{WINDOW_KEY_COLUMN}' column")
        })?;
        let window_keys = window_column
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("GroupByKey window key must be a Binary column"))?;
        let encoded_key_column = batch.column_by_name(GROUP_KEY_COLUMN).ok_or_else(|| {
            anyhow!("GroupByKey result is missing the '{GROUP_KEY_COLUMN}' column")
        })?;
        let encoded_keys = encoded_key_column
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("GroupByKey encoded key must be a Binary column"))?;
        let timestamps = batch
            .column_by_name(WINDOW_TIMESTAMP_COLUMN)
            .ok_or_else(|| anyhow!("GroupByKey is missing '{WINDOW_TIMESTAMP_COLUMN}'"))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow!("GroupByKey timestamp must be an Int64 column"))?;
        let records = record_batch_to_beamrecords(batch, table_schema)?;
        let mut completed = Vec::new();

        for (row, record) in records.into_iter().enumerate() {
            let BeamRecord::KV(kv) = record else {
                return Err(anyhow!(
                    "GroupByKey expected KV input, found {:?}",
                    record.record_type()
                ));
            };
            let encoded_key = encoded_keys.value(row).to_vec();
            let window_key = window_keys.value(row);
            let window = BeamWindow::from_binary_key(window_key)
                .ok_or_else(|| anyhow!("GroupByKey encountered a malformed binary window key"))?;
            let element = PaneElement {
                value: *kv.value,
                timestamp_millis: timestamps.value(row),
            };

            let replace_key = self
                .current
                .as_ref()
                .is_some_and(|current| current.encoded_key != encoded_key);
            if replace_key && let Some(finished) = self.current.take() {
                completed.push(DecodedKeyGroup {
                    key: finished.key,
                    windows: finished.windows,
                });
            }

            let current = self.current.get_or_insert_with(|| InProgressKeyGroup {
                encoded_key,
                key: kv.key,
                windows: Vec::new(),
            });
            match current.windows.last_mut() {
                Some((current_window, values))
                    if current_window.to_binary_key() == window.to_binary_key() =>
                {
                    values.push(element);
                }
                _ => current.windows.push((window, vec![element])),
            }
        }
        Ok(completed)
    }

    fn finish(&mut self) -> Option<DecodedKeyGroup> {
        self.current.take().map(|finished| DecodedKeyGroup {
            key: finished.key,
            windows: finished.windows,
        })
    }
}

type WindowValues = Vec<(BeamWindow, Vec<PaneElement>)>;

fn apply_merge_results(
    key: &PrimitiveValue,
    original_groups: Vec<(BeamWindow, Vec<PaneElement>)>,
    merge_results: Vec<MergedWindows>,
) -> Result<Vec<(PrimitiveValue, BeamWindow, Vec<PaneElement>)>, Error> {
    let original_to_merged: HashMap<Vec<u8>, BeamWindow> = merge_results
        .into_iter()
        .flat_map(|result| {
            let merged = result.merged;
            result
                .originals
                .into_iter()
                .map(move |original| (original.to_binary_key(), merged.clone()))
        })
        .collect();

    let mut merged_groups: HashMap<Vec<u8>, (BeamWindow, Vec<PaneElement>)> = HashMap::new();
    for (original, values) in original_groups {
        let original_key = original.to_binary_key();
        let merged_window = original_to_merged.get(&original_key).ok_or_else(|| {
            anyhow!("MergeWindows did not map an original window for key {key:?}")
        })?;
        let entry = merged_groups
            .entry(merged_window.to_binary_key())
            .or_insert_with(|| (merged_window.clone(), Vec::new()));
        entry.1.extend(values);
    }
    Ok(merged_groups
        .into_values()
        .map(|(window, values)| (key.clone(), window, values))
        .collect())
}

#[allow(clippy::too_many_arguments)]
async fn process_key_groups_batch(
    key_groups: Vec<DecodedKeyGroup>,
    needs_merge: bool,
    window_merger: Option<&WindowMergeService>,
    input_pcollection_id: &str,
    spec: &TriggerSpec,
    input_watermark: i64,
    processing_time: i64,
    state_backend: &dyn GbkStateBackend,
    output: &mut Vec<WindowedValue>,
) -> Result<usize> {
    if key_groups.is_empty() {
        return Ok(0);
    }
    let mut groups = Vec::new();
    let mut custom_merges = Vec::new();
    for key_group in key_groups {
        let is_interval = key_group
            .windows
            .iter()
            .all(|(window, _)| matches!(window, BeamWindow::Interval { .. }));
        if needs_merge && !is_interval {
            custom_merges.push(key_group);
            continue;
        }

        let groups_for_key: Vec<_> = key_group
            .windows
            .into_iter()
            .map(|(window, values)| (key_group.key.clone(), window, values))
            .collect();
        if needs_merge {
            groups.extend(merge_interval_groups(groups_for_key)?);
        } else {
            groups.extend(groups_for_key);
        }
    }

    if !custom_merges.is_empty() {
        let window_merger = window_merger.ok_or_else(|| {
            anyhow!(
                "GroupByKey input {input_pcollection_id} uses a merging window function, but the SDK window merger is unavailable"
            )
        })?;
        let requests: Vec<Vec<BeamWindow>> = custom_merges
            .iter()
            .map(|key_group| {
                key_group
                    .windows
                    .iter()
                    .map(|(window, _)| window.clone())
                    .collect()
            })
            .collect();
        let merged = window_merger
            .merge_windows_batch_for_pcollection(input_pcollection_id, &requests)
            .await?;
        for (key_group, merge_results) in custom_merges.into_iter().zip(merged) {
            groups.extend(apply_merge_results(
                &key_group.key,
                key_group.windows,
                merge_results,
            )?);
        }
    }

    let mut fired = 0;
    for (key, window, values) in groups {
        if let Some(value) = state_backend
            .apply_group(key, window, values, spec, input_watermark, processing_time)
            .await?
        {
            output.push(value);
            fired += 1;
        }
    }
    Ok(fired)
}

/// Merge standard interval session windows without crossing an SDK boundary.
///
/// Beam's built-in session window coder stores windows as intervals. For one
/// key, merging is deterministic: sort by start time and combine every pair with
/// a non-empty overlap. Opaque/custom windows still use the SDK WindowFn below.
fn merge_interval_groups(
    groups: Vec<(PrimitiveValue, BeamWindow, Vec<PaneElement>)>,
) -> Result<Vec<(PrimitiveValue, BeamWindow, Vec<PaneElement>)>> {
    let mut by_key: HashMap<String, (PrimitiveValue, Vec<(i64, i64, Vec<PaneElement>)>)> =
        HashMap::new();
    for (key, window, values) in groups {
        let BeamWindow::Interval {
            start_millis,
            end_millis,
        } = window
        else {
            return Err(anyhow!(
                "native interval merge received a non-interval window"
            ));
        };
        by_key
            .entry(format!("{key:?}"))
            .or_insert_with(|| (key, Vec::new()))
            .1
            .push((start_millis, end_millis, values));
    }

    let mut merged = Vec::new();
    for (key, mut windows) in by_key.into_values() {
        windows.sort_by_key(|(start, end, _)| (*start, *end));
        let mut iter = windows.into_iter();
        let Some((mut start, mut end, mut values)) = iter.next() else {
            continue;
        };
        for (next_start, next_end, next_values) in iter {
            if next_start < end {
                end = end.max(next_end);
                values.extend(next_values);
            } else {
                merged.push((
                    key.clone(),
                    BeamWindow::Interval {
                        start_millis: start,
                        end_millis: end,
                    },
                    std::mem::take(&mut values),
                ));
                start = next_start;
                end = next_end;
                values = next_values;
            }
        }
        merged.push((
            key,
            BeamWindow::Interval {
                start_millis: start,
                end_millis: end,
            },
            values,
        ));
    }
    Ok(merged)
}

async fn flush_output_if_full(
    writer: &mut PCollectionWriter,
    output: &mut Vec<WindowedValue>,
) -> Result<()> {
    if output.len() >= OUTPUT_BATCH_SIZE {
        writer.write(std::mem::take(output)).await?;
        output.reserve(OUTPUT_BATCH_SIZE);
    }
    Ok(())
}

fn fire_state(
    state: &mut WindowState,
    spec: &TriggerSpec,
    input_watermark: i64,
    processing_time: i64,
) -> Option<WindowedValue> {
    let end = state.window.max_timestamp_millis();
    // Beam fires the end-of-window timer only after the window's maximum
    // timestamp. For [0, 100) maxTimestamp is 99, so the earliest firing
    // watermark is 100.
    let end_of_window = end < input_watermark;
    let expired = spec.earliest_completion(end) < input_watermark;
    let trigger_ctx = TriggerContext {
        end_of_window,
        expired,
        processing_time,
    };

    let closing = expired && !state.closed;
    if !(state.trigger.should_fire(trigger_ctx) || closing) {
        state.evaluated_this_run = true;
        return None;
    }

    let is_first = !state.fired;
    let timing = pane_timing(end_of_window, is_first);
    let pane = pane_info(timing, state.pane_index, is_first, expired);

    let elements = match spec.accumulation {
        AccumulationMode::Discarding => std::mem::take(&mut state.elements),
        AccumulationMode::Accumulating | AccumulationMode::Retracting => state.elements.clone(),
    };
    let pane_timestamps = std::mem::take(&mut state.pane_timestamps);
    let timestamp_millis = output_timestamp(spec.output_time, &state.window, &pane_timestamps);

    state.trigger.on_fire(trigger_ctx);
    state.pane_index += 1;
    state.fired = true;
    state.evaluated_this_run = true;
    if expired {
        state.closed = true;
    }

    // Beam's `EmitIfNonempty` closing behavior: a firing whose pane has no
    // accumulated values carries no data and must not be emitted.
    if elements.is_empty() {
        return None;
    }

    Some(WindowedValue {
        value: BeamRecord::KV(BeamKV {
            key: state.key.clone(),
            value: Box::new(BeamRecord::ITERABLE(IterableValue::from_records(
                elements.into_iter().map(|element| element.value).collect(),
            ))),
        }),
        timestamp_millis,
        windows: vec![state.window.clone()],
        pane,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coders::primitives::{PaneInfo, PaneTiming};
    use crate::store::element_store::{FlareElementStore, ScanCollectionRequest};
    use crate::store::record::BeamKV;
    use std::sync::Arc;
    use tempfile::tempdir;

    async fn make_store() -> (tempfile::TempDir, Arc<FlareElementStore>) {
        let dir = tempdir().expect("failed to create tempdir warehouse");
        let warehouse = dir.path().to_str().expect("tempdir path utf8").to_string();
        let store = FlareElementStore::new(warehouse, "testdb".to_string(), None)
            .await
            .expect("failed to construct FlareElementStore");
        (dir, Arc::new(store))
    }

    fn interval(start: i64, end: i64) -> BeamWindow {
        BeamWindow::Interval {
            start_millis: start,
            end_millis: end,
        }
    }

    fn kv_windowed(
        key: &str,
        value: i64,
        windows: Vec<BeamWindow>,
        timestamp_millis: i64,
    ) -> WindowedValue {
        WindowedValue {
            value: BeamRecord::KV(BeamKV {
                key: PrimitiveValue::String(key.to_string()),
                value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Int64(value))),
            }),
            timestamp_millis,
            windows,
            pane: PaneInfo::no_firing(),
        }
    }

    fn kv_void_windowed(key: &str, timestamp_millis: i64) -> WindowedValue {
        WindowedValue {
            value: BeamRecord::KV(BeamKV {
                key: PrimitiveValue::String(key.to_string()),
                value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Void)),
            }),
            timestamp_millis,
            windows: vec![BeamWindow::Global],
            pane: PaneInfo::no_firing(),
        }
    }

    fn void_key_kv_windowed(value: Vec<u8>) -> WindowedValue {
        WindowedValue {
            value: BeamRecord::KV(BeamKV {
                key: PrimitiveValue::Void,
                value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(value))),
            }),
            timestamp_millis: 1,
            windows: vec![BeamWindow::Global],
            pane: PaneInfo::no_firing(),
        }
    }

    fn kv_bytes_windowed(
        key: &str,
        value: Vec<u8>,
        windows: Vec<BeamWindow>,
        timestamp_millis: i64,
    ) -> WindowedValue {
        WindowedValue {
            value: BeamRecord::KV(BeamKV {
                key: PrimitiveValue::String(key.to_string()),
                value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(value))),
            }),
            timestamp_millis,
            windows,
            pane: PaneInfo::no_firing(),
        }
    }

    async fn run_gbk(store: &Arc<FlareElementStore>, input: &str, output: &str) {
        let transform = GroupByKey::with(
            "gbk-test".to_string(),
            HashMap::from([("in".to_string(), input.to_string())]),
            HashMap::from([("out".to_string(), output.to_string())]),
            "GroupByKey".to_string(),
        );
        // A maximal watermark makes every window ready, as at bounded end.
        run_gbk_with(&transform, store, input, output, i64::MAX).await;
    }

    /// Run a `GroupByKey` instance at a specific input watermark, so a test can
    /// exercise the default-trigger readiness and the emit-once behavior across
    /// runs (the same instance keeps its `emitted_windows` set).
    async fn run_gbk_with(
        transform: &GroupByKey,
        store: &Arc<FlareElementStore>,
        input: &str,
        output: &str,
        input_watermark: i64,
    ) {
        transform
            .execute(ExecutionContext {
                store: store.clone(),
                input_pcollection_ids: vec![input.to_string()],
                output_pcollection_id: output.to_string(),
                consumer_transfrom_id: "consumer".to_string(),
                stage_id: "gbk-test".to_string(),
                windowing_strategy: None,
                processing_time: 0,
                input_watermark,
                input_is_bounded: true,
                output_coder: None,
                window_merger: None,
                source_reports: None,
            })
            .await
            .expect("GroupByKey execute failed");
    }

    /// Run with an explicit windowing strategy (trigger/accumulation/lateness), so
    /// a test can exercise non-default triggers.
    async fn run_gbk_with_strategy(
        transform: &GroupByKey,
        store: &Arc<FlareElementStore>,
        input: &str,
        output: &str,
        strategy: Option<WindowingStrategy>,
        input_watermark: i64,
    ) {
        transform
            .execute(ExecutionContext {
                store: store.clone(),
                input_pcollection_ids: vec![input.to_string()],
                output_pcollection_id: output.to_string(),
                consumer_transfrom_id: "consumer".to_string(),
                stage_id: "gbk-with-strategy".to_string(),
                windowing_strategy: strategy,
                processing_time: 0,
                input_watermark,
                input_is_bounded: true,
                output_coder: None,
                window_merger: None,
                source_reports: None,
            })
            .await
            .expect("GroupByKey execute failed");
    }

    async fn scan_output(store: &Arc<FlareElementStore>, output: &str) -> Vec<WindowedValue> {
        store
            .scan_windowed_values(ScanCollectionRequest {
                pcollection_id: output.to_string(),
            })
            .await
            .expect("scan output")
    }

    /// Collect output groups keyed by `(key, window start, window end)`.
    fn collect_groups(output: &[WindowedValue]) -> HashMap<(String, i64, i64), Vec<i64>> {
        output
            .iter()
            .map(|value| {
                let BeamRecord::KV(kv) = &value.value else {
                    panic!("expected KV output, got {:?}", value.value);
                };
                let key = match &kv.key {
                    PrimitiveValue::String(s) => s.clone(),
                    other => panic!("expected string key, got {other:?}"),
                };
                assert_eq!(
                    value.windows.len(),
                    1,
                    "each emitted group carries exactly one window"
                );
                let (start, end) = match &value.windows[0] {
                    BeamWindow::Interval {
                        start_millis,
                        end_millis,
                    } => (*start_millis, *end_millis),
                    other => panic!("expected interval window, got {other:?}"),
                };
                let BeamRecord::ITERABLE(grouped) = kv.value.as_ref() else {
                    panic!("expected iterable group values, got {:?}", kv.value);
                };
                let mut values: Vec<i64> = grouped
                    .list
                    .iter()
                    .map(|record| match record {
                        BeamRecord::PRIMITIVE(PrimitiveValue::Int64(v)) => *v,
                        other => panic!("expected int64 value, got {other:?}"),
                    })
                    .collect();
                values.sort_unstable();
                ((key, start, end), values)
            })
            .collect()
    }

    #[tokio::test]
    async fn default_group_emits_canonical_kv_with_iterable_values() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-kv-output";
        let output = "gbk-out-kv-output";
        store
            .write_windowed_value_batch(
                input,
                vec![
                    kv_windowed("a", 1, vec![BeamWindow::Global], 0),
                    kv_windowed("a", 2, vec![BeamWindow::Global], 0),
                    kv_windowed("b", 3, vec![BeamWindow::Global], 0),
                ],
            )
            .await
            .unwrap();

        run_gbk(&store, input, output).await;

        let mut groups = HashMap::new();
        for value in scan_output(&store, output).await {
            assert_eq!(value.windows, vec![BeamWindow::Global]);
            assert_eq!(value.pane.timing, PaneTiming::OnTime);
            let BeamRecord::KV(kv) = value.value else {
                panic!("GroupByKey must emit KV<K, Iterable<V>>");
            };
            let PrimitiveValue::String(key) = kv.key else {
                panic!("expected string key");
            };
            let BeamRecord::ITERABLE(values) = *kv.value else {
                panic!("GroupByKey value must be an iterable");
            };
            let values = values
                .list
                .into_iter()
                .map(|record| match record {
                    BeamRecord::PRIMITIVE(PrimitiveValue::Int64(value)) => value,
                    other => panic!("expected int64 value, got {other:?}"),
                })
                .collect::<Vec<_>>();
            let mut values = values;
            values.sort_unstable();
            groups.insert(key, values);
        }
        assert_eq!(groups.get("a"), Some(&vec![1, 2]));
        assert_eq!(groups.get("b"), Some(&vec![3]));
    }

    #[tokio::test]
    async fn group_by_key_preserves_groups_across_multiple_buckets() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-many-buckets";
        let output = "gbk-out-many-buckets";
        let values = (0..64)
            .flat_map(|index| {
                let key = format!("key-{index:02}");
                [
                    kv_windowed(&key, index * 2, vec![BeamWindow::Global], 0),
                    kv_windowed(&key, index * 2 + 1, vec![BeamWindow::Global], 0),
                ]
            })
            .collect();
        store
            .write_windowed_value_batch(input, values)
            .await
            .unwrap();

        let (streams, _) = store
            .open_incremental_arrow_by_bucket(input, None, Some(&[crate::store::KEY_COLUMN]), None)
            .await
            .unwrap();
        assert!(
            streams.len() > 1,
            "fixture must exercise multiple Paimon buckets"
        );
        drop(streams);

        run_gbk(&store, input, output).await;

        let groups = scan_output(&store, output).await;
        assert_eq!(groups.len(), 64);
        for value in groups {
            let BeamRecord::KV(kv) = value.value else {
                panic!("expected KV output");
            };
            let PrimitiveValue::String(key) = kv.key else {
                panic!("expected string key");
            };
            let index = key.strip_prefix("key-").unwrap().parse::<i64>().unwrap();
            let BeamRecord::ITERABLE(values) = *kv.value else {
                panic!("expected iterable output");
            };
            let mut values: Vec<i64> = values
                .list
                .into_iter()
                .map(|record| match record {
                    BeamRecord::PRIMITIVE(PrimitiveValue::Int64(value)) => value,
                    other => panic!("expected int64 value, got {other:?}"),
                })
                .collect();
            values.sort_unstable();
            assert_eq!(values, vec![index * 2, index * 2 + 1]);
        }
    }

    #[tokio::test]
    async fn global_window_keeps_rows_appended_after_a_max_watermark_run() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-global-rerun";
        let output = "gbk-out-global-rerun";
        let transform = GroupByKey::with(
            "gbk-global-rerun".to_string(),
            HashMap::from([("in".to_string(), input.to_string())]),
            HashMap::from([("out".to_string(), output.to_string())]),
            "GroupByKey".to_string(),
        );

        // This models the first bounded-pipeline run, which can observe an empty
        // upstream while already at the sentinel watermark.
        run_gbk_with(
            &transform,
            &store,
            input,
            output,
            crate::engine::watermark::MIN_TIMESTAMP,
        )
        .await;
        store
            .write_windowed_value_batch(
                input,
                vec![
                    kv_windowed("a", 1, vec![BeamWindow::Global], 0),
                    kv_windowed("a", 2, vec![BeamWindow::Global], 0),
                ],
            )
            .await
            .unwrap();

        run_gbk_with(
            &transform,
            &store,
            input,
            output,
            crate::engine::watermark::MAX_TIMESTAMP,
        )
        .await;

        let mut values = scan_output(&store, output)
            .await
            .into_iter()
            .map(|value| {
                let BeamRecord::KV(kv) = value.value else {
                    panic!("expected KV output");
                };
                let BeamRecord::ITERABLE(values) = *kv.value else {
                    panic!("expected iterable values");
                };
                values
                    .list
                    .into_iter()
                    .map(|value| match value {
                        BeamRecord::PRIMITIVE(PrimitiveValue::Int64(value)) => value,
                        other => panic!("expected int64 value, got {other:?}"),
                    })
                    .collect::<Vec<_>>()
            })
            .flatten()
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, vec![1, 2]);
    }

    #[tokio::test]
    async fn empty_first_run_does_not_arm_pruning_for_finite_windows() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-empty-then-finite";
        let output = "gbk-out-empty-then-finite";
        let transform = GroupByKey::with(
            "gbk-empty-then-finite".to_string(),
            HashMap::from([("in".to_string(), input.to_string())]),
            HashMap::from([("out".to_string(), output.to_string())]),
            "GroupByKey".to_string(),
        );
        let strategy = WindowingStrategy {
            window_fn: Some(FunctionSpec {
                urn: beam_urns::FIXED_WINDOWS_FN.to_string(),
                payload: Vec::new(),
            }),
            ..WindowingStrategy::default()
        };

        // PAssert and other SDK-generated graph fragments can schedule a native
        // GBK once before their upstream stage has committed any rows.
        run_gbk_with_strategy(
            &transform,
            &store,
            input,
            output,
            Some(strategy.clone()),
            crate::engine::watermark::MAX_TIMESTAMP,
        )
        .await;
        store
            .write_windowed_value_batch(
                input,
                vec![kv_windowed("a", 1, vec![interval(100, 200)], 150)],
            )
            .await
            .unwrap();

        run_gbk_with_strategy(
            &transform,
            &store,
            input,
            output,
            Some(strategy),
            crate::engine::watermark::MAX_TIMESTAMP,
        )
        .await;

        let groups = collect_groups(&scan_output(&store, output).await);
        assert_eq!(groups.get(&("a".to_string(), 100, 200)), Some(&vec![1]));
    }

    #[test]
    fn pruning_is_disabled_for_global_and_merging_windows() {
        let maximum = crate::engine::watermark::MAX_TIMESTAMP;
        assert_eq!(
            window_prune_cutoff(Some(maximum), 0, false, true, false),
            None
        );
        assert_eq!(
            window_prune_cutoff(Some(maximum), 0, true, false, false),
            None
        );
        assert_eq!(
            window_prune_cutoff(Some(maximum), 0, false, false, true),
            None
        );
        assert_eq!(
            window_prune_cutoff(Some(maximum), 1_000, false, false, false),
            Some(maximum - 1_000)
        );
    }

    #[test]
    fn gbk_sort_schema_uses_paimon_list_child_names() {
        let table_schema = ArrowSchema::new(vec![
            ArrowField::new(KEY_COLUMN, ArrowDataType::Utf8, false),
            ArrowField::new(
                VALUE_COLUMN,
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "item",
                    ArrowDataType::Binary,
                    false,
                ))),
                true,
            ),
            ArrowField::new(WINDOW_TIMESTAMP_COLUMN, ArrowDataType::Int64, false),
            ArrowField::new(WINDOW_KEY_COLUMN, ArrowDataType::Binary, true),
            ArrowField::new(
                WINDOW_EXTRA_KEYS_COLUMN,
                ArrowDataType::List(Arc::new(ArrowField::new(
                    "item",
                    ArrowDataType::Binary,
                    false,
                ))),
                true,
            ),
            ArrowField::new(WINDOW_COUNT_COLUMN, ArrowDataType::Int16, false),
        ]);

        let normalized = gbk_input_schema(&table_schema).unwrap();
        let ArrowDataType::List(child) = normalized
            .field_with_name(VALUE_COLUMN)
            .unwrap()
            .data_type()
        else {
            panic!("expected list value column");
        };
        assert_eq!(child.name(), "element");
    }

    #[tokio::test]
    async fn output_time_uses_input_event_timestamps() {
        use beam_model_rs::v1::output_time;

        let (_dir, store) = make_store().await;
        for (output_time, expected_timestamp) in [
            (output_time::Enum::EarliestInPane, 100),
            (output_time::Enum::LatestInPane, 900),
            (output_time::Enum::EndOfWindow, 999),
        ] {
            let input = format!("gbk-in-output-time-{output_time:?}");
            let output = format!("gbk-out-output-time-{output_time:?}");
            store
                .write_windowed_value_batch(
                    &input,
                    vec![
                        kv_windowed("a", 1, vec![interval(0, 1_000)], 900),
                        kv_windowed("a", 2, vec![interval(0, 1_000)], 100),
                    ],
                )
                .await
                .unwrap();
            let strategy = WindowingStrategy {
                output_time: output_time as i32,
                ..WindowingStrategy::default()
            };
            let transform = GroupByKey::with(
                format!("gbk-output-time-{output_time:?}"),
                HashMap::from([("in".to_string(), input.clone())]),
                HashMap::from([("out".to_string(), output.clone())]),
                "GroupByKey".to_string(),
            );
            run_gbk_with_strategy(
                &transform,
                &store,
                &input,
                &output,
                Some(strategy),
                i64::MAX,
            )
            .await;

            let panes = scan_output(&store, &output).await;
            assert_eq!(panes.len(), 1);
            assert_eq!(panes[0].timestamp_millis, expected_timestamp);
        }
    }

    #[tokio::test]
    async fn groups_are_separated_by_key_and_window() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-window-groups";
        let output = "gbk-out-window-groups";
        store
            .write_windowed_value_batch(
                input,
                vec![
                    kv_windowed("alice", 10, vec![interval(0, 60_000)], 1_000),
                    kv_windowed("alice", 20, vec![interval(0, 60_000)], 2_000),
                    kv_windowed("alice", 30, vec![interval(60_000, 120_000)], 61_000),
                    kv_windowed("bob", 40, vec![interval(0, 60_000)], 3_000),
                ],
            )
            .await
            .unwrap();

        run_gbk(&store, input, output).await;

        let groups = collect_groups(&scan_output(&store, output).await);
        assert_eq!(
            groups.get(&("alice".to_string(), 0, 60_000)),
            Some(&vec![10, 20])
        );
        assert_eq!(
            groups.get(&("alice".to_string(), 60_000, 120_000)),
            Some(&vec![30])
        );
        assert_eq!(groups.get(&("bob".to_string(), 0, 60_000)), Some(&vec![40]));
    }

    #[tokio::test]
    async fn an_element_in_multiple_windows_joins_each_group() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-multiple-windows";
        let output = "gbk-out-multiple-windows";
        store
            .write_windowed_value_batch(
                input,
                vec![kv_windowed(
                    "alice",
                    7,
                    vec![interval(0, 60_000), interval(30_000, 90_000)],
                    40_000,
                )],
            )
            .await
            .unwrap();

        run_gbk(&store, input, output).await;

        let groups = collect_groups(&scan_output(&store, output).await);
        assert_eq!(
            groups.get(&("alice".to_string(), 0, 60_000)),
            Some(&vec![7])
        );
        assert_eq!(
            groups.get(&("alice".to_string(), 30_000, 90_000)),
            Some(&vec![7])
        );
    }

    #[tokio::test]
    async fn void_values_remain_present_in_the_iterable() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-void-values";
        let output = "gbk-out-void-values";
        store
            .write_windowed_value_batch(
                input,
                vec![
                    kv_void_windowed("a", 1),
                    kv_void_windowed("a", 2),
                    kv_void_windowed("b", 3),
                ],
            )
            .await
            .unwrap();

        run_gbk(&store, input, output).await;

        let mut sizes = HashMap::new();
        for value in scan_output(&store, output).await {
            let BeamRecord::KV(kv) = value.value else {
                panic!("expected KV output");
            };
            let PrimitiveValue::String(key) = kv.key else {
                panic!("expected string key");
            };
            let BeamRecord::ITERABLE(values) = *kv.value else {
                panic!("expected iterable values");
            };
            assert!(
                values
                    .list
                    .iter()
                    .all(|value| matches!(value, BeamRecord::PRIMITIVE(PrimitiveValue::Void)))
            );
            sizes.insert(key, values.list.len());
        }
        assert_eq!(sizes.get("a"), Some(&2));
        assert_eq!(sizes.get("b"), Some(&1));
    }

    #[tokio::test]
    async fn one_megabyte_keys_group_without_an_intermediate_gbk_shape() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-large-key";
        let output = "gbk-out-large-key";
        let key = "x".repeat(1024 * 1024);
        let element = |value| WindowedValue {
            value: BeamRecord::KV(BeamKV {
                key: PrimitiveValue::String(key.clone()),
                value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Int64(value))),
            }),
            timestamp_millis: 0,
            windows: vec![BeamWindow::Global],
            pane: PaneInfo::no_firing(),
        };
        store
            .write_windowed_value_batch(input, vec![element(1), element(2)])
            .await
            .unwrap();

        run_gbk(&store, input, output).await;

        let panes = scan_output(&store, output).await;
        assert_eq!(panes.len(), 1);
        let BeamRecord::KV(kv) = &panes[0].value else {
            panic!("expected KV output");
        };
        assert_eq!(kv.key, PrimitiveValue::String(key));
        let BeamRecord::ITERABLE(values) = kv.value.as_ref() else {
            panic!("expected iterable values");
        };
        assert_eq!(values.list.len(), 2);
    }

    /// Regression test for Nexmark Query14 (`Reshuffle`): a second run of
    /// the same GroupByKey at a higher watermark re-evaluates an `Always` /
    /// `Discarding` group whose values were already emitted. That must be a
    /// no-op, not an empty group that changes the PCollection schema from
    /// `List<Binary>` to `List<Boolean>` and fails the Paimon write.
    #[tokio::test]
    async fn always_trigger_rerun_keeps_the_output_schema_stable() {
        use beam_model_rs::v1::{
            Trigger as TriggerProto, accumulation_mode, trigger as trigger_proto,
        };

        let (_dir, store) = make_store().await;
        let input = "gbk-in-reshuffle-rerun";
        let output = "gbk-out-reshuffle-rerun";

        let transform = GroupByKey::with(
            "gbk-reshuffle".to_string(),
            HashMap::from([("in".to_string(), input.to_string())]),
            HashMap::from([("out".to_string(), output.to_string())]),
            "GroupByKey".to_string(),
        );

        // What Beam's `Reshuffle` assigns: a 1 ms identity window, an `Always`
        // trigger, discarding accumulation, and effectively infinite lateness.
        let strategy = WindowingStrategy {
            trigger: Some(TriggerProto {
                trigger: Some(trigger_proto::Trigger::Always(trigger_proto::Always {})),
            }),
            accumulation_mode: accumulation_mode::Enum::Discarding as i32,
            allowed_lateness: crate::coders::primitives::BEAM_MAX_TIMESTAMP_MILLIS,
            ..WindowingStrategy::default()
        };

        store
            .write_windowed_value_batch(
                input,
                vec![
                    kv_bytes_windowed("a", b"one".to_vec(), vec![interval(1_000, 1_001)], 1_000),
                    kv_bytes_windowed("b", b"two".to_vec(), vec![interval(1_000, 1_001)], 1_000),
                ],
            )
            .await
            .unwrap();

        run_gbk_with_strategy(
            &transform,
            &store,
            input,
            output,
            Some(strategy.clone()),
            1_000,
        )
        .await;
        assert_eq!(scan_output(&store, output).await.len(), 2);

        // A watermark advance with no new input re-evaluates both groups. Before
        // the fix this wrote two empty groups and failed the schema check.
        run_gbk_with_strategy(
            &transform,
            &store,
            input,
            output,
            Some(strategy),
            crate::engine::watermark::MAX_TIMESTAMP,
        )
        .await;

        let panes = scan_output(&store, output).await;
        assert_eq!(panes.len(), 2, "the re-run must not emit extra panes");
        for pane in &panes {
            let BeamRecord::KV(kv) = &pane.value else {
                panic!("expected KV output, got {:?}", pane.value);
            };
            let BeamRecord::ITERABLE(group) = kv.value.as_ref() else {
                panic!("expected iterable output, got {:?}", kv.value);
            };
            assert!(!group.list.is_empty(), "no pane may be empty");
        }
    }

    /// Beam's `Reshuffle` (Nexmark Query14) groups on an `Always` trigger with
    /// `Discarding` accumulation under an identity window whose allowed lateness
    /// is effectively infinite. A later watermark advance therefore re-evaluates
    /// every group, but the values were already consumed by the previous pane.
    /// That re-evaluation must not emit an empty `KV<K, Iterable<V>>`: an empty
    /// iterable loses the element type (schema inference guesses `Boolean`),
    /// which then cannot be written to a table already typed `Array<VarBinary>`.
    #[tokio::test]
    async fn always_trigger_does_not_emit_empty_discarding_panes() {
        let backend = InMemoryGbkState::default();
        let spec = TriggerSpec {
            trigger: Trigger::Always,
            accumulation: AccumulationMode::Discarding,
            output_time: OutputTime::EndOfWindow,
            // Beam's `Reshuffle` identity-window strategy uses an effectively
            // infinite allowed lateness, so the group is never `expired` and the
            // trigger re-fires on every evaluation.
            allowed_lateness: crate::coders::primitives::BEAM_MAX_TIMESTAMP_MILLIS,
        };

        backend.begin_run().await;
        let fired = backend
            .apply_group(
                PrimitiveValue::String("a".to_string()),
                interval(0, 100),
                vec![PaneElement {
                    value: BeamRecord::PRIMITIVE(PrimitiveValue::Int64(1)),
                    timestamp_millis: 10,
                }],
                &spec,
                0,
                10,
            )
            .await
            .unwrap();
        assert!(fired.is_some(), "the first pane carries the value");

        // The next run re-evaluates the group, but there is nothing left to emit.
        backend.begin_run().await;
        let ready = backend
            .take_ready(
                &spec,
                crate::engine::watermark::MAX_TIMESTAMP,
                10,
                OUTPUT_BATCH_SIZE,
            )
            .await
            .unwrap();
        assert!(
            ready.is_empty(),
            "a discarding re-fire must not emit an empty group, got {ready:?}"
        );
    }

    #[tokio::test]
    async fn external_sort_completes_with_pool_smaller_than_input() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-spill";
        let rows = 20_000usize;
        let values = (0..rows)
            .map(|row| {
                kv_bytes_windowed(
                    "hot-key",
                    vec![(row % 251) as u8; 1024],
                    vec![BeamWindow::Global],
                    row as i64,
                )
            })
            .collect();
        store
            .write_windowed_value_batch(input, values)
            .await
            .unwrap();

        let config = GbkSortConfig {
            memory_limit_bytes: 32 * 1024 * 1024,
            spill_limit_bytes: 128 * 1024 * 1024,
            spill_reservation_bytes: 256 * 1024,
        };
        let (mut sorted, latest) = open_sorted_input_streams(&store, input, None, None, &config)
            .await
            .unwrap();
        assert_eq!(sorted.len(), 1, "one hot key belongs to one bucket");
        let sorted = &mut sorted[0].1;
        let mut observed = 0usize;
        while let Some(batch) = sorted.stream.next().await {
            let batch = batch.unwrap();
            assert_eq!(
                batch
                    .column_by_name(GROUP_KEY_COLUMN)
                    .expect("encoded sort key")
                    .data_type(),
                &ArrowDataType::Binary
            );
            observed += batch.num_rows();
        }
        assert_eq!(observed, rows);
        assert!(
            latest.is_some(),
            "the incremental input snapshot is tracked"
        );
    }
}
