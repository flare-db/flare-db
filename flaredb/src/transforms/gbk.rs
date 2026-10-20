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
use arrow_array::{Array, ArrayRef, BinaryArray, RecordBatch, builder::BinaryBuilder};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef,
};
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
use log::info;
use paimon::table::ArrowRecordBatchStream;
use tempfile::TempDir;
use tokio_stream::StreamExt;

use crate::{
    coders::primitives::{BeamWindow, WindowedValue},
    engine::{
        trigger::{
            AccumulationMode, Trigger, TriggerContext, TriggerRunner, TriggerSpec, pane_info,
            pane_timing,
        },
        window::{MergedWindows, WindowMergeService},
    },
    jobservice::urns::beam_urns,
    store::{
        KEY_COLUMN, VALUE_COLUMN,
        element_store::{FlareElementStore, PCollectionWriter, WINDOW_KEY_COLUMN},
        record::{
            BeamGbk, BeamRecord, IterableValue, PrimitiveValue, RecordTableSchema,
            primitive_value_from_array_row, record_batch_to_beamrecords,
        },
    },
    transforms::{ExecutionContext, FlareTransform},
};

const GROUP_KEY_COLUMN: &str = "__flare_gbk_group_key";
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
    values: Vec<BeamRecord>,
    trigger: TriggerRunner,
    pane_index: i64,
    /// A firing has happened for this window.
    fired: bool,
    /// The closing pane has been emitted; no further firings.
    closed: bool,
    /// This window has already been evaluated during the current GBK execution.
    evaluated_this_run: bool,
}

impl WindowState {
    fn new(key: PrimitiveValue, window: BeamWindow, trigger: TriggerRunner) -> Self {
        Self {
            key,
            window,
            values: Vec::new(),
            trigger,
            pane_index: 0,
            fired: false,
            closed: false,
            evaluated_this_run: false,
        }
    }
}

/// Storage boundary for accumulated GroupByKey state.
///
/// The current implementation is intentionally in-memory. This boundary keeps
/// sorting/group formation separate from state retention so a future Paimon +
/// SlateDB implementation can replace it without changing the bounded sort loop.
#[async_trait]
trait GbkStateBackend: Send + Sync {
    async fn begin_run(&self);

    async fn apply_group(
        &self,
        key: PrimitiveValue,
        window: BeamWindow,
        values: Vec<BeamRecord>,
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
    states: HashMap<String, WindowState>,
    ready_scan: Option<VecDeque<String>>,
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

    async fn apply_group(
        &self,
        key: PrimitiveValue,
        window: BeamWindow,
        values: Vec<BeamRecord>,
        spec: &TriggerSpec,
        input_watermark: i64,
        processing_time: i64,
    ) -> Result<Option<WindowedValue>> {
        let end = window.max_timestamp_millis();
        let expired = spec.earliest_completion(end) <= input_watermark;
        let mut inner = self.inner.lock().expect("gbk state lock not poisoned");
        if inner.states.is_empty()
            && expired
            && spec.trigger == Trigger::Default
            && spec.accumulation == AccumulationMode::Discarding
        {
            return Ok(Some(WindowedValue {
                value: BeamRecord::GBK(BeamGbk {
                    key,
                    value: IterableValue::from_records(values),
                }),
                timestamp_millis: end,
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
        for _ in 0..values.len() {
            state.trigger.on_element(processing_time);
        }
        state.values.extend(values);

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

/// Identity of a `(key, window)` group, used to key the state map.
fn group_identity(key: &PrimitiveValue, window: &BeamWindow) -> String {
    format!("{key:?}\u{1}{}", window.canonical_key())
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
        self.state.begin_run().await;

        // Read only the input appended since this stage's last run. The cursor is
        // this stage's position in the input PCollection's changelog.
        let cursor = ctx
            .store
            .cursor(&ctx.stage_id, &input_pcollection_id)
            .await?;
        let config = GbkSortConfig::from_env();
        let (sorted_input, latest) =
            open_sorted_input_stream(&ctx.store, &input_pcollection_id, cursor, &config).await?;
        let mut writer =
            PCollectionWriter::new(ctx.store.clone(), ctx.output_pcollection_id.clone());
        let mut output = Vec::with_capacity(OUTPUT_BATCH_SIZE);
        let mut fired = 0usize;

        if let Some(mut sorted_input) = sorted_input {
            let mut decoder = SortedGroupDecoder::default();
            let sort_started = Instant::now();
            let mut decoded_rows = 0usize;
            let mut next_progress = 1_000_000usize;
            let mut decode_time = std::time::Duration::ZERO;
            let mut group_time = std::time::Duration::ZERO;
            let mut write_time = std::time::Duration::ZERO;
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

        if fired == 0 {
            info!(
                "GroupByKey: no window fired at input watermark {}",
                ctx.input_watermark
            );
        } else {
            info!(
                "Executed GroupByKey: {} pane(s) fired at input watermark {}",
                fired, ctx.input_watermark
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
        let stream = stream.map(move |batch| {
            let batch = batch.map_err(|error| DataFusionError::External(Box::new(error)))?;
            let previous = rows_seen.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
            let current = previous + batch.num_rows() as u64;
            if previous / 1_000_000 != current / 1_000_000 {
                info!(
                    "GroupByKey: sorted {current} input rows in {:?}",
                    started.elapsed()
                );
            }
            add_encoded_key_column(batch)
                .map_err(|error| DataFusionError::Execution(error.to_string()))
        });
        Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&self.schema),
            stream,
        ))
    }
}

/// Open a lazy Paimon incremental read and feed it directly into DataFusion.
///
/// The sort key is a deterministic binary encoding of the Beam key followed by
/// the canonical window key. `value` is carried through the sort but never
/// participates in comparison, because Beam does not define an order for a
/// GroupByKey iterable.
async fn open_sorted_input_stream(
    store: &Arc<FlareElementStore>,
    pcollection_id: &str,
    cursor: Option<i64>,
    config: &GbkSortConfig,
) -> Result<(Option<SortedInputStream>, Option<i64>)> {
    let (stream, latest) = store
        .open_incremental_arrow_projected(
            pcollection_id,
            cursor,
            Some(&[KEY_COLUMN, VALUE_COLUMN, WINDOW_KEY_COLUMN]),
        )
        .await?;
    let Some(stream) = stream else {
        return Ok((None, latest));
    };

    let table_schema = store.registry.get(pcollection_id).ok_or_else(|| {
        anyhow!("GroupByKey input PCollection {pcollection_id} has no registered schema")
    })?;
    let schema = schema_with_group_key(&gbk_input_schema(&table_schema.arrow_schema)?);
    let spill_parent = Path::new(store.catalog.warehouse()).join(".gbk-spill");
    std::fs::create_dir_all(&spill_parent)?;
    let temp_dir = tempfile::Builder::new()
        .prefix("group-by-key-")
        .tempdir_in(&spill_parent)?;
    info!(
        "GroupByKey: starting external sort for {pcollection_id} (memory={} bytes, spill_limit={} bytes, spill_reservation={} bytes)",
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
    let df = df.unnest_columns(&[WINDOW_KEY_COLUMN])?;
    let df = df.select_columns(&[
        KEY_COLUMN,
        VALUE_COLUMN,
        WINDOW_KEY_COLUMN,
        GROUP_KEY_COLUMN,
    ])?;
    let df = df.sort(vec![
        col(GROUP_KEY_COLUMN).sort(true, false),
        col(WINDOW_KEY_COLUMN).sort(true, false),
    ])?;
    let stream = df.execute_stream().await?;

    Ok((
        Some(SortedInputStream {
            stream,
            table_schema,
            _temp_dir: temp_dir,
        }),
        latest,
    ))
}

fn gbk_input_schema(table_schema: &ArrowSchema) -> Result<Arc<ArrowSchema>> {
    let fields = [KEY_COLUMN, VALUE_COLUMN, WINDOW_KEY_COLUMN]
        .into_iter()
        .map(|name| {
            table_schema
                .field_with_name(name)
                .map(|field| Arc::new(field.clone()))
                .map_err(|_| anyhow!("GroupByKey input is missing the '{name}' column"))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(ArrowSchema::new_with_metadata(
        fields,
        table_schema.metadata().clone(),
    )))
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
        let encoded_key_column = batch.column_by_name(GROUP_KEY_COLUMN).ok_or_else(|| {
            anyhow!("GroupByKey result is missing the '{GROUP_KEY_COLUMN}' column")
        })?;
        let encoded_keys = encoded_key_column
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("GroupByKey encoded key must be a Binary column"))?;
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
            let window_key = match primitive_value_from_array_row(
                window_column.as_ref(),
                window_column.data_type(),
                row,
            )? {
                PrimitiveValue::String(value) => value,
                other => {
                    return Err(anyhow!(
                        "GroupByKey window key must be a string, got {other:?}"
                    ));
                }
            };
            let window = BeamWindow::from_canonical_key(&window_key).ok_or_else(|| {
                anyhow!("GroupByKey encountered a malformed window key '{window_key}'")
            })?;
            let value = *kv.value;

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
                    if current_window.canonical_key() == window.canonical_key() =>
                {
                    values.push(value);
                }
                _ => current.windows.push((window, vec![value])),
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

type WindowValues = Vec<(BeamWindow, Vec<BeamRecord>)>;

fn apply_merge_results(
    key: &PrimitiveValue,
    original_groups: Vec<(BeamWindow, Vec<BeamRecord>)>,
    merge_results: Vec<MergedWindows>,
) -> Result<Vec<(PrimitiveValue, BeamWindow, Vec<BeamRecord>)>, Error> {
    let original_to_merged: HashMap<String, BeamWindow> = merge_results
        .into_iter()
        .flat_map(|result| {
            let merged = result.merged;
            result
                .originals
                .into_iter()
                .map(move |original| (original.canonical_key(), merged.clone()))
        })
        .collect();

    let mut merged_groups: HashMap<String, (BeamWindow, Vec<BeamRecord>)> = HashMap::new();
    for (original, values) in original_groups {
        let original_key = original.canonical_key();
        let merged_window = original_to_merged.get(&original_key).ok_or_else(|| {
            anyhow!("MergeWindows did not map original window {original_key} for key {key:?}")
        })?;
        let entry = merged_groups
            .entry(merged_window.canonical_key())
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
    groups: Vec<(PrimitiveValue, BeamWindow, Vec<BeamRecord>)>,
) -> Result<Vec<(PrimitiveValue, BeamWindow, Vec<BeamRecord>)>> {
    let mut by_key: HashMap<String, (PrimitiveValue, Vec<(i64, i64, Vec<BeamRecord>)>)> =
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
    let end_of_window = end <= input_watermark;
    let expired = spec.earliest_completion(end) <= input_watermark;
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

    let values = match spec.accumulation {
        AccumulationMode::Discarding => std::mem::take(&mut state.values),
        AccumulationMode::Accumulating | AccumulationMode::Retracting => state.values.clone(),
    };

    let output = WindowedValue {
        value: BeamRecord::GBK(BeamGbk {
            key: state.key.clone(),
            value: IterableValue::from_records(values),
        }),
        timestamp_millis: end,
        windows: vec![state.window.clone()],
        pane,
    };

    state.trigger.on_fire(trigger_ctx);
    state.pane_index += 1;
    state.fired = true;
    state.evaluated_this_run = true;
    if expired {
        state.closed = true;
    }
    Some(output)
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
                let BeamRecord::GBK(gbk) = &value.value else {
                    panic!("expected GBK output, got {:?}", value.value);
                };
                let key = match &gbk.key {
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
                let mut values: Vec<i64> = gbk
                    .value
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

    fn custom_window(id: u8) -> BeamWindow {
        BeamWindow::Opaque {
            coder_id: "test-window-coder".to_string(),
            bytes: vec![id],
        }
    }

    #[test]
    fn merging_windows_combines_consumed_original_groups() {
        let first = custom_window(1);
        let second = custom_window(2);
        let third = custom_window(3);
        let merged = BeamWindow::Opaque {
            coder_id: "test-window-coder".to_string(),
            bytes: vec![9],
        };
        let key = PrimitiveValue::String("alice".to_string());
        let groups = apply_merge_results(
            &key,
            vec![
                (
                    first.clone(),
                    vec![BeamRecord::PRIMITIVE(PrimitiveValue::Int64(10))],
                ),
                (
                    second.clone(),
                    vec![BeamRecord::PRIMITIVE(PrimitiveValue::Int64(20))],
                ),
                (
                    third.clone(),
                    vec![BeamRecord::PRIMITIVE(PrimitiveValue::Int64(30))],
                ),
            ],
            vec![
                MergedWindows {
                    merged: merged.clone(),
                    originals: vec![first, second],
                },
                MergedWindows {
                    merged: third.clone(),
                    originals: vec![third.clone()],
                },
            ],
        )
        .unwrap();

        let by_window: HashMap<String, Vec<i64>> = groups
            .into_iter()
            .map(|(_, window, values)| {
                let mut values: Vec<i64> = values
                    .into_iter()
                    .map(|record| match record {
                        BeamRecord::PRIMITIVE(PrimitiveValue::Int64(value)) => value,
                        other => panic!("expected int64 value, got {other:?}"),
                    })
                    .collect();
                values.sort_unstable();
                (window.canonical_key(), values)
            })
            .collect();

        assert_eq!(by_window.get(&merged.canonical_key()), Some(&vec![10, 20]));
        assert_eq!(by_window.get(&third.canonical_key()), Some(&vec![30]));
    }

    #[test]
    fn native_interval_merge_combines_overlaps_without_touching_boundaries() {
        let key = PrimitiveValue::String("alice".to_string());
        let record = |value| BeamRecord::PRIMITIVE(PrimitiveValue::Int64(value));
        let merged = merge_interval_groups(vec![
            (key.clone(), interval(0, 10), vec![record(1)]),
            (key.clone(), interval(5, 20), vec![record(2)]),
            (key, interval(20, 30), vec![record(3)]),
        ])
        .unwrap();

        let by_window: HashMap<String, Vec<i64>> = merged
            .into_iter()
            .map(|(_, window, values)| {
                let values = values
                    .into_iter()
                    .map(|record| match record {
                        BeamRecord::PRIMITIVE(PrimitiveValue::Int64(value)) => value,
                        other => panic!("expected int64 value, got {other:?}"),
                    })
                    .collect();
                (window.canonical_key(), values)
            })
            .collect();

        assert_eq!(
            by_window.get("interval:0:20"),
            Some(&vec![1, 2]),
            "overlapping sessions should merge"
        );
        assert_eq!(
            by_window.get("interval:20:30"),
            Some(&vec![3]),
            "half-open sessions that only touch should remain separate"
        );
    }

    #[tokio::test]
    async fn groups_same_key_separately_per_window_and_keeps_other_keys_apart() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in";
        let output = "gbk-out";

        // "alice" appears in two adjacent windows; "bob" shares the first window.
        let elements = vec![
            kv_windowed("alice", 10, vec![interval(0, 60_000)], 1_000),
            kv_windowed("alice", 20, vec![interval(0, 60_000)], 2_000),
            kv_windowed("alice", 30, vec![interval(60_000, 120_000)], 61_000),
            kv_windowed("bob", 40, vec![interval(0, 60_000)], 3_000),
        ];
        store
            .write_windowed_value_batch(input, elements)
            .await
            .unwrap();

        run_gbk(&store, input, output).await;

        let scanned = scan_output(&store, output).await;
        let groups = collect_groups(&scanned);

        assert_eq!(
            groups.len(),
            3,
            "expected three (key, window) groups, got {groups:?}"
        );
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
    async fn output_metadata_is_derived_from_the_group_window() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-meta";
        let output = "gbk-out-meta";

        let elements = vec![
            kv_windowed("alice", 1, vec![interval(0, 60_000)], 1_000),
            kv_windowed("alice", 2, vec![interval(60_000, 120_000)], 61_000),
        ];
        store
            .write_windowed_value_batch(input, elements)
            .await
            .unwrap();

        run_gbk(&store, input, output).await;

        let scanned = scan_output(&store, output).await;
        assert_eq!(scanned.len(), 2);
        for value in &scanned {
            let BeamWindow::Interval { end_millis, .. } = value.windows[0] else {
                panic!("expected interval window, got {:?}", value.windows[0]);
            };
            // Beam derives the GroupByKey output timestamp from the window.
            assert_eq!(value.timestamp_millis, end_millis - 1);
            assert_eq!(value.pane, PaneInfo::on_time_firing());
            assert_eq!(value.pane.timing, PaneTiming::OnTime);
        }
    }

    #[tokio::test]
    async fn element_in_multiple_windows_contributes_to_each_window_group() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-sliding";
        let output = "gbk-out-sliding";

        // A sliding-window element belongs to two windows and must appear in both
        // grouped outputs, matching Beam's GroupAlsoByWindow semantics.
        let elements = vec![kv_windowed(
            "alice",
            7,
            vec![interval(0, 60_000), interval(30_000, 90_000)],
            40_000,
        )];
        store
            .write_windowed_value_batch(input, elements)
            .await
            .unwrap();

        run_gbk(&store, input, output).await;

        let scanned = scan_output(&store, output).await;
        let groups = collect_groups(&scanned);
        assert_eq!(
            groups.len(),
            2,
            "expected two window groups, got {groups:?}"
        );
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
    async fn global_window_groups_by_key_only() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-global";
        let output = "gbk-out-global";

        let elements = vec![
            kv_windowed("alice", 1, vec![BeamWindow::Global], 0),
            kv_windowed("alice", 2, vec![BeamWindow::Global], 0),
            kv_windowed("bob", 3, vec![BeamWindow::Global], 0),
        ];
        store
            .write_windowed_value_batch(input, elements)
            .await
            .unwrap();

        run_gbk(&store, input, output).await;

        let scanned = scan_output(&store, output).await;

        // Two key groups, each assigned to the global window with its max timestamp.
        let mut by_key: HashMap<String, Vec<i64>> = HashMap::new();
        for value in &scanned {
            assert_eq!(value.windows, vec![BeamWindow::Global]);
            assert_eq!(
                value.timestamp_millis,
                BeamWindow::Global.max_timestamp_millis()
            );
            let BeamRecord::GBK(gbk) = &value.value else {
                panic!("expected GBK output, got {:?}", value.value);
            };
            let PrimitiveValue::String(key) = &gbk.key else {
                panic!("expected string key");
            };
            let mut values: Vec<i64> = gbk
                .value
                .list
                .iter()
                .map(|record| match record {
                    BeamRecord::PRIMITIVE(PrimitiveValue::Int64(v)) => *v,
                    other => panic!("expected int64 value, got {other:?}"),
                })
                .collect();
            values.sort_unstable();
            by_key.insert(key.clone(), values);
        }

        assert_eq!(by_key.len(), 2, "expected two key groups, got {by_key:?}");
        assert_eq!(by_key.get("alice"), Some(&vec![1, 2]));
        assert_eq!(by_key.get("bob"), Some(&vec![3]));
    }

    #[tokio::test]
    async fn empty_input_produces_empty_output() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-empty";
        let output = "gbk-out-empty";

        // No write happens for the input, so it has zero committed elements.
        run_gbk(&store, input, output).await;

        let scanned = scan_output(&store, output).await;
        assert!(scanned.is_empty(), "expected empty output, got {scanned:?}");
    }

    /// Beam's default trigger: a window emits only once the input watermark has
    /// reached its end, and only once ever as the watermark advances.
    #[tokio::test]
    async fn emits_only_windows_the_input_watermark_has_reached() {
        let (_dir, store) = make_store().await;
        let input = "gbk-in-readiness";
        let output = "gbk-out-readiness";

        // Two interval windows: [0, 100) has max timestamp 99, [100, 200) has 199.
        let elements = vec![
            kv_windowed("a", 1, vec![interval(0, 100)], 10),
            kv_windowed("a", 2, vec![interval(100, 200)], 150),
        ];
        store
            .write_windowed_value_batch(input, elements)
            .await
            .unwrap();

        let transform = GroupByKey::with(
            "gbk-readiness".to_string(),
            HashMap::from([("in".to_string(), input.to_string())]),
            HashMap::from([("out".to_string(), output.to_string())]),
            "GroupByKey".to_string(),
        );

        // WM = 98: below both window ends, nothing is ready.
        run_gbk_with(&transform, &store, input, output, 98).await;
        assert!(
            scan_output(&store, output).await.is_empty(),
            "no window should emit before its end"
        );

        // WM = 99: the first window's end is reached (max 99 <= 99).
        run_gbk_with(&transform, &store, input, output, 99).await;
        let groups = collect_groups(&scan_output(&store, output).await);
        assert_eq!(
            groups.len(),
            1,
            "only the first window is ready, got {groups:?}"
        );
        assert_eq!(groups.get(&("a".to_string(), 0, 100)), Some(&vec![1]));

        // WM = 199: the second window is now ready; the first is not re-emitted.
        run_gbk_with(&transform, &store, input, output, 199).await;
        let groups = collect_groups(&scan_output(&store, output).await);
        assert_eq!(groups.len(), 2, "both windows ready, got {groups:?}");
        assert_eq!(groups.get(&("a".to_string(), 100, 200)), Some(&vec![2]));

        // Re-running at the same watermark emits nothing new (emit-once).
        run_gbk_with(&transform, &store, input, output, 199).await;
        assert_eq!(
            collect_groups(&scan_output(&store, output).await).len(),
            2,
            "a re-run at the same watermark must not re-emit"
        );
    }

    /// A non-default trigger emits multiple panes for one window: an on-time pane
    /// when the watermark passes the window end, then a late pane when a late
    /// element arrives and the late trigger is satisfied.
    #[tokio::test]
    async fn a_late_element_fires_a_second_late_pane() {
        use beam_model_rs::v1::{
            Trigger as TriggerProto, accumulation_mode, trigger as trigger_proto,
        };

        let (_dir, store) = make_store().await;
        let input = "gbk-in-late";
        let output = "gbk-out-late";

        let transform = GroupByKey::with(
            "gbk-late".to_string(),
            HashMap::from([("in".to_string(), input.to_string())]),
            HashMap::from([("out".to_string(), output.to_string())]),
            "GroupByKey".to_string(),
        );

        // AfterEndOfWindow { late: AfterPane.elementCount(1) }, with lateness so the
        // window stays open for the late element.
        let late = TriggerProto {
            trigger: Some(trigger_proto::Trigger::ElementCount(
                trigger_proto::ElementCount { element_count: 1 },
            )),
        };
        let strategy = WindowingStrategy {
            trigger: Some(TriggerProto {
                trigger: Some(trigger_proto::Trigger::AfterEndOfWindow(Box::new(
                    trigger_proto::AfterEndOfWindow {
                        early_firings: None,
                        late_firings: Some(Box::new(late)),
                    },
                ))),
            }),
            accumulation_mode: accumulation_mode::Enum::Discarding as i32,
            allowed_lateness: 1_000,
            ..WindowingStrategy::default()
        };

        store
            .write_windowed_value_batch(
                input,
                vec![kv_windowed("a", 1, vec![interval(0, 100)], 10)],
            )
            .await
            .unwrap();

        // Watermark reaches the window end: one on-time pane with the first value.
        run_gbk_with_strategy(
            &transform,
            &store,
            input,
            output,
            Some(strategy.clone()),
            99,
        )
        .await;
        let panes = scan_output(&store, output).await;
        assert_eq!(panes.len(), 1, "expected one on-time pane");
        assert_eq!(panes[0].pane.timing, PaneTiming::OnTime);
        assert_eq!(
            collect_groups(&panes).get(&("a".to_string(), 0, 100)),
            Some(&vec![1])
        );

        // A late element arrives for the same window; the late trigger fires again.
        store
            .write_windowed_value_batch(
                input,
                vec![kv_windowed("a", 2, vec![interval(0, 100)], 20)],
            )
            .await
            .unwrap();
        run_gbk_with_strategy(&transform, &store, input, output, Some(strategy), 99).await;

        let panes = scan_output(&store, output).await;
        assert_eq!(
            panes.len(),
            2,
            "a late element must fire a second (late) pane"
        );
        assert_eq!(panes[1].pane.timing, PaneTiming::Late);
    }

    #[test]
    fn encoded_sort_keys_match_primitive_key_equality() {
        assert_eq!(
            encoded_primitive_sort_key(&PrimitiveValue::String("key".to_string())),
            encoded_primitive_sort_key(&PrimitiveValue::String("key".to_string()))
        );
        assert_ne!(
            encoded_primitive_sort_key(&PrimitiveValue::String("key".to_string())),
            encoded_primitive_sort_key(&PrimitiveValue::Bytes(b"key".to_vec()))
        );
        assert_ne!(
            encoded_primitive_sort_key(&PrimitiveValue::Float64(0.0)),
            encoded_primitive_sort_key(&PrimitiveValue::Float64(-0.0))
        );
        let same_nan = f64::from_bits(0x7ff8_0000_0000_0042);
        let other_nan = f64::from_bits(0x7ff8_0000_0000_0043);
        assert_eq!(
            encoded_primitive_sort_key(&PrimitiveValue::Float64(same_nan)),
            encoded_primitive_sort_key(&PrimitiveValue::Float64(same_nan))
        );
        assert_ne!(
            encoded_primitive_sort_key(&PrimitiveValue::Float64(same_nan)),
            encoded_primitive_sort_key(&PrimitiveValue::Float64(other_nan))
        );
    }

    #[tokio::test]
    async fn state_backend_evaluates_each_group_at_most_once_per_run() {
        let backend = InMemoryGbkState::default();
        let spec = TriggerSpec {
            trigger: Trigger::Always,
            accumulation: AccumulationMode::Discarding,
            allowed_lateness: 0,
        };
        backend.begin_run().await;

        let fired = backend
            .apply_group(
                PrimitiveValue::String("a".to_string()),
                interval(0, 100),
                vec![BeamRecord::PRIMITIVE(PrimitiveValue::Int64(1))],
                &spec,
                0,
                10,
            )
            .await
            .unwrap();
        assert!(fired.is_some(), "always trigger should fire immediately");
        assert!(
            backend
                .take_ready(&spec, 0, 10, OUTPUT_BATCH_SIZE)
                .await
                .unwrap()
                .is_empty(),
            "a group evaluated during ingest must not fire twice in one run"
        );

        backend.begin_run().await;
        let ready = backend
            .take_ready(&spec, 0, 10, OUTPUT_BATCH_SIZE)
            .await
            .unwrap();
        assert_eq!(ready.len(), 1, "the next run must evaluate the group again");
        assert!(
            backend
                .take_ready(&spec, 0, 10, OUTPUT_BATCH_SIZE)
                .await
                .unwrap()
                .is_empty(),
            "paging ready groups must not re-fire them in the same run"
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
        let (mut sorted, latest) = open_sorted_input_stream(&store, input, None, &config)
            .await
            .unwrap();
        let sorted = sorted.as_mut().expect("spill input should open");
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
