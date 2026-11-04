use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::Cursor,
    path::Path,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, Ordering},
    },
    time::Instant,
};

use anyhow::{Error, Result, anyhow};
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int16Array, Int64Array, ListArray, RecordBatch, UInt32Array,
    builder::BinaryBuilder,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, SchemaRef,
};
use arrow_select::{concat::concat, take::take};
use async_trait::async_trait;
use beam_model_rs::v1::{
    Coder, Components, Environment, FunctionSpec, PCollection, PTransform, WindowingStrategy,
    merge_status,
};
use datafusion::{
    catalog::streaming::StreamingTable,
    datasource::TableProvider,
    error::DataFusionError,
    execution::TaskContext,
    logical_expr::Expr,
    physical_plan::{
        ExecutionPlan, ExecutionPlanProperties, SendableRecordBatchStream, common::spawn_buffered,
        sorts::sort::SortExec, stream::RecordBatchStreamAdapter, streaming::PartitionStream,
    },
    prelude::{col, lit},
};
use futures::StreamExt;
use log::{info, warn};

use tempfile::TempDir;

use crate::{
    coders::primitives::{BeamWindow, PaneInfo},
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
            WINDOW_KEY_COLUMN, WINDOW_MAX_END_COLUMN, WINDOW_TIMESTAMP_COLUMN,
            arrow_schema_to_paimon,
        },
        provider::{FlareTableProvider, SnapshotRange, gbk_session},
        record::{
            PrimitiveValue, RecordTableSchema, TableType, WindowMetadata,
            primitive_value_from_array_row,
        },
    },
    transforms::{ExecutionContext, FlareTransform},
};

const GROUP_KEY_COLUMN: &str = "flare_gbk_group_key";
const GBK_STATE_TABLE: &str = "_flare_gbk_state_values";
const GBK_STATE_GROUP_COLUMN: &str = "group_identity";
const GBK_STATE_WINDOW_COLUMN: &str = "window_identity";
const GBK_STATE_GENERATION_COLUMN: &str = "generation";
const GBK_STATE_CHUNK_COLUMN: &str = "chunk_id";
const GBK_STATE_PAYLOAD_COLUMN: &str = "arrow_ipc_payload";
const DEFAULT_SORT_MEMORY_LIMIT_BYTES: usize = 1024 * 1024 * 1024;
const DEFAULT_SORT_SPILL_LIMIT_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_SORT_SPILL_RESERVATION_BYTES: usize = 10 * 1024 * 1024;
const DEFAULT_GBK_STATE_MEMORY_LIMIT_BYTES: usize = 64 * 1024 * 1024;
const GBK_STATE_MEMORY_LIMIT_ENV: &str = "FLAREDB_GBK_STATE_MEMORY_LIMIT_BYTES";
static NEXT_GBK_SPILL_CHUNK_ID: AtomicI64 = AtomicI64::new(1);
// Match the normal Arrow batch scale instead of paying Paimon/Arrow setup costs
// for every 1,024 generated groups.
const OUTPUT_BATCH_SIZE: usize = 8 * 1024;

/// DataFusion resources used to externally sort one bundle's input.
///
/// These process-level knobs bound the external sort used by runner-native
/// GroupByKey. Accumulated GBK values have a separate Paimon spill threshold.
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
    window: BeamWindow,
    elements: Vec<ArrowValueChunk>,
    spilled: Vec<SpilledChunkRef>,
    generation: i64,
    pane_timestamp_min: Option<i64>,
    pane_timestamp_max: Option<i64>,
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

fn arrow_chunks_memory_bytes(chunks: &[ArrowValueChunk]) -> usize {
    chunks.iter().fold(0usize, |total, chunk| {
        total.saturating_add(chunk.batch.get_array_memory_size())
    })
}

impl WindowState {
    fn new(window: BeamWindow, trigger: TriggerRunner) -> Self {
        Self {
            window,
            elements: Vec::new(),
            spilled: Vec::new(),
            generation: 0,
            pane_timestamp_min: None,
            pane_timestamp_max: None,
            trigger,
            pane_index: 0,
            fired: false,
            closed: false,
            evaluated_this_run: false,
        }
    }
}

/// A contiguous group slice from a sorted Arrow batch. Cloning a chunk clones
/// Arrow references, not each element's Beam value.
#[derive(Debug, Clone)]
struct ArrowValueChunk {
    batch: RecordBatch,
    timestamp_min: i64,
    timestamp_max: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SpilledChunkRef {
    group: GroupIdentity,
    generation: i64,
    chunk_id: i64,
}

struct SpillRow {
    reference: SpilledChunkRef,
    payload: Vec<u8>,
    memory_bytes: usize,
}

#[derive(Debug)]
struct ArrowPane {
    chunks: Vec<ArrowValueChunk>,
    spilled: Vec<SpilledChunkRef>,
    timestamp_millis: i64,
    window: BeamWindow,
    pane: PaneInfo,
}

fn gbk_state_arrow_schema() -> Arc<ArrowSchema> {
    Arc::new(ArrowSchema::new(vec![
        ArrowField::new(GBK_STATE_GROUP_COLUMN, ArrowDataType::Binary, false),
        ArrowField::new(GBK_STATE_WINDOW_COLUMN, ArrowDataType::Binary, false),
        ArrowField::new(GBK_STATE_GENERATION_COLUMN, ArrowDataType::Int64, false),
        ArrowField::new(GBK_STATE_CHUNK_COLUMN, ArrowDataType::Int64, false),
        ArrowField::new(GBK_STATE_PAYLOAD_COLUMN, ArrowDataType::Binary, false),
    ]))
}

fn encode_arrow_chunk(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut payload = Vec::new();
    {
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut payload, &batch.schema())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(payload)
}

fn decode_arrow_chunk(payload: &[u8]) -> Result<RecordBatch> {
    let mut reader = arrow_ipc::reader::StreamReader::try_new(Cursor::new(payload), None)?;
    reader
        .next()
        .transpose()?
        .ok_or_else(|| anyhow!("GBK spill payload contained no Arrow batch"))
}

fn spill_rows_to_batch(rows: &[SpillRow]) -> Result<RecordBatch> {
    let schema = gbk_state_arrow_schema();
    let mut groups = BinaryBuilder::new();
    let mut windows = BinaryBuilder::new();
    let mut payloads = BinaryBuilder::new();
    let mut generations = Vec::with_capacity(rows.len());
    let mut chunk_ids = Vec::with_capacity(rows.len());
    for row in rows {
        groups.append_value(&row.reference.group.key);
        windows.append_value(&row.reference.group.window);
        generations.push(row.reference.generation);
        chunk_ids.push(row.reference.chunk_id);
        payloads.append_value(&row.payload);
    }
    Ok(RecordBatch::try_new(
        schema,
        vec![
            Arc::new(groups.finish()),
            Arc::new(windows.finish()),
            Arc::new(Int64Array::from(generations)),
            Arc::new(Int64Array::from(chunk_ids)),
            Arc::new(payloads.finish()),
        ],
    )?)
}

/// Append spilled chunks to a Paimon table. There are no primary keys, so equal
/// payloads remain distinct rows; references identify the exact appended chunks.
async fn append_gbk_spill_rows(store: &FlareElementStore, rows: &[SpillRow]) -> Result<()> {
    if rows.is_empty() {
        return Ok(());
    }
    let schema = arrow_schema_to_paimon(&gbk_state_arrow_schema())?;
    let table = store.get_or_create_table(GBK_STATE_TABLE, schema).await?;
    let batch = spill_rows_to_batch(rows)?;
    store.write_table_batches(&table, &[batch]).await
}

async fn read_gbk_spill_chunks(
    store: &FlareElementStore,
    references: &[SpilledChunkRef],
) -> Result<Vec<ArrowValueChunk>> {
    if references.is_empty() {
        return Ok(Vec::new());
    }
    let wanted: HashSet<_> = references.iter().cloned().collect();
    let mut chunks = HashMap::with_capacity(references.len());
    let Some(mut stream) = store.scan_table_batches(GBK_STATE_TABLE).await? else {
        return Err(anyhow!(
            "GBK spill table is missing for active chunk references"
        ));
    };
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let groups = batch
            .column_by_name(GBK_STATE_GROUP_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .ok_or_else(|| anyhow!("GBK state table has invalid group identity column"))?;
        let windows = batch
            .column_by_name(GBK_STATE_WINDOW_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .ok_or_else(|| anyhow!("GBK state table has invalid window identity column"))?;
        let generations = batch
            .column_by_name(GBK_STATE_GENERATION_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| anyhow!("GBK state table has invalid generation column"))?;
        let chunk_ids = batch
            .column_by_name(GBK_STATE_CHUNK_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
            .ok_or_else(|| anyhow!("GBK state table has invalid chunk id column"))?;
        let payloads = batch
            .column_by_name(GBK_STATE_PAYLOAD_COLUMN)
            .and_then(|column| column.as_any().downcast_ref::<BinaryArray>())
            .ok_or_else(|| anyhow!("GBK state table has invalid payload column"))?;
        for row in 0..batch.num_rows() {
            let reference = SpilledChunkRef {
                group: GroupIdentity {
                    key: groups.value(row).to_vec(),
                    window: windows.value(row).to_vec(),
                },
                generation: generations.value(row),
                chunk_id: chunk_ids.value(row),
            };
            if wanted.contains(&reference) {
                let batch = decode_arrow_chunk(payloads.value(row))?;
                let timestamps = batch
                    .column_by_name(WINDOW_TIMESTAMP_COLUMN)
                    .and_then(|column| column.as_any().downcast_ref::<Int64Array>())
                    .ok_or_else(|| {
                        anyhow!("GBK spill payload has invalid event timestamp column")
                    })?;
                let (mut timestamp_min, mut timestamp_max) = (i64::MAX, i64::MIN);
                for index in 0..timestamps.len() {
                    if !timestamps.is_null(index) {
                        timestamp_min = timestamp_min.min(timestamps.value(index));
                        timestamp_max = timestamp_max.max(timestamps.value(index));
                    }
                }
                if timestamp_min == i64::MAX || timestamp_max == i64::MIN {
                    return Err(anyhow!("GBK spill payload has no event timestamps"));
                }
                chunks.insert(
                    reference,
                    ArrowValueChunk {
                        batch,
                        timestamp_min,
                        timestamp_max,
                    },
                );
            }
        }
        if chunks.len() == references.len() {
            break;
        }
    }
    if chunks.len() != references.len() {
        return Err(anyhow!(
            "GBK spill lookup found {} of {} referenced chunks",
            chunks.len(),
            references.len()
        ));
    }
    references
        .iter()
        .map(|reference| {
            chunks
                .remove(reference)
                .ok_or_else(|| anyhow!("GBK spill chunk reference disappeared during lookup"))
        })
        .collect()
}

/// Storage boundary for accumulated GroupByKey state. Trigger and pane metadata
/// remain in memory; Arrow value chunks spill to Paimon above the configured
/// threshold, without changing sorting, grouping, or trigger evaluation.
#[async_trait]
trait GbkStateBackend: Send + Sync {
    async fn begin_run(&self);

    async fn last_evaluated_watermark(&self) -> Option<i64>;

    async fn mark_watermark_evaluated(&self, watermark: i64);

    async fn pending_range(&self) -> Option<SnapshotRange>;

    async fn pin_range(&self, range: SnapshotRange);

    async fn clear_range(&self);

    async fn spill_over_limit(
        &self,
        store: &FlareElementStore,
        memory_limit_bytes: usize,
    ) -> Result<()>;

    async fn apply_group(
        &self,
        key: PrimitiveValue,
        window: BeamWindow,
        elements: Vec<ArrowValueChunk>,
        spec: &TriggerSpec,
        input_watermark: i64,
        processing_time: i64,
    ) -> Result<Option<ArrowPane>>;

    async fn take_ready(
        &self,
        spec: &TriggerSpec,
        input_watermark: i64,
        processing_time: i64,
        limit: usize,
    ) -> Result<Vec<ArrowPane>>;
}

#[derive(Default)]
struct InMemoryGbkStateInner {
    states: HashMap<GroupIdentity, WindowState>,
    in_memory_bytes: usize,
    total_spilled_arrow_bytes_estimate: usize,
    total_spilled_chunks: usize,
    ready_scan: Option<VecDeque<GroupIdentity>>,
    last_evaluated_watermark: Option<i64>,
    pending_range: Option<SnapshotRange>,
}

#[derive(Clone, Default)]
struct InMemoryGbkState {
    inner: Arc<Mutex<InMemoryGbkStateInner>>,
}

impl InMemoryGbkState {
    fn memory_snapshot(&self) -> (usize, usize, usize, usize) {
        let inner = self.inner.lock().expect("gbk state lock not poisoned");
        (
            inner.in_memory_bytes,
            inner.states.len(),
            inner.total_spilled_arrow_bytes_estimate,
            inner.total_spilled_chunks,
        )
    }
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

    async fn pending_range(&self) -> Option<SnapshotRange> {
        self.inner
            .lock()
            .expect("gbk state lock not poisoned")
            .pending_range
    }

    async fn pin_range(&self, range: SnapshotRange) {
        let mut inner = self.inner.lock().expect("gbk state lock not poisoned");
        inner.pending_range.get_or_insert(range);
    }

    async fn clear_range(&self) {
        self.inner
            .lock()
            .expect("gbk state lock not poisoned")
            .pending_range = None;
    }

    async fn spill_over_limit(
        &self,
        store: &FlareElementStore,
        memory_limit_bytes: usize,
    ) -> Result<()> {
        let pending = {
            let inner = self.inner.lock().expect("gbk state lock not poisoned");
            if inner.in_memory_bytes <= memory_limit_bytes {
                return Ok(());
            }

            // Spill toward a lower watermark so the next batch does not trigger
            // another full scan/write immediately after crossing the limit.
            let target_bytes = memory_limit_bytes / 2;
            let mut remaining_bytes = inner.in_memory_bytes;
            let mut pending = Vec::new();
            for (identity, state) in &inner.states {
                for chunk in &state.elements {
                    if remaining_bytes <= target_bytes {
                        break;
                    }
                    let memory_bytes = chunk.batch.get_array_memory_size();
                    let reference = SpilledChunkRef {
                        group: identity.clone(),
                        generation: state.generation,
                        chunk_id: NEXT_GBK_SPILL_CHUNK_ID.fetch_add(1, Ordering::Relaxed),
                    };
                    let payload = encode_arrow_chunk(&chunk.batch)?;
                    if !payload.is_empty() {
                        pending.push(SpillRow {
                            reference,
                            payload,
                            memory_bytes,
                        });
                        remaining_bytes = remaining_bytes.saturating_sub(memory_bytes);
                    }
                }
                if remaining_bytes <= target_bytes {
                    break;
                }
            }
            pending
        };
        if pending.is_empty() {
            return Ok(());
        }

        append_gbk_spill_rows(store, &pending).await?;
        let mut inner = self.inner.lock().expect("gbk state lock not poisoned");
        let mut grouped: HashMap<GroupIdentity, Vec<(SpilledChunkRef, usize)>> = HashMap::new();
        for row in pending {
            grouped
                .entry(row.reference.group.clone())
                .or_default()
                .push((row.reference, row.memory_bytes));
        }
        let mut freed_bytes = 0usize;
        let mut freed_chunks = 0usize;
        for (identity, references) in grouped {
            if let Some(state) = inner.states.get_mut(&identity) {
                let count = references.len().min(state.elements.len());
                freed_bytes = freed_bytes.saturating_add(
                    references
                        .iter()
                        .take(count)
                        .map(|(_, bytes)| *bytes)
                        .sum::<usize>(),
                );
                freed_chunks = freed_chunks.saturating_add(count);
                state.elements.drain(..count);
                state.spilled.extend(
                    references
                        .into_iter()
                        .take(count)
                        .map(|(reference, _)| reference),
                );
            }
        }
        inner.in_memory_bytes = inner.in_memory_bytes.saturating_sub(freed_bytes);
        inner.total_spilled_arrow_bytes_estimate = inner
            .total_spilled_arrow_bytes_estimate
            .saturating_add(freed_bytes);
        inner.total_spilled_chunks = inner.total_spilled_chunks.saturating_add(freed_chunks);
        Ok(())
    }

    async fn apply_group(
        &self,
        key: PrimitiveValue,
        window: BeamWindow,
        elements: Vec<ArrowValueChunk>,
        spec: &TriggerSpec,
        input_watermark: i64,
        processing_time: i64,
    ) -> Result<Option<ArrowPane>> {
        let end = window.max_timestamp_millis();
        let expired = spec.earliest_completion(end) < input_watermark;
        let mut inner = self.inner.lock().expect("gbk state lock not poisoned");
        if inner.states.is_empty()
            && expired
            && spec.trigger == Trigger::Default
            && spec.accumulation == AccumulationMode::Discarding
        {
            let (timestamp_min, timestamp_max) = chunk_timestamp_extrema(&elements);
            let timestamp_millis =
                output_timestamp(spec.output_time, &window, timestamp_min, timestamp_max);
            return Ok(Some(ArrowPane {
                chunks: elements,
                spilled: Vec::new(),
                timestamp_millis,
                window,
                pane: pane_info(crate::coders::primitives::PaneTiming::OnTime, 0, true, true),
            }));
        }

        let identity = group_identity(&key, &window);
        let (bytes_before, bytes_after, closed, output) = {
            let state = inner.states.entry(identity.clone()).or_insert_with(|| {
                WindowState::new(window.clone(), TriggerRunner::new(spec.trigger.clone()))
            });
            let bytes_before = arrow_chunks_memory_bytes(&state.elements);
            for element in &elements {
                for _ in 0..element.batch.num_rows() {
                    state.trigger.on_element(processing_time);
                }
            }
            let (timestamp_min, timestamp_max) = chunk_timestamp_extrema(&elements);
            state.pane_timestamp_min = min_option(state.pane_timestamp_min, timestamp_min);
            state.pane_timestamp_max = max_option(state.pane_timestamp_max, timestamp_max);
            state.elements.extend(elements);

            let output = fire_state(state, spec, input_watermark, processing_time)?;
            (
                bytes_before,
                arrow_chunks_memory_bytes(&state.elements),
                state.closed,
                output,
            )
        };
        inner.in_memory_bytes = inner
            .in_memory_bytes
            .saturating_sub(bytes_before)
            .saturating_add(bytes_after);
        if closed {
            inner.states.remove(&identity);
            inner.in_memory_bytes = inner.in_memory_bytes.saturating_sub(bytes_after);
        }
        Ok(output)
    }

    async fn take_ready(
        &self,
        spec: &TriggerSpec,
        input_watermark: i64,
        processing_time: i64,
        limit: usize,
    ) -> Result<Vec<ArrowPane>> {
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
            let (bytes_before, bytes_after, closed, pane) = {
                let Some(state) = inner.states.get_mut(&identity) else {
                    continue;
                };
                if state.evaluated_this_run {
                    continue;
                }
                let bytes_before = arrow_chunks_memory_bytes(&state.elements);
                let pane = fire_state(state, spec, input_watermark, processing_time)?;
                (
                    bytes_before,
                    arrow_chunks_memory_bytes(&state.elements),
                    state.closed,
                    pane,
                )
            };
            inner.in_memory_bytes = inner
                .in_memory_bytes
                .saturating_sub(bytes_before)
                .saturating_add(bytes_after);
            if let Some(value) = pane {
                output.push(value);
            }
            if closed {
                inner.states.remove(&identity);
                inner.in_memory_bytes = inner.in_memory_bytes.saturating_sub(bytes_after);
            }
        }
        if inner.ready_scan.as_ref().is_some_and(VecDeque::is_empty) {
            inner.ready_scan = None;
        }
        Ok(output)
    }
}

fn output_timestamp(
    output_time: OutputTime,
    window: &BeamWindow,
    pane_timestamp_min: Option<i64>,
    pane_timestamp_max: Option<i64>,
) -> i64 {
    match output_time {
        OutputTime::LatestInPane => {
            pane_timestamp_max.unwrap_or_else(|| window.max_timestamp_millis())
        }
        OutputTime::EarliestInPane => {
            pane_timestamp_min.unwrap_or_else(|| window.max_timestamp_millis())
        }
        OutputTime::EndOfWindow => window.max_timestamp_millis(),
    }
}

fn chunk_timestamp_extrema(chunks: &[ArrowValueChunk]) -> (Option<i64>, Option<i64>) {
    chunks
        .iter()
        .fold((None, None), |(minimum, maximum), chunk| {
            (
                min_option(minimum, Some(chunk.timestamp_min)),
                max_option(maximum, Some(chunk.timestamp_max)),
            )
        })
}

fn min_option(current: Option<i64>, next: Option<i64>) -> Option<i64> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current.min(next)),
        (current, next) => current.or(next),
    }
}

fn max_option(current: Option<i64>, next: Option<i64>) -> Option<i64> {
    match (current, next) {
        (Some(current), Some(next)) => Some(current.max(next)),
        (current, next) => current.or(next),
    }
}

fn arrow_panes_to_batch(
    panes: &[ArrowPane],
) -> Result<(RecordBatch, Arc<RecordTableSchema>, Vec<WindowMetadata>)> {
    let first = panes
        .first()
        .ok_or_else(|| anyhow!("cannot build an Arrow batch from no GBK panes"))?;
    let first_chunk = first
        .chunks
        .first()
        .ok_or_else(|| anyhow!("GBK emitted a pane without input values"))?;
    let key_field = first_chunk
        .batch
        .schema()
        .field_with_name(KEY_COLUMN)?
        .clone();
    let input_batch_schema = first_chunk.batch.schema();
    let value_field = input_batch_schema.field_with_name(VALUE_COLUMN)?;

    let mut key_arrays = Vec::with_capacity(panes.len());
    let mut value_arrays = Vec::new();
    let mut offsets = Vec::with_capacity(panes.len() + 1);
    offsets.push(0_i32);
    let mut offset = 0_i32;
    for pane in panes {
        let first_chunk = pane
            .chunks
            .first()
            .ok_or_else(|| anyhow!("GBK emitted a pane without input values"))?;
        let key = first_chunk
            .batch
            .column_by_name(KEY_COLUMN)
            .ok_or_else(|| anyhow!("GBK pane is missing '{KEY_COLUMN}'"))?;
        key_arrays.push(key.slice(0, 1));

        let mut value_count = 0_i32;
        for chunk in &pane.chunks {
            let values = chunk
                .batch
                .column_by_name(VALUE_COLUMN)
                .ok_or_else(|| anyhow!("GBK pane is missing '{VALUE_COLUMN}'"))?;
            value_count = value_count
                .checked_add(i32::try_from(values.len())?)
                .ok_or_else(|| anyhow!("GBK pane exceeds Arrow List offset capacity"))?;
            value_arrays.push(Arc::clone(values));
        }
        offset = offset
            .checked_add(value_count)
            .ok_or_else(|| anyhow!("GBK output exceeds Arrow List offset capacity"))?;
        offsets.push(offset);
    }

    let key_refs: Vec<&dyn Array> = key_arrays.iter().map(AsRef::as_ref).collect();
    let key_array = concat(&key_refs)?;
    let value_refs: Vec<&dyn Array> = value_arrays.iter().map(AsRef::as_ref).collect();
    let values = concat(&value_refs)?;
    let item_nullable = matches!(
        value_field.data_type(),
        ArrowDataType::Null | ArrowDataType::Boolean
    );
    let item_field = Arc::new(ArrowField::new(
        "item",
        values.data_type().clone(),
        item_nullable,
    ));
    let list = ListArray::try_new(
        item_field,
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        values,
        None,
    )?;
    let output_schema = Arc::new(ArrowSchema::new(vec![
        key_field,
        ArrowField::new(VALUE_COLUMN, list.data_type().clone(), false),
    ]));
    let batch = RecordBatch::try_new(output_schema.clone(), vec![key_array, Arc::new(list)])?;
    let table_schema = Arc::new(RecordTableSchema {
        table_type: TableType::Kv,
        arrow_schema: output_schema,
        has_windowed_metadata: false,
    });
    let metadata = panes
        .iter()
        .map(|pane| WindowMetadata {
            timestamp_millis: pane.timestamp_millis,
            windows: vec![pane.window.clone()],
            pane: pane.pane.clone(),
        })
        .collect();
    Ok((batch, table_schema, metadata))
}

async fn write_arrow_panes(
    store: &FlareElementStore,
    writer: &mut PCollectionWriter,
    mut panes: Vec<ArrowPane>,
) -> Result<()> {
    if panes.is_empty() {
        return Ok(());
    }
    let references: Vec<_> = panes
        .iter()
        .flat_map(|pane| pane.spilled.iter().cloned())
        .collect();
    let mut loaded = read_gbk_spill_chunks(store, &references).await?.into_iter();
    for pane in &mut panes {
        let mut spilled: Vec<_> = loaded.by_ref().take(pane.spilled.len()).collect();
        spilled.append(&mut pane.chunks);
        pane.chunks = spilled;
    }
    let (batch, schema, metadata) = arrow_panes_to_batch(&panes)?;
    writer
        .write_arrow_batch_with_window_metadata(batch, schema, metadata)
        .await
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
        let state_memory_limit_bytes = parse_env_bytes(
            GBK_STATE_MEMORY_LIMIT_ENV,
            DEFAULT_GBK_STATE_MEMORY_LIMIT_BYTES,
        );
        let (sorted_inputs, latest) = open_sorted_input_streams(
            &ctx.store,
            &input_pcollection_id,
            cursor,
            window_prune_before,
            &config,
            &self.state,
        )
        .await?;
        let mut writer =
            PCollectionWriter::new(ctx.store.clone(), ctx.output_pcollection_id.clone());
        let mut output: Vec<ArrowPane> = Vec::with_capacity(OUTPUT_BATCH_SIZE);
        let mut fired = 0usize;
        let mut decoded_rows = 0usize;

        let sort_started = Instant::now();
        let mut next_progress = 1_000_000usize;
        let mut decode_time = std::time::Duration::ZERO;
        let mut group_time = std::time::Duration::ZERO;
        let mut write_time = std::time::Duration::ZERO;
        for (partition, mut sorted_input) in sorted_inputs {
            info!(
                "GroupByKey: processing provider partition {partition} for {input_pcollection_id}"
            );
            let sort_result: Result<()> = async {
                let mut decoder = SortedGroupDecoder::default();
                while let Some(batch) = sorted_input.stream.next().await {
                    let batch = batch?;
                    decoded_rows += batch.num_rows();
                    let decode_started = Instant::now();
                    let groups = decoder.push_batch(&batch)?;
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
                    self.state
                        .spill_over_limit(&ctx.store, state_memory_limit_bytes)
                        .await?;
                    group_time += group_started.elapsed();
                    let write_started = Instant::now();
                    flush_output_if_full(&ctx.store, &mut writer, &mut output).await?;
                    write_time += write_started.elapsed();
                    if decoded_rows >= next_progress {
                        let (state_bytes, state_groups, spilled_bytes, spilled_chunks) =
                            self.state.memory_snapshot();
                        info!(
                            "GroupByKey: decoded {decoded_rows} rows, fired {fired} panes in {:?} \
                             (decode={decode_time:?}, group/merge={group_time:?}, output_write={write_time:?}, \
                             state_arrow_bytes={state_bytes}, state_groups={state_groups}, \
                             cumulative_spilled_arrow_bytes_estimate={spilled_bytes}, cumulative_spilled_chunks={spilled_chunks})",
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
                    self.state
                        .spill_over_limit(&ctx.store, state_memory_limit_bytes)
                        .await?;
                }
                Ok(())
            }
            .await;
            if let Err(error) = sort_result {
                let actual_spill_bytes = directory_size_bytes(sorted_input._temp_dir.path());
                let configured_spill_limit = GbkSortConfig::from_env().spill_limit_bytes;
                warn!(
                    "GroupByKey: external sort failed for provider partition {partition} \
                     (configured_spill_limit={configured_spill_limit} bytes, \
                     actual_spill_bytes={actual_spill_bytes}): {error:#}"
                );
                return Err(anyhow!(
                    "GroupByKey external sort failed for provider partition {partition} \
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
            write_arrow_panes(&ctx.store, &mut writer, ready).await?;
        }
        flush_output_if_full(&ctx.store, &mut writer, &mut output).await?;
        if !output.is_empty() {
            write_arrow_panes(&ctx.store, &mut writer, std::mem::take(&mut output)).await?;
        }

        // Advance the input cursor only after all emitted panes are durable.
        // Otherwise a failed output commit could make a retry skip its input.
        writer.commit().await?;
        if let Some(latest) = latest {
            ctx.store
                .set_cursor(&ctx.stage_id, &input_pcollection_id, latest)
                .await?;
        }
        self.state.clear_range().await;
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
    /// DataFusion removes its spill files when the runtime is dropped, but the
    /// directory itself is owned here so it also disappears on error paths.
    _temp_dir: Arc<TempDir>,
}

struct OneShotPaimonPartition {
    schema: SchemaRef,
    stream: Mutex<Option<SendableRecordBatchStream>>,
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
    state_backend: &dyn GbkStateBackend,
) -> Result<(Vec<(usize, SortedInputStream)>, Option<i64>)> {
    let Some(table_schema) = store.registry.get(pcollection_id) else {
        return Ok((Vec::new(), cursor));
    };
    let table = store.get_table(pcollection_id, &table_schema).await?;
    let range = match state_backend.pending_range().await {
        Some(range) => range,
        None => {
            let Some(range) = SnapshotRange::resolve(&table, cursor).await? else {
                let latest = table.snapshot_manager().get_latest_snapshot_id().await?;
                return Ok((Vec::new(), latest.or(cursor)));
            };
            state_backend.pin_range(range).await;
            range
        }
    };
    let latest = Some(range.end);

    let spill_parent = Path::new(store.catalog.warehouse()).join(".gbk-spill");
    std::fs::create_dir_all(&spill_parent)?;
    let temp_dir = Arc::new(
        tempfile::Builder::new()
            .prefix("group-by-key-run-")
            .tempdir_in(&spill_parent)?,
    );
    let provider = Arc::new(FlareTableProvider::new(
        table,
        &table_schema.arrow_schema,
        range,
        Some(KEY_COLUMN.to_string()),
        std::thread::available_parallelism().map_or(1, usize::from),
    ));
    info!(
        "GroupByKey: provider planned snapshots ({}, {}]; using semantics-preserving Arrow sort \
         with one shared pool across {} partitions",
        range.start,
        range.end,
        provider.max_partitions()
    );

    let projection_names = [
        KEY_COLUMN,
        VALUE_COLUMN,
        WINDOW_TIMESTAMP_COLUMN,
        WINDOW_KEY_COLUMN,
        WINDOW_EXTRA_KEYS_COLUMN,
        WINDOW_COUNT_COLUMN,
    ];
    let projection = projection_names
        .iter()
        .map(|name| {
            provider
                .schema()
                .index_of(name)
                .map_err(anyhow::Error::from)
        })
        .collect::<Result<Vec<_>>>()?;
    let mut filters: Vec<Expr> = Vec::new();
    if let Some(cutoff) = window_prune_before {
        filters.push(
            col(WINDOW_MAX_END_COLUMN)
                .gt(lit(cutoff))
                .or(col(WINDOW_MAX_END_COLUMN).is_null()),
        );
    }
    let session = gbk_session(
        config.memory_limit_bytes,
        temp_dir.path(),
        config.spill_limit_bytes,
        provider.max_partitions(),
        8192,
        config.spill_reservation_bytes,
    )?;
    let state = session.state();
    let scan = provider
        .scan(&state, Some(&projection), &filters, None)
        .await?;
    let input_schema = gbk_input_schema(&table_schema.arrow_schema)?;
    let flattened_schema = gbk_flattened_schema(&input_schema);
    let partition_count = scan.output_partitioning().partition_count();
    if partition_count == 0 {
        return Ok((Vec::new(), latest));
    }

    let task_ctx = session.task_ctx();
    let mut source_partitions: Vec<Arc<dyn PartitionStream>> = Vec::with_capacity(partition_count);
    for partition in 0..partition_count {
        let source = scan.execute(partition, Arc::clone(&task_ctx))?;
        let transformed = source.flat_map(|batch| {
            let batches = match batch {
                Ok(batch) => add_encoded_key_column(batch)
                    .and_then(flatten_window_batch)
                    .map(|batches| batches.into_iter().map(Ok).collect::<Vec<_>>())
                    .unwrap_or_else(|error| {
                        vec![Err(DataFusionError::Execution(error.to_string()))]
                    }),
                Err(error) => vec![Err(error)],
            };
            futures::stream::iter(batches)
        });
        source_partitions.push(Arc::new(OneShotPaimonPartition {
            schema: Arc::clone(&flattened_schema),
            stream: Mutex::new(Some(Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&flattened_schema),
                transformed,
            )))),
        }));
    }
    session.register_table(
        "gbk_input",
        Arc::new(StreamingTable::try_new(
            Arc::clone(&flattened_schema),
            source_partitions,
        )?),
    )?;

    let table = session.table("gbk_input").await?;
    let physical = table.create_physical_plan().await?;
    let order = datafusion::physical_expr::LexOrdering::new(vec![
        datafusion::physical_expr::PhysicalSortExpr::new(
            Arc::new(datafusion::physical_expr::expressions::Column::new(
                GROUP_KEY_COLUMN,
                flattened_schema.index_of(GROUP_KEY_COLUMN)?,
            )),
            arrow_schema::SortOptions {
                descending: false,
                nulls_first: false,
            },
        ),
        datafusion::physical_expr::PhysicalSortExpr::new(
            Arc::new(datafusion::physical_expr::expressions::Column::new(
                WINDOW_KEY_COLUMN,
                flattened_schema.index_of(WINDOW_KEY_COLUMN)?,
            )),
            arrow_schema::SortOptions {
                descending: false,
                nulls_first: false,
            },
        ),
    ])
    .ok_or_else(|| anyhow!("GroupByKey sort requires at least one ordering expression"))?;
    let sorted: Arc<dyn ExecutionPlan> =
        Arc::new(SortExec::new(order, physical).with_preserve_partitioning(true));
    let task_ctx = session.task_ctx();
    let count = sorted.properties().output_partitioning().partition_count();
    let streams = (0..count)
        .map(|partition| {
            let stream = spawn_buffered(sorted.execute(partition, Arc::clone(&task_ctx))?, 2);
            Ok((
                partition,
                SortedInputStream {
                    stream,
                    _temp_dir: Arc::clone(&temp_dir),
                },
            ))
        })
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    Ok((streams, latest))
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
    fn push_batch(&mut self, batch: &RecordBatch) -> Result<Vec<DecodedKeyGroup>> {
        let window_keys = batch
            .column_by_name(WINDOW_KEY_COLUMN)
            .ok_or_else(|| {
                anyhow!("GroupByKey result is missing the '{WINDOW_KEY_COLUMN}' column")
            })?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("GroupByKey window key must be a Binary column"))?;
        let encoded_keys = batch
            .column_by_name(GROUP_KEY_COLUMN)
            .ok_or_else(|| anyhow!("GroupByKey result is missing the '{GROUP_KEY_COLUMN}' column"))?
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| anyhow!("GroupByKey encoded key must be a Binary column"))?;
        let key_column = batch
            .column_by_name(KEY_COLUMN)
            .ok_or_else(|| anyhow!("GroupByKey input is missing '{KEY_COLUMN}'"))?;
        let value_column = batch
            .column_by_name(VALUE_COLUMN)
            .ok_or_else(|| anyhow!("GroupByKey input is missing '{VALUE_COLUMN}'"))?;
        let timestamps = batch
            .column_by_name(WINDOW_TIMESTAMP_COLUMN)
            .ok_or_else(|| anyhow!("GroupByKey is missing '{WINDOW_TIMESTAMP_COLUMN}'"))?
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| anyhow!("GroupByKey timestamp must be an Int64 column"))?;
        let mut completed = Vec::new();
        let mut start = 0;

        while start < batch.num_rows() {
            let encoded_key = encoded_keys.value(start);
            let window_key = window_keys.value(start);
            let mut end = start + 1;
            while end < batch.num_rows()
                && encoded_keys.value(end) == encoded_key
                && window_keys.value(end) == window_key
            {
                end += 1;
            }

            let encoded_key = encoded_key.to_vec();
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

            if self.current.is_none() {
                let key = primitive_value_from_array_row(
                    key_column.as_ref(),
                    key_column.data_type(),
                    start,
                )?;
                self.current = Some(InProgressKeyGroup {
                    encoded_key: encoded_key.clone(),
                    key,
                    windows: Vec::new(),
                });
            }

            let window = BeamWindow::from_binary_key(window_key)
                .ok_or_else(|| anyhow!("GroupByKey encountered a malformed binary window key"))?;
            let timestamp_min = (start..end).map(|row| timestamps.value(row)).min().unwrap();
            let timestamp_max = (start..end).map(|row| timestamps.value(row)).max().unwrap();
            let chunk_schema = Arc::new(ArrowSchema::new_with_metadata(
                vec![
                    batch.schema().field_with_name(KEY_COLUMN)?.clone(),
                    batch.schema().field_with_name(VALUE_COLUMN)?.clone(),
                    batch
                        .schema()
                        .field_with_name(WINDOW_TIMESTAMP_COLUMN)?
                        .clone(),
                ],
                batch.schema().metadata().clone(),
            ));
            let chunk_batch = RecordBatch::try_new(
                chunk_schema,
                vec![
                    key_column.slice(start, end - start),
                    value_column.slice(start, end - start),
                    batch
                        .column_by_name(WINDOW_TIMESTAMP_COLUMN)
                        .expect("timestamp column validated above")
                        .slice(start, end - start),
                ],
            )?;
            let chunk = ArrowValueChunk {
                batch: chunk_batch,
                timestamp_min,
                timestamp_max,
            };
            let current = self.current.as_mut().expect("group initialized above");
            match current.windows.last_mut() {
                Some((current_window, chunks))
                    if current_window.to_binary_key() == window.to_binary_key() =>
                {
                    chunks.push(chunk);
                }
                _ => current.windows.push((window, vec![chunk])),
            }
            start = end;
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

type WindowValues = Vec<(BeamWindow, Vec<ArrowValueChunk>)>;

fn apply_merge_results(
    key: &PrimitiveValue,
    original_groups: Vec<(BeamWindow, Vec<ArrowValueChunk>)>,
    merge_results: Vec<MergedWindows>,
) -> Result<Vec<(PrimitiveValue, BeamWindow, Vec<ArrowValueChunk>)>, Error> {
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

    let mut merged_groups: HashMap<Vec<u8>, (BeamWindow, Vec<ArrowValueChunk>)> = HashMap::new();
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
    output: &mut Vec<ArrowPane>,
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
    groups: Vec<(PrimitiveValue, BeamWindow, Vec<ArrowValueChunk>)>,
) -> Result<Vec<(PrimitiveValue, BeamWindow, Vec<ArrowValueChunk>)>> {
    let mut by_key: HashMap<String, (PrimitiveValue, Vec<(i64, i64, Vec<ArrowValueChunk>)>)> =
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
    store: &FlareElementStore,
    writer: &mut PCollectionWriter,
    output: &mut Vec<ArrowPane>,
) -> Result<()> {
    if output.len() >= OUTPUT_BATCH_SIZE {
        write_arrow_panes(store, writer, std::mem::take(output)).await?;
        output.reserve(OUTPUT_BATCH_SIZE);
    }
    Ok(())
}

fn fire_state(
    state: &mut WindowState,
    spec: &TriggerSpec,
    input_watermark: i64,
    processing_time: i64,
) -> Result<Option<ArrowPane>> {
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
        return Ok(None);
    }

    let is_first = !state.fired;
    let timing = pane_timing(end_of_window, is_first);
    let pane = pane_info(timing, state.pane_index, is_first, expired);

    let (elements, spilled) = match spec.accumulation {
        AccumulationMode::Discarding => {
            let elements = std::mem::take(&mut state.elements);
            let spilled = std::mem::take(&mut state.spilled);
            state.generation += 1;
            (elements, spilled)
        }
        AccumulationMode::Accumulating | AccumulationMode::Retracting => {
            (state.elements.clone(), state.spilled.clone())
        }
    };
    let pane_timestamp_min = state.pane_timestamp_min.take();
    let pane_timestamp_max = state.pane_timestamp_max.take();
    let timestamp_millis = output_timestamp(
        spec.output_time,
        &state.window,
        pane_timestamp_min,
        pane_timestamp_max,
    );

    state.trigger.on_fire(trigger_ctx);
    state.pane_index += 1;
    state.fired = true;
    state.evaluated_this_run = true;
    if expired {
        state.closed = true;
    }

    // Beam's `EmitIfNonempty` closing behavior: a firing whose pane has no
    // accumulated values carries no data and must not be emitted.
    if elements.is_empty() && spilled.is_empty() {
        return Ok(None);
    }

    Ok(Some(ArrowPane {
        chunks: elements,
        spilled,
        timestamp_millis,
        window: state.window.clone(),
        pane,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coders::primitives::{PaneInfo, PaneTiming, WindowedValue};
    use crate::store::element_store::{FlareElementStore, ScanCollectionRequest};
    use crate::store::record::{BeamKV, BeamRecord};
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

    fn spill_test_chunk(value: i64, timestamp: i64) -> ArrowValueChunk {
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(KEY_COLUMN, ArrowDataType::Binary, false),
            ArrowField::new(VALUE_COLUMN, ArrowDataType::Int64, false),
            ArrowField::new(WINDOW_TIMESTAMP_COLUMN, ArrowDataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BinaryArray::from(vec![b"key".as_slice()])),
                Arc::new(Int64Array::from(vec![value])),
                Arc::new(Int64Array::from(vec![timestamp])),
            ],
        )
        .unwrap();
        ArrowValueChunk {
            batch,
            timestamp_min: timestamp,
            timestamp_max: timestamp,
        }
    }

    #[tokio::test]
    async fn gbk_spill_limit_applies_across_many_small_groups() {
        let (_dir, store) = make_store().await;
        let state = InMemoryGbkState::default();
        let spec = TriggerSpec::from_windowing_strategy(None);
        let window = BeamWindow::Global;
        let mut total_bytes = 0usize;
        let mut group_bytes = Vec::new();

        for index in 0..3 {
            let chunk = spill_test_chunk(index, index);
            let bytes = chunk.batch.get_array_memory_size();
            total_bytes += bytes;
            group_bytes.push(bytes);
            state
                .apply_group(
                    PrimitiveValue::String(format!("key-{index}")),
                    window.clone(),
                    vec![chunk],
                    &spec,
                    i64::MIN,
                    0,
                )
                .await
                .unwrap();
        }

        let limit = total_bytes / 2;
        assert!(group_bytes.iter().all(|bytes| *bytes < limit));
        assert_eq!(state.inner.lock().unwrap().in_memory_bytes, total_bytes);

        state.spill_over_limit(&store, limit).await.unwrap();

        let inner = state.inner.lock().unwrap();
        let remaining_bytes = inner
            .states
            .values()
            .map(|group| arrow_chunks_memory_bytes(&group.elements))
            .sum::<usize>();
        assert!(
            remaining_bytes <= limit / 2,
            "aggregate retained bytes {remaining_bytes} should be below spill target {}",
            limit / 2
        );
        assert_eq!(inner.in_memory_bytes, remaining_bytes);
        assert_eq!(inner.total_spilled_arrow_bytes_estimate, total_bytes);
        assert_eq!(inner.total_spilled_chunks, 3);
        assert_eq!(
            inner
                .states
                .values()
                .map(|group| group.spilled.len())
                .sum::<usize>(),
            3
        );
    }

    #[test]
    fn decoded_arrow_chunk_keeps_event_timestamps_for_spill_ipc() {
        let window = BeamWindow::Global;
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(KEY_COLUMN, ArrowDataType::Binary, false),
            ArrowField::new(VALUE_COLUMN, ArrowDataType::Int64, false),
            ArrowField::new(WINDOW_TIMESTAMP_COLUMN, ArrowDataType::Int64, false),
            ArrowField::new(WINDOW_KEY_COLUMN, ArrowDataType::Binary, false),
            ArrowField::new(GROUP_KEY_COLUMN, ArrowDataType::Binary, false),
        ]));
        let window_key = window.to_binary_key();
        let encoded_key = encoded_primitive_sort_key(&PrimitiveValue::Bytes(b"key".to_vec()));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BinaryArray::from(vec![b"key".as_slice()])),
                Arc::new(Int64Array::from(vec![9])),
                Arc::new(Int64Array::from(vec![1234])),
                Arc::new(BinaryArray::from(vec![window_key.as_slice()])),
                Arc::new(BinaryArray::from(vec![encoded_key.as_slice()])),
            ],
        )
        .unwrap();
        let mut decoder = SortedGroupDecoder::default();
        decoder.push_batch(&batch).unwrap();
        let decoded = decoder.finish().unwrap();
        let chunk = &decoded.windows[0].1[0];
        let restored = decode_arrow_chunk(&encode_arrow_chunk(&chunk.batch).unwrap()).unwrap();
        let timestamps = restored
            .column_by_name(WINDOW_TIMESTAMP_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(timestamps.value(0), 1234);
    }

    #[tokio::test]
    async fn completed_default_window_group_emits_without_retaining_state() {
        let state = InMemoryGbkState::default();
        let spec = TriggerSpec::from_windowing_strategy(None);
        let window = BeamWindow::Interval {
            start_millis: 0,
            end_millis: 1_000,
        };
        let pane = state
            .apply_group(
                PrimitiveValue::String("key".to_string()),
                window,
                vec![spill_test_chunk(3, 10), spill_test_chunk(5, 15)],
                &spec,
                i64::MAX,
                0,
            )
            .await
            .unwrap()
            .expect("final watermark fires a completed default-trigger group");

        assert_eq!(pane.chunks.len(), 2);
        let inner = state.inner.lock().unwrap();
        assert!(inner.states.is_empty());
        assert_eq!(inner.in_memory_bytes, 0);
    }

    #[tokio::test]
    async fn gbk_state_spill_releases_chunks_and_pane_combines_spilled_and_memory_values() {
        let (_dir, store) = make_store().await;
        let state = InMemoryGbkState::default();
        let mut spec = TriggerSpec::from_windowing_strategy(None);
        spec.accumulation = AccumulationMode::Accumulating;
        let window = BeamWindow::Global;
        let key = PrimitiveValue::String("key".to_string());

        state
            .apply_group(
                key.clone(),
                window.clone(),
                vec![spill_test_chunk(3, 10)],
                &spec,
                i64::MIN,
                0,
            )
            .await
            .unwrap();
        state.spill_over_limit(&store, 1).await.unwrap();
        {
            let inner = state.inner.lock().unwrap();
            let group = inner.states.values().next().unwrap();
            assert!(
                group.elements.is_empty(),
                "successful spill releases Arrow buffers"
            );
            assert_eq!(group.spilled.len(), 1);
        }

        state
            .apply_group(
                key,
                window,
                vec![spill_test_chunk(5, 15)],
                &spec,
                i64::MIN,
                0,
            )
            .await
            .unwrap();
        state.begin_run().await;
        let mut panes = state.take_ready(&spec, i64::MAX, 0, 1).await.unwrap();
        assert_eq!(panes.len(), 1);
        let mut spilled = read_gbk_spill_chunks(&store, &panes[0].spilled)
            .await
            .unwrap();
        spilled.append(&mut panes[0].chunks);
        let values: Vec<i64> = spilled
            .iter()
            .flat_map(|chunk| {
                chunk
                    .batch
                    .column_by_name(VALUE_COLUMN)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(values, vec![3, 5]);
        let timestamps: Vec<i64> = spilled
            .iter()
            .flat_map(|chunk| {
                chunk
                    .batch
                    .column_by_name(WINDOW_TIMESTAMP_COLUMN)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect();
        assert_eq!(timestamps, vec![10, 15]);
    }

    #[test]
    fn discarding_fire_advances_spill_generation_without_reusing_old_chunks() {
        let reference = SpilledChunkRef {
            group: GroupIdentity {
                key: b"key".to_vec(),
                window: interval(0, 10).to_binary_key(),
            },
            generation: 6,
            chunk_id: 91,
        };
        let mut state = WindowState::new(interval(0, 10), TriggerRunner::new(Trigger::Default));
        state.generation = 6;
        state.spilled.push(reference.clone());
        let mut spec = TriggerSpec::from_windowing_strategy(None);
        spec.accumulation = AccumulationMode::Discarding;

        let pane = fire_state(&mut state, &spec, 10, 0).unwrap().unwrap();
        assert_eq!(pane.spilled, vec![reference]);
        assert!(state.spilled.is_empty());
        assert_eq!(state.generation, 7);
    }

    #[tokio::test]
    async fn gbk_spill_table_round_trips_chunks_timestamps_and_duplicates() {
        let (_dir, store) = make_store().await;
        let schema = Arc::new(ArrowSchema::new(vec![
            ArrowField::new(KEY_COLUMN, ArrowDataType::Binary, false),
            ArrowField::new(VALUE_COLUMN, ArrowDataType::Int64, false),
            ArrowField::new(WINDOW_TIMESTAMP_COLUMN, ArrowDataType::Int64, false),
        ]));
        let input = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BinaryArray::from(vec![
                    b"key".as_slice(),
                    b"key".as_slice(),
                ])),
                Arc::new(Int64Array::from(vec![7, 7])),
                Arc::new(Int64Array::from(vec![11, 13])),
            ],
        )
        .unwrap();
        let group = GroupIdentity {
            key: b"encoded-key".to_vec(),
            window: b"encoded-window".to_vec(),
        };
        let payload = encode_arrow_chunk(&input).unwrap();
        let references = [1_i64, 2_i64].map(|chunk_id| SpilledChunkRef {
            group: group.clone(),
            generation: 4,
            chunk_id,
        });
        let rows = references
            .iter()
            .cloned()
            .map(|reference| SpillRow {
                reference,
                payload: payload.clone(),
                memory_bytes: input.get_array_memory_size(),
            })
            .collect::<Vec<_>>();

        append_gbk_spill_rows(&store, &rows).await.unwrap();
        let chunks = read_gbk_spill_chunks(&store, &references).await.unwrap();
        assert_eq!(chunks.len(), 2, "equal values must not be deduplicated");
        for chunk in chunks {
            assert_eq!(chunk.timestamp_min, 11);
            assert_eq!(chunk.timestamp_max, 13);
            let values = chunk
                .batch
                .column_by_name(VALUE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(values.values(), &[7, 7]);
        }
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
                vec![{
                    let input_schema = Arc::new(ArrowSchema::new(vec![
                        ArrowField::new(KEY_COLUMN, ArrowDataType::Utf8, false),
                        ArrowField::new(VALUE_COLUMN, ArrowDataType::Int64, false),
                    ]));
                    let batch = RecordBatch::try_new(
                        Arc::clone(&input_schema),
                        vec![
                            Arc::new(arrow_array::StringArray::from(vec!["a"])),
                            Arc::new(Int64Array::from(vec![1])),
                        ],
                    )
                    .unwrap();
                    ArrowValueChunk {
                        batch,

                        timestamp_min: 10,
                        timestamp_max: 10,
                    }
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
        let state = InMemoryGbkState::default();
        let (mut sorted, latest) =
            open_sorted_input_streams(&store, input, None, None, &config, &state)
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
