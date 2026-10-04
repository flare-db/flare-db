use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use anyhow::{Error, anyhow};
use async_trait::async_trait;
use beam_model_rs::v1::{
    Coder, Components, Environment, FunctionSpec, PCollection, PTransform, WindowingStrategy,
};
use datafusion::{
    functions_aggregate::expr_fn::array_agg,
    prelude::{SessionContext, col},
};
use log::info;

use crate::{
    coders::primitives::{BeamWindow, WindowedValue},
    engine::trigger::{
        AccumulationMode, TriggerContext, TriggerRunner, TriggerSpec, pane_info, pane_timing,
    },
    jobservice::urns::beam_urns,
    store::{
        KEY_COLUMN, VALUE_COLUMN,
        element_store::WINDOW_KEY_COLUMN,
        record::{
            BeamGbk, BeamRecord, IterableValue, PrimitiveValue, iterable_value_from_array_row,
            primitive_value_from_array_row,
        },
    },
    transforms::{ExecutionContext, FlareTransform},
};

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
        }
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
/// per-group state. DataFusion does the per-batch `(key, window)` grouping; the
/// accumulated state (values, trigger, pane) is kept here across runs.
#[derive(Clone)]
pub struct GroupByKey {
    name: String,
    id: String,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
    /// Per-`(key, window)` state. The shared `Arc` survives re-runs (the node
    /// holds one instance).
    states: Arc<Mutex<HashMap<String, WindowState>>>,
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
            states: Arc::new(Mutex::new(HashMap::new())),
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

        // Read only the input appended since this stage's last run. The cursor is
        // this stage's position in the input PCollection's changelog.
        let cursor = ctx
            .store
            .cursor(&ctx.stage_id, &input_pcollection_id)
            .await?;
        let (batches, latest) = ctx
            .store
            .read_incremental_batches(&input_pcollection_id, cursor)
            .await?;

        // Group the new rows by `(key, window)` with DataFusion, then fold them
        // into per-group state and evaluate the trigger. Everything touching the
        // state map holds one lock, so a re-run stays consistent.
        let grouped = if batches.is_empty() {
            Vec::new()
        } else {
            group_batches(batches).await?
        };

        let output = {
            let mut states = self.states.lock().expect("gbk state lock not poisoned");
            for (key, window, values) in grouped {
                let identity = group_identity(&key, &window);
                let state = states.entry(identity).or_insert_with(|| {
                    WindowState::new(key, window, TriggerRunner::new(spec.trigger.clone()))
                });
                for _ in 0..values.len() {
                    state.trigger.on_element(ctx.processing_time);
                }
                state.values.extend(values);
            }
            fire_windows(&mut states, &spec, ctx.input_watermark, ctx.processing_time)?
        };

        if let Some(latest) = latest {
            ctx.store
                .set_cursor(&ctx.stage_id, &input_pcollection_id, latest)
                .await?;
        }

        if output.is_empty() {
            info!(
                "GroupByKey: no window fired at input watermark {}",
                ctx.input_watermark
            );
            return Ok(());
        }

        info!(
            "Executed GroupByKey: {} pane(s) fired at input watermark {}",
            output.len(),
            ctx.input_watermark
        );

        ctx.store
            .write_windowed_value_batch(&ctx.output_pcollection_id, output)
            .await?;

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

/// Group incremental input batches by `(key, window)` with DataFusion.
///
/// Returns one `(key, window, values)` per group, where `values` are the `V`s
/// appended for that group since the last run. This is the per-batch half of the
/// aggregation; the accumulated state across batches lives in [`GroupByKey`].
async fn group_batches(
    batches: Vec<arrow_array::RecordBatch>,
) -> Result<Vec<(PrimitiveValue, BeamWindow, Vec<BeamRecord>)>, Error> {
    let session = SessionContext::new();
    let df = session.read_batches(batches)?;
    let df = df.unnest_columns(&[WINDOW_KEY_COLUMN])?;
    let df = df.aggregate(
        vec![col(KEY_COLUMN), col(WINDOW_KEY_COLUMN)],
        vec![array_agg(col(VALUE_COLUMN)).alias(VALUE_COLUMN)],
    )?;
    let batches = df.collect().await?;

    let mut groups = Vec::new();
    for batch in batches {
        let key_column = batch
            .column_by_name(KEY_COLUMN)
            .ok_or_else(|| anyhow!("GroupByKey result is missing the '{KEY_COLUMN}' column"))?;
        let window_column = batch.column_by_name(WINDOW_KEY_COLUMN).ok_or_else(|| {
            anyhow!("GroupByKey result is missing the '{WINDOW_KEY_COLUMN}' column")
        })?;
        let value_column = batch
            .column_by_name(VALUE_COLUMN)
            .ok_or_else(|| anyhow!("GroupByKey result is missing the '{VALUE_COLUMN}' column"))?;

        for row in 0..batch.num_rows() {
            let key =
                primitive_value_from_array_row(key_column.as_ref(), key_column.data_type(), row)?;
            let window_key = match primitive_value_from_array_row(
                window_column.as_ref(),
                window_column.data_type(),
                row,
            )? {
                PrimitiveValue::String(s) => s,
                other => {
                    return Err(anyhow!(
                        "GroupByKey window key must be a string, got {other:?}"
                    ));
                }
            };
            let window = BeamWindow::from_canonical_key(&window_key).ok_or_else(|| {
                anyhow!("GroupByKey encountered a malformed window key '{window_key}'")
            })?;
            let iterable = iterable_value_from_array_row(
                value_column.as_ref(),
                value_column.data_type(),
                row,
            )?;
            groups.push((key, window, iterable.list));
        }
    }
    Ok(groups)
}

/// Evaluate every group's trigger and build the output pane for each firing.
///
/// A window fires whenever its trigger says so. In addition, once a window's
/// expiration has passed it always gets a closing pane (Beam's closing behavior),
/// even if its trigger would not fire on its own. `is_last` is set on the closing
/// pane; for the default trigger (no lateness) the on-time pane is also the
/// closing one.
fn fire_windows(
    states: &mut HashMap<String, WindowState>,
    spec: &TriggerSpec,
    input_watermark: i64,
    processing_time: i64,
) -> Result<Vec<WindowedValue>, Error> {
    let mut output = Vec::new();
    for state in states.values_mut() {
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
            continue;
        }

        let is_first = !state.fired;
        let timing = pane_timing(end_of_window, is_first);
        let pane = pane_info(timing, state.pane_index, is_first, expired);

        let values = match spec.accumulation {
            AccumulationMode::Discarding => std::mem::take(&mut state.values),
            AccumulationMode::Accumulating | AccumulationMode::Retracting => state.values.clone(),
        };

        output.push(WindowedValue {
            value: BeamRecord::GBK(BeamGbk {
                key: state.key.clone(),
                value: IterableValue::from_records(values),
            }),
            timestamp_millis: end,
            windows: vec![state.window.clone()],
            pane,
        });

        state.trigger.on_fire(trigger_ctx);
        state.pane_index += 1;
        state.fired = true;
        if expired {
            state.closed = true;
        }
    }
    Ok(output)
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
}
