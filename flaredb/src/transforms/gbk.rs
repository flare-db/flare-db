use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Error, anyhow};
use async_trait::async_trait;
use beam_model_rs::v1::{
    Coder, Components, Environment, FunctionSpec, PCollection, PTransform, WindowingStrategy,
};
use datafusion::{
    common::TableReference,
    functions_aggregate::expr_fn::array_agg,
    prelude::{SessionContext, col},
};
use log::info;
use paimon_datafusion::PaimonTableProvider;

use crate::{
    coders::primitives::{BeamWindow, PaneInfo, WindowedValue},
    jobservice::urns::beam_urns,
    store::{
        KEY_COLUMN, VALUE_COLUMN,
        element_store::WINDOW_KEY_COLUMN,
        record::{
            BeamGbk, BeamRecord, PrimitiveValue, iterable_value_from_array_row,
            primitive_value_from_array_row,
        },
    },
    transforms::{ExecutionContext, FlareTransform},
};

/// Runner-native implementation of Beam's `GroupByKey`.
///
/// Groups input `KV<K, V>` elements by `(key, window)` and emits one
/// `KV<K, Iterable<V>>` per group. Output metadata is derived from the group's
/// window (max timestamp, single window, on-time pane).
#[derive(Clone)]
pub struct GroupByKey {
    name: String,
    id: String,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
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
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> Result<(), Error> {
        // GroupByKey consumes exactly one input PCollection.
        let input_pcollection_id = ctx
            .input_pcollection_ids
            .first()
            .cloned()
            .expect("GroupByKey expects exactly one input PCollection");

        // Zero committed elements means no schema and no table: empty output.
        if ctx.store.registry.get(&input_pcollection_id).is_none() {
            info!(
                "GroupByKey: input PCollection '{}' has no elements, producing empty output",
                input_pcollection_id
            );
            return Ok(());
        }
        let Some(table) = ctx.store.get_existing_table(&input_pcollection_id).await? else {
            info!(
                "GroupByKey: input PCollection '{}' has no elements, producing empty output",
                input_pcollection_id
            );
            return Ok(());
        };

        // Group by (key, window): unnest `__flare_window_key` so an element in
        // multiple windows becomes one row per window, then `array_agg` values per
        // group.
        let session = SessionContext::new();
        let provider = PaimonTableProvider::try_new(table)?;
        session.register_table(TableReference::bare("gbk"), Arc::new(provider))?;

        let df = session.table("gbk").await?;
        let df = df.unnest_columns(&[WINDOW_KEY_COLUMN])?;
        let df = df.aggregate(
            vec![col(KEY_COLUMN), col(WINDOW_KEY_COLUMN)],
            vec![array_agg(col(VALUE_COLUMN)).alias(VALUE_COLUMN)],
        )?;
        let batches = df.collect().await?;

        // Rebuild each group as a WindowedValue, deriving metadata from its window.
        let mut output: Vec<WindowedValue> = Vec::new();
        for batch in batches {
            let key_column = batch
                .column_by_name(KEY_COLUMN)
                .ok_or_else(|| anyhow!("GroupByKey result is missing the '{KEY_COLUMN}' column"))?;
            let window_column = batch.column_by_name(WINDOW_KEY_COLUMN).ok_or_else(|| {
                anyhow!("GroupByKey result is missing the '{WINDOW_KEY_COLUMN}' column")
            })?;
            let value_column = batch.column_by_name(VALUE_COLUMN).ok_or_else(|| {
                anyhow!("GroupByKey result is missing the '{VALUE_COLUMN}' column")
            })?;

            for row in 0..batch.num_rows() {
                let key = primitive_value_from_array_row(
                    key_column.as_ref(),
                    key_column.data_type(),
                    row,
                )?;
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
                let value = iterable_value_from_array_row(
                    value_column.as_ref(),
                    value_column.data_type(),
                    row,
                )?;

                output.push(WindowedValue {
                    value: BeamRecord::GBK(BeamGbk { key, value }),
                    timestamp_millis: window.max_timestamp_millis(),
                    windows: vec![window],
                    pane: PaneInfo::on_time_firing(),
                });
            }
        }

        info!("Executed GroupByKey: {} output groups", output.len());

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coders::primitives::PaneTiming;
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
        transform
            .execute(ExecutionContext {
                store: store.clone(),
                input_pcollection_ids: vec![input.to_string()],
                output_pcollection_id: output.to_string(),
                consumer_transfrom_id: "consumer".to_string(),
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
}
