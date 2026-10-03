use anyhow::Error;
use async_trait::async_trait;
use beam_model_rs::v1::{
    Coder, Components, Environment, FunctionSpec, PCollection, PTransform, WindowingStrategy,
};
use log::info;
use std::collections::{HashMap, HashSet};

use crate::{
    coders::primitives::WindowedValue,
    jobservice::urns::beam_urns,
    transforms::{ExecutionContext, FlareTransform},
};

/// Merges multiple input PCollections into a single output PCollection.
///
/// Flatten is a pure concatenation: every element from every input is emitted,
/// unchanged, into the single output, preserving each element's window metadata.
#[derive(Clone)]
pub struct Flatten {
    name: String,
    id: String,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
}

#[async_trait]
impl FlareTransform for Flatten {
    fn urn() -> &'static str
    where
        Self: Sized,
    {
        beam_urns::FLATTEN_TRANSFORM
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
        // Pure union: read every input as WindowedValues and re-emit unchanged,
        // preserving each element's window metadata. Inputs are read
        // incrementally from this stage's cursor, so a re-run emits only the rows
        // appended by its upstream since the previous run.
        let mut merged: Vec<WindowedValue> = Vec::new();

        for input_id in &ctx.input_pcollection_ids {
            let values = ctx
                .store
                .scan_windowed_values_since(&ctx.stage_id, input_id)
                .await?;
            merged.extend(values);
        }

        info!(
            "Executed Flatten: {} input collections, {} total elements",
            ctx.input_pcollection_ids.len(),
            merged.len()
        );

        // A Flatten whose inputs are all empty correctly produces an empty output.
        if merged.is_empty() {
            return Ok(());
        }

        ctx.store
            .write_windowed_value_batch(&ctx.output_pcollection_id, merged)
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
    use crate::coders::primitives::{BeamWindow, PaneInfo};
    use crate::store::element_store::{FlareElementStore, ScanCollectionRequest};
    use crate::store::record::{BeamKV, BeamRecord, PrimitiveValue};
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

    fn kv(key: &str, value: i64, start: i64, end: i64) -> WindowedValue {
        WindowedValue {
            value: BeamRecord::KV(BeamKV {
                key: PrimitiveValue::String(key.to_string()),
                value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Int64(value))),
            }),
            timestamp_millis: start,
            windows: vec![BeamWindow::Interval {
                start_millis: start,
                end_millis: end,
            }],
            pane: PaneInfo::on_time_firing(),
        }
    }

    #[tokio::test]
    async fn flatten_preserves_window_metadata() {
        let (_dir, store) = make_store().await;

        store
            .write_windowed_value_batch("in-0", vec![kv("a", 1, 0, 60_000)])
            .await
            .unwrap();
        store
            .write_windowed_value_batch("in-1", vec![kv("b", 2, 60_000, 120_000)])
            .await
            .unwrap();

        let transform = Flatten::with(
            "flatten-test".to_string(),
            HashMap::from([("in0".to_string(), "in-0".to_string())]),
            HashMap::from([("out".to_string(), "out".to_string())]),
            "Flatten".to_string(),
        );

        transform
            .execute(ExecutionContext {
                store: store.clone(),
                input_pcollection_ids: vec!["in-0".to_string(), "in-1".to_string()],
                output_pcollection_id: "out".to_string(),
                consumer_transfrom_id: "consumer".to_string(),
                stage_id: "flatten-test".to_string(),
                input_watermark: i64::MAX,
            })
            .await
            .expect("flatten failed");

        let output = store
            .scan_windowed_values(ScanCollectionRequest {
                pcollection_id: "out".to_string(),
            })
            .await
            .expect("scan output");

        assert_eq!(output.len(), 2);
        let windows: Vec<_> = output.iter().map(|v| v.windows.clone()).collect();
        assert!(windows.iter().any(|w| {
            *w == vec![BeamWindow::Interval {
                start_millis: 0,
                end_millis: 60_000,
            }]
        }));
        assert!(windows.iter().any(|w| {
            *w == vec![BeamWindow::Interval {
                start_millis: 60_000,
                end_millis: 120_000,
            }]
        }));
    }
}
