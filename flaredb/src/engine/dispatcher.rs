use crate::{
    engine::{
        executor::{Executor, StageExecutor},
        harness::Channels,
        runtime::BundleRuntime,
        scheduler::NodeScheduler,
    },
    fusion::pipeline::{ExecutableGraph, ExecutableNode},
    store::element_store::FlareElementStore,
};
use crate::engine::sdf::SplittableStageExecutor;
use anyhow::anyhow;
use beam_model_rs::v1::{Coder, Components};
use std::{collections::HashMap, sync::Arc};
use tokio::task::JoinSet;

/// Owns the harness channels and per-job state needed to prepare a pipeline for
/// execution. A [`StageExecutor`] is built from this prepared state so that the
/// executor itself only concerns itself with executing bundles.
pub struct ExecutorDispatcher {
    channels: Channels,
    store: Arc<FlareElementStore>,
    pipeline_coders: Arc<HashMap<String, Coder>>,
    pipeline_components: Arc<Components>,
}

impl ExecutorDispatcher {
    pub async fn new(channels: Channels) -> anyhow::Result<Self> {
        let store_path = crate::utils::path::warehouse_dir();
        let store_base = store_path.to_str().unwrap_or(".").to_string();
        let store = Arc::new(FlareElementStore::new(store_base, "flare".to_string()).await?);
        Ok(Self {
            channels,
            store,
            pipeline_coders: Arc::new(HashMap::new()),
            pipeline_components: Arc::new(Components::default()),
        })
    }

    /// Reset all channels so a new worker can connect.
    pub async fn reset_channels(&self) {
        self.channels.reset().await;
    }

    /// Point the element store at the job's warehouse database.
    pub async fn set_job_store(&mut self, job_id: &str) -> anyhow::Result<()> {
        let store_path = crate::utils::path::warehouse_dir();
        let store_base = store_path.to_str().unwrap_or(".").to_string();
        self.store = Arc::new(FlareElementStore::new(store_base, job_id.to_string()).await?);
        Ok(())
    }

    /// Wait for the worker to connect its control stream.
    pub async fn wait_connected(&self) -> anyhow::Result<()> {
        self.channels.wait_connected().await
    }

    /// Start dispatcher tasks and cache the pipeline coders/components used to
    /// build a [`StageExecutor`].
    pub fn prepare_pipeline(&mut self, pipeline_graph: &ExecutableGraph) {
        // Start data channel dispatcher to listen and demux incoming elements.
        self.channels.stream_elements();
        // Start control channel dispatcher to route responses to waiting futures.
        self.channels.stream_responses();
        self.pipeline_coders = Arc::new(pipeline_graph.components.coders.clone());
        self.pipeline_components = Arc::new(pipeline_graph.components.clone());
    }

    /// Build a [`StageExecutor`] from the currently prepared state.
    pub fn new_executor(&self) -> StageExecutor {
        StageExecutor::new(self.new_bundle_runtime())
    }

    pub async fn run_pipeline(&self, scheduler: &mut NodeScheduler) -> anyhow::Result<()> {
        let mut in_flight = JoinSet::new();

        loop {
            for (idx, node) in scheduler.next_nodes() {
                let runtime = self.new_bundle_runtime();
                let input_metadata = scheduler.input_edge_metadata(idx);
                let output_metadata = scheduler.output_edge_metadata(idx);

                in_flight.spawn(async move {
                    let result = if matches!(node, ExecutableNode::Splittable(_)) {
                        let mut executor = SplittableStageExecutor::new(runtime);
                        executor.execute(node, input_metadata, output_metadata).await
                    } else {
                        let mut executor = StageExecutor::new(runtime);
                        executor.execute(node, input_metadata, output_metadata).await
                    };
                    (idx, result)
                });
            }

            if in_flight.is_empty() {
                if scheduler.is_complete() {
                    break;
                }
                return Err(anyhow!(
                    "executable graph deadlocked: no ready nodes and no in-flight work"
                ));
            }

            if let Some(joined) = in_flight.join_next().await {
                let (idx, result) = joined?;
                result?;
                scheduler.mark_complete(idx);
            }
        }

        Ok(())
    }
    fn new_bundle_runtime(&self) -> BundleRuntime {
        BundleRuntime::new(
            self.channels.control(),
            self.channels.data(),
            self.store.clone(),
            self.pipeline_coders.clone(),
            self.pipeline_components.clone(),
        )
    }
}
