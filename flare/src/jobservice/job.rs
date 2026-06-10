use crate::errors::BeamTranslationError;
use crate::fusion::fuser::GreedyPipelineFuser;
use crate::fusion::pipeline::{ExecutableGraph, FusedPipeline, PTransformNode, QueryablePipeline};
use crate::fusion::stage::CollectionConsumers;
use beam_model_rs::v1::Pipeline;
use dashmap::DashMap;
use log::{info, warn};
use std::collections::{BTreeSet, HashSet};
use std::fs;
use std::sync::Arc;
use uuid::Uuid;
// Job is just container for tasks()
pub struct Job {
    pub job_id: String,
    pub graph: ExecutableGraph,
}

impl Job {
    pub fn new(pipeline: &Pipeline) -> Self {
        Self {
            job_id: Uuid::new_v4().to_string(),
            graph: Self::create_job(pipeline),
        }
    }

    fn create_job(pipeline: &Pipeline) -> ExecutableGraph {
        info!("Creating a new job");
        if let Err(error) = fs::write("pipeline_proto_debug.txt", format!("{pipeline:#?}")) {
            warn!("Failed to write formatted pipeline proto debug file: {error}");
        }
        let fused_pipeline = fuse_pipeline(pipeline).unwrap();
        let executable_graph = ExecutableGraph::from(
            fused_pipeline.sdk_stages().clone(),
            fused_pipeline.runner_stages().clone(),
            pipeline.components.clone().unwrap(),
        );
        info!("Built executable graph");
        executable_graph
    }

    pub fn jobid(&self) {}
}

#[derive(Clone)]
pub struct JobStore {
    jobs: Arc<DashMap<String, Arc<ExecutableGraph>>>, // store Arc so reads don't require a guard
}

impl JobStore {
    pub fn new() -> Self {
        Self {
            jobs: Arc::new(DashMap::new()),
        }
    }

    pub fn add_job(&self, id: &String, graph: ExecutableGraph) {
        self.jobs.insert(id.clone(), Arc::new(graph));
    }

    pub fn get_job(&self, id: &String) -> Option<Arc<ExecutableGraph>> {
        self.jobs.get(id).map(|entry| Arc::clone(entry.value()))
    }
    pub fn first_job_id(&self) -> Option<String> {
        self.jobs.iter().next().map(|entry| entry.key().clone())
    }
}

pub fn fuse_pipeline(p: &Pipeline) -> Result<FusedPipeline, BeamTranslationError> {
    let comps = p.components.as_ref().unwrap();

    let fuser = GreedyPipelineFuser::with(QueryablePipeline::new(comps));

    let mut unfused_root = HashSet::<PTransformNode>::new();
    let mut root_consumers = BTreeSet::<CollectionConsumers>::new();

    for root_node in fuser.pipeline.get_root_transforms() {
        if fuser
            .pipeline
            .get_environment(&root_node.transform)
            .is_none()
        {
            unfused_root.insert(root_node.clone());
        }

        // PTransfroms that consume root's ouput
        let descendants = fuser.get_root_consumers(root_node);
        unfused_root.extend(descendants.get_unfusible().iter().cloned());
        root_consumers.extend(descendants.get_fusible().iter().cloned());
    }

    let fused_pipeline = fuser.fuse_pipeline(unfused_root, root_consumers);
    info!("Created fused pipeline");
    return fused_pipeline;
}
