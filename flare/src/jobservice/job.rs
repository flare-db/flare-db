use crate::fusion::fuser::GreedyPipelineFuser;
use crate::fusion::pipeline::{PTransformNode, QueryablePipeline};
use crate::fusion::stage::CollectionConsumers;
use beam_model_rs::v1::Pipeline;
use std::collections::{BTreeSet, HashSet};
// Job is just container for tasks()
pub struct Job {
    pub job_id: String,
    pub job_graph: JobGraph,
}

impl Job {
    pub fn new(pipeline: &Pipeline) -> Self {
        Self {
            job_id: String::from("helllo"),
            job_graph: JobGraph::create(pipeline),
        }
    }
}
#[derive(Clone, Debug)]
pub struct JobGraph {}

impl JobGraph {
    pub fn create(pipeline: &Pipeline) -> Self {
        fuse_pipeline(pipeline);
        JobGraph {}
    }
}

pub fn fuse_pipeline(p: &Pipeline) {
    let comps = p.components.as_ref().unwrap();

    let fuser = GreedyPipelineFuser::with(QueryablePipeline::new(comps));

    let mut unfused_root = HashSet::<PTransformNode>::new();
    let mut root_consumers = BTreeSet::<CollectionConsumers>::new();

    for root_node in fuser.pipeline.get_root_transforms() {
        // PTransfroms that consume root's ouput
        let descendants = fuser.get_root_consumers(root_node);
        unfused_root.extend(descendants.get_unfusible().iter().cloned());
        root_consumers.extend(descendants.get_fusible().iter().cloned());
    }

    let _ = fuser.fuse_pipeline(unfused_root, root_consumers);
}
