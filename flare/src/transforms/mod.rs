use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use async_trait::async_trait;
use beam_model_rs::v1::{
    Coder, Components, Environment, PCollection, PTransform, WindowingStrategy,
};
use uuid::Uuid;

use crate::{
    engine::store::FlareElementStore,
    jobservice::urns::beam_urns,
    transforms::{gbk::GroupByKey, impluse::Impulse},
};

pub mod gbk;
pub mod impluse;

#[async_trait]
pub trait FlareTransform {
    fn urn() -> &'static str
    where
        Self: Sized;

    fn id(&self) -> String;

    fn with(
        id: String,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        name: String,
    ) -> Self
    where
        Self: Sized;

    async fn execute(&self, ctx: ExecutionContext) -> Result<(), anyhow::Error>;
    //-> Result<Elements, TransformError>;

    fn output_pcol_ids(&self) -> HashSet<String>;

    fn unique_name(&self) -> String;

    fn windowing_strategies(&self) -> HashMap<String, WindowingStrategy>;

    fn coders(&self) -> HashMap<String, Coder>;

    fn environments(&self) -> HashMap<String, Environment>;

    fn transfrom_spec(&self) -> HashMap<String, PTransform>;

    fn pcollections(&self, components: &Components) -> HashMap<String, PCollection>;

    // TODO: add methods needed to build the ProcessBundleDescriptor object.
}

// Idea: TransformConfig as input to with as we start adding more parameters

/*pub struct TransformConfig {
    pub id: String,
    pub name: String,
    pub inputs: HashMap<String, String>,
    pub outputs: HashMap<String, String>,
    pub display_name: Option<String>,
    pub environment_id: Option<String>,
    pub side_inputs: HashMap<String, String>,
} */

pub struct ExecutionContext {
    //pub instruction_id: String,
    ///pub transform_id: String,
    pub store: Arc<FlareElementStore>,
    pub input_pcollection_id: String,
    pub output_pcollection_id: String,
    pub consumer_transfrom_id: String, //pub coder: String,
}
pub type FlareRunnerTransform = Arc<dyn FlareTransform + Send + Sync>;

pub fn from_urn(
    urn: &str,
    name: String,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
) -> FlareRunnerTransform {
    match urn {
        beam_urns::IMPULSE_TRANSFORM => Arc::new(Impulse::with(
            Uuid::new_v4().to_string(),
            inputs,
            outputs,
            name,
        )) as FlareRunnerTransform,

        beam_urns::GROUP_BY_KEY_TRANSFORM => Arc::new(GroupByKey::with(
            Uuid::new_v4().to_string(),
            inputs,
            outputs,
            name,
        )) as FlareRunnerTransform,
        _ => panic!("Unknown URN {}", urn),
    }
}
