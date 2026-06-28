use anyhow::Error;
use async_trait::async_trait;
use beam_model_rs::v1::{
    ApiServiceDescriptor, Coder, Components, Environment, FunctionSpec, PCollection, PTransform,
    RemoteGrpcPort, WindowingStrategy,
};
use log::info;
use prost::Message;

use crate::engine::store::{BeamRecord, NewCollectionRequest, PrimitiveValue};
use crate::jobservice::urns::beam_urns;
use crate::transforms::{ExecutionContext, FlareTransform};
use std::collections::{HashMap, HashSet};

/// Impulse produces exactly one logical element: an empty byte[].
/// The Fn Data boundary is responsible for wrapping this logical value in
/// a WindowedValue before sending it to an SDK harness.
#[derive(Clone)]
pub struct Impulse {
    name: String,
    id: String,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
}

#[async_trait]
impl FlareTransform for Impulse {
    // type Context = ImpluseContext;

    fn urn() -> &'static str
    where
        Self: Sized,
    {
        "beam:transform:impulse:v1"
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
        info!("Executing impluse transfrom");
        let elements = vec![BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(Vec::new()))];
        let request = NewCollectionRequest {
            pcollection_id: ctx.output_pcollection_id.clone(),
            elements: elements,
        };

        info!("New Collection request {:?}", request);
        ctx.store.write_collection(request).await?;
        Ok(())
    }

    fn output_pcol_ids(&self) -> HashSet<String> {
        self.outputs.clone().into_values().collect()
    }

    fn unique_name(&self) -> String {
        self.name.clone()
    }

    fn windowing_strategies(&self) -> HashMap<String, WindowingStrategy> {
        let mut windowing = HashMap::new();
        windowing.insert(
            "window/global".to_string(),
            WindowingStrategy {
                ..Default::default() // global window
            },
        );
        windowing.clone()
    }

    fn coders(&self) -> HashMap<String, beam_model_rs::v1::Coder> {
        let mut coders = HashMap::new();
        coders.insert(
            "coder/bytes".to_string(),
            Coder {
                spec: Some(FunctionSpec {
                    urn: beam_urns::BYTES_CODER.to_string(),
                    payload: vec![],
                }),
                component_coder_ids: Vec::new(),
            },
        );
        coders
    }

    fn environments(&self) -> HashMap<String, Environment> {
        let mut environments = HashMap::new();
        environments.insert(
            "env/java/process".to_string(),
            Environment {
                // Empty payload = in‑process harness.
                ..Default::default()
            },
        );
        environments
    }

    fn transfrom_spec(&self) -> HashMap<String, PTransform> {
        // Source transform (`beam:runner:source:v1`).
        // Its payload is a serialized `RemoteGrpcPort` that tells the
        // harness which coder to use for the inbound elements.
        let payload = RemoteGrpcPort {
            api_service_descriptor: Some(ApiServiceDescriptor {
                url: "127.0.0.1:8099".to_string(),
                authentication: None,
            }),
            coder_id: "windowed_value_coder_id".to_string(),
        };
        //grpc_port.coder_id = "coder/bytes".to_string();

        let mut transforms = HashMap::<String, PTransform>::new();
        transforms.insert(
            "Create-Values-Impulse".to_string(),
            PTransform {
                spec: Some(FunctionSpec {
                    urn: beam_urns::BEAM_SOURCE.to_string(),
                    payload: payload.encode_to_vec(),
                }),
                inputs: HashMap::new(),
                outputs: self.outputs.clone(),
                environment_id: "process".to_string(),
                unique_name: self.name.clone(),
                subtransforms: Vec::new(),
                display_data: Vec::new(),
                annotations: HashMap::new(),
            },
        );

        transforms
    }
    fn pcollections(&self, components: &Components) -> HashMap<String, PCollection> {
        self.outputs
            .iter()
            .filter_map(|(name, id)| {
                components
                    .pcollections
                    .get(id)
                    .cloned()
                    .map(|pcollection| (name.clone(), pcollection))
            })
            .collect()
    }
    // transfrom spec
    // pcollections - get from edge metadata
    // windowing - default window
    // coder - get from edge meta
    // env - defalut

    fn id(&self) -> String {
        self.id.to_string().clone()
    }
}
