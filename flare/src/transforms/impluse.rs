use beam_model_rs::v1::{
    ApiServiceDescriptor, Coder, Components, Environment, FunctionSpec, PCollection, PTransform,
    RemoteGrpcPort, WindowingStrategy,
};
use log::info;
use prost::Message;

use crate::engine::coders::BeamValue;
use crate::engine::executor::NewCollectionRequest;
use crate::jobservice::urns::beam_urns;
use crate::transforms::{ExecutionContext, FlareTransform};
use std::collections::{HashMap, HashSet};

/// Impulse always produces exactly one element: an empty byte[]
/// in a GlobalWindow at MIN_TIMESTAMP. Wire format is always
/// beam:coder:windowed_value:v1:
///
///   timestamp(8) | windows(iterable) | pane(1) | element(1)
#[derive(Clone)]
pub struct Impulse {
    name: String,
    id: String,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
}

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

    fn execute(&self, ctx: ExecutionContext) {
        info!("Executing impluse transfrom");
        let elements = vec![BeamValue::Bytes(encode_windowed_empty_bytes())];
        let request = NewCollectionRequest {
            pcollection_id: ctx.pcollection_id.clone(),
            elements: elements,
        };

        ctx.store.insert_new_collection(request);
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
            "Create-Values-Impulse".clone().to_string(),
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

/*Ok(Elements {
    data: vec![elements::Data {
        //instruction_id: ctx.instruction_id.to_string(),
        transform_id: ctx.transform_id.to_string(),
        data: encode_windowed_empty_bytes(),
        is_last: true,
    }],
    timers: Vec::new(),
})*/

/// beam:coder:windowed_value:v1 wire layout:
///
///   timestamp  — 8 bytes big-endian shifted by Long.MIN_VALUE
///   windows    — fixed32(1) + GlobalWindow (empty)
///   pane       — 0xD0 (first, last, on-time)
///   element    — 0x00 (empty byte[] = varint(0))
///
/// MIN_TIMESTAMP = Long.MIN_VALUE + 1 = -9223372036854775807
/// encoded_ts   = (MIN_TIMESTAMP ^ Long.MIN_VALUE) = 1
///              → [0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01]
fn encode_windowed_empty_bytes() -> Vec<u8> {
    let mut buf = Vec::with_capacity(14);

    // 1. timestamp: MIN_TIMESTAMP
    let min_timestamp: i64 = i64::MIN + 1;
    let encoded_ts = (min_timestamp ^ i64::MIN) as u64;
    buf.extend_from_slice(&encoded_ts.to_be_bytes());

    // 2. windows: iterable of 1 GlobalWindow (GlobalWindow = empty)
    buf.extend_from_slice(&1u32.to_be_bytes());

    // 3. pane: first + last + on-time = 0xD0
    buf.push(0xD0);

    // 4. element: empty byte[] nested = varint(0) = 0x00
    buf.push(0x00);

    buf
}
