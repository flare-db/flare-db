use anyhow::anyhow;
use beam_model_rs::v1::{ApiServiceDescriptor, Elements, ProcessBundleDescriptor};
use log::info;

use crate::{
    engine::harness::{
        control::{ControlChannel, ControlResponse},
        data::DataChannel,
    },
    fusion::{pipeline::FusedPipeline, stage::ExecutableStage},
};

#[derive(Clone)]
pub struct StageExecutor {
    control: ControlChannel,
    data: DataChannel,
}

impl StageExecutor {
    pub fn new(control: ControlChannel, data: DataChannel) -> Self {
        Self { control, data }
    }

    pub async fn wait_connected(&self) -> anyhow::Result<()> {
        self.control.wait_connected().await?;
        Ok(())
    }

    pub async fn execute_pipeline(pipeline: FusedPipeline) {}

    pub async fn execute(&mut self, stage: &ExecutableStage, id: &String) -> anyhow::Result<()> {
        info!("Starting to execute stage");
        let endpoint = ApiServiceDescriptor {
            url: "127.0.0.1:8099".to_string(),
            ..Default::default()
        };

        let descriptor = ProcessBundleDescriptor {
            id: id.clone(),
            transforms: stage.ptmap(),
            pcollections: stage.components().pcollections.clone(),
            windowing_strategies: stage.components().windowing_strategies.clone(),
            coders: stage.components().coders.clone(),
            environments: stage.components().environments.clone(),
            state_api_service_descriptor: Some(endpoint.clone()),
            timer_api_service_descriptor: Some(endpoint),
        };

        let response = self.control.register_bundle(descriptor).await;
        info!("Sent register bundle request");
        match response {
            Ok(r) => {
                if r == ControlResponse::BundleRegistered {
                    info!("Bundle registered at worker");
                    let bundle_id = self.control.process_bundle(&id).await?;

                    info!("Process bundle id {}", bundle_id);

                    let _bundle_res = self
                        .control
                        .recv_process_bundle_response(&bundle_id)
                        .await?;
                    info!("Process bundle completed");

                    let elements = self.data.stream_elements().await;
                    info!("Streamed elemnts from harness");
                    Self::log_data(&elements);
                }
            }
            Err(err) => {
                return Err(anyhow!("Error while processing bundle {}", err));
            }
        }

        Ok(())
    }
    pub fn log_data(elements: &[Elements]) {
        info!("Logging elements: total_messages={}", elements.len());
        for (idx, msg) in elements.iter().enumerate() {
            info!(
                "elements message {}: data_entries={}, timer_entries={}",
                idx,
                msg.data.len(),
                msg.timers.len()
            );
            for data in &msg.data {
                let decoded = Self::decode_strings(&data.data);
                log::info!(
                    "[data] instruction={} transform={} is_last={} elements={:?}",
                    data.instruction_id,
                    data.transform_id,
                    data.is_last,
                    decoded,
                );
            }
            for timer in &msg.timers {
                info!(
                    "[timer] instruction={} transform={} timer_family_id={} is_last={}",
                    timer.instruction_id, timer.transform_id, timer.timer_family_id, timer.is_last
                );
            }
        }
    }
    fn decode_strings(raw: &[u8]) -> Vec<String> {
        info!("decoing strings");
        use std::io::{Cursor, Read};

        let mut cursor = Cursor::new(raw);
        let mut out = Vec::new();

        while cursor.position() < raw.len() as u64 {
            let mut len_buf = [0u8; 4];
            if cursor.read_exact(&mut len_buf).is_err() {
                break;
            }
            let len = u32::from_be_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            if cursor.read_exact(&mut buf).is_err() {
                break;
            }
            match String::from_utf8(buf) {
                Ok(s) => out.push(s),
                Err(_) => out.push("<invalid utf8>".to_string()),
            }
        }

        out
    }
}

// TODO
// let execute() handle the instruction
// persist the bundle id and instruction
// and add data fn in executor to just listen to data that harness is sending
// store it inmemory in hashmap of bundleid and elmeennts
// once all elemeents are scived start next stage and pass the stored elements as input to it.
