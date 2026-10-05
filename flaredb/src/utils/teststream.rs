use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{Arc, Mutex},
};

use anyhow::{Error, anyhow};
use async_trait::async_trait;
use beam_model_rs::v1::{
    ApiServiceDescriptor, Coder, Components, Environment, FunctionSpec, PCollection, PTransform,
    RemoteGrpcPort, TestStreamPayload, WindowingStrategy, test_stream_payload::event,
};
use log::{info, warn};
use prost::Message;

use crate::{
    coders::{
        StandardBeamCoders,
        primitives::{BeamWindow, PaneInfo, WindowedValue},
    },
    jobservice::urns::beam_urns,
    store::record::BeamRecord,
    transforms::{ExecutionContext, FlareTransform, SourceProgress},
};

/// Beam's `TestStream` — a source whose elements and time advances are scripted by
/// the test rather than read from real data.
///
/// Beam defines three events ([`TestStreamPayload`]): add elements (with event
/// timestamps), advance the watermark to a time, and advance processing time by a
/// duration. Playing these in order, letting the pipeline settle after each, makes
/// every time-driven behavior — window readiness, event-time and processing-time
/// timers, triggers, holds — deterministic and repeatable.
///
/// ## How it runs here
///
/// Like [`Impulse`](super::impluse::Impulse), a `TestStream` is a runner *source*
/// (a root node with no inputs). Unlike `Impulse`, it does not finish after one
/// bundle: each `execute` plays exactly **one** scripted event and then reports
/// progress back to the dispatcher through [`ExecutionContext::source_reports`].
/// The dispatcher applies the reported watermark / processing time and re-arms the
/// source for the next event, until the script is exhausted (then it reports
/// `done` and the source is finished, i.e. its output watermark becomes `+∞`).
///
/// Element events are decoded with the output PCollection's element coder (see
/// [`FlareTransform::needs_output_coder`]) and stored as ordinary
/// [`WindowedValue`]s, so downstream stages cannot tell the difference between a
/// `TestStream` and a real source.
///
/// The parsed script lives in shared state ([`Arc<Mutex<_>>`]) that survives the
/// node's re-runs; the node holds one instance for the whole job.
pub struct TestStream {
    name: String,
    id: String,
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
    state: Arc<Mutex<TestStreamState>>,
}

/// A single scripted event, in pipeline order.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ScriptedEvent {
    /// Emit one element at `timestamp`, with its payload still encoded by the
    /// output PCollection's element coder.
    Element { timestamp: i64, encoded: Vec<u8> },
    /// Advance the watermark to this absolute time (epoch millis).
    Watermark(i64),
    /// Advance the processing-time clock by this duration (millis, cumulative).
    ProcessingTime(i64),
}

/// Mutable state that survives across the node's re-runs.
struct TestStreamState {
    events: VecDeque<ScriptedEvent>,
    /// Cumulative processing-time cursor started at 0, so a run is deterministic
    /// regardless of the wall clock.
    processing_time: i64,
}

#[async_trait]
impl FlareTransform for TestStream {
    fn urn() -> &'static str
    where
        Self: Sized,
    {
        "beam:transform:teststream:v1"
    }

    fn with(
        id: String,
        inputs: HashMap<String, String>,
        outputs: HashMap<String, String>,
        name: String,
    ) -> Self {
        Self {
            name,
            id,
            inputs,
            outputs,
            state: Arc::new(Mutex::new(TestStreamState {
                events: VecDeque::new(),
                processing_time: 0,
            })),
        }
    }

    async fn execute(&self, ctx: ExecutionContext) -> Result<(), Error> {
        let event = {
            let mut state = self
                .state
                .lock()
                .expect("teststream state lock not poisoned");
            state.events.pop_front()
        };

        let mut watermark = None;
        let mut processing_time = None;

        match event {
            None => {
                // No events left: finish so downstream watermarks reach +∞.
                info!(
                    "TestStream '{}': no events left; reporting finished",
                    self.name
                );
                self.report(&ctx, None, None, true);
                return Ok(());
            }
            Some(ScriptedEvent::Watermark(new_watermark)) => {
                info!(
                    "TestStream '{}': advancing watermark to {}",
                    self.name, new_watermark
                );
                watermark = Some(new_watermark);
            }
            Some(ScriptedEvent::ProcessingTime(advance)) => {
                let now = {
                    let mut state = self
                        .state
                        .lock()
                        .expect("teststream state lock not poisoned");
                    state.processing_time += advance;
                    state.processing_time
                };
                info!(
                    "TestStream '{}': advancing processing time to {}",
                    self.name, now
                );
                processing_time = Some(now);
            }
            Some(ScriptedEvent::Element { timestamp, encoded }) => {
                let coder = ctx.output_coder.as_ref().ok_or_else(|| {
                    anyhow!("TestStream '{}': output coder was not resolved", self.name)
                })?;
                let value = self.decode_element(coder, &encoded)?;
                let windowed = WindowedValue {
                    value,
                    timestamp_millis: timestamp,
                    // A source emits in the global window; window assignment is a
                    // downstream transform.
                    windows: vec![BeamWindow::Global],
                    pane: PaneInfo::no_firing(),
                };
                // Increments the commit min event-time, so consumers' input
                // watermarks are clamped by this element until they read it.
                ctx.store
                    .write_windowed_value_batch(&ctx.output_pcollection_id, vec![windowed])
                    .await?;
                info!(
                    "TestStream '{}': emitted element at {}",
                    self.name, timestamp
                );
            }
        }

        let done = {
            let state = self
                .state
                .lock()
                .expect("teststream state lock not poisoned");
            state.events.is_empty()
        };
        self.report(&ctx, watermark, processing_time, done);
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
                ..Default::default()
            },
        );
        windowing
    }

    fn coders(&self) -> HashMap<String, Coder> {
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
                ..Default::default()
            },
        );
        environments
    }

    fn transfrom_spec(&self) -> HashMap<String, PTransform> {
        // A source boundary (`beam:runner:source:v1`), like `Impulse`. The runner
        // never actually reads from the harness; this exists so bundle
        // registration has a well-formed source descriptor.
        let payload = RemoteGrpcPort {
            api_service_descriptor: Some(ApiServiceDescriptor {
                url: crate::DEFAULT_API_SERVICE_URL.to_string(),
                authentication: None,
            }),
            coder_id: "windowed_value_coder_id".to_string(),
        };

        let mut transforms = HashMap::<String, PTransform>::new();
        transforms.insert(
            self.name.clone(),
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

    fn id(&self) -> String {
        self.id.clone()
    }

    fn needs_output_coder(&self) -> bool {
        true
    }

    fn source_auto_finishes(&self) -> bool {
        // The source reports completion itself once its events are exhausted.
        false
    }
}

impl TestStream {
    /// Parse the serialized [`TestStreamPayload`] into the scripted events.
    ///
    /// A missing/empty payload or a decode error leaves the script empty, which
    /// makes the source finish immediately — the same as a `TestStream` with no
    /// events, and safer than crashing the whole job on a malformed payload.
    pub fn set_script(&self, payload: &[u8]) {
        if payload.is_empty() {
            return;
        }
        let parsed = match TestStreamPayload::decode(payload) {
            Ok(parsed) => parsed,
            Err(err) => {
                warn!(
                    "TestStream '{}': failed to decode payload: {err}",
                    self.name
                );
                return;
            }
        };

        let events = parse_events(&parsed);
        let mut state = self
            .state
            .lock()
            .expect("teststream state lock not poisoned");
        state.events = events.into();
        info!(
            "TestStream '{}': loaded {} scripted event(s)",
            self.name,
            state.events.len()
        );
    }

    /// Decode one scripted element with the output PCollection's element coder.
    ///
    /// The payload's elements are encoded in Beam's **non-nested** element context
    /// (the coder's value is the last field), so this uses
    /// [`StandardBeamCoders::decode_element`] rather than the nested decode used for
    /// `WindowedValue` payloads.
    fn decode_element(
        &self,
        coder: &StandardBeamCoders,
        encoded: &[u8],
    ) -> Result<BeamRecord, Error> {
        let mut buf = encoded;
        coder
            .decode_element(&mut buf)
            .map_err(|err| anyhow!("TestStream '{}': element decode failed: {err}", self.name))
    }

    fn report(
        &self,
        ctx: &ExecutionContext,
        watermark: Option<i64>,
        processing_time: Option<i64>,
        done: bool,
    ) {
        let Some(sink) = &ctx.source_reports else {
            warn!(
                "TestStream '{}': no source report sink; progress will be lost",
                self.name
            );
            return;
        };
        sink.report(SourceProgress {
            stage_id: ctx.stage_id.clone(),
            watermark,
            processing_time,
            done,
        });
    }
}

/// Flatten the proto payload's events into per-element / per-advance script steps.
fn parse_events(payload: &TestStreamPayload) -> Vec<ScriptedEvent> {
    let mut events = Vec::new();
    for event in &payload.events {
        match &event.event {
            Some(event::Event::ElementEvent(add)) => {
                for element in &add.elements {
                    events.push(ScriptedEvent::Element {
                        timestamp: element.timestamp,
                        encoded: element.encoded_element.clone(),
                    });
                }
            }
            Some(event::Event::WatermarkEvent(watermark)) => {
                events.push(ScriptedEvent::Watermark(watermark.new_watermark));
            }
            Some(event::Event::ProcessingTimeEvent(advance)) => {
                events.push(ScriptedEvent::ProcessingTime(advance.advance_duration));
            }
            None => {}
        }
    }
    events
}

#[cfg(test)]
mod tests {
    use super::*;
    use beam_model_rs::v1::test_stream_payload::{Event, TimestampedElement, event as ts_event};

    fn kv_coder(coders: &mut HashMap<String, Coder>) -> String {
        // A string-keyed KV coder: KV<Utf8, Utf8>.
        let key_id = "key".to_string();
        let value_id = "value".to_string();
        coders.insert(
            key_id.clone(),
            Coder {
                spec: Some(FunctionSpec {
                    urn: beam_urns::STRING_UTF8_CODER.to_string(),
                    payload: vec![],
                }),
                component_coder_ids: Vec::new(),
            },
        );
        coders.insert(
            value_id.clone(),
            Coder {
                spec: Some(FunctionSpec {
                    urn: beam_urns::STRING_UTF8_CODER.to_string(),
                    payload: vec![],
                }),
                component_coder_ids: Vec::new(),
            },
        );
        let kv_id = "kv".to_string();
        coders.insert(
            kv_id.clone(),
            Coder {
                spec: Some(FunctionSpec {
                    urn: beam_urns::KV_CODER.to_string(),
                    payload: vec![],
                }),
                component_coder_ids: vec![key_id, value_id],
            },
        );
        kv_id
    }

    /// Encode `KV<String, String>` the way the SDK encodes a `TestStream` element:
    /// non-nested, so the key is length-prefixed and the value is raw (it is the
    /// last field). Mirrors `standard_coders.yaml`'s `kv:v1` `nested: false` case.
    fn encode_kv(key: &str, value: &str) -> Vec<u8> {
        assert!(key.len() < 128, "test key must fit a one-byte varint");
        let mut buf = Vec::new();
        buf.push(key.len() as u8);
        buf.extend_from_slice(key.as_bytes());
        buf.extend_from_slice(value.as_bytes());
        buf
    }

    fn payload_with_event(event: ts_event::Event) -> Vec<u8> {
        TestStreamPayload {
            coder_id: "kv".to_string(),
            events: vec![Event { event: Some(event) }],
            endpoint: None,
        }
        .encode_to_vec()
    }

    #[test]
    fn parses_element_watermark_and_processing_time_in_order() {
        let payload = TestStreamPayload {
            coder_id: "kv".to_string(),
            events: vec![
                Event {
                    event: Some(ts_event::Event::ElementEvent(ts_event::AddElements {
                        elements: vec![TimestampedElement {
                            encoded_element: vec![1],
                            timestamp: 100,
                        }],
                        tag: String::new(),
                    })),
                },
                Event {
                    event: Some(ts_event::Event::WatermarkEvent(
                        ts_event::AdvanceWatermark {
                            new_watermark: 200,
                            tag: String::new(),
                        },
                    )),
                },
                Event {
                    event: Some(ts_event::Event::ProcessingTimeEvent(
                        ts_event::AdvanceProcessingTime {
                            advance_duration: 50,
                        },
                    )),
                },
                Event {
                    event: Some(ts_event::Event::ElementEvent(ts_event::AddElements {
                        elements: vec![TimestampedElement {
                            encoded_element: vec![2],
                            timestamp: 150,
                        }],
                        tag: String::new(),
                    })),
                },
            ],
            endpoint: None,
        };
        let stream = TestStream::with(
            "id".to_string(),
            HashMap::new(),
            HashMap::new(),
            "TestStream".to_string(),
        );
        stream.set_script(&payload.encode_to_vec());
        let state = stream.state.lock().unwrap();
        assert_eq!(
            state.events.iter().cloned().collect::<Vec<_>>(),
            vec![
                ScriptedEvent::Element {
                    timestamp: 100,
                    encoded: vec![1]
                },
                ScriptedEvent::Watermark(200),
                ScriptedEvent::ProcessingTime(50),
                ScriptedEvent::Element {
                    timestamp: 150,
                    encoded: vec![2]
                },
            ]
        );
    }

    #[test]
    fn empty_or_malformed_payload_leaves_an_empty_script() {
        let stream = TestStream::with(
            "id".to_string(),
            HashMap::new(),
            HashMap::new(),
            "TestStream".to_string(),
        );
        stream.set_script(&[]);
        stream.set_script(&[0xff, 0xff, 0xff]);
        assert!(stream.state.lock().unwrap().events.is_empty());
    }

    #[test]
    fn encoded_element_decodes_with_the_output_coder() {
        let encoded = encode_kv("k", "v");
        assert!(!encoded.is_empty());

        let mut coders = HashMap::new();
        let id = kv_coder(&mut coders);
        let coder = StandardBeamCoders::from_urn(&id, None, Some(&coders));

        let stream = TestStream::with(
            "id".to_string(),
            HashMap::new(),
            HashMap::new(),
            "TestStream".to_string(),
        );
        let decoded = stream.decode_element(&coder, &encoded).unwrap();
        match decoded {
            BeamRecord::KV(kv) => {
                assert_eq!(
                    kv.key,
                    crate::store::record::PrimitiveValue::String("k".to_string())
                );
                assert_eq!(
                    *kv.value,
                    BeamRecord::PRIMITIVE(crate::store::record::PrimitiveValue::String(
                        "v".to_string()
                    ))
                );
            }
            other => panic!("expected KV, got {other:?}"),
        }
    }

    #[test]
    fn from_urn_registers_test_stream() {
        use crate::transforms::from_urn_with_payload;
        let payload = payload_with_event(ts_event::Event::WatermarkEvent(
            ts_event::AdvanceWatermark {
                new_watermark: 42,
                tag: String::new(),
            },
        ));
        let transform = from_urn_with_payload(
            beam_urns::TEST_STREAM_TRANSFORM,
            "TestStream".to_string(),
            HashMap::new(),
            HashMap::new(),
            &payload,
        );
        assert_eq!(transform.unique_name(), "TestStream");
        assert!(!transform.source_auto_finishes());
        assert!(transform.needs_output_coder());
    }
}
