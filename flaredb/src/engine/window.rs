//! SDK-backed support for merging custom Beam windows.

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use anyhow::{Context, anyhow, ensure};
use beam_model_rs::v1::{
    ApiServiceDescriptor, Coder, Components, Elements, FunctionSpec, PCollection, PTransform,
    ProcessBundleDescriptor, RemoteGrpcPort, Trigger, WindowingStrategy, accumulation_mode,
    closing_behavior, elements, merge_status, on_time_behavior, output_time, trigger,
};
use bytes::{Buf, BufMut, BytesMut};
use prost::Message;
use tokio::sync::Mutex;

use crate::{
    coders::primitives::{
        BEAM_MIN_TIMESTAMP_MILLIS, BeamWindow, PaneInfo, WindowCoder, decode_varint,
        decode_window_from, decode_windowed_value_header, encode_varint,
        encode_windowed_value_header,
    },
    engine::harness::{
        control::{ControlChannel, ControlResponse},
        data::{DataChannel, DataKey, ElementStreamPayload},
    },
    jobservice::urns::beam_urns,
};

const MERGE_CACHE_MAX_ENTRIES: usize = 1024;

/// One merge callback result from the SDK's `MergeWindows` transform.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MergedWindows {
    pub merged: BeamWindow,
    pub originals: Vec<BeamWindow>,
}

#[derive(Debug)]
struct MergeBundle {
    descriptor_id: String,
    source_transform_id: String,
    sink_transform_id: String,
    window_coder: WindowCoder,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MergeCacheKey {
    strategy_id: String,
    windows: Vec<String>,
}

/// Client for Beam's synthetic SDK `MergeWindows` protocol.
///
/// A merging window function is SDK-owned code. The runner cannot merge custom
/// windows natively, so for each distinct window set it registers a small
/// source → `beam:transform:merge_windows:v1` → sink bundle with the SDK
/// harness and asks the original window function to perform the merge.
pub struct WindowMergeService {
    control: ControlChannel,
    data: DataChannel,
    coders: Arc<HashMap<String, Coder>>,
    components: Arc<Components>,
    endpoint: ApiServiceDescriptor,
    bundles: Mutex<HashMap<String, Arc<MergeBundle>>>,
    cache: Mutex<HashMap<MergeCacheKey, Arc<Vec<MergedWindows>>>>,
}

impl WindowMergeService {
    pub(crate) fn new(
        control: ControlChannel,
        data: DataChannel,
        coders: Arc<HashMap<String, Coder>>,
        components: Arc<Components>,
        endpoint: ApiServiceDescriptor,
    ) -> Self {
        Self {
            control,
            data,
            coders,
            components,
            endpoint,
            bundles: Mutex::new(HashMap::new()),
            cache: Mutex::new(HashMap::new()),
        }
    }

    /// Merge several independent window sets for one PCollection in a single SDK
    /// bundle.
    ///
    /// Each request is a separate `KV<nonce, Iterable<Window>>` element, so the
    /// SDK still merges windows independently per key. Batching only amortizes
    /// the Fn API control/data round trips.
    pub(crate) async fn merge_windows_batch_for_pcollection(
        &self,
        pcollection_id: &str,
        requests: &[Vec<BeamWindow>],
    ) -> anyhow::Result<Vec<Vec<MergedWindows>>> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        let pcollection = self
            .components
            .pcollections
            .get(pcollection_id)
            .ok_or_else(|| anyhow!("PCollection not found: {pcollection_id}"))?;
        let strategy_id = &pcollection.windowing_strategy_id;
        let strategy = self
            .components
            .windowing_strategies
            .get(strategy_id)
            .ok_or_else(|| {
                anyhow!(
                    "windowing strategy not found for PCollection {pcollection_id}: {strategy_id}"
                )
            })?;
        ensure!(
            strategy.merge_status == merge_status::Enum::NeedsMerge as i32,
            "GroupByKey requested merging for PCollection {pcollection_id}, but windowing strategy {strategy_id} has merge status {}",
            strategy.merge_status
        );

        let distinct: Vec<Vec<BeamWindow>> = requests
            .iter()
            .map(|windows| distinct_windows(windows))
            .collect();
        let cache_keys: Vec<MergeCacheKey> = distinct
            .iter()
            .map(|windows| {
                let mut canonical: Vec<String> =
                    windows.iter().map(BeamWindow::canonical_key).collect();
                canonical.sort();
                MergeCacheKey {
                    strategy_id: strategy_id.to_string(),
                    windows: canonical,
                }
            })
            .collect();

        let mut results: Vec<Option<Vec<MergedWindows>>> = vec![None; distinct.len()];
        let mut misses = Vec::new();
        {
            let cache = self.cache.lock().await;
            for (index, cache_key) in cache_keys.iter().enumerate() {
                if let Some(cached) = cache.get(cache_key) {
                    results[index] = Some(cached.as_ref().clone());
                } else {
                    misses.push((index, distinct[index].clone()));
                }
            }
        }

        let bundle = self.prepare_bundle(strategy_id).await?;
        if !misses.is_empty() {
            let inputs: Vec<Vec<BeamWindow>> =
                misses.iter().map(|(_, windows)| windows.clone()).collect();
            let merged = self.invoke_batch(&bundle, &inputs).await?;
            let mut cache = self.cache.lock().await;
            for ((index, _), merged) in misses.into_iter().zip(merged) {
                insert_merge_cache(
                    &mut cache,
                    cache_keys[index].clone(),
                    Arc::new(merged.clone()),
                );
                results[index] = Some(merged);
            }
        }

        results
            .into_iter()
            .map(|result| result.ok_or_else(|| anyhow!("missing MergeWindows batch result")))
            .collect()
    }

    async fn prepare_bundle(&self, strategy_id: &str) -> anyhow::Result<Arc<MergeBundle>> {
        let mut bundles = self.bundles.lock().await;
        if let Some(bundle) = bundles.get(strategy_id) {
            return Ok(bundle.clone());
        }

        let strategy = self
            .components
            .windowing_strategies
            .get(strategy_id)
            .cloned()
            .ok_or_else(|| anyhow!("windowing strategy not found: {strategy_id}"))?;
        let (descriptor, bundle) = build_merge_descriptor(
            strategy_id,
            &strategy,
            self.coders.as_ref(),
            self.components.as_ref(),
            self.endpoint.clone(),
        )?;

        let mut control = self.control.clone();
        let response = control.register_bundle(descriptor).await?;
        ensure!(
            matches!(response, ControlResponse::BundleRegistered),
            "unexpected MergeWindows register response for strategy {strategy_id}"
        );

        let bundle = Arc::new(bundle);
        bundles.insert(strategy_id.to_string(), bundle.clone());
        Ok(bundle)
    }

    async fn invoke_batch(
        &self,
        bundle: &MergeBundle,
        requests: &[Vec<BeamWindow>],
    ) -> anyhow::Result<Vec<Vec<MergedWindows>>> {
        let requests: Vec<(Vec<u8>, Vec<BeamWindow>)> = requests
            .iter()
            .map(|windows| (uuid::Uuid::new_v4().as_bytes().to_vec(), windows.clone()))
            .collect();
        let mut encoded_input = Vec::new();
        for (nonce, windows) in &requests {
            encode_merge_input(&mut encoded_input, nonce, windows, &bundle.window_coder);
        }

        let output_key = DataKey {
            instruction_id: String::new(),
            transform_id: bundle.sink_transform_id.clone(),
        };
        // The instruction id is only known after the process request is sent.
        // Create the process request first, then obtain the receiver under the
        // same key and send the input.
        let mut control = self.control.clone();
        let (instruction_id, response_rx) = control
            .send_process_bundle_request(&bundle.descriptor_id)
            .await?;
        let output_key = DataKey {
            instruction_id: instruction_id.clone(),
            ..output_key
        };
        let receiver = self.data.get_receiver(output_key);

        self.data
            .send_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: instruction_id.clone(),
                    transform_id: bundle.source_transform_id.clone(),
                    data: encoded_input,
                    is_last: false,
                }],
                timers: Vec::new(),
            })
            .await?;
        self.data
            .send_elements(Elements {
                data: vec![elements::Data {
                    instruction_id: instruction_id.clone(),
                    transform_id: bundle.source_transform_id.clone(),
                    data: Vec::new(),
                    is_last: true,
                }],
                timers: Vec::new(),
            })
            .await?;

        let control = self.control.clone();
        let response = control.recv_process_bundle_response(&instruction_id, response_rx);
        let exchange = async {
            tokio::pin!(response);
            let mut control_response = None;
            let mut output = BytesMut::new();
            let mut output_complete = false;

            while control_response.is_none() || !output_complete {
                tokio::select! {
                    payload = async { receiver.lock().await.recv().await }, if !output_complete => {
                        match payload {
                            Some(ElementStreamPayload::Data(chunk)) => {
                                output.extend_from_slice(&chunk.data.data);
                                output_complete = chunk.data.is_last;
                            }
                            Some(ElementStreamPayload::Timers(_)) => {}
                            None if !output_complete => {
                                return Err(anyhow!(
                                    "MergeWindows output stream closed before is_last"
                                ));
                            }
                            None => output_complete = true,
                        }
                    }
                    response = &mut response, if control_response.is_none() => {
                        control_response = Some(response?);
                    }
                }
            }

            Ok((
                control_response.expect("loop exits only with a response"),
                output.freeze(),
            ))
        };

        let (response, output) = crate::engine::guard::idle_guard(
            exchange,
            crate::engine::guard::worker_idle_timeout(),
            "waiting for SDK MergeWindows output and control response",
        )
        .await?;
        ensure!(
            matches!(response, ControlResponse::ProcessBundleSuccess(_)),
            "MergeWindows bundle did not succeed"
        );

        let merged = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            decode_merge_output_batch(&output, &bundle.window_coder)
        }))
        .map_err(|_| anyhow!("malformed SDK MergeWindows output: window decoding panicked"))?
        .context("decoding SDK MergeWindows output")?;
        let mut by_nonce = HashMap::with_capacity(merged.len());
        for (nonce, merged) in merged {
            ensure!(
                by_nonce.insert(nonce.clone(), merged).is_none(),
                "SDK MergeWindows returned a duplicate nonce"
            );
        }

        let mut results = Vec::with_capacity(requests.len());
        for (nonce, windows) in requests {
            let merged = by_nonce.remove(&nonce).ok_or_else(|| {
                anyhow!("SDK MergeWindows returned no result for a batch element")
            })?;
            validate_merge_result(&windows, &merged)?;
            results.push(merged);
        }
        ensure!(
            by_nonce.is_empty(),
            "SDK MergeWindows returned results for unknown nonce(s)"
        );
        Ok(results)
    }
}

fn insert_merge_cache(
    cache: &mut HashMap<MergeCacheKey, Arc<Vec<MergedWindows>>>,
    key: MergeCacheKey,
    value: Arc<Vec<MergedWindows>>,
) {
    if cache.len() >= MERGE_CACHE_MAX_ENTRIES
        && !cache.contains_key(&key)
        && let Some(oldest) = cache.keys().next().cloned()
    {
        cache.remove(&oldest);
    }
    cache.insert(key, value);
}

fn distinct_windows(windows: &[BeamWindow]) -> Vec<BeamWindow> {
    let mut seen = HashSet::new();
    windows
        .iter()
        .filter(|window| seen.insert(window.canonical_key()))
        .cloned()
        .collect()
}

fn build_merge_descriptor(
    strategy_id: &str,
    strategy: &WindowingStrategy,
    pipeline_coders: &HashMap<String, Coder>,
    components: &Components,
    endpoint: ApiServiceDescriptor,
) -> anyhow::Result<(ProcessBundleDescriptor, MergeBundle)> {
    let window_fn = strategy
        .window_fn
        .as_ref()
        .ok_or_else(|| anyhow!("windowing strategy {strategy_id} has no window function"))?
        .clone();
    ensure!(
        pipeline_coders.contains_key(&strategy.window_coder_id),
        "windowing strategy {strategy_id} references missing window coder {}",
        strategy.window_coder_id
    );
    let window_coder = WindowCoder::try_from_coder(&strategy.window_coder_id, pipeline_coders)
        .with_context(|| {
            format!(
                "resolving window coder {} for strategy {strategy_id}",
                strategy.window_coder_id
            )
        })?;
    let descriptor_id = format!("flare-merge-windows-{}", uuid::Uuid::new_v4());
    let source_transform_id = format!("{descriptor_id}/read");
    let merge_transform_id = format!("{descriptor_id}/merge");
    let sink_transform_id = format!("{descriptor_id}/write");
    let input_pcollection_id = format!("{descriptor_id}/input");
    let output_pcollection_id = format!("{descriptor_id}/output");
    let global_strategy_id = format!("{descriptor_id}/global-windowing-strategy");

    let coder_id = |name: &str| format!("{descriptor_id}/coder/{name}");
    let bytes_coder_id = coder_id("bytes");
    let window_coder_id = strategy.window_coder_id.clone();
    let global_window_coder_id = coder_id("global-window");
    let iterable_window_coder_id = coder_id("iterable-window");
    let input_coder_id = coder_id("merge-input");
    let iterable_originals_coder_id = coder_id("iterable-originals");
    let merge_pair_coder_id = coder_id("merge-pair");
    let iterable_pairs_coder_id = coder_id("iterable-pairs");
    let output_value_coder_id = coder_id("merge-output-value");
    let output_coder_id = coder_id("merge-output");
    let windowed_input_coder_id = coder_id("windowed-input");
    let windowed_output_coder_id = coder_id("windowed-output");

    let make_coder = |urn: &str, component_ids: &[&str]| Coder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        component_coder_ids: component_ids.iter().map(|id| id.to_string()).collect(),
    };

    let mut coders = pipeline_coders.clone();
    coders.insert(
        bytes_coder_id.clone(),
        make_coder(beam_urns::BYTES_CODER, &[]),
    );
    coders.insert(
        global_window_coder_id.clone(),
        make_coder(beam_urns::GLOBAL_WINDOW_CODER, &[]),
    );
    coders.insert(
        iterable_window_coder_id.clone(),
        make_coder(beam_urns::ITERABLE_CODER, &[window_coder_id.as_str()]),
    );
    coders.insert(
        input_coder_id.clone(),
        make_coder(
            beam_urns::KV_CODER,
            &[bytes_coder_id.as_str(), iterable_window_coder_id.as_str()],
        ),
    );
    coders.insert(
        iterable_originals_coder_id.clone(),
        make_coder(beam_urns::ITERABLE_CODER, &[window_coder_id.as_str()]),
    );
    coders.insert(
        merge_pair_coder_id.clone(),
        make_coder(
            beam_urns::KV_CODER,
            &[window_coder_id.as_str(), iterable_window_coder_id.as_str()],
        ),
    );
    coders.insert(
        iterable_pairs_coder_id.clone(),
        make_coder(beam_urns::ITERABLE_CODER, &[merge_pair_coder_id.as_str()]),
    );
    coders.insert(
        output_value_coder_id.clone(),
        make_coder(
            beam_urns::KV_CODER,
            &[
                iterable_originals_coder_id.as_str(),
                iterable_pairs_coder_id.as_str(),
            ],
        ),
    );
    coders.insert(
        output_coder_id.clone(),
        make_coder(
            beam_urns::KV_CODER,
            &[bytes_coder_id.as_str(), output_value_coder_id.as_str()],
        ),
    );
    coders.insert(
        windowed_input_coder_id.clone(),
        make_coder(
            beam_urns::WINDOWED_VALUE_CODER,
            &[input_coder_id.as_str(), global_window_coder_id.as_str()],
        ),
    );
    coders.insert(
        windowed_output_coder_id.clone(),
        make_coder(
            beam_urns::WINDOWED_VALUE_CODER,
            &[output_coder_id.as_str(), global_window_coder_id.as_str()],
        ),
    );

    let global_strategy = WindowingStrategy {
        window_fn: Some(FunctionSpec {
            urn: beam_urns::GLOBAL_WINDOWS_FN.to_string(),
            payload: Vec::new(),
        }),
        merge_status: merge_status::Enum::NonMerging as i32,
        window_coder_id: global_window_coder_id.clone(),
        trigger: Some(Trigger {
            trigger: Some(trigger::Trigger::Default(trigger::Default {})),
        }),
        accumulation_mode: accumulation_mode::Enum::Discarding as i32,
        output_time: output_time::Enum::EndOfWindow as i32,
        closing_behavior: closing_behavior::Enum::EmitIfNonempty as i32,
        allowed_lateness: 0,
        on_time_behavior: on_time_behavior::Enum::FireAlways as i32,
        assigns_to_one_window: true,
        environment_id: String::new(),
    };

    let channel_payload = |coder_id: &str| {
        RemoteGrpcPort {
            api_service_descriptor: Some(endpoint.clone()),
            coder_id: coder_id.to_string(),
        }
        .encode_to_vec()
    };
    let transforms = HashMap::from([
        (
            source_transform_id.clone(),
            PTransform {
                unique_name: "MergeWindows/Read".to_string(),
                spec: Some(FunctionSpec {
                    urn: beam_urns::BEAM_SOURCE.to_string(),
                    payload: channel_payload(&windowed_input_coder_id),
                }),
                outputs: HashMap::from([("input".to_string(), input_pcollection_id.clone())]),
                ..Default::default()
            },
        ),
        (
            merge_transform_id,
            PTransform {
                unique_name: "MergeWindows/Merge".to_string(),
                spec: Some(FunctionSpec {
                    urn: beam_urns::MERGE_WINDOWS_TRANSFORM.to_string(),
                    payload: window_fn.encode_to_vec(),
                }),
                inputs: HashMap::from([("input".to_string(), input_pcollection_id.clone())]),
                outputs: HashMap::from([("output".to_string(), output_pcollection_id.clone())]),
                ..Default::default()
            },
        ),
        (
            sink_transform_id.clone(),
            PTransform {
                unique_name: "MergeWindows/Write".to_string(),
                spec: Some(FunctionSpec {
                    urn: beam_urns::BEAM_SINK.to_string(),
                    payload: channel_payload(&windowed_output_coder_id),
                }),
                inputs: HashMap::from([("output".to_string(), output_pcollection_id.clone())]),
                ..Default::default()
            },
        ),
    ]);
    let pcollections = HashMap::from([
        (
            input_pcollection_id.clone(),
            PCollection {
                unique_name: "input".to_string(),
                coder_id: input_coder_id,
                windowing_strategy_id: global_strategy_id.clone(),
                ..Default::default()
            },
        ),
        (
            output_pcollection_id,
            PCollection {
                unique_name: "output".to_string(),
                coder_id: output_coder_id,
                windowing_strategy_id: global_strategy_id.clone(),
                ..Default::default()
            },
        ),
    ]);

    let descriptor = ProcessBundleDescriptor {
        id: descriptor_id.clone(),
        transforms,
        pcollections,
        windowing_strategies: HashMap::from([(global_strategy_id, global_strategy)]),
        coders,
        environments: components.environments.clone(),
        state_api_service_descriptor: Some(endpoint.clone()),
        timer_api_service_descriptor: Some(endpoint),
    };
    let bundle = MergeBundle {
        descriptor_id,
        source_transform_id,
        sink_transform_id,
        window_coder,
    };
    Ok((descriptor, bundle))
}

fn encode_merge_input(
    buf: &mut Vec<u8>,
    nonce: &[u8],
    windows: &[BeamWindow],
    window_coder: &WindowCoder,
) {
    encode_windowed_value_header(
        BEAM_MIN_TIMESTAMP_MILLIS,
        &[BeamWindow::Global],
        &WindowCoder::Global,
        &PaneInfo::no_firing(),
        buf,
    );

    encode_varint(nonce.len() as u64, buf);
    buf.put_slice(nonce);
    buf.put_i32(
        i32::try_from(windows.len()).expect("MergeWindows input window count exceeds i32::MAX"),
    );
    for window in windows {
        window_coder.encode_window(window, buf);
    }
}

fn decode_merge_output_batch(
    bytes: &[u8],
    window_coder: &WindowCoder,
) -> anyhow::Result<Vec<(Vec<u8>, Vec<MergedWindows>)>> {
    let mut buf = bytes;
    let mut results = Vec::new();
    while buf.has_remaining() {
        results.push(decode_merge_output_element(&mut buf, window_coder)?);
    }
    Ok(results)
}

fn decode_merge_output_element(
    buf: &mut &[u8],
    window_coder: &WindowCoder,
) -> anyhow::Result<(Vec<u8>, Vec<MergedWindows>)> {
    let (_, outer_windows, _) = decode_windowed_value_header(buf, &WindowCoder::Global);
    ensure!(
        outer_windows == vec![BeamWindow::Global],
        "MergeWindows output must be in the global window"
    );

    let nonce = decode_nested_bytes(buf)?;

    let unmerged = decode_window_iterable(buf, window_coder)?;
    let pair_count = decode_count(buf)?;
    let mut merged = Vec::with_capacity(pair_count + unmerged.len());
    for window in unmerged {
        merged.push(MergedWindows {
            merged: window.clone(),
            originals: vec![window],
        });
    }
    for _ in 0..pair_count {
        let result = decode_window_from(buf, window_coder);
        let originals = decode_window_iterable(buf, window_coder)?;
        merged.push(MergedWindows {
            merged: result,
            originals,
        });
    }
    Ok((nonce, merged))
}

fn decode_count(buf: &mut impl Buf) -> anyhow::Result<usize> {
    ensure!(
        buf.remaining() >= 4,
        "MergeWindows output ended before an iterable length"
    );
    let count = buf.get_i32();
    ensure!(
        count >= 0,
        "MergeWindows output used an unsupported chunked iterable"
    );
    Ok(count as usize)
}

fn decode_nested_bytes(buf: &mut impl Buf) -> anyhow::Result<Vec<u8>> {
    let len = usize::try_from(decode_varint(buf))
        .context("MergeWindows nonce length does not fit in usize")?;
    ensure!(
        buf.remaining() >= len,
        "MergeWindows output ended before its nonce"
    );
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    Ok(bytes)
}

fn decode_window_iterable(
    buf: &mut impl Buf,
    window_coder: &WindowCoder,
) -> anyhow::Result<Vec<BeamWindow>> {
    let count = decode_count(buf)?;
    let mut windows = Vec::with_capacity(count);
    for _ in 0..count {
        windows.push(decode_window_from(buf, window_coder));
    }
    Ok(windows)
}

fn validate_merge_result(requested: &[BeamWindow], merged: &[MergedWindows]) -> anyhow::Result<()> {
    let requested: HashSet<String> = requested.iter().map(BeamWindow::canonical_key).collect();
    let mut consumed = HashSet::new();
    for merge in merged {
        ensure!(
            !merge.originals.is_empty(),
            "MergeWindows returned a merge without original windows"
        );
        for original in &merge.originals {
            let key = original.canonical_key();
            ensure!(
                requested.contains(&key),
                "MergeWindows consumed a window that was not requested: {key}"
            );
            ensure!(
                consumed.insert(key.clone()),
                "MergeWindows consumed the same original window more than once: {key}"
            );
        }
    }

    let missing: Vec<_> = requested.difference(&consumed).cloned().collect();
    ensure!(
        missing.is_empty(),
        "MergeWindows output omitted original window(s): {}",
        missing.join(", ")
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn coder(urn: &str, components: &[&str]) -> Coder {
        Coder {
            spec: Some(FunctionSpec {
                urn: urn.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: components.iter().map(|id| id.to_string()).collect(),
        }
    }

    fn custom_window_coder() -> (HashMap<String, Coder>, WindowCoder) {
        let coders = HashMap::from([
            (
                "window-outer".to_string(),
                coder(beam_urns::LENGTH_PREFIX_CODER, &["window-inner"]),
            ),
            (
                "window-inner".to_string(),
                coder(beam_urns::JAVA_SDK_CODER, &[]),
            ),
        ]);
        let window_coder = WindowCoder::from_coder("window-outer", &coders);
        (coders, window_coder)
    }

    fn output_with_merge(
        nonce: &[u8],
        unmerged: &[BeamWindow],
        merges: &[MergedWindows],
        window_coder: &WindowCoder,
    ) -> Vec<u8> {
        let mut buf = Vec::new();
        encode_windowed_value_header(
            BEAM_MIN_TIMESTAMP_MILLIS,
            &[BeamWindow::Global],
            &WindowCoder::Global,
            &PaneInfo::no_firing(),
            &mut buf,
        );
        encode_varint(nonce.len() as u64, &mut buf);
        buf.put_slice(nonce);

        buf.put_i32(unmerged.len() as i32);
        for window in unmerged {
            window_coder.encode_window(window, &mut buf);
        }

        buf.put_i32(merges.len() as i32);
        for merge in merges {
            window_coder.encode_window(&merge.merged, &mut buf);
            buf.put_i32(merge.originals.len() as i32);
            for original in &merge.originals {
                window_coder.encode_window(original, &mut buf);
            }
        }
        buf
    }

    #[test]
    fn merge_output_decodes_unmerged_and_consumed_windows() {
        let nonce = b"nonce";
        let (_, window_coder) = custom_window_coder();
        let first = BeamWindow::Opaque {
            coder_id: "window-inner".to_string(),
            bytes: vec![1, 2, 3],
        };
        let second = BeamWindow::Opaque {
            coder_id: "window-inner".to_string(),
            bytes: vec![4, 5],
        };
        let third = BeamWindow::Opaque {
            coder_id: "window-inner".to_string(),
            bytes: vec![6],
        };
        let fourth = BeamWindow::Opaque {
            coder_id: "window-inner".to_string(),
            bytes: vec![7],
        };
        let merged = BeamWindow::Opaque {
            coder_id: "window-inner".to_string(),
            bytes: vec![9, 9],
        };
        let bytes = output_with_merge(
            nonce,
            std::slice::from_ref(&first),
            &[MergedWindows {
                merged: merged.clone(),
                originals: vec![second.clone(), third.clone()],
            }],
            &window_coder,
        );

        let decoded = decode_merge_output_batch(&bytes, &window_coder).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, nonce);
        let decoded = decoded[0].1.clone();
        assert_eq!(
            decoded,
            vec![
                MergedWindows {
                    merged: first.clone(),
                    originals: vec![first.clone()],
                },
                MergedWindows {
                    merged,
                    originals: vec![second.clone(), third.clone()],
                },
            ]
        );
        validate_merge_result(&[first.clone(), second.clone(), third.clone()], &decoded).unwrap();
        assert!(validate_merge_result(&[first, second, third, fourth], &decoded).is_err());
    }

    #[test]
    fn merge_output_decodes_multiple_batch_elements() {
        let (_, window_coder) = custom_window_coder();
        let first_window = BeamWindow::Opaque {
            coder_id: "window-inner".to_string(),
            bytes: vec![1],
        };
        let second_window = BeamWindow::Opaque {
            coder_id: "window-inner".to_string(),
            bytes: vec![2],
        };
        let mut bytes = output_with_merge(
            b"first",
            std::slice::from_ref(&first_window),
            &[],
            &window_coder,
        );
        bytes.extend(output_with_merge(
            b"second",
            std::slice::from_ref(&second_window),
            &[],
            &window_coder,
        ));

        let decoded = decode_merge_output_batch(&bytes, &window_coder).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].0, b"first");
        assert_eq!(decoded[0].1[0].merged, first_window);
        assert_eq!(decoded[1].0, b"second");
        assert_eq!(decoded[1].1[0].merged, second_window);
    }

    #[test]
    fn duplicate_original_window_is_rejected() {
        let first = BeamWindow::Opaque {
            coder_id: "window-inner".to_string(),
            bytes: vec![1],
        };
        let second = BeamWindow::Opaque {
            coder_id: "window-inner".to_string(),
            bytes: vec![2],
        };
        let merged = BeamWindow::Opaque {
            coder_id: "window-inner".to_string(),
            bytes: vec![3],
        };
        let result = vec![
            MergedWindows {
                merged: merged.clone(),
                originals: vec![first.clone(), second.clone()],
            },
            MergedWindows {
                merged,
                originals: vec![second.clone()],
            },
        ];
        assert!(validate_merge_result(&[first, second], &result).is_err());
    }

    #[test]
    fn merge_descriptor_uses_the_sdk_window_fn_and_wire_coders() {
        let (pipeline_coders, _) = custom_window_coder();
        let endpoint = ApiServiceDescriptor {
            url: "http://127.0.0.1:8099".to_string(),
            ..Default::default()
        };
        let window_fn = FunctionSpec {
            urn: "beam:window_fn:serialized_java:v1".to_string(),
            payload: vec![1, 2, 3],
        };
        let strategy = WindowingStrategy {
            window_fn: Some(window_fn.clone()),
            merge_status: merge_status::Enum::NeedsMerge as i32,
            window_coder_id: "window-outer".to_string(),
            environment_id: "beam:env:docker:v1".to_string(),
            ..Default::default()
        };
        let components = Components {
            environments: HashMap::from([("beam:env:docker:v1".to_string(), Default::default())]),
            ..Default::default()
        };

        let (descriptor, bundle) = build_merge_descriptor(
            "strategy",
            &strategy,
            &pipeline_coders,
            &components,
            endpoint,
        )
        .unwrap();

        let merge_transform = descriptor
            .transforms
            .values()
            .find(|transform| {
                transform.spec.as_ref().map(|spec| spec.urn.as_str())
                    == Some(beam_urns::MERGE_WINDOWS_TRANSFORM)
            })
            .expect("merge transform");
        assert_eq!(
            FunctionSpec::decode(merge_transform.spec.as_ref().unwrap().payload.as_slice())
                .unwrap(),
            window_fn
        );
        assert_eq!(
            bundle.window_coder,
            WindowCoder::OpaqueLengthPrefixed {
                inner_coder_id: "window-inner".to_string()
            }
        );
        assert!(descriptor.coders.contains_key("window-outer"));
        assert!(descriptor.coders.contains_key("window-inner"));
    }

    #[test]
    fn merge_cache_is_bounded() {
        let mut cache = HashMap::new();
        for index in 0..=MERGE_CACHE_MAX_ENTRIES {
            insert_merge_cache(
                &mut cache,
                MergeCacheKey {
                    strategy_id: "strategy".to_string(),
                    windows: vec![index.to_string()],
                },
                Arc::new(Vec::new()),
            );
        }
        assert_eq!(cache.len(), MERGE_CACHE_MAX_ENTRIES);
    }
}
