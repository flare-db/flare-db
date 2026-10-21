use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Mutex,
};

use anyhow::{Context, anyhow, ensure};
use beam_model_rs::v1::MonitoringInfo;
use bytes::{Buf, BufMut};
use log::warn;
use prost_types::Timestamp;

use crate::coders::primitives::encode_varint;

const SUM_INT64_TYPE: &str = "beam:metrics:sum_int64:v1";
const DISTRIBUTION_INT64_TYPE: &str = "beam:metrics:distribution_int64:v1";
const LATEST_INT64_TYPE: &str = "beam:metrics:latest_int64:v1";
const SET_STRING_TYPE: &str = "beam:metrics:set_string:v1";
const BOUNDED_TRIE_TYPE: &str = "beam:metrics:bounded_trie:v1";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MetricKey {
    urn: String,
    r#type: String,
    labels: BTreeMap<String, String>,
}

impl From<&MonitoringInfo> for MetricKey {
    fn from(info: &MonitoringInfo) -> Self {
        Self {
            urn: info.urn.clone(),
            r#type: info.r#type.clone(),
            labels: info
                .labels
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }
}

/// Per-job aggregation of Beam `MonitoringInfo` values returned by SDK bundles.
///
/// Each SDK bundle reports values accumulated within that bundle. FlareDB folds
/// those values into one entry per `(URN, type, labels)` identity before serving
/// them through the Job API.
#[derive(Default)]
pub(crate) struct JobMetrics {
    metrics: Mutex<BTreeMap<MetricKey, MonitoringInfo>>,
}

impl JobMetrics {
    pub(crate) fn clear(&self) {
        self.metrics
            .lock()
            .expect("job metrics lock not poisoned")
            .clear();
    }

    pub(crate) fn record(&self, monitoring_infos: &[MonitoringInfo]) {
        if monitoring_infos.is_empty() {
            return;
        }

        let mut metrics = self.metrics.lock().expect("job metrics lock not poisoned");
        for info in monitoring_infos {
            // Beam's bounded-trie metric type does not have a portable Beam job
            // metric representation exposed by FlareDB yet. Treat it as a no-op
            // rather than warning on every bundle.
            if info.r#type == BOUNDED_TRIE_TYPE {
                continue;
            }
            if info.payload.is_empty() {
                warn!(
                    "skipping metric {} ({}) without an embedded payload",
                    info.urn, info.r#type
                );
                continue;
            }
            if !is_supported_type(&info.r#type) {
                warn!(
                    "skipping unsupported Beam metric type {} for {}",
                    info.r#type, info.urn
                );
                continue;
            }

            let key = MetricKey::from(info);
            match metrics.get_mut(&key) {
                Some(current) => {
                    if let Err(error) = merge_metric(current, info) {
                        warn!(
                            "skipping malformed Beam metric {} ({}): {error:#}",
                            info.urn, info.r#type
                        );
                    }
                }
                None => {
                    if let Err(error) = validate_payload(&info.r#type, &info.payload) {
                        warn!(
                            "skipping malformed Beam metric {} ({}): {error:#}",
                            info.urn, info.r#type
                        );
                    } else {
                        metrics.insert(key, info.clone());
                    }
                }
            }
        }
    }

    pub(crate) fn snapshot(&self) -> Vec<MonitoringInfo> {
        self.metrics
            .lock()
            .expect("job metrics lock not poisoned")
            .values()
            .cloned()
            .collect()
    }
}

fn is_supported_type(type_urn: &str) -> bool {
    matches!(
        type_urn,
        SUM_INT64_TYPE | DISTRIBUTION_INT64_TYPE | LATEST_INT64_TYPE | SET_STRING_TYPE
    )
}

fn validate_payload(type_urn: &str, payload: &[u8]) -> anyhow::Result<()> {
    match type_urn {
        SUM_INT64_TYPE => {
            decode_single_i64(payload)?;
        }
        DISTRIBUTION_INT64_TYPE => {
            decode_distribution(payload)?;
        }
        LATEST_INT64_TYPE => {
            decode_gauge(payload)?;
        }
        SET_STRING_TYPE => {
            decode_string_set(payload)?;
        }
        other => return Err(anyhow!("unsupported metric type: {other}")),
    }
    Ok(())
}

fn merge_metric(current: &mut MonitoringInfo, incoming: &MonitoringInfo) -> anyhow::Result<()> {
    current.payload = match current.r#type.as_str() {
        SUM_INT64_TYPE => merge_sum(&current.payload, &incoming.payload)?,
        DISTRIBUTION_INT64_TYPE => merge_distribution(&current.payload, &incoming.payload)?,
        LATEST_INT64_TYPE => merge_gauge(&current.payload, &incoming.payload)?,
        SET_STRING_TYPE => merge_string_set(&current.payload, &incoming.payload)?,
        other => return Err(anyhow!("unsupported metric type: {other}")),
    };
    merge_start_time(&mut current.start_time, incoming.start_time.as_ref());
    Ok(())
}

fn merge_sum(left: &[u8], right: &[u8]) -> anyhow::Result<Vec<u8>> {
    let left = decode_single_i64(left).context("decoding left counter")?;
    let right = decode_single_i64(right).context("decoding right counter")?;
    let sum = left
        .checked_add(right)
        .ok_or_else(|| anyhow!("counter overflow merging {left} and {right}"))?;
    let mut payload = Vec::new();
    encode_varint(sum as u64, &mut payload);
    Ok(payload)
}

fn merge_distribution(left: &[u8], right: &[u8]) -> anyhow::Result<Vec<u8>> {
    let (left_count, left_sum, left_min, left_max) =
        decode_distribution(left).context("decoding left distribution")?;
    let (right_count, right_sum, right_min, right_max) =
        decode_distribution(right).context("decoding right distribution")?;

    let count = left_count
        .checked_add(right_count)
        .ok_or_else(|| anyhow!("distribution count overflow"))?;
    let sum = left_sum
        .checked_add(right_sum)
        .ok_or_else(|| anyhow!("distribution sum overflow"))?;
    let min = left_min.min(right_min);
    let max = left_max.max(right_max);

    let mut payload = Vec::new();
    encode_varint(count as u64, &mut payload);
    encode_varint(sum as u64, &mut payload);
    encode_varint(min as u64, &mut payload);
    encode_varint(max as u64, &mut payload);
    Ok(payload)
}

fn merge_gauge(left: &[u8], right: &[u8]) -> anyhow::Result<Vec<u8>> {
    let left = decode_gauge(left).context("decoding left gauge")?;
    let right = decode_gauge(right).context("decoding right gauge")?;
    let (timestamp, value) = if right.0 >= left.0 { right } else { left };
    let mut payload = Vec::new();
    encode_varint(timestamp as u64, &mut payload);
    encode_varint(value as u64, &mut payload);
    Ok(payload)
}

fn merge_string_set(left: &[u8], right: &[u8]) -> anyhow::Result<Vec<u8>> {
    let mut values = decode_string_set(left).context("decoding left string set")?;
    values.extend(decode_string_set(right).context("decoding right string set")?);

    let mut payload = Vec::new();
    payload
        .put_i32(i32::try_from(values.len()).expect("string-set metric length exceeds i32::MAX"));
    for value in values {
        encode_varint(value.len() as u64, &mut payload);
        payload.put_slice(value.as_bytes());
    }
    Ok(payload)
}

fn decode_single_i64(payload: &[u8]) -> anyhow::Result<i64> {
    let mut buf = payload;
    let value = decode_signed_varint(&mut buf)?;
    ensure!(!buf.has_remaining(), "counter payload has trailing bytes");
    Ok(value)
}

fn decode_distribution(payload: &[u8]) -> anyhow::Result<(i64, i64, i64, i64)> {
    let mut buf = payload;
    let values = (
        decode_signed_varint(&mut buf)?,
        decode_signed_varint(&mut buf)?,
        decode_signed_varint(&mut buf)?,
        decode_signed_varint(&mut buf)?,
    );
    ensure!(
        !buf.has_remaining(),
        "distribution payload has trailing bytes"
    );
    Ok(values)
}

fn decode_gauge(payload: &[u8]) -> anyhow::Result<(i64, i64)> {
    let mut buf = payload;
    let values = (
        decode_signed_varint(&mut buf)?,
        decode_signed_varint(&mut buf)?,
    );
    ensure!(!buf.has_remaining(), "gauge payload has trailing bytes");
    Ok(values)
}

fn decode_string_set(payload: &[u8]) -> anyhow::Result<BTreeSet<String>> {
    let mut buf = payload;
    ensure!(buf.remaining() >= 4, "string-set payload has no count");
    let count = buf.get_i32();
    let mut values = BTreeSet::new();
    if count >= 0 {
        for _ in 0..count {
            values.insert(decode_string(&mut buf)?);
        }
    } else {
        ensure!(count == -1, "string-set payload has invalid count {count}");
        loop {
            let chunk_count = decode_varint_checked(&mut buf)?;
            if chunk_count == 0 {
                break;
            }
            for _ in 0..chunk_count {
                values.insert(decode_string(&mut buf)?);
            }
        }
    }
    ensure!(
        !buf.has_remaining(),
        "string-set payload has trailing bytes"
    );
    Ok(values)
}

fn decode_string(buf: &mut &[u8]) -> anyhow::Result<String> {
    let len = usize::try_from(decode_varint_checked(buf)?)
        .context("string-set entry length does not fit in usize")?;
    ensure!(
        buf.remaining() >= len,
        "string-set payload ended before an entry"
    );
    let mut bytes = vec![0u8; len];
    buf.copy_to_slice(&mut bytes);
    String::from_utf8(bytes).context("string-set entry is not UTF-8")
}

fn decode_signed_varint(buf: &mut &[u8]) -> anyhow::Result<i64> {
    Ok(decode_varint_checked(buf)? as i64)
}

fn decode_varint_checked(buf: &mut &[u8]) -> anyhow::Result<u64> {
    let mut result = 0u64;
    let mut shift = 0u32;
    loop {
        ensure!(buf.has_remaining(), "metric payload ended inside a varint");
        let byte = buf.get_u8();
        ensure!(shift < 64, "metric varint exceeds 64 bits");
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
}

fn merge_start_time(current: &mut Option<Timestamp>, incoming: Option<&Timestamp>) {
    let Some(incoming) = incoming else {
        return;
    };
    let replace = current.as_ref().is_none_or(|current| {
        (incoming.seconds, incoming.nanos) < (current.seconds, current.nanos)
    });
    if replace {
        *current = Some(*incoming);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn metric(type_urn: &str, labels: &[(&str, &str)], payload: Vec<u8>) -> MonitoringInfo {
        MonitoringInfo {
            urn: "beam:metric:user:test:v1".to_string(),
            r#type: type_urn.to_string(),
            payload,
            labels: labels
                .iter()
                .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                .collect(),
            start_time: None,
        }
    }

    fn int64_payload(values: &[i64]) -> Vec<u8> {
        let mut payload = Vec::new();
        for value in values {
            encode_varint(*value as u64, &mut payload);
        }
        payload
    }

    #[test]
    fn counters_are_summed_across_bundles() {
        let metrics = JobMetrics::default();
        metrics.record(&[
            metric(SUM_INT64_TYPE, &[("NAME", "count")], int64_payload(&[2])),
            metric(SUM_INT64_TYPE, &[("NAME", "count")], int64_payload(&[3])),
        ]);

        let values = metrics.snapshot();
        assert_eq!(values.len(), 1);
        assert_eq!(decode_single_i64(&values[0].payload).unwrap(), 5);
    }

    #[test]
    fn bounded_trie_metrics_are_ignored_without_being_recorded() {
        let metrics = JobMetrics::default();
        metrics.record(&[metric(BOUNDED_TRIE_TYPE, &[], vec![1, 2, 3])]);
        assert!(metrics.snapshot().is_empty());
    }

    #[test]
    fn distributions_combine_count_sum_and_bounds() {
        let metrics = JobMetrics::default();
        metrics.record(&[
            metric(
                DISTRIBUTION_INT64_TYPE,
                &[("NAME", "latency")],
                int64_payload(&[2, 10, 4, 6]),
            ),
            metric(
                DISTRIBUTION_INT64_TYPE,
                &[("NAME", "latency")],
                int64_payload(&[3, 20, 1, 9]),
            ),
        ]);

        let values = metrics.snapshot();
        assert_eq!(values.len(), 1);
        assert_eq!(
            decode_distribution(&values[0].payload).unwrap(),
            (5, 30, 1, 9)
        );
    }

    #[test]
    fn gauges_keep_the_newest_value() {
        let metrics = JobMetrics::default();
        metrics.record(&[
            metric(
                LATEST_INT64_TYPE,
                &[("NAME", "last")],
                int64_payload(&[100, 7]),
            ),
            metric(
                LATEST_INT64_TYPE,
                &[("NAME", "last")],
                int64_payload(&[200, 9]),
            ),
        ]);

        let values = metrics.snapshot();
        assert_eq!(values.len(), 1);
        assert_eq!(decode_gauge(&values[0].payload).unwrap(), (200, 9));
    }

    #[test]
    fn string_sets_are_unioned() {
        let left = {
            let mut payload = Vec::new();
            payload.put_i32(2);
            for value in ["a", "c"] {
                encode_varint(value.len() as u64, &mut payload);
                payload.put_slice(value.as_bytes());
            }
            payload
        };
        let right = {
            let mut payload = Vec::new();
            payload.put_i32(2);
            for value in ["b", "c"] {
                encode_varint(value.len() as u64, &mut payload);
                payload.put_slice(value.as_bytes());
            }
            payload
        };
        let metrics = JobMetrics::default();
        metrics.record(&[
            metric(SET_STRING_TYPE, &[("NAME", "set")], left),
            metric(SET_STRING_TYPE, &[("NAME", "set")], right),
        ]);

        let values = metrics.snapshot();
        assert_eq!(values.len(), 1);
        assert_eq!(
            decode_string_set(&values[0].payload).unwrap(),
            BTreeSet::from(["a".to_string(), "b".to_string(), "c".to_string()])
        );
    }

    #[test]
    fn labels_keep_metrics_separate() {
        let metrics = JobMetrics::default();
        metrics.record(&[
            metric(SUM_INT64_TYPE, &[("NAME", "first")], int64_payload(&[1])),
            metric(SUM_INT64_TYPE, &[("NAME", "second")], int64_payload(&[2])),
        ]);

        assert_eq!(metrics.snapshot().len(), 2);
    }

    #[test]
    fn malformed_and_unsupported_metrics_are_skipped() {
        let metrics = JobMetrics::default();
        metrics.record(&[
            metric(SUM_INT64_TYPE, &[("NAME", "bad")], vec![0x80]),
            metric("beam:metrics:histogram_int64:v1", &[], vec![1]),
        ]);

        assert!(metrics.snapshot().is_empty());
    }

    #[test]
    fn snapshot_can_be_shared_across_tasks() {
        let metrics = Arc::new(JobMetrics::default());
        let writer = metrics.clone();
        std::thread::spawn(move || {
            writer.record(&[metric(
                SUM_INT64_TYPE,
                &[("NAME", "count")],
                int64_payload(&[4]),
            )]);
        })
        .join()
        .unwrap();

        assert_eq!(
            decode_single_i64(&metrics.snapshot()[0].payload).unwrap(),
            4
        );
    }
}
