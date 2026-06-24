use crate::engine::store::BeamValue;
use beam_model_rs::v1::Coder;
use std::collections::HashMap;

use bytes::{Buf, BufMut};
use log::info;

use crate::jobservice::urns::beam_urns;

/// Beam's minimum timestamp in millis (`BoundedWindow.TIMESTAMP_MIN_VALUE`).
pub const BEAM_MIN_TIMESTAMP_MILLIS: i64 = -9_223_372_036_854_775;

#[derive(Debug, Clone)]
pub enum StandardBeamCoders {
    StringUtf8(StringUtf8Coder),
    Bytes(BytesCoder),
    VarInt(VarIntCoder),
    Bool(BoolCoder),
    Void(VoidCoder),
    Iterable(IterableCoder),
    Kv(Box<StandardBeamCoders>, Box<StandardBeamCoders>),
}

impl StandardBeamCoders {
    pub fn from_urn(
        id: &str,
        component_coder_ids: Option<Vec<String>>,
        pipeline_coders: Option<&HashMap<String, Coder>>,
    ) -> Self {
        // Lookup and get the correct coder since coder_id is not unique to urn
        let pipeline_coder = pipeline_coders.and_then(|coders| coders.get(id));

        let urn = pipeline_coder
            .and_then(|coder| coder.spec.as_ref())
            .map(|spec| spec.urn.as_str())
            .unwrap_or(id);

        info!("Resolving coder: id={}, urn={}", id, urn);

        let component_coder_ids = component_coder_ids
            .or_else(|| pipeline_coder.map(|coder| coder.component_coder_ids.clone()));

        match urn {
            beam_urns::BYTES_CODER => StandardBeamCoders::Bytes(BytesCoder),
            beam_urns::STRING_UTF8_CODER => StandardBeamCoders::StringUtf8(StringUtf8Coder),
            beam_urns::VARINT_CODER => StandardBeamCoders::VarInt(VarIntCoder),
            beam_urns::BOOL_CODER => StandardBeamCoders::Bool(BoolCoder),
            beam_urns::JAVA_SDK_CODER => {
                if id == "VoidCoder" {
                    StandardBeamCoders::Void(VoidCoder)
                } else {
                    StandardBeamCoders::Bytes(BytesCoder)
                }
            }
            beam_urns::ITERABLE_CODER => {
                let ids = component_coder_ids
                    .as_ref()
                    .expect("IterableCoder requires one component coder id");
                assert_eq!(
                    ids.len(),
                    1,
                    "IterableCoder requires exactly one component coder"
                );

                StandardBeamCoders::Iterable(IterableCoder::new(StandardBeamCoders::from_urn(
                    &ids[0],
                    None,
                    pipeline_coders,
                )))
            }
            beam_urns::KV_CODER => {
                let ids = component_coder_ids
                    .as_ref()
                    .expect("KvCoder requires component coder ids");
                assert_eq!(
                    ids.len(),
                    2,
                    "KvCoder requires exactly two component coders"
                );

                StandardBeamCoders::Kv(
                    Box::new(StandardBeamCoders::from_urn(&ids[0], None, pipeline_coders)),
                    Box::new(StandardBeamCoders::from_urn(&ids[1], None, pipeline_coders)),
                )
            }
            _ => panic!("Unknown URN: {}", urn),
        }
    }

    /// Encode in nested Beam coder context. Fn Data sends concatenated
    /// WindowedValue payloads, so the element inside each WindowedValue must be
    /// self-delimiting when the element coder requires it (bytes/string/KV).
    pub fn encode(&self, val: &BeamValue, buf: &mut impl BufMut) {
        self.encode_nested(val, buf);
    }

    fn encode_nested(&self, val: &BeamValue, buf: &mut impl BufMut) {
        match (self, val) {
            (StandardBeamCoders::StringUtf8(coder), BeamValue::String(s)) => coder.encode(s, buf),
            (StandardBeamCoders::Bytes(coder), BeamValue::Bytes(bytes)) => coder.encode(bytes, buf),
            (StandardBeamCoders::VarInt(coder), BeamValue::Int64(value)) => {
                coder.encode(value, buf)
            }
            (StandardBeamCoders::Bool(coder), BeamValue::Bool(value)) => coder.encode(value, buf),
            (StandardBeamCoders::Void(coder), BeamValue::Void) => coder.encode(val, buf),
            (StandardBeamCoders::Iterable(coder), BeamValue::Iterable(values)) => {
                coder.encode(values, buf)
            }
            (StandardBeamCoders::Kv(key_coder, value_coder), BeamValue::Kv(key, value)) => {
                key_coder.encode_nested(key, buf);
                value_coder.encode_nested(value, buf);
            }
            (StandardBeamCoders::Kv(key_coder, value_coder), BeamValue::Gbk(key, values)) => {
                key_coder.encode_nested(key, buf);
                value_coder.encode_nested(&BeamValue::Iterable(values.clone()), buf);
            }
            _ => panic!("Mismatched coder and value type"),
        }
    }

    /// Decode in nested Beam coder context.
    pub fn decode(&self, buf: &mut impl Buf) -> BeamValue {
        self.decode_nested(buf)
    }

    fn decode_nested(&self, buf: &mut impl Buf) -> BeamValue {
        match self {
            StandardBeamCoders::StringUtf8(coder) => BeamValue::String(coder.decode(buf)),
            StandardBeamCoders::Bytes(coder) => BeamValue::Bytes(coder.decode(buf)),
            StandardBeamCoders::VarInt(coder) => BeamValue::Int64(coder.decode(buf)),
            StandardBeamCoders::Bool(coder) => BeamValue::Bool(coder.decode(buf)),
            StandardBeamCoders::Void(coder) => coder.decode(buf),
            StandardBeamCoders::Iterable(coder) => BeamValue::Iterable(coder.decode(buf)),
            StandardBeamCoders::Kv(key_coder, value_coder) => BeamValue::Kv(
                Box::new(key_coder.decode_nested(buf)),
                Box::new(value_coder.decode_nested(buf)),
            ),
        }
    }
}

pub trait BeamCoder<T> {
    fn encode(&self, val: &T, buf: &mut impl BufMut);
    fn decode(&self, buf: &mut impl Buf) -> T;
}

#[derive(Debug, Clone)]
pub struct StringUtf8Coder;

impl BeamCoder<String> for StringUtf8Coder {
    fn encode(&self, val: &String, buf: &mut impl BufMut) {
        let utf8 = val.as_bytes();
        encode_varint(utf8.len() as u64, buf);
        buf.put_slice(utf8);
    }

    fn decode(&self, buf: &mut impl Buf) -> String {
        let len = decode_varint(buf) as usize;
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        String::from_utf8(bytes).unwrap()
    }
}

#[derive(Debug, Clone)]
pub struct BytesCoder;

impl BeamCoder<Vec<u8>> for BytesCoder {
    fn encode(&self, val: &Vec<u8>, buf: &mut impl BufMut) {
        encode_varint(val.len() as u64, buf);
        buf.put_slice(val);
    }

    fn decode(&self, buf: &mut impl Buf) -> Vec<u8> {
        let len = decode_varint(buf) as usize;
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        bytes
    }
}

#[derive(Debug, Clone)]
pub struct VarIntCoder;

impl BeamCoder<i64> for VarIntCoder {
    fn encode(&self, val: &i64, buf: &mut impl BufMut) {
        encode_signed_varint(*val, buf);
    }

    fn decode(&self, buf: &mut impl Buf) -> i64 {
        decode_signed_varint(buf)
    }
}

#[derive(Debug, Clone)]
pub struct BoolCoder;

impl BeamCoder<bool> for BoolCoder {
    fn encode(&self, val: &bool, buf: &mut impl BufMut) {
        buf.put_u8(u8::from(*val));
    }

    fn decode(&self, buf: &mut impl Buf) -> bool {
        buf.get_u8() != 0
    }
}

#[derive(Debug, Clone)]
pub struct IterableCoder {
    element_coder: Box<StandardBeamCoders>,
}

impl IterableCoder {
    pub fn new(element_coder: StandardBeamCoders) -> Self {
        Self {
            element_coder: Box::new(element_coder),
        }
    }
}

impl BeamCoder<Vec<BeamValue>> for IterableCoder {
    fn encode(&self, val: &Vec<BeamValue>, buf: &mut impl BufMut) {
        let count = i32::try_from(val.len()).expect("IterableCoder length exceeds i32::MAX");
        buf.put_i32(count);

        for element in val {
            self.element_coder.encode_nested(element, buf);
        }
    }

    fn decode(&self, buf: &mut impl Buf) -> Vec<BeamValue> {
        let count = buf.get_i32();
        let mut values = Vec::new();

        if count >= 0 {
            for _ in 0..count {
                values.push(self.element_coder.decode_nested(buf));
            }
            return values;
        }

        assert_eq!(count, -1, "IterableCoder length must be non-negative or -1");

        loop {
            let chunk_count = decode_varint(buf);
            if chunk_count == 0 {
                break;
            }

            for _ in 0..chunk_count {
                values.push(self.element_coder.decode_nested(buf));
            }
        }

        values
    }
}

#[derive(Debug, Clone)]
pub struct VoidCoder;

impl BeamCoder<BeamValue> for VoidCoder {
    fn encode(&self, _value: &BeamValue, _buf: &mut impl BufMut) {
        // Void encodes as nothing.
    }

    fn decode(&self, _buf: &mut impl Buf) -> BeamValue {
        // Void encodes as zero bytes - nothing to read.
        BeamValue::Void
    }
}

#[derive(Debug, Clone)]
pub struct WindowedValue {
    pub value: BeamValue,
    pub timestamp_millis: i64,
    pub windows: Vec<BeamWindow>,
    pub pane: PaneInfo,
}

impl WindowedValue {
    pub fn global(value: BeamValue) -> Self {
        Self {
            value,
            timestamp_millis: BEAM_MIN_TIMESTAMP_MILLIS,
            windows: vec![BeamWindow::Global],
            pane: PaneInfo::no_firing(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BeamWindow {
    Global,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaneTiming {
    Early = 0,
    OnTime = 1,
    Late = 2,
    Unknown = 3,
}

impl PaneTiming {
    fn from_bits(bits: u8) -> Self {
        match bits {
            0 => PaneTiming::Early,
            1 => PaneTiming::OnTime,
            2 => PaneTiming::Late,
            _ => PaneTiming::Unknown,
        }
    }
}

#[derive(Debug, Clone)]
pub struct PaneInfo {
    pub is_first: bool,
    pub is_last: bool,
    pub timing: PaneTiming,
    pub index: i64,
    pub non_speculative_index: i64,
}

impl PaneInfo {
    pub fn no_firing() -> Self {
        Self {
            is_first: true,
            is_last: true,
            timing: PaneTiming::Unknown,
            index: 0,
            non_speculative_index: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct WindowedValueCoder {
    element_coder: StandardBeamCoders,
}

impl WindowedValueCoder {
    pub fn new(element_coder: StandardBeamCoders) -> Self {
        Self { element_coder }
    }

    pub fn encode_value(&self, value: &BeamValue, buf: &mut impl BufMut) {
        let windowed_value = WindowedValue::global(value.clone());
        self.encode(&windowed_value, buf);
    }
}

impl BeamCoder<WindowedValue> for WindowedValueCoder {
    fn encode(&self, val: &WindowedValue, buf: &mut impl BufMut) {
        encode_timestamp_millis(val.timestamp_millis, buf);
        encode_global_windows(&val.windows, buf);
        encode_pane_info(&val.pane, buf);
        self.element_coder.encode_nested(&val.value, buf);
    }

    fn decode(&self, buf: &mut impl Buf) -> WindowedValue {
        let timestamp_millis = decode_timestamp_millis(buf);
        let windows = decode_global_windows(buf);
        let pane = decode_pane_info(buf);
        let value = self.element_coder.decode_nested(buf);

        WindowedValue {
            value,
            timestamp_millis,
            windows,
            pane,
        }
    }
}

fn encode_timestamp_millis(timestamp_millis: i64, buf: &mut impl BufMut) {
    let shifted = (timestamp_millis as u64) ^ 0x8000_0000_0000_0000;
    buf.put_u64(shifted);
}

fn decode_timestamp_millis(buf: &mut impl Buf) -> i64 {
    let shifted = buf.get_u64();
    (shifted ^ 0x8000_0000_0000_0000) as i64
}

fn encode_global_windows(windows: &[BeamWindow], buf: &mut impl BufMut) {
    buf.put_i32(windows.len() as i32);

    for window in windows {
        match window {
            BeamWindow::Global => {
                // GlobalWindowCoder has an empty payload.
            }
        }
    }
}

fn decode_global_windows(buf: &mut impl Buf) -> Vec<BeamWindow> {
    let count = buf.get_i32();
    let mut windows = Vec::new();

    if count >= 0 {
        for _ in 0..count {
            windows.push(BeamWindow::Global);
        }
        return windows;
    }

    // IterableCoder also allows an unknown-length encoding: -1 followed by
    // chunks of varint counts, terminated by a zero count.
    loop {
        let chunk_count = decode_varint(buf);
        if chunk_count == 0 {
            break;
        }

        for _ in 0..chunk_count {
            windows.push(BeamWindow::Global);
        }
    }

    windows
}

fn encode_pane_info(pane: &PaneInfo, buf: &mut impl BufMut) {
    let mut first_byte = (pane.timing as u8) << 2;

    if pane.is_first {
        first_byte |= 0x01;
    }
    if pane.is_last {
        first_byte |= 0x02;
    }

    let has_index = pane.index != 0;
    let derived_non_speculative_index = if pane.timing == PaneTiming::Early {
        -1
    } else {
        pane.index
    };
    let has_non_speculative_index = pane.non_speculative_index != derived_non_speculative_index;

    if has_non_speculative_index {
        first_byte |= 0x20;
    } else if has_index {
        first_byte |= 0x10;
    }

    buf.put_u8(first_byte);

    if has_non_speculative_index {
        encode_signed_varint(pane.index, buf);
        encode_signed_varint(pane.non_speculative_index, buf);
    } else if has_index {
        encode_signed_varint(pane.index, buf);
    }
}

fn decode_pane_info(buf: &mut impl Buf) -> PaneInfo {
    let first_byte = buf.get_u8();
    let encoding = first_byte >> 4;
    let timing = PaneTiming::from_bits((first_byte >> 2) & 0x03);
    let is_first = (first_byte & 0x01) != 0;
    let is_last = (first_byte & 0x02) != 0;

    let index = if encoding >= 1 {
        decode_signed_varint(buf)
    } else {
        0
    };

    let non_speculative_index = if encoding >= 2 {
        decode_signed_varint(buf)
    } else if timing == PaneTiming::Early {
        -1
    } else {
        index
    };

    PaneInfo {
        is_first,
        is_last,
        timing,
        index,
        non_speculative_index,
    }
}

fn encode_signed_varint(value: i64, buf: &mut impl BufMut) {
    encode_varint(value as u64, buf);
}

fn decode_signed_varint(buf: &mut impl Buf) -> i64 {
    decode_varint(buf) as i64
}

fn encode_varint(mut value: u64, buf: &mut impl BufMut) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;

        if value != 0 {
            byte |= 0x80;
        }

        buf.put_u8(byte);

        if value == 0 {
            break;
        }
    }
}

fn decode_varint(buf: &mut impl Buf) -> u64 {
    let mut result = 0u64;
    let mut shift = 0;

    loop {
        let byte = buf.get_u8();

        result |= ((byte & 0x7F) as u64) << shift;

        if (byte & 0x80) == 0 {
            break;
        }

        shift += 7;
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::{Bytes, BytesMut};

    #[test]
    fn encodes_varints_from_standard_coders_yaml() {
        let coder = VarIntCoder;

        for (value, expected) in [
            (0, vec![0x00]),
            (1, vec![0x01]),
            (10, vec![0x0A]),
            (200, vec![0xC8, 0x01]),
            (1000, vec![0xE8, 0x07]),
            (9001, vec![0xA9, 0x46]),
            (
                -1,
                vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01],
            ),
        ] {
            let mut encoded = BytesMut::new();
            coder.encode(&value, &mut encoded);
            assert_eq!(encoded.to_vec(), expected, "encoding {value}");

            let mut encoded = Bytes::from(expected);
            assert_eq!(coder.decode(&mut encoded), value, "decoding {value}");
        }
    }

    #[test]
    fn encodes_iterable_varints_with_known_length() {
        let coder = StandardBeamCoders::from_urn(
            beam_urns::ITERABLE_CODER,
            Some(vec![beam_urns::VARINT_CODER.to_string()]),
            None,
        );
        let value = BeamValue::Iterable(vec![
            BeamValue::Int64(1),
            BeamValue::Int64(10),
            BeamValue::Int64(200),
            BeamValue::Int64(1000),
        ]);
        let mut encoded = BytesMut::new();

        coder.encode(&value, &mut encoded);

        assert_eq!(
            encoded.to_vec(),
            vec![0x00, 0x00, 0x00, 0x04, 0x01, 0x0A, 0xC8, 0x01, 0xE8, 0x07]
        );
    }

    #[test]
    fn decodes_iterable_bytes_with_known_length() {
        let coder = StandardBeamCoders::from_urn(
            beam_urns::ITERABLE_CODER,
            Some(vec![beam_urns::BYTES_CODER.to_string()]),
            None,
        );
        let mut encoded = Bytes::from_static(b"\0\0\0\x02\x04ab\0c\x04de\0f");

        assert_eq!(
            coder.decode(&mut encoded),
            BeamValue::Iterable(vec![
                BeamValue::Bytes(b"ab\0c".to_vec()),
                BeamValue::Bytes(b"de\0f".to_vec()),
            ])
        );
    }

    #[test]
    fn decodes_iterable_bytes_with_unknown_length_chunks() {
        let coder = StandardBeamCoders::from_urn(
            beam_urns::ITERABLE_CODER,
            Some(vec![beam_urns::BYTES_CODER.to_string()]),
            None,
        );
        let mut encoded = Bytes::from_static(b"\xff\xff\xff\xff\x02\x04ab\0c\x04de\0f\0");

        assert_eq!(
            coder.decode(&mut encoded),
            BeamValue::Iterable(vec![
                BeamValue::Bytes(b"ab\0c".to_vec()),
                BeamValue::Bytes(b"de\0f".to_vec()),
            ])
        );
    }

    #[test]
    fn encodes_global_windowed_empty_bytes() {
        let coder = WindowedValueCoder::new(StandardBeamCoders::Bytes(BytesCoder));
        let mut encoded = bytes::BytesMut::new();

        coder.encode_value(&BeamValue::Bytes(Vec::new()), &mut encoded);

        // ParamWindowedValueCoder with
        // BoundedWindow.TIMESTAMP_MIN_VALUE, GlobalWindow, PaneInfo.NO_FIRING,
        // and an empty byte[] element.
        assert_eq!(
            encoded.to_vec(),
            vec![
                0x7F, 0xDF, 0x3B, 0x64, 0x5A, 0x1C, 0xAC, 0x09, // timestamp
                0x00, 0x00, 0x00, 0x01, // one GlobalWindow
                0x0F, // PaneInfo.NO_FIRING
                0x00, // empty byte[] in nested context
            ]
        );
    }
}
