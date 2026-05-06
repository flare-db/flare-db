use crate::errors::TransformError;
use crate::transforms::{FlareTransform, TransformContext};
use beam_model_rs::v1::{Elements, elements};
use std::collections::HashMap;

/// Impulse always produces exactly one element: an empty byte[]
/// in a GlobalWindow at MIN_TIMESTAMP. Wire format is always
/// beam:coder:windowed_value:v1:
///
///   timestamp(8) | windows(iterable) | pane(1) | element(1)
pub struct Impulse {
    inputs: HashMap<String, String>,
    outputs: HashMap<String, String>,
}

impl FlareTransform for Impulse {
    type Context = ImpluseContext;

    fn urn() -> &'static str
    where
        Self: Sized,
    {
        "beam:transform:impulse:v1"
    }

    fn with(inputs: HashMap<String, String>, outputs: HashMap<String, String>) -> Self {
        Self { inputs, outputs }
    }

    fn execute(&self, ctx: &Self::Context) -> std::result::Result<Elements, TransformError> {
        Ok(Elements {
            data: vec![elements::Data {
                instruction_id: ctx.instruction_id.to_string(),
                transform_id: ctx.transform_id.to_string(),
                data: encode_windowed_empty_bytes(),
                is_last: true,
            }],
            timers: Vec::new(),
        })
    }
}

pub struct ImpluseContext {
    instruction_id: String,
    transform_id: String,
}

impl TransformContext for ImpluseContext {}

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
