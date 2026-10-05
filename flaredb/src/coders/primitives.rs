use crate::{
    coders::{BeamCoder, StandardBeamCoders},
    store::record::{BeamRecord, PrimitiveValue},
    utils::errors::CodersError,
};

use bytes::{Buf, BufMut};

/// Beam's minimum timestamp in millis (`BoundedWindow.TIMESTAMP_MIN_VALUE`).
pub const BEAM_MIN_TIMESTAMP_MILLIS: i64 = -9_223_372_036_854_775;

/// Beam's maximum timestamp in millis (`BoundedWindow.TIMESTAMP_MAX_VALUE`).
pub const BEAM_MAX_TIMESTAMP_MILLIS: i64 = 9_223_372_036_854_775;

/// Max timestamp of Beam's global window in millis
/// (`BeamConstants.Constants.GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS`).
///
/// One standard day is subtracted from [`BEAM_MAX_TIMESTAMP_MILLIS`] so the value
/// stays below the absolute maximum even after rounding up to coarser units.
pub const GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS: i64 = 9_223_371_950_454_775;

#[derive(Debug, Clone)]
pub struct StringUtf8Coder;

impl BeamCoder<String> for StringUtf8Coder {
    fn encode(&self, val: String, buf: &mut impl BufMut) {
        let utf8 = val.as_bytes();
        encode_varint(utf8.len() as u64, buf);
        buf.put_slice(utf8);
    }

    fn decode(&self, buf: &mut impl Buf) -> Result<String, CodersError> {
        let len = decode_varint(buf) as usize;
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        Ok(String::from_utf8(bytes).unwrap())
    }
}

#[derive(Debug, Clone)]
pub struct BytesCoder;

impl BeamCoder<Vec<u8>> for BytesCoder {
    fn encode(&self, val: Vec<u8>, buf: &mut impl BufMut) {
        encode_varint(val.len() as u64, buf);
        buf.put_slice(val.as_slice());
    }

    fn decode(&self, buf: &mut impl Buf) -> Result<Vec<u8>, CodersError> {
        let len = decode_varint(buf) as usize;
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        Ok(bytes)
    }
}

#[derive(Debug, Clone)]
pub struct VarIntCoder;

impl BeamCoder<i64> for VarIntCoder {
    fn encode(&self, val: i64, buf: &mut impl BufMut) {
        encode_signed_varint(val, buf);
    }

    fn decode(&self, buf: &mut impl Buf) -> Result<i64, CodersError> {
        Ok(decode_signed_varint(buf))
    }
}

/// Coder for Java's `org.apache.beam.sdk.coders.VarIntCoder`, 32-bit
/// `Integer` coder.
/// It differs from [`VarIntCoder`] (the model's 64-bit `beam:coder:varint:v1`)
/// only for negative values: it zero-extends the 32-bit two's-complement pattern
/// before LEB128-encoding, so `-1` is 5 bytes (`FF FF FF FF 0F`) and reads back
/// as `0xFFFFFFFF`, whereas the 64-bit coder sign-extends `-1` to 10 bytes.
/// Values in `0..=i32::MAX` are byte-identical between the two.
#[derive(Debug, Clone)]
pub struct VarInt32Coder;

impl BeamCoder<i64> for VarInt32Coder {
    fn encode(&self, val: i64, buf: &mut impl BufMut) {
        debug_assert!(
            (i32::MIN as i64..=i32::MAX as i64).contains(&val),
            "VarInt32Coder value out of i32 range: {val}"
        );
        // Mirror Java's `VarInt.convertIntToLongNoSignExtend(v) = v & 0xFFFFFFFFL`.
        encode_varint((val as i32 as u32) as u64, buf);
    }

    fn decode(&self, buf: &mut impl Buf) -> Result<i64, CodersError> {
        let raw = decode_varint(buf);
        // Java's `VarInt.decodeInt` rejects anything outside the 32-bit range
        // (this also rejects values whose top bit is set, i.e. `raw` as i64 < 0).
        if raw >= (1u64 << 32) {
            return Err(CodersError::WhileDecoding(format!(
                "VarInt32Coder: varint value {raw} out of 32-bit range"
            )));
        }
        // Reinterpret the 32-bit pattern as signed, then widen to i64.
        Ok((raw as u32 as i32) as i64)
    }
}

#[derive(Debug, Clone)]
pub struct BoolCoder;

impl BeamCoder<bool> for BoolCoder {
    fn encode(&self, val: bool, buf: &mut impl BufMut) {
        buf.put_u8(u8::from(val));
    }

    fn decode(&self, buf: &mut impl Buf) -> Result<bool, CodersError> {
        Ok(buf.get_u8() != 0)
    }
}

/// Encodes `f64` as 8 bytes in big-endian IEEE 754 format.
#[derive(Debug, Clone)]
pub struct DoubleCoder;

impl BeamCoder<f64> for DoubleCoder {
    fn encode(&self, val: f64, buf: &mut impl BufMut) {
        buf.put_f64(val);
    }

    fn decode(&self, buf: &mut impl Buf) -> Result<f64, CodersError> {
        if buf.remaining() < 8 {
            return Err(CodersError::WhileDecoding(
                "DoubleCoder: insufficient bytes".to_string(),
            ));
        }
        Ok(buf.get_f64())
    }
}

/// Coder for `beam:coder:iterable:v1`.
///
/// Elements are decoded recursively, so an iterable may hold primitives or
/// tuples (e.g. a GroupByKey value of reified metadata tuples).
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

// ToDo: add seperate method for encoding list
impl BeamCoder<Vec<BeamRecord>> for IterableCoder {
    fn encode(&self, val: Vec<BeamRecord>, buf: &mut impl BufMut) {
        let values = val;
        let count = i32::try_from(values.len()).expect("IterableCoder length exceeds i32::MAX");
        buf.put_i32(count);

        for element in values {
            self.element_coder.encode(element, buf);
        }
    }

    fn decode(&self, buf: &mut impl Buf) -> Result<Vec<BeamRecord>, CodersError> {
        let count = buf.get_i32();
        let mut values = Vec::new();

        if count >= 0 {
            for _ in 0..count {
                values.push(self.element_coder.decode_nested(buf)?);
            }
            return Ok(values);
        }

        assert_eq!(count, -1, "IterableCoder length must be non-negative or -1");

        loop {
            let chunk_count = decode_varint(buf);
            if chunk_count == 0 {
                break;
            }

            for _ in 0..chunk_count {
                values.push(self.element_coder.decode_nested(buf)?);
            }
        }

        Ok(values)
    }
}

/// Coder for Beam tuple objects (`beam:coder:tuple:v1`).
///
/// A tuple is encoded as its components in order, each with its own component
/// coder, and carries no length prefix of its own. Every component must be
/// self-delimiting when nested, which is why each component is encoded/decoded
/// as if it were nested (mirroring Beam's `TupleCoderImpl`, which marks all but
/// the last component as nested).
#[derive(Debug, Clone)]
pub struct TupleCoder {
    component_coders: Vec<StandardBeamCoders>,
}

impl TupleCoder {
    pub fn new(component_coders: Vec<StandardBeamCoders>) -> Self {
        Self { component_coders }
    }

    pub fn component_coders(&self) -> &[StandardBeamCoders] {
        &self.component_coders
    }
}

impl BeamCoder<Vec<PrimitiveValue>> for TupleCoder {
    fn encode(&self, val: Vec<PrimitiveValue>, buf: &mut impl BufMut) {
        assert_eq!(
            val.len(),
            self.component_coders.len(),
            "TupleCoder: value has {} components but the coder has {}",
            val.len(),
            self.component_coders.len()
        );

        for (coder, value) in self.component_coders.iter().zip(val) {
            // Use the full `encode` (not `encode_primitive`): a component coder
            // may itself be composite (e.g. `Nullable`, `length_prefix`), which
            // `encode_primitive` does not know how to handle.
            coder.encode(BeamRecord::PRIMITIVE(value), buf);
        }
    }

    fn decode(&self, buf: &mut impl Buf) -> Result<Vec<PrimitiveValue>, CodersError> {
        let mut values = Vec::with_capacity(self.component_coders.len());
        for coder in &self.component_coders {
            values.push(coder.decode_primitive(buf)?);
        }
        Ok(values)
    }
}

/// Coder for Python pickled payloads (`beam:coder:pickled_python:v1`).
///
/// The runner deliberately does not decode Python pickles. The payload is
/// treated as opaque bytes and stored as [`PrimitiveValue::Bytes`], preserving
/// the exact pickled representation across a round trip. The wire format is a
/// varint length prefix followed by the pickled bytes (the same framing as
/// `beam:coder:bytes:v1`), matching Beam's nested pickle encoding.
#[derive(Debug, Clone)]
pub struct PickleCoder;

impl BeamCoder<Vec<u8>> for PickleCoder {
    fn encode(&self, val: Vec<u8>, buf: &mut impl BufMut) {
        encode_varint(val.len() as u64, buf);
        buf.put_slice(val.as_slice());
    }

    fn decode(&self, buf: &mut impl Buf) -> Result<Vec<u8>, CodersError> {
        let len = decode_varint(buf) as usize;
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        Ok(bytes)
    }
}

/// Coder for `beam:coder:nullable:v1` (`typing.Optional`).
///
/// The wire format matches the standard coder (`standard_coders.yaml`): a
/// single prefix byte (`0x00` for null, `0x01` for present) followed by the
/// component value when present. A null is surfaced as
/// [`PrimitiveValue::Void`]. The component is encoded in the same nested
/// context as the nullable coder itself, mirroring `NullableCoderImpl`.
#[derive(Debug, Clone)]
pub struct NullableCoder {
    value_coder: Box<StandardBeamCoders>,
}

impl NullableCoder {
    pub fn new(value_coder: StandardBeamCoders) -> Self {
        Self {
            value_coder: Box::new(value_coder),
        }
    }

    /// The wrapped coder, for classifying the coder (e.g. whether it decodes to a
    /// primitive).
    pub fn inner(&self) -> &StandardBeamCoders {
        &self.value_coder
    }
}

const NULLABLE_ENCODE_NULL: u8 = 0;
const NULLABLE_ENCODE_PRESENT: u8 = 1;

impl BeamCoder<BeamRecord> for NullableCoder {
    fn encode(&self, val: BeamRecord, buf: &mut impl BufMut) {
        match val {
            BeamRecord::PRIMITIVE(PrimitiveValue::Void) => buf.put_u8(NULLABLE_ENCODE_NULL),
            present => {
                buf.put_u8(NULLABLE_ENCODE_PRESENT);
                self.value_coder.encode(present, buf);
            }
        }
    }

    fn decode(&self, buf: &mut impl Buf) -> Result<BeamRecord, CodersError> {
        match buf.get_u8() {
            NULLABLE_ENCODE_NULL => Ok(BeamRecord::PRIMITIVE(PrimitiveValue::Void)),
            NULLABLE_ENCODE_PRESENT => self.value_coder.decode_nested(buf),
            other => Err(CodersError::WhileDecoding(format!(
                "NullableCoder: unexpected null indicator byte {other}"
            ))),
        }
    }
}

/// Coder for `beam:coder:length_prefix:v1`.
///
/// Wraps an inner coder with a varint byte length. The runner uses this to make
/// opaque Python leaf coders self-delimiting: several distinct Python coders
/// (`PickleCoder`, `FastPrimitivesCoder`, `PaneInfoCoder`) are all emitted under
/// the single `beam:coder:pickled_python:v1` URN yet use different wire formats,
/// so the runner asks the SDK to length-prefix them and stores the bytes without
/// interpreting them.
#[derive(Debug, Clone)]
pub struct LengthPrefixCoder;

impl BeamCoder<Vec<u8>> for LengthPrefixCoder {
    fn encode(&self, val: Vec<u8>, buf: &mut impl BufMut) {
        encode_varint(val.len() as u64, buf);
        buf.put_slice(val.as_slice());
    }

    fn decode(&self, buf: &mut impl Buf) -> Result<Vec<u8>, CodersError> {
        let len = decode_varint(buf) as usize;
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        Ok(bytes)
    }
}

#[derive(Debug, Clone)]
pub struct VoidCoder;

impl BeamCoder<()> for VoidCoder {
    fn encode(&self, _value: (), _buf: &mut impl BufMut) {
        // Void encodes as nothing.
    }

    fn decode(&self, _buf: &mut impl Buf) -> Result<(), CodersError> {
        // Void encodes as zero bytes - nothing to read.
        Ok(())
    }
}

/// A Beam element together with its event-time and window metadata.
#[derive(Debug, Clone)]
pub struct WindowedValue {
    /// The logical element payload.
    pub value: BeamRecord,
    /// Event timestamp in milliseconds since the Unix epoch.
    pub timestamp_millis: i64,
    /// Windows assigned to the element by the SDK.
    pub windows: Vec<BeamWindow>,
    /// Beam trigger/pane metadata.
    pub pane: PaneInfo,
}

impl WindowedValue {
    /// Creates a value in the global window with Beam's minimum timestamp.
    pub fn global(value: BeamRecord) -> Self {
        Self {
            value,
            timestamp_millis: BEAM_MIN_TIMESTAMP_MILLIS,
            windows: vec![BeamWindow::Global],
            pane: PaneInfo::no_firing(),
        }
    }
}

/// A Beam window carried by a [`WindowedValue`].
///
/// Interval bounds are expressed in milliseconds since the Unix epoch and use
/// Beam's half-open interval convention: `[start_millis, end_millis)`.
///
/// Derives `serde` so a window can be round-tripped through the JSON-encoded
/// window metadata persisted by the element store, and `Hash`/`Eq` so it can
/// participate in a grouping key (for example `GroupByKey`'s `(key, window)`
/// identity).
#[derive(Debug, Clone, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum BeamWindow {
    /// The single global window.
    Global,
    /// A half-open interval window.
    Interval { start_millis: i64, end_millis: i64 },
}

impl BeamWindow {
    /// The maximum timestamp this window can hold, in milliseconds since the Unix
    /// epoch, mirroring Beam's `BoundedWindow.maxTimestamp()`.
    ///
    /// Interval bounds are half-open `[start, end)`, so the last instant inside the
    /// window is `end - 1`. The global window's max timestamp is the Beam constant
    /// `GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS`, which is deliberately smaller than
    /// [`BEAM_MAX_TIMESTAMP_MILLIS`].
    pub fn max_timestamp_millis(&self) -> i64 {
        match self {
            BeamWindow::Global => GLOBAL_WINDOW_MAX_TIMESTAMP_MILLIS,
            BeamWindow::Interval { end_millis, .. } => end_millis - 1,
        }
    }

    /// A compact, deterministic string identity for this window.
    ///
    /// The element store persists one such key per window in the reserved
    /// `__flare_window_key` column so runner-native transforms (for example
    /// `GroupByKey`) can group by window in the query engine instead of decoding
    /// the JSON metadata per element. Use [`Self::from_canonical_key`] to recover
    /// the window.
    pub fn canonical_key(&self) -> String {
        match self {
            BeamWindow::Global => "global".to_string(),
            BeamWindow::Interval {
                start_millis,
                end_millis,
            } => format!("interval:{start_millis}:{end_millis}"),
        }
    }

    /// Parses a key produced by [`Self::canonical_key`] back into a window.
    ///
    /// Returns `None` for an unrecognized key rather than guessing, so callers
    /// surface malformed stored metadata instead of silently mis-grouping.
    pub fn from_canonical_key(key: &str) -> Option<Self> {
        if key == "global" {
            return Some(BeamWindow::Global);
        }
        let bounds = key.strip_prefix("interval:")?;
        let (start, end) = bounds.split_once(':')?;
        Some(BeamWindow::Interval {
            start_millis: start.parse().ok()?,
            end_millis: end.parse().ok()?,
        })
    }
}

/// Beam pane timing, mirroring the portable `PaneInfo` timing enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

/// Beam pane information carried by a [`WindowedValue`].
///
/// Fields mirror the portable `PaneInfo` message and describe how a particular
/// firing of a window relates to the whole (first/last, timing, and indices).
/// Derives `serde` so it can round-trip through the store's JSON window
/// metadata.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

    /// A single on-time firing that both opens and closes the window's triggering.
    ///
    /// Used when a runner-native transform (for example `GroupByKey`) emits exactly
    /// one aggregated pane per window: the output is the first and last pane and is
    /// produced on time rather than speculatively.
    pub fn on_time_firing() -> Self {
        Self {
            is_first: true,
            is_last: true,
            timing: PaneTiming::OnTime,
            index: 0,
            non_speculative_index: 0,
        }
    }
}

/// The Beam window coder selected from a PCollection's coder graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowCoder {
    /// `beam:coder:global_window:v1`.
    Global,
    /// `beam:coder:interval_window:v1`.
    Interval,
}

impl WindowCoder {
    /// Resolves a Beam window-coder URN.
    ///
    /// # Panics
    ///
    /// Panics when the URN is not a supported Beam window coder.
    pub fn from_urn(urn: &str) -> Self {
        match urn {
            crate::jobservice::urns::beam_urns::GLOBAL_WINDOW_CODER => Self::Global,
            crate::jobservice::urns::beam_urns::INTERVAL_WINDOW_CODER => Self::Interval,
            _ => panic!("Unsupported Beam window coder: {urn}"),
        }
    }
}

/// Encodes and decodes Beam `WindowedValue` framing around an element coder.
///
/// The window coder is part of the Beam `windowed_value:v1` coder graph and
/// must match the windows carried by each value. Use [`Self::new`] when FlareDB
/// intentionally creates global-window values, or [`Self::with_window_coder`]
/// when resolving an existing PCollection boundary.
#[derive(Debug, Clone)]
pub struct WindowedValueCoder {
    element_coder: StandardBeamCoders,
    window_coder: WindowCoder,
}

impl WindowedValueCoder {
    /// Creates a coder for values that FlareDB creates in the global window.
    pub fn new(element_coder: StandardBeamCoders) -> Self {
        Self::with_window_coder(element_coder, WindowCoder::Global)
    }

    /// Creates a coder using the window coder resolved from the Beam graph.
    pub fn with_window_coder(element_coder: StandardBeamCoders, window_coder: WindowCoder) -> Self {
        Self {
            element_coder,
            window_coder,
        }
    }

    /// Encodes a new element as a global-window `WindowedValue`.
    ///
    /// This helper is for newly created FlareDB values. Existing window,
    /// timestamp, and pane metadata must be encoded with [`Self::encode`].
    pub fn encode_value(&self, value: BeamRecord, buf: &mut impl BufMut) {
        let windowed_value = WindowedValue::global(value.clone());
        self.encode(windowed_value, buf);
    }
}

impl BeamCoder<WindowedValue> for WindowedValueCoder {
    fn encode(&self, val: WindowedValue, buf: &mut impl BufMut) {
        encode_timestamp_millis(val.timestamp_millis, buf);
        encode_windows(&val.windows, self.window_coder, buf);
        encode_pane_info(&val.pane, buf);
        self.element_coder.encode(val.value, buf);
    }

    fn decode(&self, buf: &mut impl Buf) -> Result<WindowedValue, CodersError> {
        let timestamp_millis = decode_timestamp_millis(buf);
        let windows = decode_windows(buf, self.window_coder);
        let pane = decode_pane_info(buf);
        let value = self.element_coder.decode_nested(buf)?;

        Ok(WindowedValue {
            value,
            timestamp_millis,
            windows,
            pane,
        })
    }
}

/// A decoded Beam user timer (`beam:coder:timer:v1`).
///
/// The key is kept as raw encoded bytes so it can be re-emitted verbatim when
/// the timer fires; the windows are decoded so the runner can reason about
/// event-time expiry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Timer {
    pub user_key: Vec<u8>,
    pub tag: String,
    pub windows: Vec<BeamWindow>,
    pub clear: bool,
    pub fire_timestamp: i64,
    pub hold_timestamp: i64,
    pub pane: PaneInfo,
}

/// Encodes and decodes `beam:coder:timer:v1`.
///
/// Wire form: `key (key coder), tag (nested string), windows (window coder),
/// clear bit (1 byte)`, then, when set, `fire timestamp, hold timestamp, pane`.
/// `key_coder` is the timer family's key coder and `window_coder` its window
/// coder, resolved from the pipeline's `TimerFamilySpec`.
#[derive(Debug, Clone)]
pub struct TimerCoder {
    key_coder: StandardBeamCoders,
    window_coder: WindowCoder,
}

impl TimerCoder {
    pub fn new(key_coder: StandardBeamCoders, window_coder: WindowCoder) -> Self {
        Self {
            key_coder,
            window_coder,
        }
    }

    /// Decode one timer from `buf`, advancing it past the timer.
    pub fn decode(&self, buf: &mut impl Buf) -> Result<Timer, CodersError> {
        // Decode the key, then re-encode it in nested form: the timer's own
        // framing stores the key bytes verbatim, so a fired timer can emit them
        // without the decoder needing to track reader positions.
        let key = self
            .key_coder
            .decode(buf)
            .map_err(|e| CodersError::WhileDecoding(format!("timer key: {e}")))?;
        let mut user_key = Vec::new();
        self.key_coder.encode(key, &mut user_key);

        let tag_len = decode_varint(buf) as usize;
        let mut tag_bytes = vec![0u8; tag_len];
        buf.copy_to_slice(&mut tag_bytes);
        let tag = String::from_utf8(tag_bytes)
            .map_err(|e| CodersError::WhileDecoding(format!("timer tag: {e}")))?;

        let windows = decode_windows(buf, self.window_coder);
        let clear = buf.get_u8() != 0;

        let (fire_timestamp, hold_timestamp, pane) = if clear {
            (0, 0, PaneInfo::no_firing())
        } else {
            (
                decode_timestamp_millis(buf),
                decode_timestamp_millis(buf),
                decode_pane_info(buf),
            )
        };

        Ok(Timer {
            user_key,
            tag,
            windows,
            clear,
            fire_timestamp,
            hold_timestamp,
            pane,
        })
    }

    /// Encode a timer in the `beam:coder:timer:v1` wire form.
    pub fn encode_into(&self, timer: &Timer, buf: &mut impl BufMut) {
        buf.put_slice(&timer.user_key);
        encode_varint(timer.tag.len() as u64, buf);
        buf.put_slice(timer.tag.as_bytes());
        encode_windows(&timer.windows, self.window_coder, buf);
        buf.put_u8(u8::from(timer.clear));
        if !timer.clear {
            encode_timestamp_millis(timer.fire_timestamp, buf);
            encode_timestamp_millis(timer.hold_timestamp, buf);
            encode_pane_info(&timer.pane, buf);
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

fn encode_windows(windows: &[BeamWindow], coder: WindowCoder, buf: &mut impl BufMut) {
    buf.put_i32(windows.len() as i32);

    for window in windows {
        match (coder, window) {
            (WindowCoder::Global, BeamWindow::Global) => {}
            (
                WindowCoder::Interval,
                BeamWindow::Interval {
                    start_millis,
                    end_millis,
                },
            ) => {
                // IntervalWindowCoder encodes the end timestamp followed by the
                // non-negative span (end - start), both in Beam's timestamp/varint format.
                encode_timestamp_millis(*end_millis, buf);
                encode_varint((*end_millis - *start_millis) as u64, buf);
            }
            _ => panic!("Window value does not match the configured Beam window coder"),
        }
    }
}

fn decode_windows(buf: &mut impl Buf, coder: WindowCoder) -> Vec<BeamWindow> {
    let count = buf.get_i32();
    let mut windows = Vec::new();

    if count >= 0 {
        for _ in 0..count {
            windows.push(decode_window(buf, coder));
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
            windows.push(decode_window(buf, coder));
        }
    }

    windows
}

fn decode_window(buf: &mut impl Buf, coder: WindowCoder) -> BeamWindow {
    match coder {
        WindowCoder::Global => BeamWindow::Global,
        WindowCoder::Interval => {
            let end_millis = decode_timestamp_millis(buf);
            let span_millis = decode_varint(buf) as i64;
            BeamWindow::Interval {
                start_millis: end_millis - span_millis,
                end_millis,
            }
        }
    }
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

pub(crate) fn encode_signed_varint(value: i64, buf: &mut impl BufMut) {
    encode_varint(value as u64, buf);
}

pub(crate) fn decode_signed_varint(buf: &mut impl Buf) -> i64 {
    decode_varint(buf) as i64
}

pub(crate) fn encode_varint(mut value: u64, buf: &mut impl BufMut) {
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

pub(crate) fn decode_varint(buf: &mut impl Buf) -> u64 {
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

    use crate::store::record::{
        BeamGbk, BeamKV, BeamRecord, IterableValue, PrimitiveValue, TupleValue,
    };

    fn roundtrip(coder: &StandardBeamCoders, value: BeamRecord) -> BeamRecord {
        let mut buf = BytesMut::new();

        coder.encode(value.clone(), &mut buf);

        let mut bytes = buf.freeze();
        coder.decode(&mut bytes).unwrap()
    }

    fn str_value(s: &str) -> PrimitiveValue {
        PrimitiveValue::String(s.to_string())
    }

    fn bytes_value(v: &[u8]) -> PrimitiveValue {
        PrimitiveValue::Bytes(v.to_vec())
    }

    fn records(values: Vec<PrimitiveValue>) -> Vec<BeamRecord> {
        values.into_iter().map(BeamRecord::PRIMITIVE).collect()
    }

    #[test]
    fn string_utf8_roundtrip() {
        let coder = StandardBeamCoders::StringUtf8(StringUtf8Coder);

        let decoded = roundtrip(&coder, BeamRecord::PRIMITIVE(str_value("hello beam")));

        match decoded {
            BeamRecord::PRIMITIVE(PrimitiveValue::String(v)) => {
                assert_eq!(v, "hello beam");
            }
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn bytes_roundtrip() {
        let coder = StandardBeamCoders::Bytes(BytesCoder);

        let decoded = roundtrip(&coder, BeamRecord::PRIMITIVE(bytes_value(&[1, 2, 3, 4, 5])));

        match decoded {
            BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(v)) => {
                assert_eq!(v, vec![1, 2, 3, 4, 5]);
            }
            _ => panic!("expected bytes"),
        }
    }

    #[test]
    fn varint_roundtrip() {
        let coder = StandardBeamCoders::VarInt(VarIntCoder);

        let values = [0, 1, -1, 42, 1000, i64::MAX, i64::MIN + 1];

        for value in values {
            let decoded = roundtrip(&coder, BeamRecord::PRIMITIVE(PrimitiveValue::Int64(value)));

            match decoded {
                BeamRecord::PRIMITIVE(PrimitiveValue::Int64(v)) => {
                    assert_eq!(v, value);
                }
                _ => panic!("expected int"),
            }
        }
    }

    #[test]
    fn varint32_roundtrip() {
        let coder = StandardBeamCoders::VarInt32(VarInt32Coder);

        let values = [0, 1, -1, 42, 1000, i32::MAX as i64, i32::MIN as i64];

        for value in values {
            let decoded = roundtrip(&coder, BeamRecord::PRIMITIVE(PrimitiveValue::Int64(value)));

            match decoded {
                BeamRecord::PRIMITIVE(PrimitiveValue::Int64(v)) => {
                    assert_eq!(v, value, "roundtrip mismatch for {value}");
                }
                other => panic!("expected int64, got {other:?}"),
            }
        }
    }

    #[test]
    fn varint32_matches_java_wire_format() {
        // Byte-for-byte the encodings produced by Java's VarIntCoder.
        let coder = VarInt32Coder;
        let cases: [(i64, &[u8]); 6] = [
            (0, &[0x00]),
            (1, &[0x01]),
            (42, &[0x2A]),
            (127, &[0x7F]),
            (128, &[0x80, 0x01]),
            // Java zero-extends the 32-bit pattern: -1 -> 0xFFFFFFFF (5 bytes).
            (-1, &[0xFF, 0xFF, 0xFF, 0xFF, 0x0F]),
        ];

        for (value, expected) in cases {
            let mut buf = BytesMut::new();
            coder.encode(value, &mut buf);
            assert_eq!(buf.as_ref(), expected, "encoding mismatch for {value}");

            let mut bytes = buf.freeze();
            assert_eq!(coder.decode(&mut bytes).unwrap(), value);
        }
    }

    #[test]
    fn varint32_agrees_with_varlong_for_non_negative_values() {
        // Both coders share the LEB128 algorithm for non-negative values.
        let varlong = StandardBeamCoders::VarInt(VarIntCoder);
        let varint32 = StandardBeamCoders::VarInt32(VarInt32Coder);

        for value in [0i64, 1, 127, 128, 1000, i32::MAX as i64] {
            let record = BeamRecord::PRIMITIVE(PrimitiveValue::Int64(value));

            let mut a = BytesMut::new();
            varlong.encode(record.clone(), &mut a);
            let mut b = BytesMut::new();
            varint32.encode(record, &mut b);

            assert_eq!(a.as_ref(), b.as_ref(), "mismatch for {value}");
        }
    }

    #[test]
    fn varint32_rejects_out_of_range_value() {
        // 2^32 = 0x1_0000_0000 -> LEB128 80 80 80 80 10
        let mut bytes = Bytes::from_static(&[0x80, 0x80, 0x80, 0x80, 0x10]);
        assert!(VarInt32Coder.decode(&mut bytes).is_err());
    }

    #[test]
    fn bool_roundtrip() {
        let coder = StandardBeamCoders::Bool(BoolCoder);

        for value in [true, false] {
            let decoded = roundtrip(&coder, BeamRecord::PRIMITIVE(PrimitiveValue::Bool(value)));

            match decoded {
                BeamRecord::PRIMITIVE(PrimitiveValue::Bool(v)) => {
                    assert_eq!(v, value);
                }
                _ => panic!("expected bool"),
            }
        }
    }

    #[test]
    fn void_roundtrip() {
        let coder = StandardBeamCoders::Void(VoidCoder);

        let decoded = roundtrip(&coder, BeamRecord::PRIMITIVE(PrimitiveValue::Void));

        assert!(matches!(
            decoded,
            BeamRecord::PRIMITIVE(PrimitiveValue::Void)
        ));
    }

    #[test]
    fn is_void_only_for_void_coder() {
        assert!(StandardBeamCoders::Void(VoidCoder).is_void());
        assert!(!StandardBeamCoders::Bytes(BytesCoder).is_void());
        assert!(
            !StandardBeamCoders::Iterable(IterableCoder::new(StandardBeamCoders::Void(VoidCoder)))
                .is_void()
        );
    }

    #[test]
    fn opaque_void_frames_slice_back_to_exact_wire_bytes() {
        // The opaque path locates each VoidCoder element frame by decoding the
        // WindowedValue framing, then preserves the original bytes untouched.
        // Reassembling those slices must reproduce the input wire bytes exactly.
        let coder = WindowedValueCoder::new(StandardBeamCoders::Void(VoidCoder));

        let mut buf = BytesMut::new();
        coder.encode_value(BeamRecord::PRIMITIVE(PrimitiveValue::Void), &mut buf);
        coder.encode_value(BeamRecord::PRIMITIVE(PrimitiveValue::Void), &mut buf);
        coder.encode_value(BeamRecord::PRIMITIVE(PrimitiveValue::Void), &mut buf);
        let wire = buf.freeze();

        let mut cursor = std::io::Cursor::new(&wire[..]);
        let mut frames = Vec::new();
        while (cursor.position() as usize) < wire.len() {
            let start = cursor.position() as usize;
            coder.decode(&mut cursor).unwrap();
            let end = cursor.position() as usize;
            frames.push(wire[start..end].to_vec());
        }

        assert_eq!(frames.len(), 3);
        assert_eq!(frames.concat(), wire.to_vec());
    }

    #[test]
    fn iterable_varint_roundtrip() {
        let coder = StandardBeamCoders::Iterable(IterableCoder::new(StandardBeamCoders::VarInt(
            VarIntCoder,
        )));

        let original = BeamRecord::ITERABLE(IterableValue::new(vec![
            PrimitiveValue::Int64(1),
            PrimitiveValue::Int64(2),
            PrimitiveValue::Int64(3),
            PrimitiveValue::Int64(100),
        ]));

        let decoded = roundtrip(&coder, original);

        match decoded {
            BeamRecord::ITERABLE(values) => {
                assert_eq!(
                    values.list,
                    records(vec![
                        PrimitiveValue::Int64(1),
                        PrimitiveValue::Int64(2),
                        PrimitiveValue::Int64(3),
                        PrimitiveValue::Int64(100),
                    ])
                );
            }
            _ => panic!("expected iterable"),
        }
    }

    #[test]
    fn empty_iterable_roundtrip() {
        let coder = StandardBeamCoders::Iterable(IterableCoder::new(
            StandardBeamCoders::StringUtf8(StringUtf8Coder),
        ));

        let original = BeamRecord::ITERABLE(IterableValue { list: Vec::new() });

        let decoded = roundtrip(&coder, original);

        match decoded {
            BeamRecord::ITERABLE(values) => {
                assert!(values.list.is_empty());
            }
            _ => panic!("expected iterable"),
        }
    }

    #[test]
    fn tuple_roundtrip() {
        let coder = StandardBeamCoders::Tuple(TupleCoder::new(vec![
            StandardBeamCoders::StringUtf8(StringUtf8Coder),
            StandardBeamCoders::VarInt(VarIntCoder),
            StandardBeamCoders::Double(DoubleCoder),
        ]));

        let original = BeamRecord::TUPLE(TupleValue::new(vec![
            str_value("hello"),
            PrimitiveValue::Int64(42),
            PrimitiveValue::Float64(1.5),
        ]));

        let decoded = roundtrip(&coder, original);

        match decoded {
            BeamRecord::TUPLE(values) => {
                assert_eq!(
                    values.values.as_slice(),
                    &[
                        str_value("hello"),
                        PrimitiveValue::Int64(42),
                        PrimitiveValue::Float64(1.5),
                    ]
                );
            }
            _ => panic!("expected tuple"),
        }
    }

    #[test]
    fn tuple_with_nullable_component_roundtrip() {
        // Mirrors the reshuffle metadata tuple: `(bytes, nullable[opaque], opaque)`.
        // Regression test for encoding a tuple whose component coder is composite
        // (`Nullable` / `length_prefix`) rather than a bare primitive.
        let coder = StandardBeamCoders::Tuple(TupleCoder::new(vec![
            StandardBeamCoders::Bytes(BytesCoder),
            StandardBeamCoders::Nullable(NullableCoder::new(StandardBeamCoders::LengthPrefix(
                LengthPrefixCoder,
            ))),
            StandardBeamCoders::LengthPrefix(LengthPrefixCoder),
        ]));

        // Present nullable component.
        let present = roundtrip(
            &coder,
            BeamRecord::TUPLE(TupleValue::new(vec![
                PrimitiveValue::Bytes(b"key".to_vec()),
                PrimitiveValue::Bytes(b"window".to_vec()),
                PrimitiveValue::Bytes(b"pane".to_vec()),
            ])),
        );
        match present {
            BeamRecord::TUPLE(values) => assert_eq!(
                values.values,
                vec![
                    PrimitiveValue::Bytes(b"key".to_vec()),
                    PrimitiveValue::Bytes(b"window".to_vec()),
                    PrimitiveValue::Bytes(b"pane".to_vec()),
                ]
            ),
            other => panic!("expected tuple, got {other:?}"),
        }

        // Null nullable component.
        let null = roundtrip(
            &coder,
            BeamRecord::TUPLE(TupleValue::new(vec![
                PrimitiveValue::Bytes(b"key".to_vec()),
                PrimitiveValue::Void,
                PrimitiveValue::Bytes(b"pane".to_vec()),
            ])),
        );
        match null {
            BeamRecord::TUPLE(values) => assert_eq!(
                values.values,
                vec![
                    PrimitiveValue::Bytes(b"key".to_vec()),
                    PrimitiveValue::Void,
                    PrimitiveValue::Bytes(b"pane".to_vec()),
                ]
            ),
            other => panic!("expected tuple, got {other:?}"),
        }
    }

    #[test]
    fn pickle_roundtrip_preserves_raw_bytes() {
        let coder = StandardBeamCoders::Pickle(PickleCoder);

        // A byte sequence that begins with a pickle protocol-5 opcode stream.
        let payload = vec![0x80, 0x05, 0x95, 0x00, 0x01, 0x00, 0x00, 0x00, 0x2e];
        let decoded = roundtrip(
            &coder,
            BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(payload.clone())),
        );

        match decoded {
            BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(v)) => {
                assert_eq!(v, payload);
            }
            _ => panic!("expected bytes"),
        }
    }

    #[test]
    fn nullable_roundtrip_matches_standard_coder() {
        // Matches standard_coders.yaml for `beam:coder:nullable:v1` with a bytes
        // component: "\u0001\u0003abc" -> "abc", "\u0000" -> null.
        let coder =
            StandardBeamCoders::Nullable(NullableCoder::new(StandardBeamCoders::Bytes(BytesCoder)));

        // null encodes as a single 0x00 byte.
        let mut buf = BytesMut::new();
        coder.encode(BeamRecord::PRIMITIVE(PrimitiveValue::Void), &mut buf);
        assert_eq!(buf.as_ref(), b"\x00");

        // present "abc" encodes as 0x01 followed by the nested bytes coder.
        let mut buf = BytesMut::new();
        coder.encode(
            BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(b"abc".to_vec())),
            &mut buf,
        );
        assert_eq!(buf.as_ref(), b"\x01\x03abc");

        // decoding the yaml example yields "abc".
        let mut present = Bytes::from_static(b"\x01\x03abc");
        match coder.decode(&mut present).unwrap() {
            BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(v)) => assert_eq!(v, b"abc"),
            other => panic!("expected bytes, got {other:?}"),
        }

        // decoding the null example yields Void.
        let mut null = Bytes::from_static(b"\x00");
        assert!(matches!(
            coder.decode(&mut null).unwrap(),
            BeamRecord::PRIMITIVE(PrimitiveValue::Void)
        ));
    }

    #[test]
    fn kv_roundtrip() {
        let coder = StandardBeamCoders::Kv(
            Box::new(StandardBeamCoders::StringUtf8(StringUtf8Coder)),
            Box::new(StandardBeamCoders::VarInt(VarIntCoder)),
        );

        let original = BeamRecord::KV(BeamKV {
            key: str_value("user-1"),
            value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Int64(42))),
        });

        let decoded = roundtrip(&coder, original);

        match decoded {
            BeamRecord::KV(kv) => {
                assert_eq!(kv.key, str_value("user-1"));
                assert!(matches!(
                    *kv.value,
                    BeamRecord::PRIMITIVE(PrimitiveValue::Int64(42))
                ));
            }
            _ => panic!("expected kv"),
        }
    }

    #[test]
    fn gbk_roundtrip() {
        let coder = StandardBeamCoders::Gbk(
            Box::new(StandardBeamCoders::StringUtf8(StringUtf8Coder)),
            IterableCoder::new(StandardBeamCoders::VarInt(VarIntCoder)),
        );

        let original = BeamRecord::GBK(BeamGbk {
            key: str_value("group"),
            value: IterableValue::new(vec![
                PrimitiveValue::Int64(10),
                PrimitiveValue::Int64(20),
                PrimitiveValue::Int64(30),
            ]),
        });

        let decoded = roundtrip(&coder, original);

        match decoded {
            BeamRecord::GBK(gbk) => {
                assert_eq!(gbk.key, str_value("group"));

                assert_eq!(
                    gbk.value.list,
                    records(vec![
                        PrimitiveValue::Int64(10),
                        PrimitiveValue::Int64(20),
                        PrimitiveValue::Int64(30),
                    ])
                );
            }
            _ => panic!("expected gbk"),
        }
    }

    #[test]
    fn windowed_value_roundtrip() {
        let coder = WindowedValueCoder::new(StandardBeamCoders::VarInt(VarIntCoder));

        let original = WindowedValue::global(BeamRecord::PRIMITIVE(PrimitiveValue::Int64(12345)));

        let mut buf = BytesMut::new();
        coder.encode(original.clone(), &mut buf);

        let mut bytes = buf.freeze();
        let decoded = coder.decode(&mut bytes).unwrap();

        assert_eq!(decoded.timestamp_millis, original.timestamp_millis);
        assert_eq!(decoded.windows, original.windows);
        assert_eq!(decoded.pane.is_first, original.pane.is_first);
        assert_eq!(decoded.pane.is_last, original.pane.is_last);

        match decoded.value {
            BeamRecord::PRIMITIVE(PrimitiveValue::Int64(v)) => {
                assert_eq!(v, 12345);
            }
            _ => panic!("expected int"),
        }
    }

    #[test]
    fn interval_window_roundtrip_preserves_windowed_value_metadata() {
        let coder = WindowedValueCoder::with_window_coder(
            StandardBeamCoders::VarInt(VarIntCoder),
            WindowCoder::Interval,
        );
        let original = WindowedValue {
            value: BeamRecord::PRIMITIVE(PrimitiveValue::Int64(12345)),
            timestamp_millis: 17_000,
            windows: vec![BeamWindow::Interval {
                start_millis: 0,
                end_millis: 60_000,
            }],
            pane: PaneInfo {
                is_first: false,
                is_last: true,
                timing: PaneTiming::OnTime,
                index: 3,
                non_speculative_index: 3,
            },
        };

        let mut buf = BytesMut::new();
        coder.encode(original.clone(), &mut buf);
        // IntervalWindowCoder is end timestamp followed by unsigned span.
        assert_eq!(
            &buf[8..23],
            &[
                0, 0, 0, 1, 0x80, 0, 0, 0, 0, 0, 0xea, 0x60, 0xe0, 0xd4, 0x03,
            ]
        );

        let mut bytes = buf.freeze();
        let decoded = coder.decode(&mut bytes).unwrap();
        assert_eq!(decoded.timestamp_millis, original.timestamp_millis);
        assert_eq!(decoded.windows, original.windows);
        assert_eq!(decoded.pane, original.pane);
        assert_eq!(decoded.value, original.value);
    }

    #[test]
    fn interval_window_matches_standard_coders_yaml_wire_example() {
        let mut encoded = BytesMut::new();
        encode_windows(
            &[BeamWindow::Interval {
                start_millis: 1454293425000 - 3_600_000,
                end_millis: 1454293425000,
            }],
            WindowCoder::Interval,
            &mut encoded,
        );
        assert_eq!(
            encoded.as_ref(),
            &[
                0, 0, 0, 1, 0x80, 0x00, 0x01, 0x52, 0x9a, 0xa4, 0x9b, 0x68, 0x80, 0xdd, 0xdb, 0x01,
            ]
        );

        let mut bytes = encoded.freeze();
        assert_eq!(
            decode_windows(&mut bytes, WindowCoder::Interval),
            vec![BeamWindow::Interval {
                start_millis: 1454293425000 - 3_600_000,
                end_millis: 1454293425000,
            }]
        );
    }

    #[test]
    fn interval_window_coder_resolves_from_beam_urn() {
        assert_eq!(
            WindowCoder::from_urn(crate::jobservice::urns::beam_urns::INTERVAL_WINDOW_CODER),
            WindowCoder::Interval
        );
        assert_eq!(
            WindowCoder::from_urn(crate::jobservice::urns::beam_urns::GLOBAL_WINDOW_CODER),
            WindowCoder::Global
        );
    }

    #[test]
    fn multiple_interval_windows_roundtrip() {
        let coder = WindowedValueCoder::with_window_coder(
            StandardBeamCoders::VarInt(VarIntCoder),
            WindowCoder::Interval,
        );
        let original = WindowedValue {
            value: BeamRecord::PRIMITIVE(PrimitiveValue::Int64(1)),
            timestamp_millis: 1,
            windows: vec![
                BeamWindow::Interval {
                    start_millis: 0,
                    end_millis: 60_000,
                },
                BeamWindow::Interval {
                    start_millis: 120_000,
                    end_millis: 180_000,
                },
            ],
            pane: PaneInfo::no_firing(),
        };
        let mut buf = BytesMut::new();
        coder.encode(original.clone(), &mut buf);
        let mut bytes = buf.freeze();
        let decoded = coder.decode(&mut bytes).unwrap();
        assert_eq!(decoded.windows, original.windows);
    }

    #[test]
    fn encode_value_helper() {
        let coder = WindowedValueCoder::new(StandardBeamCoders::StringUtf8(StringUtf8Coder));

        let mut buf = BytesMut::new();

        coder.encode_value(BeamRecord::PRIMITIVE(str_value("beam")), &mut buf);

        let mut bytes = buf.freeze();
        let decoded = coder.decode(&mut bytes).unwrap();

        match decoded.value {
            BeamRecord::PRIMITIVE(PrimitiveValue::String(s)) => {
                assert_eq!(s, "beam");
            }
            _ => panic!("expected string"),
        }
    }

    #[test]
    fn iterable_unknown_length_encoding() {
        let coder = IterableCoder::new(StandardBeamCoders::VarInt(VarIntCoder));

        let values = vec![
            PrimitiveValue::Int64(1),
            PrimitiveValue::Int64(2),
            PrimitiveValue::Int64(3),
        ];

        let mut buf = BytesMut::new();
        coder.encode(records(values.clone()), &mut buf);

        let mut bytes = buf.freeze();
        let decoded = coder.decode(&mut bytes).unwrap();

        assert_eq!(decoded, records(values));
    }
}

#[cfg(test)]
mod beam_wire_tests {
    use super::*;

    use bytes::{Bytes, BytesMut};

    use crate::store::record::{BeamKV, BeamRecord, PrimitiveValue};

    fn str_value(s: &str) -> PrimitiveValue {
        PrimitiveValue::String(s.to_string())
    }

    fn records(values: Vec<PrimitiveValue>) -> Vec<BeamRecord> {
        values.into_iter().map(BeamRecord::PRIMITIVE).collect()
    }

    /*  fn bytes_value(v: &[u8]) -> PrimitiveValue {
        PrimitiveValue::Bytes(v.to_vec())
    }*/

    #[test]
    fn beam_varint_wire_encode() {
        let coder = VarIntCoder;

        let cases = [
            (0, vec![0x00]),
            (1, vec![0x01]),
            (10, vec![0x0A]),
            (200, vec![0xC8, 0x01]),
            (1000, vec![0xE8, 0x07]),
            (
                -1,
                vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01],
            ),
        ];

        for (value, expected) in cases {
            let mut buf = BytesMut::new();
            coder.encode(value, &mut buf);

            assert_eq!(buf.as_ref(), expected.as_slice());
        }
    }

    #[test]
    fn beam_varint_wire_decode() {
        let coder = VarIntCoder;

        let cases = [
            (vec![0x00], 0),
            (vec![0x01], 1),
            (vec![0x0A], 10),
            (vec![0xC8, 0x01], 200),
            (vec![0xE8, 0x07], 1000),
            (
                vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x01],
                -1,
            ),
        ];

        for (encoded, expected) in cases {
            let mut bytes = Bytes::from(encoded);
            assert_eq!(coder.decode(&mut bytes).unwrap(), expected);
        }
    }

    #[test]
    fn beam_bool_wire_encode() {
        let coder = BoolCoder;

        let mut buf = BytesMut::new();
        coder.encode(true, &mut buf);
        assert_eq!(buf.as_ref(), &[0x01]);

        buf.clear();

        coder.encode(false, &mut buf);
        assert_eq!(buf.as_ref(), &[0x00]);
    }

    #[test]
    fn beam_bool_wire_decode() {
        let coder = BoolCoder;

        let mut bytes = Bytes::from_static(&[0x01]);
        assert!(coder.decode(&mut bytes).unwrap());

        let mut bytes = Bytes::from_static(&[0x00]);
        assert!(!coder.decode(&mut bytes).unwrap());
    }

    #[test]
    fn beam_string_wire_encode() {
        let coder = StringUtf8Coder;

        let mut buf = BytesMut::new();
        coder.encode("abc".to_string(), &mut buf);

        assert_eq!(buf.as_ref(), &[0x03, b'a', b'b', b'c']);
    }

    #[test]
    fn beam_string_wire_decode() {
        let coder = StringUtf8Coder;

        let mut bytes = Bytes::from_static(&[0x03, b'a', b'b', b'c']);

        assert_eq!(coder.decode(&mut bytes).unwrap(), "abc");
    }

    #[test]
    fn beam_bytes_wire_encode() {
        let coder = BytesCoder;

        let mut buf = BytesMut::new();
        coder.encode(vec![1, 2, 3], &mut buf);

        assert_eq!(buf.as_ref(), &[0x03, 1, 2, 3]);
    }

    #[test]
    fn beam_bytes_wire_decode() {
        let coder = BytesCoder;

        let mut bytes = Bytes::from_static(&[0x03, 1, 2, 3]);

        assert_eq!(coder.decode(&mut bytes).unwrap(), vec![1, 2, 3]);
    }

    #[test]
    fn beam_kv_wire_encode() {
        let coder = StandardBeamCoders::Kv(
            Box::new(StandardBeamCoders::StringUtf8(StringUtf8Coder)),
            Box::new(StandardBeamCoders::VarInt(VarIntCoder)),
        );

        let mut buf = BytesMut::new();

        coder.encode(
            BeamRecord::KV(BeamKV {
                key: str_value("abc"),
                value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Int64(10))),
            }),
            &mut buf,
        );

        assert_eq!(buf.as_ref(), &[0x03, b'a', b'b', b'c', 0x0A,]);
    }

    #[test]
    fn beam_kv_wire_decode() {
        let coder = StandardBeamCoders::Kv(
            Box::new(StandardBeamCoders::StringUtf8(StringUtf8Coder)),
            Box::new(StandardBeamCoders::VarInt(VarIntCoder)),
        );

        let mut bytes = Bytes::from_static(&[0x03, b'a', b'b', b'c', 0x0A]);

        match coder.decode(&mut bytes).unwrap() {
            BeamRecord::KV(kv) => {
                assert_eq!(kv.key, str_value("abc"));
                assert!(matches!(
                    *kv.value,
                    BeamRecord::PRIMITIVE(PrimitiveValue::Int64(10))
                ));
            }
            _ => panic!("expected kv"),
        }
    }

    #[test]
    fn beam_iterable_wire_encode() {
        let coder = IterableCoder::new(StandardBeamCoders::VarInt(VarIntCoder));

        let values = vec![
            PrimitiveValue::Int64(1),
            PrimitiveValue::Int64(10),
            PrimitiveValue::Int64(200),
            PrimitiveValue::Int64(1000),
        ];

        let mut buf = BytesMut::new();
        coder.encode(records(values), &mut buf);

        assert_eq!(
            buf.as_ref(),
            &[0x00, 0x00, 0x00, 0x04, 0x01, 0x0A, 0xC8, 0x01, 0xE8, 0x07,]
        );
    }

    #[test]
    fn beam_iterable_wire_decode() {
        let coder = IterableCoder::new(StandardBeamCoders::VarInt(VarIntCoder));

        let mut bytes =
            Bytes::from_static(&[0x00, 0x00, 0x00, 0x04, 0x01, 0x0A, 0xC8, 0x01, 0xE8, 0x07]);

        let decoded = coder.decode(&mut bytes).unwrap();

        assert_eq!(
            decoded,
            records(vec![
                PrimitiveValue::Int64(1),
                PrimitiveValue::Int64(10),
                PrimitiveValue::Int64(200),
                PrimitiveValue::Int64(1000),
            ])
        );
    }
}

#[cfg(test)]
mod timer_coder_tests {
    use super::*;

    fn coder() -> TimerCoder {
        TimerCoder::new(
            StandardBeamCoders::StringUtf8(StringUtf8Coder),
            WindowCoder::Global,
        )
    }

    /// The set-timer example from Beam's `standard_coders.yaml`.
    fn set_timer_bytes() -> Vec<u8> {
        vec![
            3, b'k', b'e', b'y', 3, b't', b'a', b'g', 0, 0, 0, 1, 0, 0x80, 0, 0, 0, 0, 0, 0x04,
            0xd2, 0x80, 0, 0, 0, 0, 0, 0x16, 0x2e, 0x0f,
        ]
    }

    #[test]
    fn decodes_a_set_timer() {
        let bytes = set_timer_bytes();
        let mut buf: &[u8] = &bytes;
        let timer = coder().decode(&mut buf).unwrap();

        assert!(!timer.clear);
        assert_eq!(timer.tag, "tag");
        assert_eq!(timer.user_key, vec![3, b'k', b'e', b'y']);
        assert_eq!(timer.windows, vec![BeamWindow::Global]);
        assert_eq!(timer.fire_timestamp, 1234);
        assert_eq!(timer.hold_timestamp, 5678);
        assert_eq!(timer.pane.timing, PaneTiming::Unknown);
        assert!(timer.pane.is_first && timer.pane.is_last);
        assert!(
            buf.is_empty(),
            "decoder must consume exactly the timer bytes"
        );
    }

    #[test]
    fn decodes_a_clear_timer() {
        let bytes: Vec<u8> = vec![3, b'k', b'e', b'y', 3, b't', b'a', b'g', 0, 0, 0, 1, 1];
        let mut buf: &[u8] = &bytes;
        let timer = coder().decode(&mut buf).unwrap();

        assert!(timer.clear);
        assert_eq!(timer.tag, "tag");
        assert!(buf.is_empty());
    }

    #[test]
    fn encode_round_trips_the_wire_bytes() {
        let bytes = set_timer_bytes();
        let mut buf: &[u8] = &bytes;
        let timer = coder().decode(&mut buf).unwrap();

        let mut out = Vec::new();
        coder().encode_into(&timer, &mut out);
        assert_eq!(out, bytes);
    }
}
