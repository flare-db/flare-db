pub mod primitives;
pub mod row;
pub mod schema;

use crate::store::record::{BeamGbk, BeamKV, BeamRecord, IterableValue, TupleValue};
use crate::{
    coders::primitives::{
        BoolCoder, BytesCoder, DoubleCoder, IterableCoder, LengthPrefixCoder, NullableCoder,
        PickleCoder, StringUtf8Coder, TupleCoder, VarInt32Coder, VarIntCoder, VoidCoder,
    },
    jobservice::urns::beam_urns,
    store::record::PrimitiveValue,
    utils::errors::CodersError,
};
use beam_model_rs::v1::{Coder, FunctionSpec};
use bytes::{Buf, BufMut};
use log::debug;
use std::collections::{HashMap, HashSet};

/// Encodes a value to Beam wire bytes and back.
///
/// Coders are composed: composite coders call their components, so a coder used
/// inside another must be self-delimiting on decode.
pub trait BeamCoder<T> {
    fn encode(&self, val: T, buf: &mut impl BufMut);
    fn decode(&self, buf: &mut impl Buf) -> Result<T, CodersError>;
}

/// The Beam coders the runner resolves from a pipeline.
///
/// Ported from Beam's `standard_coders.yaml` where a standard coder exists.
/// Coders that only ever carry opaque Python bytes (`Pickle`, `LengthPrefix`)
/// are stored as [`PrimitiveValue::Bytes`]; `Tuple`, `Nullable` and the
/// recursive `Iterable`/`Kv`/`Gbk` forms map onto the [`BeamRecord`] model.
#[derive(Debug, Clone)]
pub enum StandardBeamCoders {
    StringUtf8(StringUtf8Coder),
    Bytes(BytesCoder),
    VarInt(VarIntCoder),
    /// Java's 32-bit `VarIntCoder`,
    VarInt32(VarInt32Coder),
    Bool(BoolCoder),
    Double(DoubleCoder),
    Void(VoidCoder),
    Iterable(IterableCoder),
    /// `beam:coder:nullable:v1` (`typing.Optional`).
    Nullable(NullableCoder),
    /// `beam:coder:length_prefix:v1`; self-delimits an opaque leaf so it can be
    /// stored without interpreting it (see [`length_prefix_pickled_leaves`]).
    LengthPrefix(LengthPrefixCoder),
    /// `beam:coder:pickled_python:v1`, used directly as an element coder.
    Pickle(PickleCoder),
    /// `beam:coder:tuple:v1` — fixed-arity, possibly heterogeneous tuple.
    Tuple(TupleCoder),
    Kv(Box<StandardBeamCoders>, Box<StandardBeamCoders>),
    Gbk(Box<StandardBeamCoders>, IterableCoder),
}

impl StandardBeamCoders {
    /// Resolve a coder id/URN into a [`StandardBeamCoders`].
    ///
    /// `id` is looked up in `pipeline_coders` first, because coder ids are not
    /// unique to a URN (several Python coders share `pickled_python`). Composite
    /// coders resolve their `component_coder_ids` recursively.
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

        debug!("Resolving coder: id={}, urn={}", id, urn);

        let component_coder_ids = component_coder_ids
            .or_else(|| pipeline_coder.map(|coder| coder.component_coder_ids.clone()));

        match urn {
            beam_urns::BYTES_CODER => StandardBeamCoders::Bytes(BytesCoder),
            beam_urns::STRING_UTF8_CODER => StandardBeamCoders::StringUtf8(StringUtf8Coder),
            beam_urns::VARINT_CODER => StandardBeamCoders::VarInt(VarIntCoder),
            beam_urns::BOOL_CODER => StandardBeamCoders::Bool(BoolCoder),
            beam_urns::DOUBLE_CODER => StandardBeamCoders::Double(DoubleCoder),
            beam_urns::PYTHON_PICKLE_CODER => StandardBeamCoders::Pickle(PickleCoder),
            beam_urns::LENGTH_PREFIX_CODER => StandardBeamCoders::LengthPrefix(LengthPrefixCoder),
            beam_urns::JAVA_SDK_CODER => match id {
                "VoidCoder" => StandardBeamCoders::Void(VoidCoder),
                "VarIntCoder" => StandardBeamCoders::VarInt32(VarInt32Coder),
                _ => StandardBeamCoders::Bytes(BytesCoder),
            },
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
            beam_urns::TUPLE_CODER => {
                let ids = component_coder_ids
                    .as_ref()
                    .expect("TupleCoder requires component coder ids");
                assert!(
                    !ids.is_empty(),
                    "TupleCoder requires at least one component coder"
                );

                let component_coders = ids
                    .iter()
                    .map(|component_id| {
                        StandardBeamCoders::from_urn(component_id, None, pipeline_coders)
                    })
                    .collect();

                StandardBeamCoders::Tuple(TupleCoder::new(component_coders))
            }
            beam_urns::NULLABLE_CODER => {
                let ids = component_coder_ids
                    .as_ref()
                    .expect("NullableCoder requires one component coder id");
                assert_eq!(
                    ids.len(),
                    1,
                    "NullableCoder requires exactly one component coder"
                );

                StandardBeamCoders::Nullable(NullableCoder::new(StandardBeamCoders::from_urn(
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

                let key_coder = StandardBeamCoders::from_urn(&ids[0], None, pipeline_coders);
                let val_coder = StandardBeamCoders::from_urn(&ids[1], None, pipeline_coders);

                match val_coder {
                    StandardBeamCoders::Iterable(iterable_coder) => {
                        StandardBeamCoders::Gbk(Box::new(key_coder), iterable_coder)
                    }
                    _ => StandardBeamCoders::Kv(Box::new(key_coder), Box::new(val_coder)),
                }
                /*let ids = component_coder_ids
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
                )*/
            }
            _ => panic!("Unknown URN: {}", urn),
        }
    }

    /// Whether this is the Beam `VoidCoder`.
    ///
    /// A `VoidCoder` element has a zero-byte payload, but the surrounding
    /// `WindowedValue` still carries framing (timestamp, windows, pane) whose
    /// exact wire representation must be preserved. Such PCollections are
    /// therefore treated opaquely: their encoded `WindowedValue` bytes are
    /// stored and forwarded without ever materializing a [`BeamRecord`].
    pub fn is_void(&self) -> bool {
        matches!(self, StandardBeamCoders::Void(_))
    }

    /// Whether decoding this coder always yields a [`BeamRecord::PRIMITIVE`].
    ///
    /// A KV/GBK key may be any coder. Primitive keys are stored in their typed
    /// [`PrimitiveValue`] slot; composite keys (KV, GBK, iterable, tuple, or a
    /// nullable wrapping one) cannot be, so the runner keeps their nested encoding
    /// as opaque [`PrimitiveValue::Bytes`] — enough for structural grouping and a
    /// faithful round-trip, which is all the runner needs of a key.
    fn is_primitive(&self) -> bool {
        match self {
            StandardBeamCoders::Nullable(coder) => coder.inner().is_primitive(),
            StandardBeamCoders::Iterable(_)
            | StandardBeamCoders::Tuple(_)
            | StandardBeamCoders::Kv(_, _)
            | StandardBeamCoders::Gbk(_, _) => false,
            _ => true,
        }
    }

    /// Decode a KV/GBK **key** (Beam encodes it in nested, self-delimiting context).
    fn decode_key(&self, buf: &mut impl Buf) -> Result<PrimitiveValue, CodersError> {
        if self.is_primitive() {
            return self.decode_primitive(buf);
        }
        // Composite key: consume it and retain its nested encoding as opaque bytes.
        let record = self.decode_nested(buf)?;
        let mut raw = bytes::BytesMut::new();
        self.encode(record, &mut raw);
        Ok(PrimitiveValue::Bytes(raw.to_vec()))
    }

    /// Encode a KV/GBK **key**; the inverse of [`Self::decode_key`].
    fn encode_key(&self, key: &PrimitiveValue, buf: &mut impl BufMut) {
        if self.is_primitive() {
            self.encode_primitive(key.clone(), buf);
            return;
        }
        match key {
            PrimitiveValue::Bytes(raw) => buf.put_slice(raw),
            other => panic!("composite key coder requires opaque bytes, got {other:?}"),
        }
    }

    fn decode_primitive(&self, buf: &mut impl Buf) -> Result<PrimitiveValue, CodersError> {
        self.decode_nested(buf)?
            .get_primitive()
            .map_err(|e| CodersError::WhileDecoding(e.to_string()))
    }

    /// Encode in nested Beam coder context. Fn Data sends concatenated
    /// WindowedValue payloads, so the element inside each WindowedValue must be
    /// self-delimiting when the element coder requires it (bytes/string/KV).
    pub fn encode(&self, record: BeamRecord, buf: &mut impl BufMut) {
        //self.encode_nested(val, buf);
        match (self, record) {
            (StandardBeamCoders::Iterable(_), BeamRecord::ITERABLE(v)) => {
                self.encode_iterable(v, buf);
            }

            (StandardBeamCoders::Tuple(coder), BeamRecord::TUPLE(v)) => {
                coder.encode(v.values, buf);
            }

            (StandardBeamCoders::Nullable(coder), record) => {
                coder.encode(record, buf);
            }

            (StandardBeamCoders::Kv(key_coder, value_coder), BeamRecord::KV(kv)) => {
                key_coder.encode_key(&kv.key, buf);
                value_coder.encode(*kv.value, buf);
            }

            (StandardBeamCoders::Gbk(key_coder, value_coder), BeamRecord::GBK(kv)) => {
                key_coder.encode_key(&kv.key, buf);
                value_coder.encode(kv.value.list, buf);
            }

            (_, BeamRecord::PRIMITIVE(v)) => {
                self.encode_primitive(v, buf);
            }

            _ => panic!("Mismatched coder"),
        }
    }

    fn encode_iterable(&self, value: IterableValue, buf: &mut impl BufMut) {
        match self {
            StandardBeamCoders::Iterable(coder) => coder.encode(value.list, buf),
            _ => panic!("Expected iterable coder"),
        }
    }
    fn encode_primitive(&self, value: PrimitiveValue, buf: &mut impl BufMut) {
        match (self, value) {
            (StandardBeamCoders::StringUtf8(coder), PrimitiveValue::String(s)) => {
                coder.encode(s, buf)
            }
            (StandardBeamCoders::Bytes(coder), PrimitiveValue::Bytes(bytes)) => {
                coder.encode(bytes, buf)
            }
            (StandardBeamCoders::VarInt(coder), PrimitiveValue::Int64(value)) => {
                coder.encode(value, buf)
            }
            (StandardBeamCoders::VarInt32(coder), PrimitiveValue::Int64(value)) => {
                coder.encode(value, buf)
            }
            (StandardBeamCoders::Bool(coder), PrimitiveValue::Bool(value)) => {
                coder.encode(value, buf)
            }
            (StandardBeamCoders::Double(coder), PrimitiveValue::Float64(value)) => {
                coder.encode(value, buf)
            }
            (StandardBeamCoders::Void(coder), PrimitiveValue::Void) => coder.encode((), buf),
            (StandardBeamCoders::Pickle(coder), PrimitiveValue::Bytes(bytes)) => {
                coder.encode(bytes, buf)
            }
            (StandardBeamCoders::LengthPrefix(coder), PrimitiveValue::Bytes(bytes)) => {
                coder.encode(bytes, buf)
            }
            _ => panic!("Mismatched coder: {:?}", std::any::type_name::<Self>()),
        }
    }
    /// Decode in nested Beam coder context.
    pub fn decode(&self, buf: &mut impl Buf) -> Result<BeamRecord, CodersError> {
        self.decode_nested(buf)
    }

    /// Decode one element in the **non-nested** (top-level element) context.
    ///
    /// Beam's standard coders distinguish nested from non-nested encoding (see
    /// `standard_coders.yaml`): a length-prefixed `string_utf8`/`bytes` is nested,
    /// raw is non-nested; a `KvCoder` always length-prefixes its key but encodes its
    /// value in the *same* nesting as the KV. PCollection elements inside a
    /// `WindowedValue` are nested (what [`Self::decode`] handles); a `TestStream`
    /// payload encodes each element in the non-nested context, where the value is
    /// the last field and runs to the end of the buffer.
    pub fn decode_element(&self, buf: &mut impl Buf) -> Result<BeamRecord, CodersError> {
        match self {
            StandardBeamCoders::StringUtf8(_) => {
                let bytes = buf.copy_to_bytes(buf.remaining());
                let value = String::from_utf8(bytes.to_vec()).map_err(|err| {
                    CodersError::WhileDecoding(format!("invalid utf8 element: {err}"))
                })?;
                Ok(BeamRecord::PRIMITIVE(PrimitiveValue::String(value)))
            }
            StandardBeamCoders::Bytes(_) => {
                let bytes = buf.copy_to_bytes(buf.remaining());
                Ok(BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(bytes.to_vec())))
            }
            StandardBeamCoders::Kv(key_coder, value_coder) => Ok(BeamRecord::KV(BeamKV {
                key: key_coder.decode_key(buf)?,
                value: Box::new(value_coder.decode_element(buf)?),
            })),
            StandardBeamCoders::Gbk(key_coder, value_coder) => Ok(BeamRecord::GBK(BeamGbk {
                key: key_coder.decode_key(buf)?,
                value: IterableValue {
                    list: value_coder.decode(buf)?,
                },
            })),
            // Self-delimiting (varint/bool/double) and nested composites (iterable,
            // tuple, nullable, length_prefix, pickle) encode identically non-nested.
            _ => self.decode_nested(buf),
        }
    }

    fn decode_nested(&self, buf: &mut impl Buf) -> Result<BeamRecord, CodersError> {
        match self {
            StandardBeamCoders::StringUtf8(coder) => Ok(BeamRecord::PRIMITIVE(
                PrimitiveValue::String(coder.decode(buf)?),
            )),
            StandardBeamCoders::Bytes(coder) => Ok(BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(
                coder.decode(buf)?,
            ))),
            StandardBeamCoders::VarInt(coder) => Ok(BeamRecord::PRIMITIVE(PrimitiveValue::Int64(
                coder.decode(buf)?,
            ))),
            StandardBeamCoders::VarInt32(coder) => Ok(BeamRecord::PRIMITIVE(
                PrimitiveValue::Int64(coder.decode(buf)?),
            )),
            StandardBeamCoders::Bool(coder) => Ok(BeamRecord::PRIMITIVE(PrimitiveValue::Bool(
                coder.decode(buf)?,
            ))),
            StandardBeamCoders::Double(coder) => Ok(BeamRecord::PRIMITIVE(
                PrimitiveValue::Float64(coder.decode(buf)?),
            )),
            StandardBeamCoders::Void(_) => Ok(BeamRecord::PRIMITIVE(PrimitiveValue::Void)),
            StandardBeamCoders::Iterable(coder) => Ok(BeamRecord::ITERABLE(IterableValue {
                list: coder.decode(buf)?,
            })),
            StandardBeamCoders::Pickle(coder) => Ok(BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(
                coder.decode(buf)?,
            ))),
            StandardBeamCoders::LengthPrefix(coder) => Ok(BeamRecord::PRIMITIVE(
                PrimitiveValue::Bytes(coder.decode(buf)?),
            )),
            StandardBeamCoders::Tuple(coder) => Ok(BeamRecord::TUPLE(TupleValue {
                values: coder.decode(buf)?,
            })),
            StandardBeamCoders::Nullable(coder) => coder.decode(buf),
            StandardBeamCoders::Kv(key_coder, value_coder) => Ok(BeamRecord::KV(BeamKV {
                key: key_coder.decode_key(buf)?,
                value: Box::new(value_coder.decode_nested(buf)?),
            })),
            StandardBeamCoders::Gbk(key_coder, value_coder) => Ok(BeamRecord::GBK(BeamGbk {
                key: key_coder.decode_key(buf)?,
                value: IterableValue {
                    list: value_coder.decode(buf)?,
                },
            })),
        }
    }
}

/// The suffix appended to a coder id to name its length-prefixed wrapper.
fn length_prefixed_suffix() -> &'static str {
    "_lp"
}

/// The id of the length-prefixed wrapper for `id`.
fn length_prefixed_coder_id(id: &str) -> String {
    format!("{id}{}", length_prefixed_suffix())
}

/// Wrap every `beam:coder:pickled_python:v1` leaf coder in a
/// `beam:coder:length_prefix:v1` coder and repoint all component references.
///
/// The Python SDK emits several distinct coders under the single
/// `pickled_python` URN — `PickleCoder` (varint length + pickled bytes),
/// `FastPrimitivesCoder` (a type-marker byte followed by a nested value) and
/// `PaneInfoCoder` — and the runner cannot tell them apart from the URN alone.
/// Guessing wrong desynchronizes the enclosing coder stream. Instead, we ask the
/// SDK to length-prefix these leaves so their bytes become self-delimiting and can
/// be stored opaquely.
///
/// This is idempotent: an already-wrapped leaf resolves to a
/// `beam:coder:length_prefix:v1` coder, which is not wrapped again.
pub fn length_prefix_pickled_leaves(coders: &mut HashMap<String, Coder>) {
    let leaf_ids: Vec<String> = coders
        .iter()
        .filter(|(_, coder)| {
            coder
                .spec
                .as_ref()
                .map_or(false, |spec| spec.urn == beam_urns::PYTHON_PICKLE_CODER)
        })
        .map(|(id, _)| id.clone())
        .collect();

    if leaf_ids.is_empty() {
        return;
    }

    let mut remap: HashMap<String, String> = HashMap::new();
    for id in leaf_ids {
        let lp_id = length_prefixed_coder_id(&id);
        coders.entry(lp_id.clone()).or_insert_with(|| Coder {
            spec: Some(FunctionSpec {
                urn: beam_urns::LENGTH_PREFIX_CODER.to_string(),
                payload: Vec::new(),
            }),
            component_coder_ids: vec![id.clone()],
        });
        remap.insert(id, lp_id);
    }

    // Do not rewrite the wrappers themselves: a wrapper's single component is
    // the original leaf, which must stay unwrapped so the SDK decodes it with
    // its real coder.
    let wrapper_ids: HashSet<String> = remap.values().cloned().collect();
    for (id, coder) in coders.iter_mut() {
        if wrapper_ids.contains(id) {
            continue;
        }
        for component in coder.component_coder_ids.iter_mut() {
            if let Some(lp) = remap.get(component) {
                *component = lp.clone();
            }
        }
    }
}

/// Resolve the coder id the SDK boundary should use for `id`: the
/// length-prefixed wrapper if one exists, otherwise `id` unchanged. Needed for
/// element coders that are themselves opaque leaves (a composite element coder
/// already references its wrapped leaves through the rewritten coders map).
pub fn resolve_length_prefixed_coder_id(id: &str, coders: &HashMap<String, Coder>) -> String {
    let lp_id = length_prefixed_coder_id(id);
    if coders.contains_key(&lp_id) {
        lp_id
    } else {
        id.to_string()
    }
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
            component_coder_ids: components.iter().map(|c| c.to_string()).collect(),
        }
    }

    #[test]
    fn java_sdk_coders_resolve_custom_ids() {
        let mut coders = HashMap::new();
        coders.insert(
            "VoidCoder".to_string(),
            coder(beam_urns::JAVA_SDK_CODER, &[]),
        );
        coders.insert(
            "VarIntCoder".to_string(),
            coder(beam_urns::JAVA_SDK_CODER, &[]),
        );
        coders.insert(
            "SomethingElseCoder".to_string(),
            coder(beam_urns::JAVA_SDK_CODER, &[]),
        );

        assert!(StandardBeamCoders::from_urn("VoidCoder", None, Some(&coders)).is_void());
        assert!(matches!(
            StandardBeamCoders::from_urn("VarIntCoder", None, Some(&coders)),
            StandardBeamCoders::VarInt32(_)
        ));
        // Unrecognized java-sdk coders still fall back to opaque bytes.
        assert!(matches!(
            StandardBeamCoders::from_urn("SomethingElseCoder", None, Some(&coders)),
            StandardBeamCoders::Bytes(_)
        ));
    }

    #[test]
    fn pickled_leaves_are_length_prefixed_and_referenced() {
        let mut coders = HashMap::new();
        coders.insert("bytes".to_string(), coder(beam_urns::BYTES_CODER, &[]));
        coders.insert(
            "fast".to_string(),
            coder(beam_urns::PYTHON_PICKLE_CODER, &[]),
        );
        coders.insert(
            "nullable".to_string(),
            coder(beam_urns::NULLABLE_CODER, &["fast"]),
        );
        coders.insert(
            "tuple".to_string(),
            coder(beam_urns::TUPLE_CODER, &["bytes", "nullable"]),
        );

        length_prefix_pickled_leaves(&mut coders);

        // A length-prefix wrapper is created around the opaque leaf.
        let wrapper = coders.get("fast_lp").expect("expected wrapper coder");
        assert_eq!(
            wrapper.spec.as_ref().unwrap().urn,
            beam_urns::LENGTH_PREFIX_CODER
        );
        assert_eq!(wrapper.component_coder_ids, vec!["fast".to_string()]);

        // The composite now points at the wrapper, not the raw leaf.
        assert_eq!(
            coders["nullable"].component_coder_ids,
            vec!["fast_lp".to_string()]
        );
        // Unrelated references are left untouched.
        assert_eq!(
            coders["tuple"].component_coder_ids,
            vec!["bytes".to_string(), "nullable".to_string()]
        );

        // Rewriting is idempotent.
        length_prefix_pickled_leaves(&mut coders);
        assert_eq!(
            coders["nullable"].component_coder_ids,
            vec!["fast_lp".to_string()]
        );
        assert_eq!(
            coders["fast_lp"].component_coder_ids,
            vec!["fast".to_string()]
        );
    }

    #[test]
    fn kv_element_decodes_the_value_non_nested() {
        // From standard_coders.yaml, `beam:coder:kv:v1` (bytes, bytes):
        //   nested: false -> "\x03abcdef"   (key length-prefixed, value raw)
        //   nested: true  -> "\x03abc\x03def" (both length-prefixed)
        // TestStream elements use the non-nested form; WindowedValue payloads use
        // the nested one.
        let mut coders = HashMap::new();
        coders.insert("b".to_string(), coder(beam_urns::BYTES_CODER, &[]));
        coders.insert("kv".to_string(), coder(beam_urns::KV_CODER, &["b", "b"]));
        let kv = StandardBeamCoders::from_urn("kv", None, Some(&coders));

        let mut buf: &[u8] = b"\x03abcdef";
        match kv.decode_element(&mut buf).unwrap() {
            BeamRecord::KV(v) => {
                assert_eq!(v.key, PrimitiveValue::Bytes(b"abc".to_vec()));
                assert_eq!(
                    *v.value,
                    BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(b"def".to_vec()))
                );
            }
            other => panic!("expected KV, got {other:?}"),
        }
        assert_eq!(buf.remaining(), 0);

        let mut nested: &[u8] = b"\x03abc\x03def";
        match kv.decode(&mut nested).unwrap() {
            BeamRecord::KV(v) => {
                assert_eq!(v.key, PrimitiveValue::Bytes(b"abc".to_vec()));
                assert_eq!(
                    *v.value,
                    BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(b"def".to_vec()))
                );
            }
            other => panic!("expected KV, got {other:?}"),
        }
        assert_eq!(nested.remaining(), 0);
    }

    #[test]
    fn kv_with_composite_key_roundtrips() {
        // Outer KV<KvCoder<bytes,bytes>, varint> — the key is itself a KV, as in
        // Beam's Combine.perKeyWithFanout(AddNonce) output (the Q7 failure).
        let mut coders = HashMap::new();
        coders.insert("b".to_string(), coder(beam_urns::BYTES_CODER, &[]));
        coders.insert("v".to_string(), coder(beam_urns::VARINT_CODER, &[]));
        coders.insert("kv".to_string(), coder(beam_urns::KV_CODER, &["b", "b"]));
        coders.insert(
            "outer".to_string(),
            coder(beam_urns::KV_CODER, &["kv", "v"]),
        );
        let outer = StandardBeamCoders::from_urn("outer", None, Some(&coders));

        // Nested encoding of the composite key KV<bytes,bytes>("a", "b").
        let composite_key = vec![0x01, b'a', 0x01, b'b'];
        let element = BeamRecord::KV(BeamKV {
            key: PrimitiveValue::Bytes(composite_key.clone()),
            value: Box::new(BeamRecord::PRIMITIVE(PrimitiveValue::Int64(7))),
        });

        let mut encoded = Vec::new();
        outer.encode(element, &mut encoded);
        // The composite key is written verbatim (it already is its nested encoding),
        // then the varint value — no extra length prefix around the key.
        assert_eq!(encoded, vec![0x01, b'a', 0x01, b'b', 0x07]);

        let mut buf: &[u8] = &encoded;
        let decoded = outer.decode_nested(&mut buf).unwrap();
        assert_eq!(buf.remaining(), 0);
        match decoded {
            BeamRecord::KV(kv) => {
                assert_eq!(kv.key, PrimitiveValue::Bytes(composite_key));
                assert_eq!(*kv.value, BeamRecord::PRIMITIVE(PrimitiveValue::Int64(7)));
            }
            other => panic!("expected KV, got {other:?}"),
        }
    }

    #[test]
    fn is_primitive_classifies_key_coders() {
        let primitive = StandardBeamCoders::StringUtf8(StringUtf8Coder);
        assert!(primitive.is_primitive());

        let composite = StandardBeamCoders::Kv(
            Box::new(StandardBeamCoders::StringUtf8(StringUtf8Coder)),
            Box::new(StandardBeamCoders::VarInt(VarIntCoder)),
        );
        assert!(!composite.is_primitive());

        // Nullable is primitive iff its inner coder is.
        assert!(StandardBeamCoders::Nullable(NullableCoder::new(primitive)).is_primitive());
        assert!(!StandardBeamCoders::Nullable(NullableCoder::new(composite)).is_primitive());
    }

    #[test]
    fn resolve_length_prefixed_id_picks_wrapper_for_leaves_only() {
        let mut coders = HashMap::new();
        coders.insert(
            "fast".to_string(),
            coder(beam_urns::PYTHON_PICKLE_CODER, &[]),
        );
        coders.insert("bytes".to_string(), coder(beam_urns::BYTES_CODER, &[]));

        length_prefix_pickled_leaves(&mut coders);

        assert_eq!(resolve_length_prefixed_coder_id("fast", &coders), "fast_lp");
        assert_eq!(resolve_length_prefixed_coder_id("bytes", &coders), "bytes");
    }
}
