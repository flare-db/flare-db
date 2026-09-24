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
use log::info;
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

        info!("Resolving coder: id={}, urn={}", id, urn);

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
                key_coder.encode_primitive(kv.key, buf);
                value_coder.encode(*kv.value, buf);
            }

            (StandardBeamCoders::Gbk(key_coder, value_coder), BeamRecord::GBK(kv)) => {
                key_coder.encode_primitive(kv.key, buf);
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
                key: key_coder.decode_primitive(buf)?,
                value: Box::new(value_coder.decode_nested(buf)?),
            })),
            StandardBeamCoders::Gbk(key_coder, value_coder) => Ok(BeamRecord::GBK(BeamGbk {
                key: key_coder.decode_primitive(buf)?,
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
/// Guessing wrong desynchronizes the enclosing coder stream. Following the
/// Prism runner, we instead ask the SDK to length-prefix these leaves so their
/// bytes become self-delimiting and can be stored opaquely.
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
