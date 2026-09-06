pub mod primitives;

use crate::store::record::{BeamGbk, BeamKV, BeamRecord, IterableValue};
use crate::{
    coders::primitives::{
        BoolCoder, BytesCoder, DoubleCoder, IterableCoder, StringUtf8Coder, VarIntCoder, VoidCoder,
    },
    jobservice::urns::beam_urns,
    store::record::PrimitiveValue,
    utils::errors::CodersError,
};
use beam_model_rs::v1::Coder;
use bytes::{Buf, BufMut};
use log::info;
use std::collections::HashMap;

pub trait BeamCoder<T> {
    fn encode(&self, val: T, buf: &mut impl BufMut);
    fn decode(&self, buf: &mut impl Buf) -> Result<T, CodersError>;
}

#[derive(Debug, Clone)]
pub enum StandardBeamCoders {
    StringUtf8(StringUtf8Coder),
    Bytes(BytesCoder),
    VarInt(VarIntCoder),
    Bool(BoolCoder),
    Double(DoubleCoder),
    Void(VoidCoder),
    Iterable(IterableCoder),
    Kv(Box<StandardBeamCoders>, Box<StandardBeamCoders>),
    Gbk(Box<StandardBeamCoders>, IterableCoder),
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
            beam_urns::DOUBLE_CODER => StandardBeamCoders::Double(DoubleCoder),
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

            (StandardBeamCoders::Kv(key_coder, value_coder), BeamRecord::KV(kv)) => {
                key_coder.encode_primitive(kv.key, buf);
                value_coder.encode_primitive(kv.value, buf);
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
            (StandardBeamCoders::Bool(coder), PrimitiveValue::Bool(value)) => {
                coder.encode(value, buf)
            }
            (StandardBeamCoders::Double(coder), PrimitiveValue::Float64(value)) => {
                coder.encode(value, buf)
            }
            (StandardBeamCoders::Void(coder), PrimitiveValue::Void) => coder.encode((), buf),
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
            StandardBeamCoders::Kv(key_coder, value_coder) => Ok(BeamRecord::KV(BeamKV {
                key: key_coder.decode_primitive(buf)?,
                value: value_coder.decode_primitive(buf)?,
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
