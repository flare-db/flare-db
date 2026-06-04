use bytes::{Buf, BufMut};
use log::info;

#[derive(Debug, Clone)]
pub enum BeamValue {
    String(String),
    Bytes(Vec<u8>), // Int64(i64), // Easy to expand later
}

pub enum StandardCoders {
    StringUtf8(StringUtf8Coder),
    Bytes(BytesCoder),
}

impl StandardCoders {
    pub fn from_urn(id: &str) -> Self {
        match id {
            "StringUtf8Coder" => StandardCoders::StringUtf8(StringUtf8Coder),
            "ByteArrayCoder" => StandardCoders::Bytes(BytesCoder),
            _ => panic!("Unknown URN: {}", id),
        }
    }

    // Delegate the encoding down to the underlying coder struct
    pub fn encode(&self, val: &BeamValue, buf: &mut impl BufMut) {
        match (self, val) {
            (StandardCoders::StringUtf8(coder), BeamValue::String(s)) => {
                coder.encode(s, buf);
            }
            (StandardCoders::Bytes(coder), BeamValue::Bytes(bytes)) => {
                coder.encode(bytes, buf);
            }
            _ => panic!("Mismatched coder and value type"),
        }
    }

    // Decode bytes out into a structured BeamValue
    pub fn decode(&self, buf: &mut impl Buf) -> BeamValue {
        info!("Starting to decode");
        match self {
            StandardCoders::StringUtf8(coder) => BeamValue::String(coder.decode(buf)),
            StandardCoders::Bytes(coder) => BeamValue::Bytes(coder.decode(buf)),
        }
    }
}

pub trait BeamCoder<T> {
    fn encode(&self, val: &T, buf: &mut impl BufMut);
    fn decode(&self, buf: &mut impl Buf) -> T;
}

pub struct StringUtf8Coder;

impl BeamCoder<String> for StringUtf8Coder {
    fn encode(&self, val: &String, buf: &mut impl BufMut) {
        let utf8 = val.as_bytes();
        encode_varint(utf8.len() as u64, buf);
        buf.put_slice(utf8);
    }

    fn decode(&self, buf: &mut impl Buf) -> String {
        info!("Decodeing String");
        let len = decode_varint(buf) as usize;
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        String::from_utf8(bytes).unwrap()
    }
}

pub struct BytesCoder;

impl BeamCoder<Vec<u8>> for BytesCoder {
    fn encode(&self, val: &Vec<u8>, buf: &mut impl BufMut) {
        encode_varint(val.len() as u64, buf);
        buf.put_slice(val);
    }

    fn decode(&self, buf: &mut impl Buf) -> Vec<u8> {
        info!("decoing bytes");
        let len = decode_varint(buf) as usize;
        let mut bytes = vec![0u8; len];
        buf.copy_to_slice(&mut bytes);
        bytes
    }
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
