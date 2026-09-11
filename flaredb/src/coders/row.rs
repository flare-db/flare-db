use std::sync::Arc;

use bytes::{Buf, BufMut};

use crate::{
    coders::{
        primitives::{decode_signed_varint, decode_varint, encode_signed_varint, encode_varint},
        schema::{BeamFieldType, BeamSchema, BeamTypeKind},
    },
    utils::errors::CodersError,
};

/// A single row based [`BeamSchema`].
///
/// Fields ordered according to the schema definition order(the order of
/// fields in [`BeamSchema::fields`]). Null values are represented as `None`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct BeamRow {
    /// Values for each field in schema definition order.
    pub fields: Vec<Option<FieldValue>>,
}

impl BeamRow {
    /// Construct a `BeamRow` from a vector of optional field values.
    pub fn new(fields: Vec<Option<FieldValue>>) -> Self {
        Self { fields }
    }

    /// Get reference to field value at `index` (in definition order).
    pub fn get(&self, index: usize) -> Option<&Option<FieldValue>> {
        self.fields.get(index)
    }

    /// Set field value at `index` (in definition order).
    pub fn set(&mut self, index: usize, value: Option<FieldValue>) {
        if index < self.fields.len() {
            self.fields[index] = value;
        }
    }
}

/// Representation of a single field value in a [`BeamRow`].
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    /// 1-byte signed integer (`AtomicType::Byte`).
    Byte(i8),
    /// 2-byte big-endian signed integer (`AtomicType::Int16`).
    Int16(i16),
    /// Variable-length signed integer (`AtomicType::Int32`).
    Int32(i32),
    /// Variable-length signed integer (`AtomicType::Int64`).
    Int64(i64),
    /// 4-byte big-endian single precision float (`AtomicType::Float`).
    Float(f32),
    /// 8-byte big-endian double precision float (`AtomicType::Double`).
    Double(f64),
    /// UTF-8 string (`AtomicType::String`).
    String(String),
    /// Boolean flag (`AtomicType::Boolean`).
    Boolean(bool),
    /// Byte sequence (`AtomicType::Bytes`).
    Bytes(Vec<u8>),
    /// Sequence of elements; each element carries its own nullability flag.
    Array(Vec<Option<FieldValue>>),
    /// Variable-length sequence of elements.
    Iterable(Vec<Option<FieldValue>>),
    /// Key-value pairs; keys and values carry their own nullability flags.
    Map(Vec<(Option<FieldValue>, Option<FieldValue>)>),
    /// Nested Beam Row.
    Row(BeamRow),
    /// Logical type wrapping its wire representation value.
    Logical(Box<FieldValue>),
}

/// Beam Row coder
#[derive(Debug, Clone)]
pub struct RowCoder {
    /// The resolved schema used for encoding and decoding.
    pub schema: Arc<BeamSchema>,
}

impl RowCoder {
    /// Create a `RowCoder` for a given [`BeamSchema`].
    pub fn new(schema: Arc<BeamSchema>) -> Self {
        Self { schema }
    }

    /// Create a `RowCoder` by parsing a protobuf-encoded `Schema` payload.
    pub fn from_payload(payload: &[u8]) -> Result<Self, CodersError> {
        let schema = BeamSchema::from_payload(payload)?;
        Ok(Self::new(Arc::new(schema)))
    }

    /// Encode a [`BeamRow`] into a buffer according to `beam:coder:row:v1`.
    pub fn encode(&self, row: &BeamRow, buf: &mut impl BufMut) -> Result<(), CodersError> {
        let num_fields = self.schema.fields.len();

        if row.fields.len() != num_fields {
            return Err(CodersError::WhileEncoding(format!(
                "row field count mismatch: schema expects {}, row has {}",
                num_fields,
                row.fields.len()
            )));
        }

        // 1. Check if any field in encode_order is null
        let has_nulls = self
            .schema
            .encode_order
            .iter()
            .any(|&idx| row.fields[idx].is_none());

        // 2. Write row header: total field count as varint
        encode_varint(num_fields as u64, buf);

        // 3. Write null bitmap header and bytes
        if has_nulls {
            let null_byte_count = (num_fields + 7) / 8;
            encode_varint(null_byte_count as u64, buf);

            let mut null_bitmap = vec![0u8; null_byte_count];
            for (pos, &field_idx) in self.schema.encode_order.iter().enumerate() {
                if row.fields[field_idx].is_none() {
                    let byte_idx = pos / 8;
                    let bit_idx = pos % 8;
                    null_bitmap[byte_idx] |= 1 << bit_idx;
                }
            }
            buf.put_slice(&null_bitmap);
        } else {
            encode_varint(0, buf);
        }

        // 4. Write field values in encode_order
        for &field_idx in &self.schema.encode_order {
            let field_def = &self.schema.fields[field_idx];
            match &row.fields[field_idx] {
                Some(val) => {
                    encode_field_value(&field_def.field_type, val, buf)?;
                }
                None => {
                    if !field_def.field_type.nullable {
                        return Err(CodersError::WhileEncoding(format!(
                            "field '{}' is not nullable but row value is null",
                            field_def.name
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Decode a [`BeamRow`] from a buffer according to `beam:coder:row:v1`.
    pub fn decode(&self, buf: &mut impl Buf) -> Result<BeamRow, CodersError> {
        let num_fields = self.schema.fields.len();

        let header_field_count = decode_varint(buf) as usize;
        if header_field_count != num_fields {
            return Err(CodersError::WhileDecoding(format!(
                "row field count mismatch: schema expects {}, wire header had {}",
                num_fields, header_field_count
            )));
        }

        let null_byte_count = decode_varint(buf) as usize;
        let null_bitmap = if null_byte_count > 0 {
            if buf.remaining() < null_byte_count {
                return Err(CodersError::WhileDecoding(
                    "insufficient bytes for null bitmap".to_string(),
                ));
            }
            let mut bitmap = vec![0u8; null_byte_count];
            buf.copy_to_slice(&mut bitmap);
            Some(bitmap)
        } else {
            None
        };

        // Determine null status per field position in encode_order
        let mut is_null = vec![false; num_fields];
        if let Some(bitmap) = &null_bitmap {
            for pos in 0..num_fields {
                let byte_idx = pos / 8;
                let bit_idx = pos % 8;
                if byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0 {
                    is_null[pos] = true;
                }
            }
        }

        // Initialize empty row fields (in schema definition order)
        let mut fields: Vec<Option<FieldValue>> = vec![None; num_fields];

        // Decode fields in encode_order
        for (pos, &field_idx) in self.schema.encode_order.iter().enumerate() {
            let field_def = &self.schema.fields[field_idx];
            if is_null[pos] {
                if !field_def.field_type.nullable {
                    return Err(CodersError::WhileDecoding(format!(
                        "field '{}' marked null in wire bitmap but schema states non-nullable",
                        field_def.name
                    )));
                }
                fields[field_idx] = None;
            } else {
                let val = decode_field_value(&field_def.field_type, buf)?;
                fields[field_idx] = Some(val);
            }
        }

        Ok(BeamRow { fields })
    }
}

// Encoding / Decoding

fn encode_field_value(
    ft: &BeamFieldType,
    val: &FieldValue,
    buf: &mut impl BufMut,
) -> Result<(), CodersError> {
    match (&ft.kind, val) {
        (BeamTypeKind::Byte, FieldValue::Byte(v)) => buf.put_i8(*v),
        (BeamTypeKind::Int16, FieldValue::Int16(v)) => buf.put_i16(*v),
        (BeamTypeKind::Int32, FieldValue::Int32(v)) => encode_signed_varint(*v as i64, buf),
        (BeamTypeKind::Int64, FieldValue::Int64(v)) => encode_signed_varint(*v, buf),
        (BeamTypeKind::Float, FieldValue::Float(v)) => buf.put_f32(*v),
        (BeamTypeKind::Double, FieldValue::Double(v)) => buf.put_f64(*v),
        (BeamTypeKind::String, FieldValue::String(v)) => {
            let bytes = v.as_bytes();
            encode_varint(bytes.len() as u64, buf);
            buf.put_slice(bytes);
        }
        (BeamTypeKind::Boolean, FieldValue::Boolean(v)) => buf.put_u8(if *v { 1 } else { 0 }),
        (BeamTypeKind::Bytes, FieldValue::Bytes(v)) => {
            encode_varint(v.len() as u64, buf);
            buf.put_slice(v);
        }
        (BeamTypeKind::Array(elem_ft), FieldValue::Array(items)) => {
            buf.put_i32(items.len() as i32);
            for item in items {
                encode_element_value(elem_ft, item.as_ref(), buf)?;
            }
        }
        (BeamTypeKind::Iterable(elem_ft), FieldValue::Iterable(items)) => {
            buf.put_i32(items.len() as i32);
            for item in items {
                encode_element_value(elem_ft, item.as_ref(), buf)?;
            }
        }
        (BeamTypeKind::Map(key_ft, val_ft), FieldValue::Map(entries)) => {
            buf.put_i32(entries.len() as i32);
            for (k, v) in entries {
                encode_element_value(key_ft, k.as_ref(), buf)?;
                encode_element_value(val_ft, v.as_ref(), buf)?;
            }
        }
        (BeamTypeKind::Row(sub_schema), FieldValue::Row(sub_row)) => {
            let coder = RowCoder::new(sub_schema.clone());
            coder.encode(sub_row, buf)?;
        }
        (BeamTypeKind::Logical { repr, .. }, FieldValue::Logical(inner)) => {
            encode_field_value(repr, inner, buf)?;
        }
        (BeamTypeKind::Logical { repr, .. }, inner_val) => {
            encode_field_value(repr, inner_val, buf)?;
        }
        (expected_kind, actual_val) => {
            return Err(CodersError::WhileEncoding(format!(
                "type mismatch: expected {:?}, got {:?}",
                expected_kind, actual_val
            )));
        }
    }
    Ok(())
}

fn encode_element_value(
    elem_ft: &BeamFieldType,
    val: Option<&FieldValue>,
    buf: &mut impl BufMut,
) -> Result<(), CodersError> {
    if elem_ft.nullable {
        match val {
            Some(v) => {
                buf.put_u8(1);
                encode_field_value(elem_ft, v, buf)?;
            }
            None => {
                buf.put_u8(0);
            }
        }
    } else {
        match val {
            Some(v) => encode_field_value(elem_ft, v, buf)?,
            None => {
                return Err(CodersError::WhileEncoding(
                    "null element for non-nullable element type".to_string(),
                ));
            }
        }
    }
    Ok(())
}

fn decode_field_value(ft: &BeamFieldType, buf: &mut impl Buf) -> Result<FieldValue, CodersError> {
    match &ft.kind {
        BeamTypeKind::Byte => {
            if buf.remaining() < 1 {
                return Err(CodersError::WhileDecoding(
                    "insufficient bytes for Byte".into(),
                ));
            }
            Ok(FieldValue::Byte(buf.get_i8()))
        }
        BeamTypeKind::Int16 => {
            if buf.remaining() < 2 {
                return Err(CodersError::WhileDecoding(
                    "insufficient bytes for Int16".into(),
                ));
            }
            Ok(FieldValue::Int16(buf.get_i16()))
        }
        BeamTypeKind::Int32 => {
            let val = decode_signed_varint(buf) as i32;
            Ok(FieldValue::Int32(val))
        }
        BeamTypeKind::Int64 => {
            let val = decode_signed_varint(buf);
            Ok(FieldValue::Int64(val))
        }
        BeamTypeKind::Float => {
            if buf.remaining() < 4 {
                return Err(CodersError::WhileDecoding(
                    "insufficient bytes for Float".into(),
                ));
            }
            Ok(FieldValue::Float(buf.get_f32()))
        }
        BeamTypeKind::Double => {
            if buf.remaining() < 8 {
                return Err(CodersError::WhileDecoding(
                    "insufficient bytes for Double".into(),
                ));
            }
            Ok(FieldValue::Double(buf.get_f64()))
        }
        BeamTypeKind::String => {
            let len = decode_varint(buf) as usize;
            if buf.remaining() < len {
                return Err(CodersError::WhileDecoding(
                    "insufficient bytes for String".into(),
                ));
            }
            let mut bytes = vec![0u8; len];
            buf.copy_to_slice(&mut bytes);
            let s = String::from_utf8(bytes).map_err(|e| {
                CodersError::WhileDecoding(format!("invalid UTF-8 string in Row: {e}"))
            })?;
            Ok(FieldValue::String(s))
        }
        BeamTypeKind::Boolean => {
            if buf.remaining() < 1 {
                return Err(CodersError::WhileDecoding(
                    "insufficient bytes for Boolean".into(),
                ));
            }
            Ok(FieldValue::Boolean(buf.get_u8() != 0))
        }
        BeamTypeKind::Bytes => {
            let len = decode_varint(buf) as usize;
            if buf.remaining() < len {
                return Err(CodersError::WhileDecoding(
                    "insufficient bytes for Bytes".into(),
                ));
            }
            let mut bytes = vec![0u8; len];
            buf.copy_to_slice(&mut bytes);
            Ok(FieldValue::Bytes(bytes))
        }
        BeamTypeKind::Array(elem_ft) => {
            if buf.remaining() < 4 {
                return Err(CodersError::WhileDecoding(
                    "insufficient bytes for Array length".into(),
                ));
            }
            let count = buf.get_i32();
            if count < 0 {
                return Err(CodersError::WhileDecoding("negative Array length".into()));
            }
            let mut items = Vec::with_capacity(count as usize);
            for _ in 0..count {
                items.push(decode_element_value(elem_ft, buf)?);
            }
            Ok(FieldValue::Array(items))
        }
        BeamTypeKind::Iterable(elem_ft) => {
            if buf.remaining() < 4 {
                return Err(CodersError::WhileDecoding(
                    "insufficient bytes for Iterable length".into(),
                ));
            }
            let count = buf.get_i32();
            let mut items = Vec::new();
            if count >= 0 {
                items.reserve(count as usize);
                for _ in 0..count {
                    items.push(decode_element_value(elem_ft, buf)?);
                }
            } else {
                loop {
                    let chunk_count = decode_varint(buf) as usize;
                    if chunk_count == 0 {
                        break;
                    }
                    for _ in 0..chunk_count {
                        items.push(decode_element_value(elem_ft, buf)?);
                    }
                }
            }
            Ok(FieldValue::Iterable(items))
        }
        BeamTypeKind::Map(key_ft, val_ft) => {
            if buf.remaining() < 4 {
                return Err(CodersError::WhileDecoding(
                    "insufficient bytes for Map count".into(),
                ));
            }
            let count = buf.get_i32();
            if count < 0 {
                return Err(CodersError::WhileDecoding("negative Map size".into()));
            }
            let mut entries = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let k = decode_element_value(key_ft, buf)?;
                let v = decode_element_value(val_ft, buf)?;
                entries.push((k, v));
            }
            Ok(FieldValue::Map(entries))
        }
        BeamTypeKind::Row(sub_schema) => {
            let coder = RowCoder::new(sub_schema.clone());
            let sub_row = coder.decode(buf)?;
            Ok(FieldValue::Row(sub_row))
        }
        BeamTypeKind::Logical { repr, .. } => {
            let inner = decode_field_value(repr, buf)?;
            Ok(FieldValue::Logical(Box::new(inner)))
        }
    }
}

fn decode_element_value(
    elem_ft: &BeamFieldType,
    buf: &mut impl Buf,
) -> Result<Option<FieldValue>, CodersError> {
    if elem_ft.nullable {
        if buf.remaining() < 1 {
            return Err(CodersError::WhileDecoding(
                "insufficient bytes for nullable element flag".into(),
            ));
        }
        let is_present = buf.get_u8() != 0;
        if is_present {
            let val = decode_field_value(elem_ft, buf)?;
            Ok(Some(val))
        } else {
            Ok(None)
        }
    } else {
        let val = decode_field_value(elem_ft, buf)?;
        Ok(Some(val))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn test_non_nullable_row_vector() {
        // Schema: str: STRING, i32: INT32, f64: DOUBLE, arr: ARRAY<STRING>
        let payload = b"\n\t\n\x03str\x1a\x02\x10\x07\n\t\n\x03i32\x1a\x02\x10\x03\n\t\n\x03f64\x1a\x02\x10\x06\n\r\n\x03arr\x1a\x06\x1a\x04\n\x02\x10\x07\x12$4e5e554c-d4c1-4a5d-b5e1-f3293a6b9f05";
        let coder = RowCoder::from_payload(payload).unwrap();

        let row = BeamRow::new(vec![
            Some(FieldValue::String("foo".into())),
            Some(FieldValue::Int32(9001)),
            Some(FieldValue::Double(0.1)),
            Some(FieldValue::Array(vec![
                Some(FieldValue::String("foo".into())),
                Some(FieldValue::String("bar".into())),
                Some(FieldValue::String("baz".into())),
            ])),
        ]);

        let mut buf = BytesMut::new();
        coder.encode(&row, &mut buf).unwrap();

        let expected_wire = b"\x04\x00\x03foo\xa9F?\xb9\x99\x99\x99\x99\x99\x9a\x00\x00\x00\x03\x03foo\x03bar\x03baz";
        assert_eq!(buf.as_ref(), expected_wire);

        let decoded = coder.decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded, row);
    }

    #[test]
    fn test_nullable_row_vector() {
        // Schema: str: nullable STRING, i32: nullable INT32, f64: nullable DOUBLE
        let payload = b"\n\x0b\n\x03str\x1a\x04\x08\x01\x10\x07\n\x0b\n\x03i32\x1a\x04\x08\x01\x10\x03\n\x0b\n\x03f64\x1a\x04\x08\x01\x10\x06\x12$b20c6545-57af-4bc8-b2a9-51ace21c7393";
        let coder = RowCoder::from_payload(payload).unwrap();

        // 1. All null
        let all_null_row = BeamRow::new(vec![None, None, None]);
        let mut buf = BytesMut::new();
        coder.encode(&all_null_row, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), b"\x03\x01\x07");

        let decoded = coder.decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded, all_null_row);

        // 2. Partial null: {str: "foo", i32: 9001, f64: null}
        let partial_null_row = BeamRow::new(vec![
            Some(FieldValue::String("foo".into())),
            Some(FieldValue::Int32(9001)),
            None,
        ]);
        let mut buf = BytesMut::new();
        coder.encode(&partial_null_row, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), b"\x03\x01\x04\x03foo\xa9F");

        let decoded = coder.decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded, partial_null_row);

        // 3. No nulls: {str: "foo", i32: 9001, f64: 0.1}
        let no_null_row = BeamRow::new(vec![
            Some(FieldValue::String("foo".into())),
            Some(FieldValue::Int32(9001)),
            Some(FieldValue::Double(0.1)),
        ]);
        let mut buf = BytesMut::new();
        coder.encode(&no_null_row, &mut buf).unwrap();
        assert_eq!(
            buf.as_ref(),
            b"\x03\x00\x03foo\xa9F?\xb9\x99\x99\x99\x99\x99\x9a"
        );

        let decoded = coder.decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded, no_null_row);
    }

    #[test]
    fn test_bool_bytes_row_vector() {
        // Schema: f_bool: BOOLEAN, f_bytes: nullable BYTES
        let payload = b"\n\x0c\n\x06f_bool\x1a\x02\x10\x08\n\x0f\n\x07f_bytes\x1a\x04\x08\x01\x10\t\x12$eea1b747-7571-43d3-aafa-9255afdceafb";
        let coder = RowCoder::from_payload(payload).unwrap();

        // {f_bool: true, f_bytes: null}
        let row1 = BeamRow::new(vec![Some(FieldValue::Boolean(true)), None]);
        let mut buf = BytesMut::new();
        coder.encode(&row1, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), b"\x02\x01\x02\x01");
        let decoded1 = coder.decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded1, row1);

        // {f_bool: false, f_bytes: "ab\0c"}
        let row2 = BeamRow::new(vec![
            Some(FieldValue::Boolean(false)),
            Some(FieldValue::Bytes(b"ab\0c".to_vec())),
        ]);
        let mut buf = BytesMut::new();
        coder.encode(&row2, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), b"\x02\x00\x00\x04ab\x00c");
        let decoded2 = coder.decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded2, row2);
    }

    #[test]
    fn test_map_row_vector() {
        // Schema: f_map: MAP<STRING, nullable INT64>
        let payload = b"\n\x15\n\x05f_map\x1a\x0c*\n\n\x02\x10\x07\x12\x04\x08\x01\x10\x04\x12$d8c8f969-14e6-457f-a8b5-62a1aec7f1cd";
        let coder = RowCoder::from_payload(payload).unwrap();

        // Empty map
        let empty_map_row = BeamRow::new(vec![Some(FieldValue::Map(vec![]))]);
        let mut buf = BytesMut::new();
        coder.encode(&empty_map_row, &mut buf).unwrap();
        assert_eq!(buf.as_ref(), b"\x01\x00\x00\x00\x00\x00");
        let decoded = coder.decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded, empty_map_row);

        // Map with values
        let map_row = BeamRow::new(vec![Some(FieldValue::Map(vec![
            (
                Some(FieldValue::String("foo".into())),
                Some(FieldValue::Int64(9001)),
            ),
            (
                Some(FieldValue::String("bar".into())),
                Some(FieldValue::Int64(i64::MAX)),
            ),
        ]))]);
        let mut buf = BytesMut::new();
        coder.encode(&map_row, &mut buf).unwrap();
        assert_eq!(
            buf.as_ref(),
            b"\x01\x00\x00\x00\x00\x02\x03foo\x01\xa9F\x03bar\x01\xff\xff\xff\xff\xff\xff\xff\xff\x7f"
        );
        let decoded = coder.decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded, map_row);
    }

    #[test]
    fn test_reordered_fields_vector() {
        // Schema with encoding_positions_set = true
        // Fields in proto: str (pos 1), f_bool (pos 2), i32 (pos 0, nullable)
        let payload = b"\n\x0b\n\x03str\x1a\x02\x10\x07(\x01\n\x0e\n\x06f_bool\x1a\x02\x10\x08(\x02\n\x0b\n\x03i32\x1a\x04\x08\x01\x10\x03\x12$30ea5a25-dcd8-4cdb-abeb-5332d15ab4b9 \x01";
        let coder = RowCoder::from_payload(payload).unwrap();

        // Row values in schema definition order: str: "str2", f_bool: false, i32: 21
        let row = BeamRow::new(vec![
            Some(FieldValue::String("str2".into())),
            Some(FieldValue::Boolean(false)),
            Some(FieldValue::Int32(21)),
        ]);

        let mut buf = BytesMut::new();
        coder.encode(&row, &mut buf).unwrap();
        // Encoded wire order: i32 (pos 0) -> \x15, str (pos 1) -> \x04str2, f_bool (pos 2) -> \x00
        assert_eq!(buf.as_ref(), b"\x03\x00\x15\x04str2\x00");

        let decoded = coder.decode(&mut buf.freeze()).unwrap();
        assert_eq!(decoded, row);
    }

    #[test]
    fn test_logical_types_vector() {
        // Schema: f_char: fixed_char(5), f_varchar: var_char(5), f_bytes: fixed_bytes(5), f_varbytes: var_bytes(5)
        let payload = b"\n=\n\x06f_char\x1a3\x08\x01:/\n\x1fbeam:logical_type:fixed_char:v1\x1a\x02\x10\x07\"\x02\x10\x03*\x04\n\x02\x18\x05\nB\n\tf_varchar\x1a1\x08\x01:-\n\x1dbeam:logical_type:var_char:v1\x1a\x02\x10\x07\"\x02\x10\x03*\x04\n\x02\x18\n \x01(\x01\nC\n\x07f_bytes\x1a4\x08\x01:0\n beam:logical_type:fixed_bytes:v1\x1a\x02\x10\t\"\x02\x10\x03*\x04\n\x02\x18\x05 \x02(\x02\nD\n\nf_varbytes\x1a2\x08\x01:.\n\x1ebeam:logical_type:var_bytes:v1\x1a\x02\x10\t\"\x02\x10\x03*\x04\n\x02\x18\n \x03(\x03\x12$f0ffb3a4-f46f-41ca-a942-85e3e939452a";
        let coder = RowCoder::from_payload(payload).unwrap();

        let row = BeamRow::new(vec![
            Some(FieldValue::String("ABCDE".into())),
            Some(FieldValue::String("ABCDE".into())),
            Some(FieldValue::Bytes(b"ABCDE".to_vec())),
            Some(FieldValue::Bytes(b"ABCDE".to_vec())),
        ]);

        let mut buf = BytesMut::new();
        coder.encode(&row, &mut buf).unwrap();
        assert_eq!(
            buf.as_ref(),
            b"\x04\x00\x05ABCDE\x05ABCDE\x05ABCDE\x05ABCDE"
        );

        let decoded = coder.decode(&mut buf.freeze()).unwrap();
        // Decoded values will be wrapped in FieldValue::Logical
        assert_eq!(
            decoded.fields[0],
            Some(FieldValue::Logical(Box::new(FieldValue::String(
                "ABCDE".into()
            ))))
        );
    }
}
