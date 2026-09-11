//! Schema utilities for `beam:coder:row:v1`.
//!
//! [`BeamSchema`] is a Rust-native, resolved form of the proto `Schema` message.
//! It is constructed from the coder payload bytes (a protobuf-encoded `Schema`)
//! via [`BeamSchema::from_payload`] or [`BeamSchema::from_proto`].

use std::sync::Arc;

use beam_model_rs::v1::{AtomicType, FieldType, Schema, field_type::TypeInfo};
use prost::Message;

use crate::utils::errors::CodersError;

/// A Beam schema field type.
#[derive(Debug, Clone)]
pub struct BeamFieldType {
    /// Whether `null` is a valid value for this type (mirrors `FieldType.nullable`).
    pub nullable: bool,
    /// The kind of this field.
    pub kind: BeamTypeKind,
}

/// Beam field type kinds.
#[derive(Debug, Clone)]
pub enum BeamTypeKind {
    // Atomic types
    /// 1-byte signed integer.
    Byte,
    /// 2-byte big-endian signed integer.
    Int16,
    /// Variable-length signed integer (same encoding as `beam:coder:varint:v1`).
    Int32,
    /// Variable-length signed integer.
    Int64,
    /// 4-byte big-endian IEEE 754 single-precision float.
    Float,
    /// 8-byte big-endian IEEE 754 double-precision float.
    Double,
    /// Length-prefixed UTF-8 string (varint length).
    String,
    /// Single byte: `0x00` = false, `0x01` = true.
    Boolean,
    /// Length-prefixed raw bytes (varint length).
    Bytes,

    // Composite types
    /// Fixed-length sequence; element type carries its own `nullable` flag.
    Array(Box<BeamFieldType>),
    /// Variable-length sequence; same wire format as `Array`.
    Iterable(Box<BeamFieldType>),
    /// Key/value map; both key and value types carry their own `nullable` flags.
    Map(Box<BeamFieldType>, Box<BeamFieldType>),
    /// Nested row, encoded recursively with its own `RowCoder`.
    Row(Arc<BeamSchema>),

    //  Logical type
    /// A logical type identified by `urn`; encoded using its representation type.
    ///
    /// Examples: `beam:logical_type:micros_instant:v1`, `beam:logical_type:decimal:v1`.
    Logical {
        /// URN of the logical type
        urn: std::string::String,
        /// The wire representation type
        repr: Box<BeamFieldType>,
    },
}

/// A single field within a [`BeamSchema`].
#[derive(Debug, Clone)]
pub struct BeamField {
    /// Field name
    pub name: std::string::String,
    /// Resolved field type
    pub field_type: BeamFieldType,
    /// Encoding position declared in the proto (`Field.encoding_position`).
    ///  when `BeamSchema::encoding_positions_set` is true.
    pub encoding_position: i32,
}

/// Portable Beam schema
#[derive(Debug, Clone)]
pub struct BeamSchema {
    /// Schema UUID
    pub id: std::string::String,
    /// Fields in schema definition order.
    pub fields: Vec<BeamField>,
    /// Indices into `fields` in **encoding order**.
    ///
    /// - When `encoding_positions_set` is false this is simply `[0, 1, 2, …]`.
    /// - When `encoding_positions_set` is true the fields are sorted by their
    ///   `encoding_position` value, allowing backwards-compatible schema evolution.
    pub encode_order: Vec<usize>,
}

impl BeamSchema {
    /// Parse a `BeamSchema` from the raw coder payload bytes.
    ///
    /// The payload is a protobuf-encoded `Schema` message (the same bytes stored
    /// in `Coder.spec.payload` for a `beam:coder:row:v1` coder).
    pub fn from_payload(payload: &[u8]) -> Result<Self, CodersError> {
        let proto = Schema::decode(payload).map_err(|e| {
            CodersError::WhileDecoding(format!("failed to decode Schema proto: {e}"))
        })?;
        Self::from_proto(&proto)
    }

    /// Build a `BeamSchema` from an decoded proto [`Schema`] message.
    pub fn from_proto(schema: &Schema) -> Result<Self, CodersError> {
        let mut fields = Vec::with_capacity(schema.fields.len());

        for proto_field in &schema.fields {
            let proto_type = proto_field.r#type.as_ref().ok_or_else(|| {
                CodersError::WhileDecoding(format!(
                    "schema field '{}' is missing its type",
                    proto_field.name
                ))
            })?;

            let beam_type = resolve_field_type(proto_type)?;

            fields.push(BeamField {
                name: proto_field.name.clone(),
                field_type: beam_type,
                encoding_position: proto_field.encoding_position,
            });
        }

        // Build encoding order
        let encode_order: Vec<usize> = if schema.encoding_positions_set {
            // Sort field indices by their declared encoding_position.
            let mut indexed: Vec<(i32, usize)> = fields
                .iter()
                .enumerate()
                .map(|(idx, f)| (f.encoding_position, idx))
                .collect();
            indexed.sort_by_key(|&(pos, _)| pos);
            indexed.into_iter().map(|(_, idx)| idx).collect()
        } else {
            (0..fields.len()).collect()
        };

        Ok(BeamSchema {
            id: schema.id.clone(),
            fields,
            encode_order,
        })
    }
}

/// Recursively resolve a proto [`FieldType`] into a [`BeamFieldType`].
fn resolve_field_type(ft: &FieldType) -> Result<BeamFieldType, CodersError> {
    let nullable = ft.nullable;

    let kind = match ft.type_info.as_ref().ok_or_else(|| {
        CodersError::WhileDecoding("FieldType is missing type_info oneof".to_string())
    })? {
        // Atomic
        TypeInfo::AtomicType(atom_i32) => {
            match AtomicType::try_from(*atom_i32).unwrap_or(AtomicType::Unspecified) {
                AtomicType::Byte => BeamTypeKind::Byte,
                AtomicType::Int16 => BeamTypeKind::Int16,
                AtomicType::Int32 => BeamTypeKind::Int32,
                AtomicType::Int64 => BeamTypeKind::Int64,
                AtomicType::Float => BeamTypeKind::Float,
                AtomicType::Double => BeamTypeKind::Double,
                AtomicType::String => BeamTypeKind::String,
                AtomicType::Boolean => BeamTypeKind::Boolean,
                AtomicType::Bytes => BeamTypeKind::Bytes,
                AtomicType::Unspecified => {
                    return Err(CodersError::WhileDecoding(
                        "AtomicType is Unspecified".to_string(),
                    ));
                }
            }
        }

        TypeInfo::ArrayType(arr) => {
            let elem_ft = arr.element_type.as_ref().ok_or_else(|| {
                CodersError::WhileDecoding("ArrayType is missing element_type".to_string())
            })?;
            BeamTypeKind::Array(Box::new(resolve_field_type(elem_ft)?))
        }

        TypeInfo::IterableType(it) => {
            let elem_ft = it.element_type.as_ref().ok_or_else(|| {
                CodersError::WhileDecoding("IterableType is missing element_type".to_string())
            })?;
            BeamTypeKind::Iterable(Box::new(resolve_field_type(elem_ft)?))
        }

        TypeInfo::MapType(map) => {
            let key_ft = map.key_type.as_ref().ok_or_else(|| {
                CodersError::WhileDecoding("MapType is missing key_type".to_string())
            })?;
            let val_ft = map.value_type.as_ref().ok_or_else(|| {
                CodersError::WhileDecoding("MapType is missing value_type".to_string())
            })?;
            BeamTypeKind::Map(
                Box::new(resolve_field_type(key_ft)?),
                Box::new(resolve_field_type(val_ft)?),
            )
        }

        // Nested Row
        TypeInfo::RowType(row_type) => {
            let sub_schema = row_type.schema.as_ref().ok_or_else(|| {
                CodersError::WhileDecoding("RowType is missing schema".to_string())
            })?;
            BeamTypeKind::Row(Arc::new(BeamSchema::from_proto(sub_schema)?))
        }

        // Logical type
        TypeInfo::LogicalType(logical) => {
            let repr_ft = logical.representation.as_ref().ok_or_else(|| {
                CodersError::WhileDecoding(format!(
                    "LogicalType '{}' is missing its representation type",
                    logical.urn
                ))
            })?;
            BeamTypeKind::Logical {
                urn: logical.urn.clone(),
                repr: Box::new(resolve_field_type(repr_ft)?),
            }
        }
    };

    Ok(BeamFieldType { nullable, kind })
}
