use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int64Array, ListArray, NullArray,
    RecordBatch, StringArray,
};
use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType as ArrowDataType, Field as ArrowField, Schema, TimeUnit};
use std::hash::{Hash, Hasher};

use paimon::spec::{
    ArrayType, BigIntType, BooleanType, DataField, DataType as PaimonDataType, DateType,
    DecimalType, DoubleType, FloatType, IntType, LocalZonedTimestampType, MapType, RowType,
    SmallIntType, TimeType, TimestampType, TinyIntType, VarBinaryType, VarCharType, VariantType,
    VectorType,
};

use super::{KEY_COLUMN, VALUE_COLUMN};

#[derive(Debug, Clone)]
pub enum BeamRecord {
    PRIMITIVE(PrimitiveValue),
    ITERABLE(IterableValue),
    KV(BeamKV),
    GBK(BeamGbk),
}

#[derive(Debug, Clone)]
pub enum BeamRecordType {
    Primitive,
    Iterable,
    Kv,
    Gbk,
}

impl BeamRecord {
    pub fn record_type(&self) -> BeamRecordType {
        match self {
            BeamRecord::PRIMITIVE(_) => BeamRecordType::Primitive,
            BeamRecord::ITERABLE(_) => BeamRecordType::Iterable,
            BeamRecord::GBK(_) => BeamRecordType::Gbk,
            BeamRecord::KV(_) => BeamRecordType::Kv,
        }
    }

    pub fn get_primitive(&self) -> Result<PrimitiveValue> {
        match self {
            BeamRecord::PRIMITIVE(value) => Ok(value.clone()),
            _ => Err(anyhow!("excluded other types")),
        }
    }

    pub fn get_kv(self) -> Result<BeamKV> {
        match self {
            BeamRecord::KV(value) => Ok(value.clone()),
            _ => Err(anyhow!("excluded other types")),
        }
    }

    pub fn get_gbk(self) -> Result<BeamGbk> {
        match self {
            BeamRecord::GBK(value) => Ok(value.clone()),
            _ => Err(anyhow!("excluded other types")),
        }
    }

    pub fn get_iterable(&self) -> Result<IterableValue> {
        match self {
            BeamRecord::ITERABLE(value) => Ok(value.clone()),
            _ => Err(anyhow!("excluded other types")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BeamGbk {
    pub(crate) key: PrimitiveValue,
    pub(crate) value: IterableValue,
}

#[derive(Debug, Clone)]
pub struct BeamKV {
    pub(crate) key: PrimitiveValue,
    pub(crate) value: PrimitiveValue,
}

#[derive(Debug, Clone)]
pub struct IterableValue {
    pub(crate) list: Vec<PrimitiveValue>,
}

impl IterableValue {
    pub fn new(list: Vec<PrimitiveValue>) -> Self {
        Self { list }
    }
}

#[derive(Debug, Clone)]
pub enum PrimitiveValue {
    String(String),
    Bytes(Vec<u8>),
    Int64(i64),
    Float64(f64),
    Bool(bool),
    Void,
}

impl Hash for PrimitiveValue {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::String(value) => {
                0_u8.hash(state);
                value.hash(state);
            }
            Self::Bytes(value) => {
                1_u8.hash(state);
                value.hash(state);
            }
            Self::Int64(value) => {
                2_u8.hash(state);
                value.hash(state);
            }
            Self::Float64(value) => {
                // f64 does not implement Hash; hash the bits instead.
                3_u8.hash(state);
                value.to_bits().hash(state);
            }
            Self::Bool(value) => {
                4_u8.hash(state);
                value.hash(state);
            }
            Self::Void => {
                5_u8.hash(state);
            }
        }
    }
}

impl PartialEq for PrimitiveValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::String(left), Self::String(right)) => left == right,
            (Self::Bytes(left), Self::Bytes(right)) => left == right,
            (Self::Int64(left), Self::Int64(right)) => left == right,
            (Self::Float64(left), Self::Float64(right)) => left.to_bits() == right.to_bits(),
            (Self::Bool(left), Self::Bool(right)) => left == right,
            (Self::Void, Self::Void) => true,
            _ => false,
        }
    }
}

impl Eq for PrimitiveValue {}

/// What kind of Beam record this table stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableType {
    /// Single `value` column of a primitive type.
    Primitive,
    /// Single `value` column holding a `List<Primitive>`.
    Iterable,
    /// Two columns: `key`, `value` — both primitive types.
    Kv,
    /// Two columns: `key` (primitive), `value` (List<Primitive>).
    Gbk,
}

impl TableType {
    pub fn as_str(self) -> &'static str {
        match self {
            TableType::Primitive => "primitive",
            TableType::Iterable => "iterable",
            TableType::Kv => "kv",
            TableType::Gbk => "gbk",
        }
    }

    pub fn from_str(value: &str) -> Result<Self> {
        match value {
            "primitive" => Ok(TableType::Primitive),
            "iterable" => Ok(TableType::Iterable),
            "kv" => Ok(TableType::Kv),
            "gbk" => Ok(TableType::Gbk),
            other => Err(anyhow!("unknown table type: {other}")),
        }
    }
}

fn table_type_of(record: &BeamRecord) -> TableType {
    match record {
        BeamRecord::PRIMITIVE(_) => TableType::Primitive,
        BeamRecord::ITERABLE(_) => TableType::Iterable,
        BeamRecord::KV(_) => TableType::Kv,
        BeamRecord::GBK(_) => TableType::Gbk,
    }
}

/// Describes how a Beam PCollection is laid out as columns in a Paimon table.
#[derive(Debug, Clone)]
pub struct RecordTableSchema {
    pub table_type: TableType,
    pub arrow_schema: Arc<Schema>,
}

// Value-type helpers

fn primitive_data_type(value: &PrimitiveValue) -> ArrowDataType {
    match value {
        PrimitiveValue::String(_) => ArrowDataType::Utf8,
        PrimitiveValue::Bytes(_) => ArrowDataType::Binary,
        PrimitiveValue::Int64(_) => ArrowDataType::Int64,
        PrimitiveValue::Float64(_) => ArrowDataType::Float64,
        PrimitiveValue::Bool(_) => ArrowDataType::Boolean,
        PrimitiveValue::Void => ArrowDataType::Null,
    }
}

fn primitive_type_matches(value: &PrimitiveValue, data_type: &ArrowDataType) -> bool {
    &primitive_data_type(value) == data_type
}

fn iterable_values(iterable: &IterableValue) -> &[PrimitiveValue] {
    iterable.list.as_slice()
}

fn infer_iterable_item_data_type(iterables: &[IterableValue]) -> ArrowDataType {
    iterables
        .iter()
        .flat_map(iterable_values)
        .next()
        .map(primitive_data_type)
        .unwrap_or(ArrowDataType::Null)
}

fn build_offsets(lengths: &[usize]) -> Result<OffsetBuffer<i32>> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    offsets.push(0_i32);

    let mut running = 0_i32;
    for len in lengths {
        let len = i32::try_from(*len).context("list length exceeds i32::MAX")?;
        running = running
            .checked_add(len)
            .ok_or_else(|| anyhow!("list offsets exceed i32::MAX"))?;
        offsets.push(running);
    }

    Ok(OffsetBuffer::new(ScalarBuffer::from(offsets)))
}

pub fn primitive_values_to_array(
    values: &[PrimitiveValue],
    data_type: &ArrowDataType,
) -> Result<ArrayRef> {
    match data_type {
        ArrowDataType::Utf8 => {
            let strings = values
                .iter()
                .map(|value| match value {
                    PrimitiveValue::String(value) => Ok(value.clone()),
                    other => Err(anyhow!(
                        "mixed primitive variants in batch: expected String, found {:?}",
                        other
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StringArray::from(strings)))
        }
        ArrowDataType::Binary => {
            let bytes = values
                .iter()
                .map(|value| match value {
                    PrimitiveValue::Bytes(value) => Ok(value.as_slice()),
                    other => Err(anyhow!(
                        "mixed primitive variants in batch: expected Bytes, found {:?}",
                        other
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(BinaryArray::from(bytes)))
        }
        ArrowDataType::Int64 => {
            let ints = values
                .iter()
                .map(|value| match value {
                    PrimitiveValue::Int64(value) => Ok(*value),
                    other => Err(anyhow!(
                        "mixed primitive variants in batch: expected Int64, found {:?}",
                        other
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Int64Array::from(ints)))
        }
        ArrowDataType::Boolean => {
            let bools = values
                .iter()
                .map(|value| match value {
                    PrimitiveValue::Bool(value) => Ok(*value),
                    other => Err(anyhow!(
                        "mixed primitive variants in batch: expected Bool, found {:?}",
                        other
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(BooleanArray::from(bools)))
        }
        ArrowDataType::Float64 => {
            let floats = values
                .iter()
                .map(|value| match value {
                    PrimitiveValue::Float64(value) => Ok(*value),
                    other => Err(anyhow!(
                        "mixed primitive variants in batch: expected Float64, found {:?}",
                        other
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Float64Array::from(floats)))
        }
        ArrowDataType::Null => {
            if values
                .iter()
                .all(|value| matches!(value, PrimitiveValue::Void))
            {
                Ok(Arc::new(NullArray::new(values.len())))
            } else {
                Err(anyhow!(
                    "mixed primitive variants in batch: expected Void values"
                ))
            }
        }
        other => Err(anyhow!("unsupported primitive storage type: {other:?}")),
    }
}

pub fn iterable_values_to_array(
    iterables: &[IterableValue],
    item_data_type: &ArrowDataType,
) -> Result<ArrayRef> {
    let mut lengths = Vec::with_capacity(iterables.len());
    let mut flattened = Vec::new();

    for iterable in iterables {
        let values = iterable_values(iterable);
        lengths.push(values.len());
        flattened.extend(values.iter().cloned());
    }

    let offsets = build_offsets(&lengths)?;
    let child = primitive_values_to_array(&flattened, item_data_type)?;

    let nullable = matches!(item_data_type, ArrowDataType::Null);
    let item_field = Arc::new(ArrowField::new("item", item_data_type.clone(), nullable));

    Ok(Arc::new(ListArray::new(item_field, offsets, child, None)))
}

fn primitive_value_from_array_row(
    array: &dyn Array,
    data_type: &ArrowDataType,
    row: usize,
) -> Result<PrimitiveValue> {
    if array.is_null(row) {
        return Ok(PrimitiveValue::Void);
    }

    match data_type {
        ArrowDataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected StringArray for Utf8 primitive column"))?;
            Ok(PrimitiveValue::String(array.value(row).to_string()))
        }
        ArrowDataType::Binary => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow!("expected BinaryArray for Binary primitive column"))?;
            Ok(PrimitiveValue::Bytes(array.value(row).to_vec()))
        }
        ArrowDataType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64Array for Int64 primitive column"))?;
            Ok(PrimitiveValue::Int64(array.value(row)))
        }
        ArrowDataType::Boolean => {
            let array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("expected BooleanArray for Boolean primitive column"))?;
            Ok(PrimitiveValue::Bool(array.value(row)))
        }
        ArrowDataType::Float64 => {
            let array = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| anyhow!("expected Float64Array for Float64 primitive column"))?;
            Ok(PrimitiveValue::Float64(array.value(row)))
        }
        ArrowDataType::Null => Ok(PrimitiveValue::Void),
        other => Err(anyhow!("unsupported primitive storage type: {other:?}")),
    }
}

fn iterable_value_from_array_row(
    array: &dyn Array,
    data_type: &ArrowDataType,
    row: usize,
) -> Result<IterableValue> {
    let ArrowDataType::List(item_field) = data_type else {
        return Err(anyhow!(
            "expected List column for iterable, found {data_type:?}"
        ));
    };

    if array.is_null(row) {
        return Ok(IterableValue::new(Vec::new()));
    }

    let list_array = array
        .as_any()
        .downcast_ref::<ListArray>()
        .ok_or_else(|| anyhow!("expected ListArray for iterable column"))?;

    let offsets = list_array.value_offsets();
    let start = usize::try_from(offsets[row]).context("negative list offset")?;
    let end = usize::try_from(offsets[row + 1]).context("negative list offset")?;
    let values_array = list_array.values();

    let mut values = Vec::with_capacity(end.saturating_sub(start));
    for index in start..end {
        values.push(primitive_value_from_array_row(
            values_array.as_ref(),
            item_field.data_type(),
            index,
        )?);
    }

    Ok(IterableValue::new(values))
}

// Schema derivation

fn validate_iterable_item_types(
    pcollection_id: &str,
    iterables: &[IterableValue],
    item_data_type: &ArrowDataType,
) -> Result<()> {
    for iterable in iterables {
        for value in iterable_values(iterable) {
            if !primitive_type_matches(value, item_data_type) {
                return Err(anyhow!(
                    "mixed iterable item types in pcollection {pcollection_id}: expected {:?}, found {:?}",
                    item_data_type,
                    primitive_data_type(value)
                ));
            }
        }
    }
    Ok(())
}

/// Derives the table schema from a non-empty slice of [`BeamRecord`]s.
pub fn derive_table_schema(
    pcollection_id: &str,
    records: &[BeamRecord],
) -> Result<RecordTableSchema> {
    let first = records
        .first()
        .ok_or_else(|| anyhow!("cannot derive schema for empty pcollection {pcollection_id}"))?;
    let table_type = table_type_of(first);

    let arrow_schema = match table_type {
        // Primitive: single `value` column
        TableType::Primitive => {
            let BeamRecord::PRIMITIVE(first_value) = first else {
                unreachable!();
            };
            let data_type = primitive_data_type(first_value);

            for record in records {
                let BeamRecord::PRIMITIVE(value) = record else {
                    return Err(anyhow!(
                        "mixed BeamRecord variants in pcollection {pcollection_id}: expected primitive"
                    ));
                };
                if !primitive_type_matches(value, &data_type) {
                    return Err(anyhow!(
                        "mixed primitive types in pcollection {pcollection_id}: expected {:?}, found {:?}",
                        data_type,
                        primitive_data_type(value)
                    ));
                }
            }
            let nullable = matches!(data_type, ArrowDataType::Null);
            Arc::new(Schema::new(vec![ArrowField::new(
                VALUE_COLUMN,
                data_type,
                nullable,
            )]))
        }

        //Iterable: single `value` column, List<…>
        TableType::Iterable => {
            let mut iterables = Vec::with_capacity(records.len());
            for record in records {
                let BeamRecord::ITERABLE(iterable) = record else {
                    return Err(anyhow!(
                        "mixed BeamRecord variants in pcollection {pcollection_id}: expected iterable"
                    ));
                };
                iterables.push(iterable.clone());
            }

            let item_data_type = infer_iterable_item_data_type(&iterables);
            validate_iterable_item_types(pcollection_id, &iterables, &item_data_type)?;
            let nullable = matches!(item_data_type, ArrowDataType::Null);

            Arc::new(Schema::new(vec![ArrowField::new(
                VALUE_COLUMN,
                ArrowDataType::List(Arc::new(ArrowField::new("item", item_data_type, nullable))),
                true,
            )]))
        }

        //KV: `key` + `value` (both primitive)
        TableType::Kv => {
            let BeamRecord::KV(first_kv) = first else {
                unreachable!();
            };
            let key_data_type = primitive_data_type(&first_kv.key);
            let value_data_type = primitive_data_type(&first_kv.value);

            for record in records {
                let BeamRecord::KV(kv) = record else {
                    return Err(anyhow!(
                        "mixed BeamRecord variants in pcollection {pcollection_id}: expected kv"
                    ));
                };
                if !primitive_type_matches(&kv.key, &key_data_type) {
                    return Err(anyhow!(
                        "mixed KV key types in pcollection {pcollection_id}: expected {:?}, found {:?}",
                        key_data_type,
                        primitive_data_type(&kv.key)
                    ));
                }
                if !primitive_type_matches(&kv.value, &value_data_type) {
                    return Err(anyhow!(
                        "mixed KV value types in pcollection {pcollection_id}: expected {:?}, found {:?}",
                        value_data_type,
                        primitive_data_type(&kv.value)
                    ));
                }
            }

            let key_nullable = matches!(key_data_type, ArrowDataType::Null);
            let value_nullable = matches!(value_data_type, ArrowDataType::Null);

            Arc::new(Schema::new(vec![
                ArrowField::new(KEY_COLUMN, key_data_type, key_nullable),
                ArrowField::new(VALUE_COLUMN, value_data_type, value_nullable),
            ]))
        }

        // GBK: `key` (primitive) + `value` (List<…>)
        TableType::Gbk => {
            let BeamRecord::GBK(first_gbk) = first else {
                unreachable!();
            };
            let key_data_type = primitive_data_type(&first_gbk.key);

            let mut values = Vec::with_capacity(records.len());
            for record in records {
                let BeamRecord::GBK(gbk) = record else {
                    return Err(anyhow!(
                        "mixed BeamRecord variants in pcollection {pcollection_id}: expected gbk"
                    ));
                };
                if !primitive_type_matches(&gbk.key, &key_data_type) {
                    return Err(anyhow!(
                        "mixed GBK key types in pcollection {pcollection_id}: expected {:?}, found {:?}",
                        key_data_type,
                        primitive_data_type(&gbk.key)
                    ));
                }
                values.push(gbk.value.clone());
            }

            let item_data_type = infer_iterable_item_data_type(&values);
            validate_iterable_item_types(pcollection_id, &values, &item_data_type)?;
            let nullable = matches!(item_data_type, ArrowDataType::Null);

            Arc::new(Schema::new(vec![
                ArrowField::new(KEY_COLUMN, key_data_type, false),
                ArrowField::new(
                    VALUE_COLUMN,
                    ArrowDataType::List(Arc::new(ArrowField::new(
                        "item",
                        item_data_type,
                        nullable,
                    ))),
                    true,
                ),
            ]))
        }
    };

    Ok(RecordTableSchema {
        table_type,
        arrow_schema,
    })
}

/// Convert [`BeamRecord`]s into an Arrow [`RecordBatch`].
pub fn beamrecords_to_record_batch(
    records: &[BeamRecord],
    table_schema: &RecordTableSchema,
) -> Result<RecordBatch> {
    if records.is_empty() {
        return Err(anyhow!("cannot build record batch from empty records"));
    }

    let row_count = records.len();
    let mut columns: Vec<ArrayRef> = Vec::new();

    match table_schema.table_type {
        TableType::Primitive => {
            let data_type = table_schema
                .arrow_schema
                .field_with_name(VALUE_COLUMN)
                .with_context(|| format!("missing {VALUE_COLUMN} column"))?
                .data_type();
            let values = records
                .iter()
                .map(|record| match record {
                    BeamRecord::PRIMITIVE(value) => Ok(value.clone()),
                    _ => Err(anyhow!("expected primitive record")),
                })
                .collect::<Result<Vec<_>>>()?;
            columns.push(primitive_values_to_array(&values, data_type)?);
        }
        TableType::Iterable => {
            let data_type = table_schema
                .arrow_schema
                .field_with_name(VALUE_COLUMN)
                .with_context(|| format!("missing {VALUE_COLUMN} column"))?
                .data_type();
            let ArrowDataType::List(item_field) = data_type else {
                return Err(anyhow!("iterable value field must be a List"));
            };
            let iterables = records
                .iter()
                .map(|record| match record {
                    BeamRecord::ITERABLE(value) => Ok(value.clone()),
                    _ => Err(anyhow!("expected iterable record")),
                })
                .collect::<Result<Vec<_>>>()?;
            columns.push(iterable_values_to_array(
                &iterables,
                item_field.data_type(),
            )?);
        }
        TableType::Kv => {
            let key_data_type = table_schema
                .arrow_schema
                .field_with_name(KEY_COLUMN)
                .with_context(|| format!("missing {KEY_COLUMN} column"))?
                .data_type();
            let value_data_type = table_schema
                .arrow_schema
                .field_with_name(VALUE_COLUMN)
                .with_context(|| format!("missing {VALUE_COLUMN} column"))?
                .data_type();
            let mut keys = Vec::with_capacity(row_count);
            let mut values = Vec::with_capacity(row_count);

            for record in records {
                let BeamRecord::KV(kv) = record else {
                    return Err(anyhow!("expected kv record"));
                };
                keys.push(kv.key.clone());
                values.push(kv.value.clone());
            }

            columns.push(primitive_values_to_array(&keys, key_data_type)?);
            columns.push(primitive_values_to_array(&values, value_data_type)?);
        }
        TableType::Gbk => {
            let key_data_type = table_schema
                .arrow_schema
                .field_with_name(KEY_COLUMN)
                .with_context(|| format!("missing {KEY_COLUMN} column"))?
                .data_type();
            let value_data_type = table_schema
                .arrow_schema
                .field_with_name(VALUE_COLUMN)
                .with_context(|| format!("missing {VALUE_COLUMN} column"))?
                .data_type();
            let ArrowDataType::List(item_field) = value_data_type else {
                return Err(anyhow!("gbk value field must be a List"));
            };

            let mut keys = Vec::with_capacity(row_count);
            let mut values = Vec::with_capacity(row_count);

            for record in records {
                let BeamRecord::GBK(gbk) = record else {
                    return Err(anyhow!("expected gbk record"));
                };
                keys.push(gbk.key.clone());
                values.push(gbk.value.clone());
            }

            columns.push(primitive_values_to_array(&keys, key_data_type)?);
            columns.push(iterable_values_to_array(&values, item_field.data_type())?);
        }
    }

    RecordBatch::try_new(table_schema.arrow_schema.clone(), columns)
        .context("failed to build record batch")
}

/// Convert an Arrow [`RecordBatch`] back into [`BeamRecord`]s.
pub fn record_batch_to_beamrecords(
    batch: &RecordBatch,
    table_schema: &RecordTableSchema,
) -> Result<Vec<BeamRecord>> {
    let mut records = Vec::with_capacity(batch.num_rows());

    match table_schema.table_type {
        TableType::Primitive => {
            let column = batch
                .column_by_name(VALUE_COLUMN)
                .ok_or_else(|| anyhow!("missing {VALUE_COLUMN} column"))?;
            for row in 0..batch.num_rows() {
                records.push(BeamRecord::PRIMITIVE(primitive_value_from_array_row(
                    column.as_ref(),
                    column.data_type(),
                    row,
                )?));
            }
        }
        TableType::Iterable => {
            let column = batch
                .column_by_name(VALUE_COLUMN)
                .ok_or_else(|| anyhow!("missing {VALUE_COLUMN} column"))?;
            for row in 0..batch.num_rows() {
                records.push(BeamRecord::ITERABLE(iterable_value_from_array_row(
                    column.as_ref(),
                    column.data_type(),
                    row,
                )?));
            }
        }
        TableType::Kv => {
            let key_column = batch
                .column_by_name(KEY_COLUMN)
                .ok_or_else(|| anyhow!("missing {KEY_COLUMN} column"))?;
            let value_column = batch
                .column_by_name(VALUE_COLUMN)
                .ok_or_else(|| anyhow!("missing {VALUE_COLUMN} column"))?;

            for row in 0..batch.num_rows() {
                records.push(BeamRecord::KV(BeamKV {
                    key: primitive_value_from_array_row(
                        key_column.as_ref(),
                        key_column.data_type(),
                        row,
                    )?,
                    value: primitive_value_from_array_row(
                        value_column.as_ref(),
                        value_column.data_type(),
                        row,
                    )?,
                }));
            }
        }
        TableType::Gbk => {
            let key_column = batch
                .column_by_name(KEY_COLUMN)
                .ok_or_else(|| anyhow!("missing {KEY_COLUMN} column"))?;
            let value_column = batch
                .column_by_name(VALUE_COLUMN)
                .ok_or_else(|| anyhow!("missing {VALUE_COLUMN} column"))?;

            for row in 0..batch.num_rows() {
                records.push(BeamRecord::GBK(BeamGbk {
                    key: primitive_value_from_array_row(
                        key_column.as_ref(),
                        key_column.data_type(),
                        row,
                    )?,
                    value: iterable_value_from_array_row(
                        value_column.as_ref(),
                        value_column.data_type(),
                        row,
                    )?,
                }));
            }
        }
    }

    Ok(records)
}

/// Paimon cannot store Arrow `Null`. Rewrite top-level `Null` columns as
/// nullable `Boolean` columns where every value is null. This is a lossless
/// surrogate for `PrimitiveValue::Void`.
pub fn materialize_void_columns(batch: RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let mut new_fields = Vec::new();
    let mut new_columns: Vec<ArrayRef> = Vec::new();
    let mut modified = false;

    for (i, field) in schema.fields().iter().enumerate() {
        if field.data_type() == &ArrowDataType::Null {
            modified = true;
            new_fields.push(Arc::new(ArrowField::new(
                field.name(),
                ArrowDataType::Boolean,
                true,
            )));
            let nulls = Some(NullBuffer::new_null(batch.num_rows()));
            let values = BooleanBuffer::new_unset(batch.num_rows());
            new_columns.push(Arc::new(BooleanArray::new(values, nulls)));
        } else {
            new_fields.push(field.clone());
            new_columns.push(batch.column(i).clone());
        }
    }

    if !modified {
        return Ok(batch);
    }

    let new_schema = Arc::new(Schema::new(new_fields));
    RecordBatch::try_new(new_schema, new_columns)
        .context("failed to materialize Null columns for Paimon storage")
}

/// Convert Arrow fields to Paimon [`DataField`]s with auto-assigned IDs starting from 0.
pub fn arrow_fields_to_paimon(fields: &[ArrowField]) -> Result<Vec<DataField>> {
    fields
        .iter()
        .enumerate()
        .map(|(i, f)| {
            let paimon_type = arrow_to_paimon_type(f.data_type(), f.is_nullable())?;
            Ok(DataField::new(i as i32, f.name().clone(), paimon_type))
        })
        .collect()
}
/// Convert an Arrow [`DataType`](ArrowDataType) to a Paimon [`DataType`](PaimonDataType).
pub fn arrow_to_paimon_type(arrow_type: &ArrowDataType, nullable: bool) -> Result<PaimonDataType> {
    match arrow_type {
        ArrowDataType::Boolean => Ok(PaimonDataType::Boolean(BooleanType::with_nullable(
            nullable,
        ))),
        ArrowDataType::Int8 => Ok(PaimonDataType::TinyInt(TinyIntType::with_nullable(
            nullable,
        ))),
        ArrowDataType::Int16 => Ok(PaimonDataType::SmallInt(SmallIntType::with_nullable(
            nullable,
        ))),
        ArrowDataType::Int32 => Ok(PaimonDataType::Int(IntType::with_nullable(nullable))),
        ArrowDataType::Int64 => Ok(PaimonDataType::BigInt(BigIntType::with_nullable(nullable))),
        ArrowDataType::Float32 => Ok(PaimonDataType::Float(FloatType::with_nullable(nullable))),
        ArrowDataType::Float64 => Ok(PaimonDataType::Double(DoubleType::with_nullable(nullable))),
        ArrowDataType::Utf8 | ArrowDataType::LargeUtf8 | ArrowDataType::Utf8View => {
            Ok(PaimonDataType::VarChar(VarCharType::with_nullable(
                nullable,
                VarCharType::MAX_LENGTH,
            )?))
        }
        ArrowDataType::Binary | ArrowDataType::LargeBinary | ArrowDataType::BinaryView => Ok(
            PaimonDataType::VarBinary(VarBinaryType::try_new(nullable, VarBinaryType::MAX_LENGTH)?),
        ),
        ArrowDataType::Date32 => Ok(PaimonDataType::Date(DateType::with_nullable(nullable))),
        ArrowDataType::Timestamp(unit, tz) => {
            let precision = match unit {
                TimeUnit::Second => 0,
                TimeUnit::Millisecond => 3,
                TimeUnit::Microsecond => 6,
                TimeUnit::Nanosecond => 9,
            };
            if tz.is_some() {
                Ok(PaimonDataType::LocalZonedTimestamp(
                    LocalZonedTimestampType::with_nullable(nullable, precision)?,
                ))
            } else {
                Ok(PaimonDataType::Timestamp(TimestampType::with_nullable(
                    nullable, precision,
                )?))
            }
        }
        ArrowDataType::Time32(_) | ArrowDataType::Time64(_) => {
            let precision = match arrow_type {
                ArrowDataType::Time32(TimeUnit::Second) => 0,
                ArrowDataType::Time32(TimeUnit::Millisecond) => 3,
                ArrowDataType::Time64(TimeUnit::Microsecond) => 6,
                ArrowDataType::Time64(TimeUnit::Nanosecond) => 9,
                _ => 0,
            };
            Ok(PaimonDataType::Time(TimeType::with_nullable(
                nullable, precision,
            )?))
        }
        ArrowDataType::Decimal128(p, s) => Ok(PaimonDataType::Decimal(DecimalType::with_nullable(
            nullable, *p as u32, *s as u32,
        )?)),
        ArrowDataType::List(field) | ArrowDataType::LargeList(field) => {
            let element = arrow_to_paimon_type(field.data_type(), field.is_nullable())?;
            Ok(PaimonDataType::Array(ArrayType::with_nullable(
                nullable, element,
            )))
        }
        ArrowDataType::Map(entries_field, _) => {
            if let ArrowDataType::Struct(fields) = entries_field.data_type() {
                if fields.len() == 2 {
                    let key = arrow_to_paimon_type(fields[0].data_type(), fields[0].is_nullable())?;
                    let value =
                        arrow_to_paimon_type(fields[1].data_type(), fields[1].is_nullable())?;
                    return Ok(PaimonDataType::Map(MapType::with_nullable(
                        nullable, key, value,
                    )));
                }
            }
            Err(anyhow!("Unsupported Map structure"))
        }
        ArrowDataType::Struct(fields) => {
            if is_variant_arrow_fields(fields) && has_variant_arrow_field_ids(fields) {
                return Ok(PaimonDataType::Variant(VariantType::with_nullable(
                    nullable,
                )));
            }
            let field_slice: Vec<ArrowField> = fields.iter().map(|f| f.as_ref().clone()).collect();
            let paimon_fields = arrow_fields_to_paimon(&field_slice)?;
            Ok(PaimonDataType::Row(RowType::with_nullable(
                nullable,
                paimon_fields,
            )))
        }
        ArrowDataType::FixedSizeList(field, size) => {
            let element = arrow_to_paimon_type(field.data_type(), field.is_nullable())?;
            // FixedSizeList size is i32; reject non-positive sizes with a clear error
            // rather than casting a negative into a huge u32.
            let length = u32::try_from(*size)
                .map_err(|_| anyhow!("Invalid vector (FixedSizeList) length: {size}"))?;
            Ok(PaimonDataType::Vector(VectorType::try_new(
                nullable, length, element,
            )?))
        }

        ArrowDataType::Null => Ok(PaimonDataType::Boolean(BooleanType::with_nullable(true))),
        _ => Err(anyhow!(
            "Unsupported Arrow type for Paimon conversion: {arrow_type:?}"
        )),
    }
}

pub(crate) fn is_variant_arrow_fields(fields: &arrow_schema::Fields) -> bool {
    fields.len() == 2
        && fields[0].name() == "value"
        && fields[0].data_type() == &ArrowDataType::Binary
        && !fields[0].is_nullable()
        && fields[1].name() == "metadata"
        && fields[1].data_type() == &ArrowDataType::Binary
        && !fields[1].is_nullable()
}

fn has_variant_arrow_field_ids(fields: &arrow_schema::Fields) -> bool {
    fields.len() == 2
        && arrow_field_id(&fields[0]) == Some(0)
        && arrow_field_id(&fields[1]) == Some(1)
}

fn arrow_field_id(field: &ArrowField) -> Option<i32> {
    field.metadata().get("PARQUET:field_id")?.parse().ok()
}
#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, BinaryArray, BooleanArray, Int64Array, ListArray, StringArray};
    use arrow_schema::DataType;

    // helpers

    fn s(v: &str) -> PrimitiveValue {
        PrimitiveValue::String(v.to_string())
    }
    fn i(v: i64) -> PrimitiveValue {
        PrimitiveValue::Int64(v)
    }
    fn b(v: bool) -> PrimitiveValue {
        PrimitiveValue::Bool(v)
    }
    fn bytes(v: &[u8]) -> PrimitiveValue {
        PrimitiveValue::Bytes(v.to_vec())
    }

    fn assert_primitive(record: &BeamRecord, expected: &PrimitiveValue) {
        match record {
            BeamRecord::PRIMITIVE(v) => assert_eq!(v, expected),
            other => panic!("expected PRIMITIVE, got {other:?}"),
        }
    }

    fn assert_kv(record: &BeamRecord, key: &PrimitiveValue, value: &PrimitiveValue) {
        match record {
            BeamRecord::KV(kv) => {
                assert_eq!(&kv.key, key);
                assert_eq!(&kv.value, value);
            }
            other => panic!("expected KV, got {other:?}"),
        }
    }

    fn assert_iterable(record: &BeamRecord, expected: &[PrimitiveValue]) {
        match record {
            BeamRecord::ITERABLE(it) => assert_eq!(it.list, expected),
            other => panic!("expected ITERABLE, got {other:?}"),
        }
    }

    fn assert_gbk(record: &BeamRecord, key: &PrimitiveValue, values: &[PrimitiveValue]) {
        match record {
            BeamRecord::GBK(gbk) => {
                assert_eq!(&gbk.key, key);
                assert_eq!(gbk.value.list, values);
            }
            other => panic!("expected GBK, got {other:?}"),
        }
    }

    // PrimitiveValue equality

    #[test]
    fn primitive_value_eq_same_variant() {
        assert_eq!(s("a"), s("a"));
        assert_ne!(s("a"), s("b"));
        assert_eq!(i(5), i(5));
        assert_ne!(i(5), i(6));
        assert_eq!(b(true), b(true));
        assert_ne!(b(true), b(false));
        assert_eq!(bytes(&[1, 2]), bytes(&[1, 2]));
        assert_ne!(bytes(&[1, 2]), bytes(&[1, 3]));
        assert_eq!(PrimitiveValue::Void, PrimitiveValue::Void);
    }

    #[test]
    fn primitive_value_eq_across_variants_is_false() {
        // Same "logical" content, different variant -> not equal.
        assert_ne!(i(1), PrimitiveValue::Bool(true));
        assert_ne!(s("1"), i(1));
    }

    // TableType

    #[test]
    fn table_type_as_str_from_str_roundtrip() {
        for tt in [
            TableType::Primitive,
            TableType::Iterable,
            TableType::Kv,
            TableType::Gbk,
        ] {
            assert_eq!(TableType::from_str(tt.as_str()).unwrap(), tt);
        }
    }

    #[test]
    fn table_type_from_str_unknown_errors() {
        assert!(TableType::from_str("bogus").is_err());
    }

    // BeamRecord accessors

    #[test]
    /*fn beam_record_record_type_matches_variant() {
        assert_eq!(
            BeamRecord::PRIMITIVE(i(1)).record_type(),
            BeamRecordType::Primitive
        );
        assert_eq!(
            BeamRecord::ITERABLE(IterableValue::new(vec![])).record_type(),
            BeamRecordType::Iterable
        );
        assert_eq!(
            BeamRecord::KV(BeamKV {
                key: i(1),
                value: i(2)
            })
            .record_type(),
            BeamRecordType::Kv
        );
        assert_eq!(
            BeamRecord::GBK(BeamGbk {
                key: i(1),
                value: IterableValue::new(vec![])
            })
            .record_type(),
            BeamRecordType::Gbk
        );
    }*/
    #[test]
    fn beam_record_get_primitive_happy_and_wrong_variant() {
        assert_eq!(BeamRecord::PRIMITIVE(i(7)).get_primitive().unwrap(), i(7));
        let kv = BeamRecord::KV(BeamKV {
            key: i(1),
            value: i(2),
        });
        assert!(kv.get_primitive().is_err());
    }

    #[test]
    fn beam_record_get_kv_happy_and_wrong_variant() {
        let kv = BeamRecord::KV(BeamKV {
            key: s("k"),
            value: i(9),
        });
        let extracted = kv.clone().get_kv().unwrap();
        assert_eq!(extracted.key, s("k"));
        assert_eq!(extracted.value, i(9));
        assert!(BeamRecord::PRIMITIVE(i(1)).get_kv().is_err());
    }

    #[test]
    fn beam_record_get_gbk_happy_and_wrong_variant() {
        let gbk = BeamRecord::GBK(BeamGbk {
            key: s("k"),
            value: IterableValue::new(vec![i(1), i(2)]),
        });
        let extracted = gbk.clone().get_gbk().unwrap();
        assert_eq!(extracted.key, s("k"));
        assert_eq!(extracted.value.list, vec![i(1), i(2)]);
        assert!(BeamRecord::PRIMITIVE(i(1)).get_gbk().is_err());
    }

    #[test]
    fn beam_record_get_iterable_happy_and_wrong_variant() {
        let it = BeamRecord::ITERABLE(IterableValue::new(vec![i(1)]));
        assert_eq!(it.get_iterable().unwrap().list, vec![i(1)]);
        assert!(BeamRecord::PRIMITIVE(i(1)).get_iterable().is_err());
    }

    // primitive_values_to_array

    #[test]
    fn primitive_values_to_array_utf8() {
        let values = vec![s("a"), s("b"), s("c")];
        let array = primitive_values_to_array(&values, &DataType::Utf8).unwrap();
        let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(arr.len(), 3);
        assert_eq!(arr.value(0), "a");
        assert_eq!(arr.value(2), "c");
    }

    #[test]
    fn primitive_values_to_array_binary() {
        let values = vec![bytes(&[1, 2]), bytes(&[3])];
        let array = primitive_values_to_array(&values, &DataType::Binary).unwrap();
        let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
        assert_eq!(arr.value(0), &[1, 2]);
        assert_eq!(arr.value(1), &[3]);
    }

    #[test]
    fn primitive_values_to_array_int64() {
        let values = vec![i(10), i(-5), i(0)];
        let array = primitive_values_to_array(&values, &DataType::Int64).unwrap();
        let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(arr.values(), &[10, -5, 0]);
    }

    #[test]
    fn primitive_values_to_array_boolean() {
        let values = vec![b(true), b(false), b(true)];
        let array = primitive_values_to_array(&values, &DataType::Boolean).unwrap();
        let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
        assert_eq!(arr.value(0), true);
        assert_eq!(arr.value(1), false);
    }
    /*
    #[test]
    fn primitive_values_to_array_null_all_void() {
        let values = vec![PrimitiveValue::Void, PrimitiveValue::Void];
        let array = primitive_values_to_array(&values, &DataType::Null).unwrap();
        assert_eq!(array.len(), 2);
        assert_eq!(array.null_count(), 2);
    }*/

    #[test]
    fn primitive_values_to_array_null_with_non_void_errors() {
        let values = vec![PrimitiveValue::Void, i(1)];
        assert!(primitive_values_to_array(&values, &DataType::Null).is_err());
    }

    #[test]
    fn primitive_values_to_array_mixed_variant_errors() {
        let values = vec![s("a"), i(1)];
        assert!(primitive_values_to_array(&values, &DataType::Utf8).is_err());
    }

    #[test]
    fn primitive_values_to_array_unsupported_storage_type_errors() {
        let values = vec![i(1)];
        assert!(primitive_values_to_array(&values, &DataType::Float64).is_err());
    }

    #[test]
    fn primitive_values_to_array_empty_slice() {
        let values: Vec<PrimitiveValue> = vec![];
        let array = primitive_values_to_array(&values, &DataType::Int64).unwrap();
        assert_eq!(array.len(), 0);
    }

    // iterable_values_to_array

    #[test]
    fn iterable_values_to_array_varying_lengths() {
        let iterables = vec![
            IterableValue::new(vec![i(1), i(2)]),
            IterableValue::new(vec![]),
            IterableValue::new(vec![i(3)]),
        ];
        let array = iterable_values_to_array(&iterables, &DataType::Int64).unwrap();
        let list = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 3);
        assert_eq!(list.value_length(0), 2);
        assert_eq!(list.value_length(1), 0);
        assert_eq!(list.value_length(2), 1);

        let row0 = list.value(0);
        let row0 = row0.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(row0.values(), &[1, 2]);

        let row2 = list.value(2);
        let row2 = row2.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(row2.values(), &[3]);
    }

    #[test]
    fn iterable_values_to_array_all_empty() {
        let iterables = vec![IterableValue::new(vec![]), IterableValue::new(vec![])];
        // infer_iterable_item_data_type would fall back to Null here.
        let array = iterable_values_to_array(&iterables, &DataType::Null).unwrap();
        let list = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 2);
        assert_eq!(list.value_length(0), 0);
        assert_eq!(list.value_length(1), 0);
    }

    #[test]
    fn iterable_values_to_array_no_rows() {
        let iterables: Vec<IterableValue> = vec![];
        let array = iterable_values_to_array(&iterables, &DataType::Int64).unwrap();
        assert_eq!(array.len(), 0);
    }

    // primitive_value_from_array_row

    #[test]
    fn primitive_value_from_array_row_all_types() {
        let strings = StringArray::from(vec!["x", "y"]);
        assert_eq!(
            primitive_value_from_array_row(&strings, &DataType::Utf8, 1).unwrap(),
            s("y")
        );

        let ints = Int64Array::from(vec![10, 20]);
        assert_eq!(
            primitive_value_from_array_row(&ints, &DataType::Int64, 0).unwrap(),
            i(10)
        );

        let bools = BooleanArray::from(vec![true, false]);
        assert_eq!(
            primitive_value_from_array_row(&bools, &DataType::Boolean, 1).unwrap(),
            b(false)
        );

        let bin = BinaryArray::from(vec![&b"hi"[..], &b"lo"[..]]);
        assert_eq!(
            primitive_value_from_array_row(&bin, &DataType::Binary, 0).unwrap(),
            bytes(b"hi")
        );
    }

    #[test]
    fn primitive_value_from_array_row_null_becomes_void() {
        let strings = StringArray::from(vec![Some("x"), None]);
        assert_eq!(
            primitive_value_from_array_row(&strings, &DataType::Utf8, 1).unwrap(),
            PrimitiveValue::Void
        );
    }

    #[test]
    fn primitive_value_from_array_row_wrong_downcast_errors() {
        let ints = Int64Array::from(vec![1, 2]);
        // Claiming Utf8 for an Int64Array should fail the downcast.
        assert!(primitive_value_from_array_row(&ints, &DataType::Utf8, 0).is_err());
    }

    #[test]
    fn primitive_value_from_array_row_unsupported_type_errors() {
        let ints = Int64Array::from(vec![1]);
        assert!(primitive_value_from_array_row(&ints, &DataType::Float32, 0).is_err());
    }

    //  iterable_value_from_array_row

    #[test]
    fn iterable_value_from_array_row_roundtrips_via_builder() {
        let iterables = vec![
            IterableValue::new(vec![i(1), i(2), i(3)]),
            IterableValue::new(vec![]),
        ];
        let array = iterable_values_to_array(&iterables, &DataType::Int64).unwrap();
        let list_type = DataType::List(Arc::new(ArrowField::new("item", DataType::Int64, false)));

        let row0 = iterable_value_from_array_row(array.as_ref(), &list_type, 0).unwrap();
        assert_eq!(row0.list, vec![i(1), i(2), i(3)]);

        let row1 = iterable_value_from_array_row(array.as_ref(), &list_type, 1).unwrap();
        assert_eq!(row1.list, Vec::<PrimitiveValue>::new());
    }

    #[test]
    fn iterable_value_from_array_row_null_row_yields_empty_list() {
        let item_field = Arc::new(ArrowField::new("item", DataType::Int64, false));
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0i32, 0]));
        let child = Int64Array::from(Vec::<i64>::new());
        let validity = arrow_buffer::NullBuffer::from(vec![false]); // row 0 is null
        let list = ListArray::new(item_field, offsets, Arc::new(child), Some(validity));

        let list_type = DataType::List(Arc::new(ArrowField::new("item", DataType::Int64, false)));
        let result = iterable_value_from_array_row(&list, &list_type, 0).unwrap();
        assert_eq!(result.list, Vec::<PrimitiveValue>::new());
    }

    #[test]
    fn iterable_value_from_array_row_non_list_type_errors() {
        let ints = Int64Array::from(vec![1]);
        assert!(iterable_value_from_array_row(&ints, &DataType::Int64, 0).is_err());
    }

    //  derive_table_schema

    #[test]
    fn derive_table_schema_primitive() {
        let records = vec![BeamRecord::PRIMITIVE(s("a")), BeamRecord::PRIMITIVE(s("b"))];
        let schema = derive_table_schema("pc1", &records).unwrap();
        assert_eq!(schema.table_type, TableType::Primitive);
        assert_eq!(schema.arrow_schema.fields().len(), 1);
        let field = schema.arrow_schema.field_with_name(VALUE_COLUMN).unwrap();
        assert_eq!(field.data_type(), &DataType::Utf8);
        assert!(!field.is_nullable());
    }

    #[test]
    fn derive_table_schema_primitive_mixed_types_errors() {
        let records = vec![BeamRecord::PRIMITIVE(s("a")), BeamRecord::PRIMITIVE(i(1))];
        assert!(derive_table_schema("pc1", &records).is_err());
    }

    #[test]
    fn derive_table_schema_iterable() {
        let records = vec![
            BeamRecord::ITERABLE(IterableValue::new(vec![i(1), i(2)])),
            BeamRecord::ITERABLE(IterableValue::new(vec![])),
        ];
        let schema = derive_table_schema("pc2", &records).unwrap();
        assert_eq!(schema.table_type, TableType::Iterable);
        let field = schema.arrow_schema.field_with_name(VALUE_COLUMN).unwrap();
        match field.data_type() {
            DataType::List(item) => assert_eq!(item.data_type(), &DataType::Int64),
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn derive_table_schema_iterable_mixed_item_types_errors() {
        let records = vec![BeamRecord::ITERABLE(IterableValue::new(vec![i(1), s("x")]))];
        assert!(derive_table_schema("pc2", &records).is_err());
    }

    #[test]
    fn derive_table_schema_kv() {
        let records = vec![
            BeamRecord::KV(BeamKV {
                key: s("k1"),
                value: i(1),
            }),
            BeamRecord::KV(BeamKV {
                key: s("k2"),
                value: i(2),
            }),
        ];
        let schema = derive_table_schema("pc3", &records).unwrap();
        assert_eq!(schema.table_type, TableType::Kv);
        assert_eq!(
            schema
                .arrow_schema
                .field_with_name(KEY_COLUMN)
                .unwrap()
                .data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema
                .arrow_schema
                .field_with_name(VALUE_COLUMN)
                .unwrap()
                .data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn derive_table_schema_kv_mixed_key_types_errors() {
        let records = vec![
            BeamRecord::KV(BeamKV {
                key: s("k1"),
                value: i(1),
            }),
            BeamRecord::KV(BeamKV {
                key: i(9),
                value: i(2),
            }),
        ];
        assert!(derive_table_schema("pc3", &records).is_err());
    }

    #[test]
    fn derive_table_schema_kv_mixed_value_types_errors() {
        let records = vec![
            BeamRecord::KV(BeamKV {
                key: s("k1"),
                value: i(1),
            }),
            BeamRecord::KV(BeamKV {
                key: s("k2"),
                value: s("oops"),
            }),
        ];
        assert!(derive_table_schema("pc3", &records).is_err());
    }

    #[test]
    fn derive_table_schema_gbk() {
        let records = vec![
            BeamRecord::GBK(BeamGbk {
                key: i(1),
                value: IterableValue::new(vec![s("a"), s("b")]),
            }),
            BeamRecord::GBK(BeamGbk {
                key: i(2),
                value: IterableValue::new(vec![]),
            }),
        ];
        let schema = derive_table_schema("pc4", &records).unwrap();
        assert_eq!(schema.table_type, TableType::Gbk);
        assert_eq!(
            schema
                .arrow_schema
                .field_with_name(KEY_COLUMN)
                .unwrap()
                .data_type(),
            &DataType::Int64
        );
        match schema
            .arrow_schema
            .field_with_name(VALUE_COLUMN)
            .unwrap()
            .data_type()
        {
            DataType::List(item) => assert_eq!(item.data_type(), &DataType::Utf8),
            other => panic!("expected List, got {other:?}"),
        }
    }

    #[test]
    fn derive_table_schema_gbk_mixed_key_types_errors() {
        let records = vec![
            BeamRecord::GBK(BeamGbk {
                key: i(1),
                value: IterableValue::new(vec![]),
            }),
            BeamRecord::GBK(BeamGbk {
                key: s("bad"),
                value: IterableValue::new(vec![]),
            }),
        ];
        assert!(derive_table_schema("pc4", &records).is_err());
    }

    #[test]
    fn derive_table_schema_empty_records_errors() {
        let records: Vec<BeamRecord> = vec![];
        assert!(derive_table_schema("pc5", &records).is_err());
    }

    #[test]
    fn derive_table_schema_mixed_record_variants_errors() {
        let records = vec![
            BeamRecord::PRIMITIVE(i(1)),
            BeamRecord::KV(BeamKV {
                key: i(1),
                value: i(2),
            }),
        ];
        assert!(derive_table_schema("pc6", &records).is_err());
    }

    //  beamrecords_to_record_batch / record_batch_to_beamrecords roundtrips

    #[test]
    fn roundtrip_primitive() {
        let records = vec![
            BeamRecord::PRIMITIVE(i(1)),
            BeamRecord::PRIMITIVE(i(2)),
            BeamRecord::PRIMITIVE(i(3)),
        ];
        let schema = derive_table_schema("pc", &records).unwrap();
        let batch = beamrecords_to_record_batch(&records, &schema).unwrap();
        assert_eq!(batch.num_rows(), 3);

        let back = record_batch_to_beamrecords(&batch, &schema).unwrap();
        assert_eq!(back.len(), 3);
        assert_primitive(&back[0], &i(1));
        assert_primitive(&back[1], &i(2));
        assert_primitive(&back[2], &i(3));
    }

    #[test]
    fn roundtrip_kv() {
        let records = vec![
            BeamRecord::KV(BeamKV {
                key: s("a"),
                value: i(1),
            }),
            BeamRecord::KV(BeamKV {
                key: s("b"),
                value: i(2),
            }),
        ];
        let schema = derive_table_schema("pc", &records).unwrap();
        let batch = beamrecords_to_record_batch(&records, &schema).unwrap();
        let back = record_batch_to_beamrecords(&batch, &schema).unwrap();
        assert_kv(&back[0], &s("a"), &i(1));
        assert_kv(&back[1], &s("b"), &i(2));
    }

    #[test]
    fn roundtrip_iterable() {
        let records = vec![
            BeamRecord::ITERABLE(IterableValue::new(vec![i(1), i(2)])),
            BeamRecord::ITERABLE(IterableValue::new(vec![])),
            BeamRecord::ITERABLE(IterableValue::new(vec![i(3)])),
        ];
        let schema = derive_table_schema("pc", &records).unwrap();
        let batch = beamrecords_to_record_batch(&records, &schema).unwrap();
        let back = record_batch_to_beamrecords(&batch, &schema).unwrap();
        assert_iterable(&back[0], &[i(1), i(2)]);
        assert_iterable(&back[1], &[]);
        assert_iterable(&back[2], &[i(3)]);
    }

    #[test]
    fn roundtrip_gbk() {
        let records = vec![
            BeamRecord::GBK(BeamGbk {
                key: s("k1"),
                value: IterableValue::new(vec![i(10), i(20), i(30)]),
            }),
            BeamRecord::GBK(BeamGbk {
                key: s("k2"),
                value: IterableValue::new(vec![]),
            }),
        ];
        let schema = derive_table_schema("pc", &records).unwrap();
        let batch = beamrecords_to_record_batch(&records, &schema).unwrap();
        let back = record_batch_to_beamrecords(&batch, &schema).unwrap();
        assert_gbk(&back[0], &s("k1"), &[i(10), i(20), i(30)]);
        assert_gbk(&back[1], &s("k2"), &[]);
    }

    #[test]
    fn roundtrip_boolean_and_bytes_primitive() {
        let records = vec![
            BeamRecord::PRIMITIVE(b(true)),
            BeamRecord::PRIMITIVE(b(false)),
        ];
        let schema = derive_table_schema("pc", &records).unwrap();
        let batch = beamrecords_to_record_batch(&records, &schema).unwrap();
        let back = record_batch_to_beamrecords(&batch, &schema).unwrap();
        assert_primitive(&back[0], &b(true));
        assert_primitive(&back[1], &b(false));

        let records = vec![
            BeamRecord::PRIMITIVE(bytes(&[1, 2, 3])),
            BeamRecord::PRIMITIVE(bytes(&[])),
        ];
        let schema = derive_table_schema("pc", &records).unwrap();
        let batch = beamrecords_to_record_batch(&records, &schema).unwrap();
        let back = record_batch_to_beamrecords(&batch, &schema).unwrap();
        assert_primitive(&back[0], &bytes(&[1, 2, 3]));
        assert_primitive(&back[1], &bytes(&[]));
    }

    #[test]
    fn beamrecords_to_record_batch_empty_records_errors() {
        let records: Vec<BeamRecord> = vec![];
        // Schema itself can't be derived from empty input either, so build
        // a schema from a throwaway non-empty batch and feed empty records in.
        let seed = vec![BeamRecord::PRIMITIVE(i(1))];
        let schema = derive_table_schema("pc", &seed).unwrap();
        assert!(beamrecords_to_record_batch(&records, &schema).is_err());
    }

    #[test]
    fn beamrecords_to_record_batch_variant_mismatch_errors() {
        // Schema derived as Kv, but records passed in are Primitive.
        let kv_seed = vec![BeamRecord::KV(BeamKV {
            key: i(1),
            value: i(2),
        })];
        let schema = derive_table_schema("pc", &kv_seed).unwrap();
        let wrong_records = vec![BeamRecord::PRIMITIVE(i(1))];
        assert!(beamrecords_to_record_batch(&wrong_records, &schema).is_err());
    }

    //  large-ish iterable to sanity check offset accumulation

    #[test]
    fn iterable_values_to_array_many_rows() {
        let iterables: Vec<IterableValue> = (0..100)
            .map(|n| IterableValue::new((0..n % 5).map(i).collect()))
            .collect();
        let total: usize = iterables.iter().map(|it| it.list.len()).sum();
        let array = iterable_values_to_array(&iterables, &DataType::Int64).unwrap();
        let list = array.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(list.len(), 100);
        assert_eq!(list.values().len(), total);
    }
}
