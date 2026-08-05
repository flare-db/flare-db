use std::sync::Arc;

use anyhow::{Context, Result, anyhow};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, ListArray, NullArray, RecordBatch,
    StringArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use std::hash::{Hash, Hasher};

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
            Self::Bool(value) => {
                3_u8.hash(state);
                value.hash(state);
            }
            Self::Void => {
                4_u8.hash(state);
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

fn primitive_data_type(value: &PrimitiveValue) -> DataType {
    match value {
        PrimitiveValue::String(_) => DataType::Utf8,
        PrimitiveValue::Bytes(_) => DataType::Binary,
        PrimitiveValue::Int64(_) => DataType::Int64,
        PrimitiveValue::Bool(_) => DataType::Boolean,
        PrimitiveValue::Void => DataType::Null,
    }
}

fn primitive_type_matches(value: &PrimitiveValue, data_type: &DataType) -> bool {
    &primitive_data_type(value) == data_type
}

fn iterable_values(iterable: &IterableValue) -> &[PrimitiveValue] {
    iterable.list.as_slice()
}

fn infer_iterable_item_data_type(iterables: &[IterableValue]) -> DataType {
    iterables
        .iter()
        .flat_map(iterable_values)
        .next()
        .map(primitive_data_type)
        .unwrap_or(DataType::Null)
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
    data_type: &DataType,
) -> Result<ArrayRef> {
    match data_type {
        DataType::Utf8 => {
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
        DataType::Binary => {
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
        DataType::Int64 => {
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
        DataType::Boolean => {
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
        DataType::Null => {
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
    item_data_type: &DataType,
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

    let nullable = matches!(item_data_type, DataType::Null);
    let item_field = Arc::new(Field::new("item", item_data_type.clone(), nullable));

    Ok(Arc::new(ListArray::new(item_field, offsets, child, None)))
}

fn primitive_value_from_array_row(
    array: &dyn Array,
    data_type: &DataType,
    row: usize,
) -> Result<PrimitiveValue> {
    if array.is_null(row) {
        return Ok(PrimitiveValue::Void);
    }

    match data_type {
        DataType::Utf8 => {
            let array = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| anyhow!("expected StringArray for Utf8 primitive column"))?;
            Ok(PrimitiveValue::String(array.value(row).to_string()))
        }
        DataType::Binary => {
            let array = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| anyhow!("expected BinaryArray for Binary primitive column"))?;
            Ok(PrimitiveValue::Bytes(array.value(row).to_vec()))
        }
        DataType::Int64 => {
            let array = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| anyhow!("expected Int64Array for Int64 primitive column"))?;
            Ok(PrimitiveValue::Int64(array.value(row)))
        }
        DataType::Boolean => {
            let array = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| anyhow!("expected BooleanArray for Boolean primitive column"))?;
            Ok(PrimitiveValue::Bool(array.value(row)))
        }
        DataType::Null => Ok(PrimitiveValue::Void),
        other => Err(anyhow!("unsupported primitive storage type: {other:?}")),
    }
}

fn iterable_value_from_array_row(
    array: &dyn Array,
    data_type: &DataType,
    row: usize,
) -> Result<IterableValue> {
    let DataType::List(item_field) = data_type else {
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
    item_data_type: &DataType,
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

            Arc::new(Schema::new(vec![Field::new(
                VALUE_COLUMN,
                data_type,
                false,
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
            let nullable = matches!(item_data_type, DataType::Null);

            Arc::new(Schema::new(vec![Field::new(
                VALUE_COLUMN,
                DataType::List(Arc::new(Field::new("item", item_data_type, nullable))),
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

            Arc::new(Schema::new(vec![
                Field::new(KEY_COLUMN, key_data_type, false),
                Field::new(VALUE_COLUMN, value_data_type, false),
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
            let nullable = matches!(item_data_type, DataType::Null);

            Arc::new(Schema::new(vec![
                Field::new(KEY_COLUMN, key_data_type, false),
                Field::new(
                    VALUE_COLUMN,
                    DataType::List(Arc::new(Field::new("item", item_data_type, nullable))),
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
            let DataType::List(item_field) = data_type else {
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
            let DataType::List(item_field) = value_data_type else {
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
