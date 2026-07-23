use std::{collections::HashMap, sync::Arc};

use anyhow::{Context, Result, anyhow};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, ListArray, NullArray, RecordBatch,
    StringArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use dashmap::DashMap;
use fusio::disk::TokioFs;
use fusio::executor::tokio::TokioExecutor;
use log::info;
use std::hash::{Hash, Hasher};
use tokio_util::task::LocalPoolHandle;
use tonbo::db::{DB, DbBuilder};
use tonbo::prelude::*;
use typed_arrow::{List, Null};
use uuid::Uuid;

const ELEMENT_ID_COLUMN: &str = "element_id";
//const PCOLLECTION_ID_COLUMN: &str = "pcollection_id";
const COLLECTION_COLUMN: &str = "collection";
const KEY_COLUMN: &str = "key";
const VALUE_COLUMN: &str = "value";
const RECORD_TYPE_METADATA_KEY: &str = "flare.record_type";
const TABLE_NAME: &str = "table_name";

#[derive(Debug, Clone)]
pub enum BeamRecord {
    PRIMITIVE(PrimitiveValue),
    //COMPOSITE(BeamKV),
    ITERABLE(IterableValue),
    KV(BeamKV),
    GBK(BeamGbk),
    //VOID,
}
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
            // BeamRecord::COMPOSITE(_) => BeamRecordType::Composite,
            BeamRecord::ITERABLE(_) => BeamRecordType::Iterable,
            BeamRecord::GBK(_) => BeamRecordType::Gbk,
            BeamRecord::KV(_) => BeamRecordType::Kv,
            //BeamRecord::VOID => BeamRecordType::Void,
        }
    }

    pub fn get_primitive(&self) -> Result<PrimitiveValue> {
        match self {
            BeamRecord::PRIMITIVE(value) => Ok(value.clone()),
            _ => Err(anyhow!("exculuded other types")),
        }
    }

    pub fn get_kv(self) -> Result<BeamKV> {
        match self {
            BeamRecord::KV(value) => Ok(value.clone()),
            _ => Err(anyhow!("exculuded other types")),
        }
    }

    pub fn get_gbk(self) -> Result<BeamGbk> {
        match self {
            BeamRecord::GBK(value) => Ok(value.clone()),
            _ => Err(anyhow!("exculuded other types")),
        }
    }

    pub fn get_iterable(&self) -> Result<IterableValue> {
        match self {
            BeamRecord::ITERABLE(value) => Ok(value.clone()),
            _ => Err(anyhow!("exculuded other types")),
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
    pub(crate) list: List<PrimitiveValue>,
}

impl IterableValue {
    pub fn new(list: List<PrimitiveValue>) -> Self {
        Self { list }
    }
}

#[derive(Debug, Clone)]
pub enum PrimitiveValue {
    String(String),
    Bytes(Vec<u8>),
    Int64(i64),
    Bool(bool),
    Void(Null),
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
            Self::Void(_) => {
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
            (Self::Void(_), Self::Void(_)) => true,
            _ => false,
        }
    }
}

impl Eq for PrimitiveValue {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageRecordType {
    Primitive,
    Iterable,
    Kv,
    Gbk,
}

impl StorageRecordType {
    fn as_str(self) -> &'static str {
        match self {
            StorageRecordType::Primitive => "primitive",
            StorageRecordType::Iterable => "iterable",
            StorageRecordType::Kv => "kv",
            StorageRecordType::Gbk => "gbk",
        }
    }

    fn from_str(value: &str) -> Result<Self> {
        match value {
            "primitive" => Ok(StorageRecordType::Primitive),
            "iterable" => Ok(StorageRecordType::Iterable),
            "kv" => Ok(StorageRecordType::Kv),
            "gbk" => Ok(StorageRecordType::Gbk),
            other => Err(anyhow!("unknown store record type: {other}")),
        }
    }
}

fn storage_record_type(record: &BeamRecord) -> StorageRecordType {
    match record {
        BeamRecord::PRIMITIVE(_) => StorageRecordType::Primitive,
        BeamRecord::ITERABLE(_) => StorageRecordType::Iterable,
        BeamRecord::KV(_) => StorageRecordType::Kv,
        BeamRecord::GBK(_) => StorageRecordType::Gbk,
    }
}

pub fn create_schema_with_record_type(
    fields: Vec<Field>,
    record_type: &str,
    pcollection_id: &str,
) -> Result<Arc<Schema>> {
    StorageRecordType::from_str(record_type)?;

    // add metada for the schema
    let mut metadata = HashMap::new();
    metadata.insert(
        RECORD_TYPE_METADATA_KEY.to_string(),
        record_type.to_string(),
    );
    metadata.insert(TABLE_NAME.to_string(), pcollection_id.to_string());

    Ok(Arc::new(Schema::new(fields).with_metadata(metadata)))
}

fn primitive_data_type(value: &PrimitiveValue) -> DataType {
    match value {
        PrimitiveValue::String(_) => DataType::Utf8,
        PrimitiveValue::Bytes(_) => DataType::Binary,
        PrimitiveValue::Int64(_) => DataType::Int64,
        PrimitiveValue::Bool(_) => DataType::Boolean,
        PrimitiveValue::Void(_) => DataType::Null,
    }
}

fn primitive_type_matches(value: &PrimitiveValue, data_type: &DataType) -> bool {
    &primitive_data_type(value) == data_type
}

fn iterable_values(iterable: &IterableValue) -> &[PrimitiveValue] {
    iterable.list.values()
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

fn primitive_values_to_array(values: &[PrimitiveValue], data_type: &DataType) -> Result<ArrayRef> {
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
                .all(|value| matches!(value, PrimitiveValue::Void(_)))
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

fn iterable_values_to_array(
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

    //let item_field = Arc::new(Field::new("item", item_data_type.clone(), true));
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
        return Ok(PrimitiveValue::Void(Null));
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
        DataType::Null => Ok(PrimitiveValue::Void(Null)),
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
        return Ok(IterableValue::new(List::new(Vec::new())));
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

    Ok(IterableValue::new(List::new(values)))
}

fn record_type_from_schema(schema: &Schema) -> Result<StorageRecordType> {
    let record_type = schema
        .metadata()
        .get(RECORD_TYPE_METADATA_KEY)
        .ok_or_else(|| anyhow!("missing {RECORD_TYPE_METADATA_KEY} schema metadata"))?;

    StorageRecordType::from_str(record_type)
}

fn field_data_type<'a>(schema: &'a Schema, name: &str) -> Result<&'a DataType> {
    schema
        .field_with_name(name)
        .map(Field::data_type)
        .with_context(|| format!("missing schema field {name}"))
}

/// Generates arrow schema for Beam types
pub fn derive_schema_from_records(
    pcollection_id: &str,
    records: &[BeamRecord],
) -> Result<Arc<Schema>> {
    let first = records
        .first()
        .ok_or_else(|| anyhow!("cannot derive schema for empty pcollection {pcollection_id}"))?;
    let record_type = storage_record_type(first);

    match record_type {
        StorageRecordType::Primitive => {
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

            create_schema_with_record_type(
                vec![
                    Field::new(ELEMENT_ID_COLUMN, DataType::Utf8, false),
                    //Field::new(PCOLLECTION_ID_COLUMN, DataType::Utf8, false),
                    Field::new(COLLECTION_COLUMN, data_type, false),
                ],
                record_type.as_str(),
                pcollection_id,
            )
        }
        StorageRecordType::Iterable => {
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

            create_schema_with_record_type(
                vec![
                    Field::new(ELEMENT_ID_COLUMN, DataType::Utf8, false),
                    //Field::new(PCOLLECTION_ID_COLUMN, DataType::Utf8, false),
                    Field::new(
                        COLLECTION_COLUMN,
                        // DataType::List(Arc::new(Field::new("item", item_data_type, true))),
                        DataType::List(Arc::new(Field::new("item", item_data_type, nullable))),
                        true,
                    ),
                ],
                record_type.as_str(),
                pcollection_id,
            )
        }
        StorageRecordType::Kv => {
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

            create_schema_with_record_type(
                vec![
                    Field::new(ELEMENT_ID_COLUMN, DataType::Utf8, false),
                    // Field::new(PCOLLECTION_ID_COLUMN, DataType::Utf8, false),
                    Field::new(KEY_COLUMN, key_data_type, false),
                    Field::new(VALUE_COLUMN, value_data_type, false),
                ],
                record_type.as_str(),
                pcollection_id,
            )
        }
        StorageRecordType::Gbk => {
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

            create_schema_with_record_type(
                vec![
                    Field::new(ELEMENT_ID_COLUMN, DataType::Utf8, false),
                    // Field::new(PCOLLECTION_ID_COLUMN, DataType::Utf8, false),
                    Field::new(KEY_COLUMN, key_data_type, false),
                    Field::new(
                        VALUE_COLUMN,
                        //DataType::List(Arc::new(Field::new("item", item_data_type, true))),
                        DataType::List(Arc::new(Field::new("item", item_data_type, nullable))),
                        true,
                    ),
                ],
                record_type.as_str(),
                pcollection_id,
            )
        }
    }
}

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
/// conters beam records to arrow record batch
pub fn beamrecords_to_record_batch(
    _pcollection_id: &str,
    records: &[BeamRecord],
    schema: Arc<Schema>,
) -> Result<RecordBatch> {
    if records.is_empty() {
        return Err(anyhow!("cannot build record batch from empty records"));
    }

    let record_type = record_type_from_schema(&schema)?;
    info!("record type: {:?}", record_type);
    let row_count = records.len();
    let element_ids: Vec<String> = (0..row_count).map(|_| Uuid::new_v4().to_string()).collect();

    // TODO: fix this we removed pcollection_id column, so its not needed during conversion
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(StringArray::from(element_ids)),
        /*Arc::new(StringArray::from(vec![
            pcollection_id.to_string();
            row_count
        ])),*/
    ];

    match record_type {
        StorageRecordType::Primitive => {
            let data_type = field_data_type(&schema, COLLECTION_COLUMN)?;
            let values = records
                .iter()
                .map(|record| match record {
                    BeamRecord::PRIMITIVE(value) => Ok(value.clone()),
                    _ => Err(anyhow!("expected primitive record")),
                })
                .collect::<Result<Vec<_>>>()?;
            columns.push(primitive_values_to_array(&values, data_type)?);
        }
        StorageRecordType::Iterable => {
            let data_type = field_data_type(&schema, COLLECTION_COLUMN)?;
            let DataType::List(item_field) = data_type else {
                return Err(anyhow!("iterable collection field must be a List"));
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
        StorageRecordType::Kv => {
            let key_data_type = field_data_type(&schema, KEY_COLUMN)?;
            let value_data_type = field_data_type(&schema, VALUE_COLUMN)?;
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
        StorageRecordType::Gbk => {
            let key_data_type = field_data_type(&schema, KEY_COLUMN)?;
            let value_data_type = field_data_type(&schema, VALUE_COLUMN)?;
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

    RecordBatch::try_new(schema, columns).context("failed to build store record batch")
}

/// convert arrow record batch to beam records
pub fn record_batch_to_beamrecords(
    batch: &RecordBatch,
    schema: &Schema,
) -> Result<Vec<BeamRecord>> {
    let record_type = record_type_from_schema(schema)?;
    let mut records = Vec::with_capacity(batch.num_rows());

    match record_type {
        StorageRecordType::Primitive => {
            let column = batch
                .column_by_name(COLLECTION_COLUMN)
                .ok_or_else(|| anyhow!("missing {COLLECTION_COLUMN} column"))?;
            for row in 0..batch.num_rows() {
                records.push(BeamRecord::PRIMITIVE(primitive_value_from_array_row(
                    column.as_ref(),
                    column.data_type(),
                    row,
                )?));
            }
        }
        StorageRecordType::Iterable => {
            let column = batch
                .column_by_name(COLLECTION_COLUMN)
                .ok_or_else(|| anyhow!("missing {COLLECTION_COLUMN} column"))?;
            for row in 0..batch.num_rows() {
                records.push(BeamRecord::ITERABLE(iterable_value_from_array_row(
                    column.as_ref(),
                    column.data_type(),
                    row,
                )?));
            }
        }
        StorageRecordType::Kv => {
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
        StorageRecordType::Gbk => {
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

// Registry for maintaing each Pcollection's schema
#[derive(Clone, Default)]
pub struct FlareSchemaRegistry {
    schemas: Arc<DashMap<String, Arc<Schema>>>,
}

impl FlareSchemaRegistry {
    pub fn new() -> Self {
        Self {
            schemas: Arc::new(DashMap::new()),
        }
    }

    pub fn get(&self, pcollection_id: &str) -> Option<Arc<Schema>> {
        self.schemas
            .get(pcollection_id)
            .map(|schema| schema.clone())
    }

    pub fn register_schema(&self, pcollection_id: &str, schema: Arc<Schema>) {
        self.schemas.insert(pcollection_id.to_string(), schema);
    }

    pub fn register_schema_if_absent(&self, pcollection_id: &str, schema: Arc<Schema>) {
        self.schemas
            .entry(pcollection_id.to_string())
            .or_insert(schema);
    }
}

#[derive(Clone)]
pub struct FlareElementStore {
    pub(crate) registry: FlareSchemaRegistry,
    open_dbs: Arc<DashMap<String, Arc<DB<TokioFs, TokioExecutor>>>>,
    local_pool: LocalPoolHandle,
    base_path: String,
}

impl FlareElementStore {
    async fn ingest_batch(
        &self,
        pcollection_id: &str,
        schema: Arc<Schema>,
        batch: RecordBatch,
    ) -> Result<()> {
        self.registry
            .register_schema_if_absent(pcollection_id, schema.clone());

        let db = self.resolve_db(pcollection_id, Some(schema)).await?;

        db.ingest(batch)
            .await
            .with_context(|| format!("failed to ingest pcollection {pcollection_id}"))?;

        Ok(())
    }

    fn prepare_record_batch(
        &self,
        pcollection_id: &str,
        batch: RecordBatch,
        schema: Arc<Schema>,
    ) -> Result<(Arc<Schema>, RecordBatch)> {
        // if ELEMENT_ID_COLUMN is present, return the schema.
        let full_schema = if schema.field_with_name(ELEMENT_ID_COLUMN).is_ok() {
            schema.clone()
        } else {
            // create fields with ELEMENT_ID_COLUMN, cause sometimes transfroms(like gbk)
            // can only prouce record batches with projected schema i.e, filtered output
            // that only return the required columns, but ELEMENT_ID_COLUMN is required primary key
            // column for tonbo So, we add that to output schema.
            let mut fields = Vec::with_capacity(schema.fields().len() + 1);
            fields.push(Field::new(ELEMENT_ID_COLUMN, DataType::Utf8, false));
            fields.extend(schema.fields().iter().map(|field| field.as_ref().clone()));
            // create schema using fields.
            create_schema_with_record_type(
                fields,
                record_type_from_schema(&schema)?.as_str(),
                pcollection_id,
            )?
        };

        if batch
            .schema_ref()
            .field_with_name(ELEMENT_ID_COLUMN)
            .is_ok()
        {
            return Ok((full_schema, batch));
        }

        let row_count = batch.num_rows();
        let element_ids: Vec<String> = (0..row_count).map(|_| Uuid::new_v4().to_string()).collect();
        let mut columns: Vec<ArrayRef> = vec![Arc::new(StringArray::from(element_ids))];
        columns.extend(batch.columns().iter().cloned());

        let batch = RecordBatch::try_new(full_schema.clone(), columns)
            .context("failed to build store record batch")?;

        Ok((full_schema, batch))
    }

    pub fn new(registry: FlareSchemaRegistry) -> Self {
        let default_base = crate::utils::path::base_dir().join("store");
        Self::with_base_path(registry, default_base.to_str().unwrap_or(".").to_string())
    }

    pub fn with_base_path(registry: FlareSchemaRegistry, base_path: String) -> Self {
        Self {
            registry,
            open_dbs: Arc::new(DashMap::new()),
            local_pool: LocalPoolHandle::new(1),
            base_path,
        }
    }

    // each db is per pcollection_id, So, each db has its own schema and the data stored
    // in each db belongs to that pcollection_id only.
    pub async fn resolve_db(
        &self,
        pcollection_id: &str,
        schema: Option<Arc<Schema>>,
    ) -> Result<Arc<DB<TokioFs, TokioExecutor>>> {
        if let Some(db) = self.open_dbs.get(pcollection_id) {
            return Ok(db.value().clone());
        }

        let schema = match self.registry.get(pcollection_id) {
            Some(schema) => schema,
            None => {
                let schema = schema
                    .ok_or_else(|| anyhow!("schema not found for pcollection {pcollection_id}"))?;
                self.registry
                    .register_schema(pcollection_id, schema.clone());
                schema
            }
        };

        let safe_id = pcollection_id.replace(['/', '.', ' '], "_");

        let db = DbBuilder::from_schema_key_name(schema, ELEMENT_ID_COLUMN)?
            .on_disk(format!("{}/{safe_id}", self.base_path))?
            .open()
            .await?;

        let db = Arc::new(db);
        self.open_dbs.insert(pcollection_id.to_string(), db.clone());

        Ok(db)
    }

    // used when a transfrom/stage produces beam records and that needs to be converted
    // to arrow record batch before ingesting into db.
    pub async fn write_beamrecord_batch(&self, req: NewCollectionRequest) -> Result<()> {
        info!("store: starting to write collection");

        let schema = match self.registry.get(&req.pcollection_id) {
            Some(schema) => schema,
            None => derive_schema_from_records(&req.pcollection_id, &req.elements)?,
        };

        let batch =
            beamrecords_to_record_batch(&req.pcollection_id, &req.elements, schema.clone())?;

        self.ingest_batch(&req.pcollection_id, schema, batch).await
    }

    // used when a transfrom can directly produce arrow record batch
    pub async fn write_record_batch(
        &self,
        pcollection_id: &str,
        batch: RecordBatch,
        schema: Arc<Schema>,
    ) -> Result<()> {
        let schema = self.registry.get(pcollection_id).unwrap_or(schema);

        let (schema, batch) = self.prepare_record_batch(pcollection_id, batch, schema)?;

        self.ingest_batch(pcollection_id, schema, batch).await
    }

    pub async fn scan_collection(&self, req: ScanCollectionRequest) -> Result<Vec<BeamRecord>> {
        let db = self.resolve_db(&req.pcollection_id, None).await?;
        let schema = self
            .registry
            .get(&req.pcollection_id)
            .ok_or_else(|| anyhow!("schema not found for pcollection {}", req.pcollection_id))?;

        /*let filter = Expr::eq(
            PCOLLECTION_ID_COLUMN,
            ScalarValue::from(req.pcollection_id.clone()),
        );*/

        //Tonbo's scan is a !Send so we isolate that in a separate thread.
        let batches = self
            .local_pool
            //.spawn_pinned(move || async move { db.scan().filter(filter).collect().await })
            .spawn_pinned(move || async move { db.scan().collect().await })
            .await
            .map_err(|error| anyhow!("scan task panicked: {error}"))??;

        let mut records = Vec::new();
        for batch in batches {
            records.extend(record_batch_to_beamrecords(&batch, &schema)?);
        }

        Ok(records)
    }
}

#[derive(Debug)]
pub struct NewCollectionRequest {
    pub(crate) pcollection_id: String,
    pub(crate) elements: Vec<BeamRecord>,
}

#[derive(Debug)]
pub struct ScanCollectionRequest {
    pub(crate) pcollection_id: String,
}

#[cfg(test)]
mod tests {

    use std::sync::Arc;

    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field};
    use typed_arrow::{List, Null};

    use super::{KEY_COLUMN, VALUE_COLUMN, iterable_values_to_array, primitive_values_to_array};
    use crate::engine::store::{
        BeamGbk, BeamKV, BeamRecord, FlareElementStore, FlareSchemaRegistry, IterableValue,
        NewCollectionRequest, PrimitiveValue, ScanCollectionRequest, beamrecords_to_record_batch,
        create_schema_with_record_type, derive_schema_from_records, record_batch_to_beamrecords,
    };

    // helpers

    fn str(s: &str) -> PrimitiveValue {
        PrimitiveValue::String(s.to_string())
    }

    fn bytes(b: &[u8]) -> PrimitiveValue {
        PrimitiveValue::Bytes(b.to_vec())
    }

    fn int(i: i64) -> PrimitiveValue {
        PrimitiveValue::Int64(i)
    }

    fn void() -> PrimitiveValue {
        PrimitiveValue::Void(Null)
    }

    fn iterable(values: Vec<PrimitiveValue>) -> IterableValue {
        IterableValue::new(List::new(values))
    }

    fn primitive(v: PrimitiveValue) -> BeamRecord {
        BeamRecord::PRIMITIVE(v)
    }

    fn kv(k: PrimitiveValue, v: PrimitiveValue) -> BeamRecord {
        BeamRecord::KV(BeamKV { key: k, value: v })
    }

    fn gbk(k: PrimitiveValue, v: IterableValue) -> BeamRecord {
        BeamRecord::GBK(BeamGbk { key: k, value: v })
    }

    fn iter_record(v: IterableValue) -> BeamRecord {
        BeamRecord::ITERABLE(v)
    }

    // PrimitiveValue: Hash + PartialEq

    #[test]
    fn primitive_value_equality() {
        assert_eq!(str("hello"), str("hello"));
        assert_ne!(str("hello"), str("world"));
        assert_eq!(int(42), int(42));
        assert_ne!(int(42), int(43));
        assert_eq!(bytes(b"abc"), bytes(b"abc"));
        assert_ne!(bytes(b"abc"), bytes(b"xyz"));
        assert_eq!(PrimitiveValue::Bool(true), PrimitiveValue::Bool(true));
        assert_ne!(PrimitiveValue::Bool(true), PrimitiveValue::Bool(false));
        assert_eq!(void(), void());
        // cross-variant inequality
        assert_ne!(str("1"), int(1));
    }

    #[test]
    fn primitive_value_hash_consistency() {
        use std::collections::HashMap;
        let mut map = HashMap::new();
        map.insert(str("key"), 1u32);
        map.insert(int(99), 2u32);
        assert_eq!(map[&str("key")], 1);
        assert_eq!(map[&int(99)], 2);
    }

    // derive_schema_from_records

    #[test]
    fn schema_primitive_bytes() {
        let records = vec![primitive(bytes(b""))];
        let schema = derive_schema_from_records("p1", &records).unwrap();
        assert_eq!(
            schema.field_with_name("collection").unwrap().data_type(),
            &DataType::Binary
        );
        assert_eq!(schema.metadata()["flare.record_type"], "primitive");
    }

    #[test]
    fn schema_primitive_string() {
        let records = vec![primitive(str("hello")), primitive(str("world"))];
        let schema = derive_schema_from_records("p1", &records).unwrap();
        assert_eq!(
            schema.field_with_name("collection").unwrap().data_type(),
            &DataType::Utf8
        );
    }

    #[test]
    fn schema_primitive_int64() {
        let records = vec![primitive(int(1)), primitive(int(2))];
        let schema = derive_schema_from_records("p1", &records).unwrap();
        assert_eq!(
            schema.field_with_name("collection").unwrap().data_type(),
            &DataType::Int64
        );
    }

    #[test]
    fn schema_primitive_void() {
        let records = vec![primitive(void())];
        let schema = derive_schema_from_records("p1", &records).unwrap();
        assert_eq!(
            schema.field_with_name("collection").unwrap().data_type(),
            &DataType::Null
        );
    }

    #[test]
    fn schema_primitive_mixed_types_errors() {
        let records = vec![primitive(str("a")), primitive(int(1))];
        let result = derive_schema_from_records("p1", &records);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("mixed primitive"));
    }

    #[test]
    fn schema_kv_string_void() {
        let records = vec![kv(str("a"), void()), kv(str("b"), void())];
        let schema = derive_schema_from_records("p1", &records).unwrap();
        assert_eq!(schema.metadata()["flare.record_type"], "kv");
        assert_eq!(
            schema.field_with_name("key").unwrap().data_type(),
            &DataType::Utf8
        );
        assert_eq!(
            schema.field_with_name("value").unwrap().data_type(),
            &DataType::Null
        );
    }

    #[test]
    fn schema_kv_mixed_key_types_errors() {
        let records = vec![kv(str("a"), void()), kv(int(1), void())];
        let result = derive_schema_from_records("p1", &records);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("mixed KV key"));
    }

    #[test]
    fn schema_gbk_string_void_iterable() {
        let records = vec![
            gbk(str("hello"), iterable(vec![void()])),
            gbk(str("world"), iterable(vec![void(), void()])),
        ];
        let schema = derive_schema_from_records("p1", &records).unwrap();
        assert_eq!(schema.metadata()["flare.record_type"], "gbk");
        assert_eq!(
            schema.field_with_name("key").unwrap().data_type(),
            &DataType::Utf8
        );
        // value should be List<Null> with nullable item field
        let value_field = schema.field_with_name("value").unwrap();
        match value_field.data_type() {
            DataType::List(item_field) => {
                assert_eq!(item_field.data_type(), &DataType::Null);
                assert!(item_field.is_nullable()); // nullable because Void
            }
            other => panic!("expected List, got {:?}", other),
        }
    }

    #[test]
    fn schema_iterable_string() {
        let records = vec![
            iter_record(iterable(vec![str("a"), str("b")])),
            iter_record(iterable(vec![str("c")])),
        ];
        let schema = derive_schema_from_records("p1", &records).unwrap();
        assert_eq!(schema.metadata()["flare.record_type"], "iterable");
        let col_field = schema.field_with_name("collection").unwrap();
        match col_field.data_type() {
            DataType::List(item_field) => {
                assert_eq!(item_field.data_type(), &DataType::Utf8);
                assert!(!item_field.is_nullable()); // non-void → not nullable
            }
            other => panic!("expected List, got {:?}", other),
        }
    }

    #[test]
    fn schema_empty_records_errors() {
        let result = derive_schema_from_records("p1", &[]);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("empty pcollection")
        );
    }

    //records_to_record_batch + record_batch_to_beamrecords round-trips

    fn round_trip(pcol_id: &str, records: Vec<BeamRecord>) -> Vec<BeamRecord> {
        let schema = derive_schema_from_records(pcol_id, &records).unwrap();
        let batch = beamrecords_to_record_batch(pcol_id, &records, schema.clone()).unwrap();
        record_batch_to_beamrecords(&batch, &schema).unwrap()
    }

    #[test]
    fn round_trip_primitive_bytes() {
        let records = vec![primitive(bytes(b"hello")), primitive(bytes(b"world"))];
        let result = round_trip("p1", records);
        assert_eq!(result.len(), 2);
        assert!(
            matches!(&result[0], BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(b)) if b == b"hello")
        );
        assert!(
            matches!(&result[1], BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(b)) if b == b"world")
        );
    }

    #[test]
    fn round_trip_primitive_string() {
        let records = vec![primitive(str("foo")), primitive(str("bar"))];
        let result = round_trip("p1", records);
        assert!(
            matches!(&result[0], BeamRecord::PRIMITIVE(PrimitiveValue::String(s)) if s == "foo")
        );
        assert!(
            matches!(&result[1], BeamRecord::PRIMITIVE(PrimitiveValue::String(s)) if s == "bar")
        );
    }

    #[test]
    fn round_trip_primitive_int64() {
        let records = vec![primitive(int(100)), primitive(int(-42))];
        let result = round_trip("p1", records);
        assert!(matches!(
            &result[0],
            BeamRecord::PRIMITIVE(PrimitiveValue::Int64(100))
        ));
        assert!(matches!(
            &result[1],
            BeamRecord::PRIMITIVE(PrimitiveValue::Int64(-42))
        ));
    }

    #[test]
    fn round_trip_primitive_void() {
        let records = vec![primitive(void()), primitive(void())];
        let result = round_trip("p1", records);
        assert!(matches!(
            &result[0],
            BeamRecord::PRIMITIVE(PrimitiveValue::Void(_))
        ));
        assert!(matches!(
            &result[1],
            BeamRecord::PRIMITIVE(PrimitiveValue::Void(_))
        ));
    }

    #[test]
    fn round_trip_kv_string_void() {
        let records = vec![kv(str("apple"), void()), kv(str("banana"), void())];
        let result = round_trip("p1", records);
        assert!(matches!(
            &result[0],
            BeamRecord::KV(BeamKV { key: PrimitiveValue::String(k), value: PrimitiveValue::Void(_) })
            if k == "apple"
        ));
        assert!(matches!(
            &result[1],
            BeamRecord::KV(BeamKV { key: PrimitiveValue::String(k), value: PrimitiveValue::Void(_) })
            if k == "banana"
        ));
    }

    #[test]
    fn round_trip_kv_string_int64() {
        let records = vec![kv(str("count"), int(5)), kv(str("total"), int(100))];
        let result = round_trip("p1", records);
        assert!(matches!(
            &result[0],
            BeamRecord::KV(BeamKV { key: PrimitiveValue::String(k), value: PrimitiveValue::Int64(5) })
            if k == "count"
        ));
    }

    #[test]
    fn round_trip_gbk_string_void_iterable() {
        let records = vec![
            gbk(str("word"), iterable(vec![void(), void(), void()])),
            gbk(str("other"), iterable(vec![void()])),
        ];
        let result = round_trip("p1", records);
        assert_eq!(result.len(), 2);

        let BeamRecord::GBK(gbk0) = &result[0] else {
            panic!("expected GBK")
        };
        assert!(matches!(&gbk0.key, PrimitiveValue::String(s) if s == "word"));
        assert_eq!(gbk0.value.list.values().len(), 3);

        let BeamRecord::GBK(gbk1) = &result[1] else {
            panic!("expected GBK")
        };
        assert!(matches!(&gbk1.key, PrimitiveValue::String(s) if s == "other"));
        assert_eq!(gbk1.value.list.values().len(), 1);
    }

    #[test]
    fn round_trip_gbk_empty_iterable() {
        // GBK with an empty value list — valid edge case
        let records = vec![gbk(str("key"), iterable(vec![]))];
        let schema = derive_schema_from_records("p1", &records).unwrap();
        let batch = beamrecords_to_record_batch("p1", &records, schema.clone()).unwrap();
        let result = record_batch_to_beamrecords(&batch, &schema).unwrap();
        let Some(BeamRecord::GBK(g)) = result.first() else {
            panic!("expected GBK")
        };
        assert_eq!(g.value.list.values().len(), 0);
    }

    #[test]
    fn round_trip_iterable_strings() {
        let records = vec![
            iter_record(iterable(vec![str("a"), str("b")])),
            iter_record(iterable(vec![str("c")])),
        ];
        let result = round_trip("p1", records);
        let BeamRecord::ITERABLE(iv0) = &result[0] else {
            panic!()
        };
        assert_eq!(iv0.list.values().len(), 2);
        let BeamRecord::ITERABLE(iv1) = &result[1] else {
            panic!()
        };
        assert_eq!(iv1.list.values().len(), 1);
    }

    #[test]
    fn round_trip_preserves_pcollection_count() {
        let records: Vec<BeamRecord> = (0..50).map(|i| primitive(int(i))).collect();
        let result = round_trip("large_pcol", records);
        assert_eq!(result.len(), 50);
    }

    #[test]
    fn records_to_batch_empty_errors() {
        let schema = derive_schema_from_records("p1", &[primitive(int(1))]).unwrap();
        let result = beamrecords_to_record_batch("p1", &[], schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("empty records"));
    }

    // nullable item field for Void iterable

    #[test]
    fn void_iterable_item_field_is_nullable() {
        // The Arrow ListArray item field must be nullable when item type is Null,
        // otherwise Arrow panics on construction.
        let records = vec![gbk(str("k"), iterable(vec![void()]))];
        // This must not panic:
        let schema = derive_schema_from_records("p1", &records).unwrap();
        let batch = beamrecords_to_record_batch("p1", &records, schema);
        assert!(batch.is_ok());
    }

    //  FlareElementStore write + scan

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_write_and_scan_primitive() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_base(dir.path().to_str().unwrap());

        let records = vec![primitive(bytes(b"hello")), primitive(bytes(b"world"))];
        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "pcol1".to_string(),
                elements: records.clone(),
            })
            .await
            .unwrap();

        let result = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pcol1".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        let mut values: Vec<Vec<u8>> = result
            .into_iter()
            .map(|r| match r {
                BeamRecord::PRIMITIVE(PrimitiveValue::Bytes(b)) => b,
                _ => panic!("unexpected record type"),
            })
            .collect();
        values.sort();
        assert_eq!(values, vec![b"hello".to_vec(), b"world".to_vec()]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_write_and_scan_kv() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_base(dir.path().to_str().unwrap());

        let records = vec![kv(str("apple"), void()), kv(str("banana"), void())];
        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "kv_pcol".to_string(),
                elements: records,
            })
            .await
            .unwrap();

        let result = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "kv_pcol".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        let mut keys: Vec<String> = result
            .into_iter()
            .map(|r| match r {
                BeamRecord::KV(BeamKV {
                    key: PrimitiveValue::String(k),
                    ..
                }) => k,
                _ => panic!("unexpected record type"),
            })
            .collect();
        keys.sort();
        assert_eq!(keys, vec!["apple", "banana"]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_write_and_scan_gbk() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_base(dir.path().to_str().unwrap());

        let records = vec![
            gbk(str("to"), iterable(vec![void(), void()])),
            gbk(str("be"), iterable(vec![void(), void(), void(), void()])),
        ];
        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "gbk_pcol".to_string(),
                elements: records,
            })
            .await
            .unwrap();

        let result = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "gbk_pcol".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        let mut entries: Vec<(String, usize)> = result
            .into_iter()
            .map(|r| match r {
                BeamRecord::GBK(BeamGbk {
                    key: PrimitiveValue::String(k),
                    value: v,
                }) => (k, v.list.values().len()),
                _ => panic!("unexpected record type"),
            })
            .collect();
        entries.sort_by_key(|(k, _)| k.clone());
        assert_eq!(entries, vec![("be".to_string(), 4), ("to".to_string(), 2)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_write_projected_batch_and_scan_gbk() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_base(dir.path().to_str().unwrap());

        let keys = vec![str("to"), str("be")];
        let values = vec![
            iterable(vec![void(), void()]),
            iterable(vec![void(), void(), void(), void()]),
        ];

        let batch_schema = create_schema_with_record_type(
            vec![
                Field::new(KEY_COLUMN, DataType::Utf8, false),
                Field::new(
                    VALUE_COLUMN,
                    DataType::List(Arc::new(Field::new("item", DataType::Null, true))),
                    true,
                ),
            ],
            "gbk",
            "gbk_projected_pcol",
        )
        .unwrap();

        let batch = RecordBatch::try_new(
            batch_schema.clone(),
            vec![
                primitive_values_to_array(&keys, &DataType::Utf8).unwrap(),
                iterable_values_to_array(&values, &DataType::Null).unwrap(),
            ],
        )
        .unwrap();

        store
            .write_record_batch("gbk_projected_pcol", batch, batch_schema)
            .await
            .unwrap();

        let result = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "gbk_projected_pcol".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result.len(), 2);
        let mut entries: Vec<(String, usize)> = result
            .into_iter()
            .map(|r| match r {
                BeamRecord::GBK(BeamGbk {
                    key: PrimitiveValue::String(k),
                    value: v,
                }) => (k, v.list.values().len()),
                _ => panic!("unexpected record type"),
            })
            .collect();
        entries.sort_by_key(|(k, _)| k.clone());
        assert_eq!(entries, vec![("be".to_string(), 4), ("to".to_string(), 2)]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_separate_pcollections_dont_interfere() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_base(dir.path().to_str().unwrap());

        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "pcol_a".to_string(),
                elements: vec![primitive(str("from_a"))],
            })
            .await
            .unwrap();

        store
            .write_beamrecord_batch(NewCollectionRequest {
                pcollection_id: "pcol_b".to_string(),
                elements: vec![primitive(str("from_b"))],
            })
            .await
            .unwrap();

        let result_a = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pcol_a".to_string(),
            })
            .await
            .unwrap();

        let result_b = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "pcol_b".to_string(),
            })
            .await
            .unwrap();

        assert_eq!(result_a.len(), 1);
        assert_eq!(result_b.len(), 1);
        assert!(
            matches!(&result_a[0], BeamRecord::PRIMITIVE(PrimitiveValue::String(s)) if s == "from_a")
        );
        assert!(
            matches!(&result_b[0], BeamRecord::PRIMITIVE(PrimitiveValue::String(s)) if s == "from_b")
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn store_scan_unknown_pcollection_errors() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_with_base(dir.path().to_str().unwrap());

        let result = store
            .scan_collection(ScanCollectionRequest {
                pcollection_id: "nonexistent".to_string(),
            })
            .await;

        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("schema not found"));
    }

    fn store_with_base(base: &str) -> FlareElementStore {
        FlareElementStore::with_base_path(FlareSchemaRegistry::new(), base.to_string())
    }
}
