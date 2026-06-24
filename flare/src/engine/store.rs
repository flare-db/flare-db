use crate::utils::errors::ElementStoreError;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, ListArray, NullArray, RecordBatch,
    StringArray, StructArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, Schema};
use fusio::disk::TokioFs;
use fusio::executor::tokio::TokioExecutor;
use std::sync::Arc;
use tonbo::prelude::*;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum BeamValue {
    String(String),
    Bytes(Vec<u8>),
    Int64(i64),
    Bool(bool),
    Kv(Box<BeamValue>, Box<BeamValue>),
    Iterable(Vec<BeamValue>),
    Gbk(Box<BeamValue>, Vec<BeamValue>),
    Void,
}

impl BeamValue {
    fn data_type(&self) -> DataType {
        match self {
            BeamValue::String(_) => DataType::Utf8,
            BeamValue::Bytes(_) => DataType::Binary,
            BeamValue::Int64(_) => DataType::Int64,
            BeamValue::Bool(_) => DataType::Boolean,
            BeamValue::Kv(key, value) => DataType::Struct(
                vec![
                    Field::new("key", key.data_type(), key.is_nullable()),
                    Field::new("value", value.data_type(), value.is_nullable()),
                ]
                .into(),
            ),
            BeamValue::Iterable(values) => {
                let item_type = values
                    .first()
                    .map(|v| v.data_type())
                    .unwrap_or(DataType::Null);

                DataType::List(Arc::new(Field::new("item", item_type, true)))
            }
            BeamValue::Gbk(key, values) => {
                let item_type = values
                    .first()
                    .map(|v| v.data_type())
                    .unwrap_or(DataType::Null);

                DataType::Struct(
                    vec![
                        Field::new("key", key.data_type(), key.is_nullable()),
                        Field::new(
                            "values",
                            DataType::List(Arc::new(Field::new("item", item_type, true))),
                            true,
                        ),
                    ]
                    .into(),
                )
            }
            BeamValue::Void => DataType::Null,
        }
    }

    fn is_nullable(&self) -> bool {
        matches!(self, BeamValue::Void)
    }

    fn fields(&self, prefix: &str) -> Vec<Field> {
        vec![Field::new(prefix, self.data_type(), self.is_nullable())]
    }

    pub fn schema(&self) -> Schema {
        let mut fields = Vec::<Field>::new();

        fields.push(Field::new("element_id", DataType::Utf8, false));
        fields.push(Field::new("pcollection_id", DataType::Utf8, false));
        fields.extend(self.fields("Elements"));

        Schema::new(fields)
    }

    fn from_record_batch(batch: &RecordBatch, row: usize) -> Result<Self, ElementStoreError> {
        let column = batch
            .column_by_name("Elements")
            .ok_or_else(|| ElementStoreError::MissingField("Elements".to_string()))?;

        Self::from_array_row(column.as_ref(), column.data_type(), row)
    }

    fn from_array_row(
        array: &dyn Array,
        data_type: &DataType,
        row: usize,
    ) -> Result<Self, ElementStoreError> {
        match data_type {
            DataType::Utf8 => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .ok_or_else(|| {
                        ElementStoreError::InvalidData("Elements expected StringArray".to_string())
                    })?;

                Ok(BeamValue::String(arr.value(row).to_string()))
            }
            DataType::Binary => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .ok_or_else(|| {
                        ElementStoreError::InvalidData("Elements expected BinaryArray".to_string())
                    })?;

                Ok(BeamValue::Bytes(arr.value(row).to_vec()))
            }
            DataType::Int64 => {
                let arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                    ElementStoreError::InvalidData("Elements expected Int64Array".to_string())
                })?;

                Ok(BeamValue::Int64(arr.value(row)))
            }
            DataType::Boolean => {
                let arr = array
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        ElementStoreError::InvalidData("Elements expected BooleanArray".to_string())
                    })?;

                Ok(BeamValue::Bool(arr.value(row)))
            }
            DataType::Null => Ok(BeamValue::Void),
            DataType::List(item_field) => {
                let arr = array.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                    ElementStoreError::InvalidData("Elements expected ListArray".to_string())
                })?;

                let values = Self::decode_list_row(arr, row, item_field.data_type())?;
                Ok(BeamValue::Iterable(values))
            }
            DataType::Struct(fields) => {
                let arr = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        ElementStoreError::InvalidData("Elements expected StructArray".to_string())
                    })?;

                if fields.len() != 2 {
                    return Err(ElementStoreError::UnsupportedType(format!(
                        "unsupported struct field count: {}",
                        fields.len()
                    )));
                }

                let first_name = fields[0].name();
                let second_name = fields[1].name();

                match (first_name.as_str(), second_name.as_str()) {
                    ("key", "value") => {
                        let key = Self::from_array_row(
                            arr.column(0).as_ref(),
                            fields[0].data_type(),
                            row,
                        )?;
                        let value = Self::from_array_row(
                            arr.column(1).as_ref(),
                            fields[1].data_type(),
                            row,
                        )?;
                        Ok(BeamValue::Kv(Box::new(key), Box::new(value)))
                    }
                    ("key", "values") => {
                        let key = Self::from_array_row(
                            arr.column(0).as_ref(),
                            fields[0].data_type(),
                            row,
                        )?;
                        let list = arr
                            .column(1)
                            .as_any()
                            .downcast_ref::<ListArray>()
                            .ok_or_else(|| {
                                ElementStoreError::InvalidData(
                                    "Gbk values expected ListArray".to_string(),
                                )
                            })?;

                        let value_item_type = match fields[1].data_type() {
                            DataType::List(item_field) => item_field.data_type(),
                            other => {
                                return Err(ElementStoreError::UnsupportedType(format!(
                                    "Gbk values expected List type, found {:?}",
                                    other
                                )));
                            }
                        };

                        let values = Self::decode_list_row(list, row, value_item_type)?;
                        Ok(BeamValue::Gbk(Box::new(key), values))
                    }
                    _ => {
                        return Err(ElementStoreError::UnsupportedType(format!(
                            "unsupported struct shape for BeamValue: ({}, {})",
                            first_name, second_name
                        )));
                    }
                }
            }
            other => Err(ElementStoreError::UnsupportedType(format!(
                "unsupported Elements data type: {:?}",
                other
            ))),
        }
    }

    fn decode_list_row(
        list: &ListArray,
        row: usize,
        item_type: &DataType,
    ) -> Result<Vec<BeamValue>, ElementStoreError> {
        let offsets = list.value_offsets();
        let start = usize::try_from(offsets[row])
            .map_err(|_| ElementStoreError::OffsetOverflow("negative list offset".to_string()))?;
        let end = usize::try_from(offsets[row + 1])
            .map_err(|_| ElementStoreError::OffsetOverflow("negative list offset".to_string()))?;

        let values_array = list.values();

        let mut out = Vec::with_capacity(end.saturating_sub(start));
        for i in start..end {
            out.push(Self::from_array_row(values_array.as_ref(), item_type, i)?);
        }

        Ok(out)
    }
}

fn build_offsets(lengths: &[usize]) -> Result<OffsetBuffer<i32>, ElementStoreError> {
    let mut offsets = Vec::with_capacity(lengths.len() + 1);
    offsets.push(0_i32);

    let mut running = 0_i32;
    for len in lengths {
        let len_i32 = i32::try_from(*len).map_err(|_| {
            ElementStoreError::OffsetOverflow("list length exceeds i32::MAX".to_string())
        })?;
        running = running.checked_add(len_i32).ok_or_else(|| {
            ElementStoreError::OffsetOverflow("list offsets exceed i32::MAX".to_string())
        })?;
        offsets.push(running);
    }

    Ok(OffsetBuffer::new(ScalarBuffer::from(offsets)))
}

fn values_to_array(
    values: &[BeamValue],
    data_type: &DataType,
) -> Result<ArrayRef, ElementStoreError> {
    match data_type {
        DataType::Utf8 => {
            let strings = values
                .iter()
                .map(|v| match v {
                    BeamValue::String(s) => Ok(s.clone()),
                    _ => Err(ElementStoreError::InvalidData(
                        "mixed BeamValue variants in batch: expected String".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>, ElementStoreError>>()?;
            Ok(Arc::new(StringArray::from(strings)))
        }
        DataType::Binary => {
            let bytes = values
                .iter()
                .map(|v| match v {
                    BeamValue::Bytes(b) => Ok(b.as_slice()),
                    _ => Err(ElementStoreError::InvalidData(
                        "mixed BeamValue variants in batch: expected Bytes".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>, ElementStoreError>>()?;
            Ok(Arc::new(BinaryArray::from(bytes)))
        }
        DataType::Int64 => {
            let ints = values
                .iter()
                .map(|v| match v {
                    BeamValue::Int64(i) => Ok(*i),
                    _ => Err(ElementStoreError::InvalidData(
                        "mixed BeamValue variants in batch: expected Int64".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>, ElementStoreError>>()?;
            Ok(Arc::new(Int64Array::from(ints)))
        }
        DataType::Boolean => {
            let bools = values
                .iter()
                .map(|v| match v {
                    BeamValue::Bool(b) => Ok(*b),
                    _ => Err(ElementStoreError::InvalidData(
                        "mixed BeamValue variants in batch: expected Bool".to_string(),
                    )),
                })
                .collect::<Result<Vec<_>, ElementStoreError>>()?;
            Ok(Arc::new(BooleanArray::from(bools)))
        }
        DataType::Null => {
            if values.iter().all(|v| matches!(v, BeamValue::Void)) {
                Ok(Arc::new(NullArray::new(values.len())))
            } else {
                Err(ElementStoreError::InvalidData(
                    "mixed BeamValue variants in batch: expected Void".to_string(),
                ))
            }
        }
        DataType::List(item_field) => {
            let mut lengths = Vec::with_capacity(values.len());
            let mut flat = Vec::new();

            for v in values {
                match v {
                    BeamValue::Iterable(items) => {
                        lengths.push(items.len());
                        flat.extend(items.iter().cloned());
                    }
                    _ => {
                        return Err(ElementStoreError::InvalidData(
                            "mixed BeamValue variants in batch: expected Iterable".to_string(),
                        ));
                    }
                }
            }

            let offsets = build_offsets(&lengths)?;
            let child = values_to_array(&flat, item_field.data_type())?;
            Ok(Arc::new(ListArray::new(
                item_field.clone(),
                offsets,
                child,
                None,
            )))
        }
        DataType::Struct(fields) => {
            if fields.len() != 2 {
                return Err(ElementStoreError::UnsupportedType(format!(
                    "unsupported struct field count: {}",
                    fields.len()
                )));
            }

            let first_name = fields[0].name();
            let second_name = fields[1].name();

            match (first_name.as_str(), second_name.as_str()) {
                ("key", "value") => {
                    let mut keys = Vec::with_capacity(values.len());
                    let mut vals = Vec::with_capacity(values.len());

                    for v in values {
                        match v {
                            BeamValue::Kv(key, value) => {
                                keys.push((**key).clone());
                                vals.push((**value).clone());
                            }
                            _ => {
                                return Err(ElementStoreError::InvalidData(
                                    "mixed BeamValue variants in batch: expected Kv".to_string(),
                                ));
                            }
                        }
                    }

                    let key_array = values_to_array(&keys, fields[0].data_type())?;
                    let value_array = values_to_array(&vals, fields[1].data_type())?;

                    Ok(Arc::new(StructArray::new(
                        fields.clone(),
                        vec![key_array, value_array],
                        None,
                    )))
                }
                ("key", "values") => {
                    let mut keys = Vec::with_capacity(values.len());
                    let mut lengths = Vec::with_capacity(values.len());
                    let mut flat_values = Vec::new();

                    for v in values {
                        match v {
                            BeamValue::Gbk(key, group_values) => {
                                keys.push((**key).clone());
                                lengths.push(group_values.len());
                                flat_values.extend(group_values.iter().cloned());
                            }
                            _ => {
                                return Err(ElementStoreError::InvalidData(
                                    "mixed BeamValue variants in batch: expected Gbk".to_string(),
                                ));
                            }
                        }
                    }

                    let key_array = values_to_array(&keys, fields[0].data_type())?;
                    let values_item_field = match fields[1].data_type() {
                        DataType::List(item_field) => item_field.clone(),
                        other => {
                            return Err(ElementStoreError::UnsupportedType(format!(
                                "Gbk values field expected List, found {:?}",
                                other
                            )));
                        }
                    };

                    let offsets = build_offsets(&lengths)?;
                    let child_values =
                        values_to_array(&flat_values, values_item_field.data_type())?;
                    let grouped_values = Arc::new(ListArray::new(
                        values_item_field,
                        offsets,
                        child_values,
                        None,
                    ));

                    Ok(Arc::new(StructArray::new(
                        fields.clone(),
                        vec![key_array, grouped_values],
                        None,
                    )))
                }
                _ => {
                    return Err(ElementStoreError::UnsupportedType(format!(
                        "unsupported struct shape for BeamValue: ({}, {})",
                        first_name, second_name
                    )));
                }
            }
        }
        other => Err(ElementStoreError::UnsupportedType(format!(
            "unsupported Elements data type: {:?}",
            other
        ))),
    }
}

pub fn from_record_batches(batches: &[RecordBatch]) -> Result<Vec<BeamValue>, ElementStoreError> {
    let mut values = Vec::new();

    for batch in batches {
        for row in 0..batch.num_rows() {
            values.push(BeamValue::from_record_batch(batch, row)?);
        }
    }

    Ok(values)
}

pub fn to_record_batch(
    values: Vec<BeamValue>,
    pcol_id: String,
) -> Result<RecordBatch, ElementStoreError> {
    let first = values
        .first()
        .ok_or_else(|| ElementStoreError::InvalidData("empty values".to_string()))?;

    let schema = Arc::new(first.schema());

    let mut columns: Vec<ArrayRef> = Vec::new();

    for field in schema.fields() {
        match field.name().as_str() {
            "element_id" => {
                let ids: Vec<String> = (0..values.len())
                    .map(|_| Uuid::new_v4().to_string())
                    .collect();

                columns.push(Arc::new(StringArray::from(ids)));
            }
            "pcollection_id" => {
                columns.push(Arc::new(StringArray::from(vec![
                    pcol_id.clone();
                    values.len()
                ])));
            }
            "Elements" => {
                columns.push(values_to_array(&values, field.data_type())?);
            }
            _ => return Err(ElementStoreError::UnknownField(field.name().to_string())),
        }
    }

    RecordBatch::try_new(schema, columns).map_err(|e| ElementStoreError::Schema(e.to_string()))
}

pub struct FlareElementStore {
    db: DB<TokioFs, TokioExecutor>,
}

impl FlareElementStore {
    pub async fn open(schema: Arc<Schema>) -> Result<Self, ElementStoreError> {
        let db = DbBuilder::from_schema_key_name(schema, "element_id")
            .map_err(|e| ElementStoreError::Schema(e.to_string()))?
            .on_disk("/home/ganesh/flare-db/tonbo/data4")
            .map_err(|e| ElementStoreError::Open(e.to_string()))?
            .open()
            .await
            .map_err(|e| ElementStoreError::Open(e.to_string()))?;

        Ok(Self { db })
    }

    async fn write_collection(&self, req: NewCollectionRequest) -> Result<(), ElementStoreError> {
        let record_batch = to_record_batch(req.elements, req.pcollection_id)
            .map_err(|e| ElementStoreError::Schema(e.to_string()))?;

        self.db
            .ingest(record_batch)
            .await
            .map_err(|e| ElementStoreError::Write(e.to_string()))?;

        Ok(())
    }

    pub async fn scan_collection(
        &self,
        req: GetCollectionRequest,
    ) -> Result<Vec<BeamValue>, ElementStoreError> {
        let filter = Expr::eq("pcollection_id", ScalarValue::from(req.pcollection_id));

        let batches = self
            .db
            .scan()
            .filter(filter)
            .collect()
            .await
            .map_err(|e| ElementStoreError::Read(e.to_string()))?;

        Ok(from_record_batches(&batches).map_err(|e| ElementStoreError::Read(e.to_string()))?)
    }

    async fn upsert_element() {}
}

#[derive(Debug)]
pub struct NewCollectionRequest {
    // consumed pcollection id
    pub(crate) pcollection_id: String,
    // collection
    pub(crate) elements: Vec<BeamValue>,
}

#[derive(Debug)]
pub struct GetCollectionRequest {
    pub(crate) pcollection_id: String,
}

pub struct UpdateCollectionRequest {
    pub(crate) pcollection_id: String,
    pub(crate) key: BeamValue,
    pub(crate) value: BeamValue,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_roundtrip(values: Vec<BeamValue>) {
        let expected = values.clone();
        let batch = to_record_batch(values, "pcol-1".to_string()).expect("encode record batch");
        let decoded = from_record_batches(&[batch]).expect("decode record batches");

        assert_eq!(decoded, expected);
    }

    #[test]
    fn roundtrip_string() {
        assert_roundtrip(vec![
            BeamValue::String("a".to_string()),
            BeamValue::String("b".to_string()),
        ]);
    }

    #[test]
    fn roundtrip_bytes() {
        assert_roundtrip(vec![
            BeamValue::Bytes(vec![0, 1, 2]),
            BeamValue::Bytes(vec![255, 10, 0]),
        ]);
    }

    #[test]
    fn roundtrip_int64() {
        assert_roundtrip(vec![BeamValue::Int64(-7), BeamValue::Int64(42)]);
    }

    #[test]
    fn roundtrip_bool() {
        assert_roundtrip(vec![BeamValue::Bool(true), BeamValue::Bool(false)]);
    }

    #[test]
    fn roundtrip_void() {
        assert_roundtrip(vec![BeamValue::Void, BeamValue::Void, BeamValue::Void]);
    }

    #[test]
    fn roundtrip_iterable() {
        assert_roundtrip(vec![
            BeamValue::Iterable(vec![BeamValue::Int64(1), BeamValue::Int64(2)]),
            BeamValue::Iterable(vec![]),
            BeamValue::Iterable(vec![BeamValue::Int64(9)]),
        ]);
    }

    #[test]
    fn roundtrip_kv() {
        assert_roundtrip(vec![
            BeamValue::Kv(
                Box::new(BeamValue::String("k1".to_string())),
                Box::new(BeamValue::Int64(100)),
            ),
            BeamValue::Kv(
                Box::new(BeamValue::String("k2".to_string())),
                Box::new(BeamValue::Int64(200)),
            ),
        ]);
    }

    #[test]
    fn roundtrip_gbk() {
        assert_roundtrip(vec![
            BeamValue::Gbk(
                Box::new(BeamValue::String("k1".to_string())),
                vec![BeamValue::Int64(1), BeamValue::Int64(2)],
            ),
            BeamValue::Gbk(Box::new(BeamValue::String("k2".to_string())), vec![]),
        ]);
    }

    #[test]
    fn roundtrip_nested_kv_iterable() {
        assert_roundtrip(vec![
            BeamValue::Kv(
                Box::new(BeamValue::String("left".to_string())),
                Box::new(BeamValue::Iterable(vec![
                    BeamValue::Bool(true),
                    BeamValue::Bool(false),
                ])),
            ),
            BeamValue::Kv(
                Box::new(BeamValue::String("right".to_string())),
                Box::new(BeamValue::Iterable(vec![BeamValue::Bool(true)])),
            ),
        ]);
    }
}
