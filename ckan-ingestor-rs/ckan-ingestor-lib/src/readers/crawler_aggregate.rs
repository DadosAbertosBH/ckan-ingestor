// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// ckan-ingestor-rs is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with ckan-ingestor-rs.  If not, see <https://www.gnu.org/licenses/>.

use std::{collections::BTreeMap, fs::File, sync::Arc};

use anyhow::{Context, Result};
use arrow::{
    array::{new_empty_array, new_null_array, ArrayRef, StringArray},
    compute::cast,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use parquet::{arrow::arrow_reader::ParquetRecordBatchReaderBuilder, file::metadata::KeyValue};

use crate::{parquet_output::ParquetOutput, readers::ckan_reader::SuccessResult};

pub(crate) struct SchemaAccumulator {
    fields: BTreeMap<String, Field>,
    saw_geo: bool,
    saw_plain: bool,
    geo_metadata: Option<KeyValue>,
    sources: usize,
}

impl SchemaAccumulator {
    pub fn new() -> Self {
        Self {
            fields: BTreeMap::new(),
            saw_geo: false,
            saw_plain: false,
            geo_metadata: None,
            sources: 0,
        }
    }

    pub fn add(&mut self, result: &SuccessResult) -> Result<()> {
        let metadata = result
            .parquet
            .metadata()
            .context("reader produced unfinished Parquet")?;
        let geo = metadata
            .file_metadata()
            .key_value_metadata()
            .and_then(|items| items.iter().find(|item| item.key == "geo"))
            .cloned()
            .map(normalize_geo_metadata)
            .transpose()?;
        if let Some(geo) = geo {
            self.saw_geo = true;
            if let Some(existing) = &self.geo_metadata {
                anyhow::ensure!(
                    existing.value == geo.value,
                    "cannot combine GeoJSON files with incompatible GeoParquet metadata"
                );
            } else {
                self.geo_metadata = Some(geo);
            }
        } else {
            self.saw_plain = true;
        }
        anyhow::ensure!(
            !(self.saw_geo && self.saw_plain),
            "cannot combine GeoJSON and plain JSON in one resource"
        );
        for field in result.parquet.schema.fields() {
            self.fields
                .entry(field.name().clone())
                .and_modify(|existing| {
                    if existing.data_type() != field.data_type() {
                        *existing = Field::new(field.name(), DataType::Utf8, true);
                    } else if field.is_nullable() {
                        *existing = existing.clone().with_nullable(true);
                    }
                })
                .or_insert_with(|| field.as_ref().clone().with_nullable(true));
        }
        self.sources += 1;
        Ok(())
    }

    pub fn build(self, tabular: bool) -> Result<AggregateWriter> {
        anyhow::ensure!(self.sources > 0, "no matching files found");
        let source_name = if tabular {
            let mut candidate = "__source_file".to_string();
            let mut index = 1;
            while self.fields.contains_key(&candidate) {
                candidate = format!("__source_file_{index}");
                index += 1;
            }
            Some(candidate)
        } else {
            None
        };
        let replace_name = !tabular && self.fields.contains_key("name");
        let mut fields: Vec<Field> = self.fields.into_values().collect();
        if let Some(name) = &source_name {
            fields.push(Field::new(name, DataType::Utf8, false));
        }
        let schema = Arc::new(Schema::new(fields));
        let empty = RecordBatch::try_new(
            schema.clone(),
            schema
                .fields()
                .iter()
                .map(|field| new_empty_array(field.data_type()))
                .collect(),
        )?;
        let mut output = ParquetOutput::try_new(&empty)?;
        if let Some(geo) = self.geo_metadata {
            output.append_key_value_metadata(geo);
        }
        Ok(AggregateWriter {
            output,
            schema,
            source_name,
            replace_name,
        })
    }
}

fn normalize_geo_metadata(mut metadata: KeyValue) -> Result<KeyValue> {
    if let Some(value) = metadata.value.as_deref() {
        let mut document: serde_json::Value = serde_json::from_str(value)?;
        if let Some(columns) = document
            .get_mut("columns")
            .and_then(serde_json::Value::as_object_mut)
        {
            for column in columns.values_mut() {
                if let Some(column) = column.as_object_mut() {
                    column.remove("bbox");
                }
            }
        }
        metadata.value = Some(serde_json::to_string(&document)?);
    }
    Ok(metadata)
}

pub(crate) struct AggregateWriter {
    output: ParquetOutput,
    schema: Arc<Schema>,
    source_name: Option<String>,
    replace_name: bool,
}

impl AggregateWriter {
    pub fn append(&mut self, result: &SuccessResult, source: &str) -> Result<()> {
        let file = File::open(result.parquet.path())?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(8192)
            .build()?;
        for batch in reader {
            let batch = batch?;
            let columns =
                self.schema
                    .fields()
                    .iter()
                    .map(|field| {
                        if self.source_name.as_deref() == Some(field.name()) {
                            return Ok(Arc::new(StringArray::from(vec![source; batch.num_rows()]))
                                as ArrayRef);
                        }
                        if self.replace_name && field.name() == "name" {
                            return Ok(Arc::new(StringArray::from(vec![
                                source_file_name(source);
                                batch.num_rows()
                            ])) as ArrayRef);
                        }
                        match batch.schema().index_of(field.name()) {
                            Ok(index) => convert(batch.column(index), field.data_type()),
                            Err(_) => Ok(new_null_array(field.data_type(), batch.num_rows())),
                        }
                    })
                    .collect::<Result<Vec<_>>>()?;
            self.output
                .write(&RecordBatch::try_new(self.schema.clone(), columns)?)?;
        }
        Ok(())
    }

    pub fn finish(mut self) -> Result<ParquetOutput> {
        anyhow::ensure!(self.output.rows > 0, "no rows produced by matching files");
        self.output.finish()?;
        Ok(self.output)
    }
}

fn source_file_name(source: &str) -> &str {
    source
        .rsplit("!/")
        .next()
        .unwrap_or(source)
        .split('?')
        .next()
        .unwrap_or(source)
        .rsplit('/')
        .next()
        .filter(|name| !name.is_empty())
        .unwrap_or(source)
}

fn convert(array: &ArrayRef, target: &DataType) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(array.clone());
    }
    if let Ok(converted) = cast(array, target) {
        return Ok(converted);
    }
    anyhow::ensure!(
        target == &DataType::Utf8,
        "cannot convert {:?} to {:?}",
        array.data_type(),
        target
    );
    let values = (0..array.len())
        .map(|index| {
            if array.is_null(index) {
                Ok(None)
            } else {
                Ok(Some(arrow::util::display::array_value_to_string(
                    array, index,
                )?))
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(StringArray::from(values)))
}
