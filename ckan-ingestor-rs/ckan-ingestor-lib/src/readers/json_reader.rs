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

use anyhow::Result;
use arrow::datatypes::{DataType, Field, Fields, Schema};
use arrow_json::reader::{infer_json_schema_from_iterator, ReaderBuilder};
use serde_json::Value;
use std::fs::File;
use std::sync::Arc;

use crate::{
    ckan_resource::CkanResource,
    parquet_output::ParquetOutput,
    readers::{
        ckan_reader::{download_to_temp, CkanReader, FailedResult, ReadResult, SuccessResult},
        geojson::{geojson_to_record_batch, write_geoparquet},
        temp_file_cleanup::TempFileCleanup,
    },
};
use reqwest::blocking::Client;

pub struct JsonReader {
    client: Client,
    supported_formats: Vec<String>,
}

impl JsonReader {
    pub fn new() -> Self {
        Self::with_client(Client::new())
    }

    pub fn with_client(client: Client) -> Self {
        Self {
            client,
            supported_formats: vec!["JSON".to_string()],
        }
    }

    fn read_batches(&self, resource: &CkanResource) -> ReadResult {
        let is_remote = resource.url.starts_with("http://") || resource.url.starts_with("https://");
        let json_path = if is_remote {
            download_to_temp(&self.client, &resource.url, ".json")?
        } else {
            resource.url.clone()
        };
        let mut cleanup = TempFileCleanup::from_path(json_path.clone().into());
        if !is_remote {
            cleanup.commit();
        }
        let output = self.try_read_json(&json_path)?;
        if output.rows == 0 {
            return Err(FailedResult::from_string(
                "No data",
                self.reader_name().to_string(),
            ));
        }
        Ok(SuccessResult::new(output, self.reader_name().to_string()))
    }

    fn try_read_json(&self, path: &str) -> Result<ParquetOutput> {
        let document = serde_json::from_reader(File::open(path)?)?;
        if is_geojson(&document) {
            write_geoparquet(geojson_to_record_batch(serde_json::from_value(document)?)?)
        } else {
            self.read_json_document(document)
        }
    }

    fn read_json_document(&self, document: serde_json::Value) -> Result<ParquetOutput> {
        let records = match document {
            serde_json::Value::Array(records) => records,
            record => vec![record],
        };
        let schema =
            infer_json_schema_from_iterator(records.iter().map(Ok::<_, arrow::error::ArrowError>))?;
        let mut decoder = ReaderBuilder::new(Arc::new(normalize_null_fields(schema)))
            .with_batch_size(8192)
            .build_decoder()?;
        let mut output = None;
        for records in records.chunks(8192) {
            decoder.serialize(records)?;
            if let Some(batch) = decoder.flush()? {
                if output.is_none() {
                    output = Some(ParquetOutput::try_new(&batch)?);
                }
                output
                    .as_mut()
                    .expect("Parquet output initialized")
                    .write(&batch)?;
            }
        }
        let mut output = output.ok_or_else(|| anyhow::anyhow!("No data"))?;
        output.finish()?;
        Ok(output)
    }
}

fn is_geojson(document: &serde_json::Value) -> bool {
    document.get("type").and_then(Value::as_str) == Some("FeatureCollection")
}

fn normalize_null_fields(schema: Schema) -> Schema {
    let metadata = schema.metadata;
    let fields = schema
        .fields
        .into_iter()
        .map(|field| normalize_null_field(field.as_ref()))
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, metadata)
}

fn normalize_null_field(field: &Field) -> Field {
    Field::new(
        field.name(),
        normalize_null_data_type(field.data_type()),
        field.is_nullable(),
    )
}

fn normalize_null_data_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Null => DataType::Utf8,
        DataType::Struct(fields) => DataType::Struct(Fields::from(
            fields
                .iter()
                .map(|field| normalize_null_field(field))
                .collect::<Vec<_>>(),
        )),
        DataType::List(field) => DataType::List(Arc::new(normalize_null_field(field))),
        data_type => data_type.clone(),
    }
}

impl Default for JsonReader {
    fn default() -> Self {
        Self::new()
    }
}

impl CkanReader for JsonReader {
    fn supported_formats(&self) -> &[String] {
        &self.supported_formats
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        self.read_batches(resource)
    }
}
