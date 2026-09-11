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
use std::sync::Arc;

use crate::{
    ckan_resource::CkanResource,
    parquet_output::ParquetOutput,
    readers::ckan_reader::{format_contains, CkanReader, FailedResult, ReadResult, SuccessResult},
};
use anyhow::{Context, Result};
use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use reqwest::blocking::Client;
use serde_json::Value;

const MAX_RECORDS_FETCH: usize = 8_192;
const MAX_ERROR_BODY_LENGTH: usize = 512;

fn decode_json_body(body: String, url: &str) -> Result<Value> {
    let body_preview: String = body.chars().take(MAX_ERROR_BODY_LENGTH).collect();
    serde_json::from_str(&body).with_context(|| {
        format!(
            "error decoding response body from {url}: {body_preview}{}",
            if body.chars().count() > MAX_ERROR_BODY_LENGTH {
                "..."
            } else {
                ""
            }
        )
    })
}

/// Mirrors Python's `DatastoreReader` exactly.
pub struct DatastoreReader {
    datastore_url: String,
    datastore_metadata_url: String,
    client: Client,
    supported_formats: Vec<String>,
}

impl DatastoreReader {
    pub fn new(ckan_base_url: String, client: Client) -> Self {
        let ckan_base_url = ckan_base_url.trim_end_matches('/');
        Self {
            datastore_url: format!("{ckan_base_url}/datastore/dump"),
            datastore_metadata_url: format!("{ckan_base_url}/api/3/action/datastore_search"),
            client,
            supported_formats: vec!["CSV".to_string(), "JSON".to_string()],
        }
    }

    pub fn get_row_and_column_count(&self, resource_id: &str) -> Result<(usize, usize)> {
        let url = format!(
            "{}?resource_id={}&limit=0",
            self.datastore_metadata_url, resource_id
        );
        let json = self.fetch_json(&url)?;
        let json = json.get("result").unwrap_or(&json);

        let total = json
            .get("total")
            .unwrap_or_default()
            .as_u64()
            .unwrap_or_default();
        let rows = usize::try_from(total)?;

        let fields = json
            .get("fields")
            .unwrap_or_default()
            .as_array()
            .map(|v| v.as_slice())
            .unwrap_or(&[]);
        let columns = fields.len();
        Ok((rows, columns))
    }

    fn fetch_json(&self, url: &str) -> Result<Value> {
        let response = self.client.get(url).send()?;
        let status = response.status();
        if !status.is_success() {
            anyhow::bail!("datastore request failed with HTTP status {status}: {url}");
        }
        let body = response.text()?;
        decode_json_body(body, url)
    }

    /// Read datastore records and return Arrow batches.
    /// Returns empty vec for empty datastore (instead of Err).
    /// Mirrors Python's `DatastoreReader.read()`.
    pub fn read_batches(&self, resource: &CkanResource) -> ReadResult {
        let resource_id = &resource.id;
        let mut offset = 0;
        let mut parquet = None;
        let (rows, columns) = self.get_row_and_column_count(resource_id)?;
        loop {
            let url = format!(
                "{}/{}?format=json&offset={}&limit={}",
                self.datastore_url, resource_id, offset, MAX_RECORDS_FETCH
            );
            log::debug!(
                "Fetching datastore page for resource {}: offset={}, limit={}, url={}",
                resource_id,
                offset,
                MAX_RECORDS_FETCH,
                url,
            );
            let json = self.fetch_json(&url)?;
            let json = json.get("result").unwrap_or(&json);
            let records = json.get("records").ok_or_else(|| {
                FailedResult::from_string("records missing", self.reader_name().to_string())
            })?;
            let fields = json.get("fields").ok_or_else(|| {
                FailedResult::from_string("fields missing", self.reader_name().to_string())
            })?;
            let records = records.as_array().ok_or_else(|| {
                FailedResult::from_string("records not array", self.reader_name().to_string())
            })?;
            let column_names: Vec<String> = fields
                .as_array()
                .ok_or_else(|| {
                    FailedResult::from_string("fields not array", self.reader_name().to_string())
                })?
                .iter()
                .filter_map(|field| field.get("id").and_then(Value::as_str).map(str::to_string))
                .collect();

            if parquet.is_none() {
                let schema = Arc::new(Schema::new(
                    column_names
                        .iter()
                        .map(|name| Field::new(name, DataType::Utf8, true))
                        .collect::<Vec<_>>(),
                ));
                let empty_arrays = column_names
                    .iter()
                    .map(|_| Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef)
                    .collect();
                let empty_batch = RecordBatch::try_new(schema, empty_arrays)?;
                parquet = Some(ParquetOutput::try_new(&empty_batch)?);
            }

            if records.is_empty() {
                break;
            }

            let mut batch_columns = vec![Vec::with_capacity(records.len()); column_names.len()];
            for record in records {
                let values = record.as_array().ok_or_else(|| {
                    FailedResult::from_string("record not array", self.reader_name().to_string())
                })?;
                for (index, column) in batch_columns.iter_mut().enumerate() {
                    column.push(match values.get(index) {
                        Some(Value::String(value)) => value.clone(),
                        Some(Value::Null) | None => String::new(),
                        Some(value) => value.to_string(),
                    });
                }
            }
            let arrays = batch_columns
                .into_iter()
                .map(|column| Arc::new(StringArray::from(column)) as ArrayRef)
                .collect();
            let schema = parquet
                .as_ref()
                .expect("Parquet output initialized")
                .schema
                .clone();
            let batch = RecordBatch::try_new(schema, arrays)?;
            parquet
                .as_mut()
                .expect("Parquet output initialized")
                .write(&batch)?;
            offset += MAX_RECORDS_FETCH;
        }

        let mut parquet = parquet.ok_or_else(|| anyhow::anyhow!("No data"))?;
        parquet.finish()?;
        Ok(SuccessResult::from_datastore(
            parquet,
            rows,
            columns,
            self.reader_name().to_string(),
        ))
    }
}

impl CkanReader for DatastoreReader {
    fn supported_formats(&self) -> &[String] {
        &self.supported_formats
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        self.read_batches(resource)
    }

    fn can_read(&self, resource: &CkanResource) -> bool {
        !resource.url.ends_with(".gz")
            && self
                .supported_formats()
                .iter()
                .any(|format| format_contains(&resource.format, format))
            && resource.datastore_active
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn resource(url: &str, datastore_active: bool) -> CkanResource {
        CkanResource {
            id: "resource-id".to_string(),
            package_id: String::new(),
            url: url.to_string(),
            format: "CSV".to_string(),
            datastore_active,
            last_modified: String::new(),
        }
    }

    #[test]
    fn cannot_read_gzip_resources_even_when_datastore_is_active() {
        let reader =
            DatastoreReader::new("https://ckan.example.test/api".to_string(), Client::new());

        assert!(!reader.can_read(&resource("https://example.test/data.csv.gz", true)));
    }

    #[test]
    fn can_read_non_gzip_csv_resources_when_datastore_is_active() {
        let reader =
            DatastoreReader::new("https://ckan.example.test/api".to_string(), Client::new());

        assert!(reader.can_read(&resource("https://example.test/data.csv", true)));
    }

    #[test]
    fn can_read_lowercase_csv_formats_when_datastore_is_active() {
        let reader =
            DatastoreReader::new("https://ckan.example.test/api".to_string(), Client::new());
        let mut resource = resource("https://example.test/data.csv", true);
        resource.format = "csv".to_string();

        assert!(reader.can_read(&resource));
    }
}
