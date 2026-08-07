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

use crate::ckan_reader::CkanReader;
use crate::ckan_resource::CkanResource;
use anyhow::{anyhow, Result};
use duckdb::arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use reqwest::blocking::Client;
use serde_json::Value;

const MAX_RECORDS_FETCH: usize = 100_000;

/// Mirrors Python's `DatastoreReader` exactly.
pub struct DatastoreReader {
    datastore_url: String,
}

impl DatastoreReader {
    pub fn new(datastore_url: String) -> Self {
        Self { datastore_url }
    }

    /// Fetch the total record count from CKAN Datastore API.
    /// Mirrors Python's `DatastoreReader.get_total()`.
    pub fn get_total(&self, resource_id: &str) -> Option<i64> {
        let url = format!(
            "{}/{}?format=json&offset=0&limit=0",
            self.datastore_url, resource_id
        );
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0")
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .ok()?;
        let resp = client.get(&url).send().ok()?;
        let json: Value = resp.json().ok()?;
        json.get("total")?.as_i64()
    }

    /// Fetch the number of fields (columns) from CKAN Datastore API.
    /// Mirrors Python's `DatastoreReader.get_field_count()`.
    pub fn get_field_count(&self, resource_id: &str) -> Option<usize> {
        let url = format!(
            "{}/{}?format=json&offset=0&limit=0",
            self.datastore_url, resource_id
        );
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0")
            .timeout(std::time::Duration::from_secs(10))
            .build()
            .ok()?;
        let resp = client.get(&url).send().ok()?;
        let json: Value = resp.json().ok()?;
        let fields = json.get("fields")?.as_array()?;
        if fields.is_empty() {
            None
        } else {
            Some(fields.len())
        }
    }

    /// Read datastore records and return Arrow batches.
    /// Returns empty vec for empty datastore (instead of Err).
    /// Mirrors Python's `DatastoreReader.read()`.
    pub fn read_batches(&self, resource: &CkanResource) -> Result<Vec<RecordBatch>> {
        let resource_id = &resource.id;
        let client = Client::builder()
            .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0")
            .build()?;
        let mut offset = 0;
        let mut batches = Vec::new();

        loop {
            let url = format!(
                "{}/{}?format=json&offset={}&limit={}",
                self.datastore_url, resource_id, offset, MAX_RECORDS_FETCH
            );
            let resp = client.get(&url).send()?;
            let json: Value = resp.json()?;

            let recs = json
                .get("records")
                .ok_or_else(|| anyhow!("records missing"))?;
            let fields = json
                .get("fields")
                .ok_or_else(|| anyhow!("fields missing"))?;
            let recs = recs
                .as_array()
                .ok_or_else(|| anyhow!("records not array"))?;

            if recs.is_empty() {
                break;
            }

            let column_names: Vec<String> = fields
                .as_array()
                .ok_or_else(|| anyhow!("fields not array"))?
                .iter()
                .filter_map(|f| f.get("id").and_then(|v| v.as_str()).map(|s| s.to_string()))
                .collect();

            let mut columns: Vec<Vec<String>> = vec![Vec::new(); column_names.len()];
            for rec in recs {
                let arr = rec.as_array().ok_or_else(|| anyhow!("record not array"))?;
                for (i, _name) in column_names.iter().enumerate() {
                    let val = arr
                        .get(i)
                        .map(|v| match v {
                            Value::String(s) => s.clone(),
                            Value::Null => String::new(),
                            _ => v.to_string(),
                        })
                        .unwrap_or_default();
                    columns[i].push(val);
                }
            }

            let arrays: Vec<ArrayRef> = columns
                .into_iter()
                .map(|c| Arc::new(StringArray::from(c)) as ArrayRef)
                .collect();

            let schema_fields: Vec<Field> = column_names
                .iter()
                .map(|n| Field::new(n, DataType::Utf8, true))
                .collect();

            let schema = Arc::new(Schema::new(schema_fields));
            let batch = RecordBatch::try_new(schema, arrays)?;
            batches.push(batch);
            offset += MAX_RECORDS_FETCH;
        }

        // Return empty vec for empty datastore (Python returns None)
        Ok(batches)
    }
}

impl CkanReader for DatastoreReader {
    fn supported_formats(&self) -> Vec<String> {
        vec![]
    }

    fn do_read(&self, resource: &CkanResource) -> Result<Vec<RecordBatch>> {
        self.read_batches(resource)
    }

    fn can_read(&self, resource: &CkanResource) -> bool {
        resource.datastore_active
    }
}
