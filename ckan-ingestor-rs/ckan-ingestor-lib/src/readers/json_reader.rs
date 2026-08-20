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
use duckdb::{arrow::array::RecordBatch, Connection};

use crate::{
    ckan_resource::CkanResource,
    readers::ckan_reader::{CkanReader, FailedResult, ReadResult, SuccessResult},
};

pub struct JsonReader<'a> {
    conn: &'a Connection,
    supported_formats: Vec<String>,
}

impl<'a> JsonReader<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self {
            conn,
            supported_formats: vec!["JSON".to_string()],
        }
    }

    fn read_batches(&self, resource: &CkanResource) -> ReadResult {
        let batches = self.try_read_json(&resource.url)?;
        if batches.is_empty() || batches.iter().all(|batch| batch.num_rows() == 0) {
            return Err(FailedResult::from_string("No data", self.reader_name().to_string()));
        }
        Ok(SuccessResult::new(batches, self.reader_name().to_string()))
    }

    fn try_read_json(&self, path: &str) -> Result<Vec<RecordBatch>> {
        let mut statement = self
            .conn
            .prepare(&format!("SELECT * FROM read_json_auto('{}')", path))?;
        Ok(statement.query_arrow([])?.collect())
    }
}

impl CkanReader for JsonReader<'_> {
    fn supported_formats(&self) -> &[String] {
        &self.supported_formats
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        self.read_batches(resource)
    }
}
