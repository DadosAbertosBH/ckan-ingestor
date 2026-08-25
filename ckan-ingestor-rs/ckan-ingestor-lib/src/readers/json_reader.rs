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
use arrow_json::reader::{infer_json_schema, ReaderBuilder};
use std::fs::File;
use std::io::{BufReader, Seek, SeekFrom};
use std::sync::Arc;

use crate::{
    arrow_ipc_output::ArrowIpcOutput,
    ckan_resource::CkanResource,
    readers::ckan_reader::{CkanReader, FailedResult, ReadResult, SuccessResult},
};

pub struct JsonReader {
    supported_formats: Vec<String>,
}

impl JsonReader {
    pub fn new() -> Self {
        Self {
            supported_formats: vec!["JSON".to_string()],
        }
    }

    fn read_batches(&self, resource: &CkanResource) -> ReadResult {
        let output = self.try_read_json(&resource.url)?;
        if output.rows == 0 {
            return Err(FailedResult::from_string(
                "No data",
                self.reader_name().to_string(),
            ));
        }
        Ok(SuccessResult::new(output, self.reader_name().to_string()))
    }

    fn try_read_json(&self, path: &str) -> Result<ArrowIpcOutput> {
        let file = File::open(path)?;
        let mut schema_reader = BufReader::new(file);
        let (schema, _) = infer_json_schema(&mut schema_reader, None)?;
        schema_reader.seek(SeekFrom::Start(0))?;
        let schema = Arc::new(schema);
        let reader = ReaderBuilder::new(schema)
            .with_batch_size(8192)
            .build(schema_reader)?;
        let mut output = None;
        for batch_result in reader {
            let batch = batch_result?;
            if output.is_none() {
                output = Some(ArrowIpcOutput::try_new(&batch)?);
            }
            output
                .as_mut()
                .expect("Arrow IPC output initialized")
                .write(&batch)?;
        }
        let mut output = output.ok_or_else(|| anyhow::anyhow!("No data"))?;
        output.finish()?;
        Ok(output)
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
