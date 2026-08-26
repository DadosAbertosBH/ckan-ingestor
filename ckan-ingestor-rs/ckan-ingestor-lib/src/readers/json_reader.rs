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
use std::io::{BufRead, BufReader, Cursor, Seek, SeekFrom};
use std::sync::Arc;

use crate::{
    arrow_ipc_output::ArrowIpcOutput,
    ckan_resource::CkanResource,
    readers::{
        ckan_reader::{download_to_temp, CkanReader, FailedResult, ReadResult, SuccessResult},
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

    fn try_read_json(&self, path: &str) -> Result<ArrowIpcOutput> {
        match self.read_json(BufReader::new(File::open(path)?)) {
            Ok(output) => Ok(output),
            Err(line_delimited_error) => {
                let document = match serde_json::from_reader(File::open(path)?) {
                    Ok(document) => document,
                    Err(_) => return Err(line_delimited_error),
                };
                self.read_json_document(document)
            }
        }
    }

    fn read_json_document(&self, document: serde_json::Value) -> Result<ArrowIpcOutput> {
        let schema_data = json_lines(&document)?;
        let (schema, _) = infer_json_schema(&mut Cursor::new(schema_data), None)?;
        let mut decoder = ReaderBuilder::new(Arc::new(schema))
            .with_batch_size(8192)
            .build_decoder()?;
        let records = match document {
            serde_json::Value::Array(records) => records,
            record => vec![record],
        };
        let mut output = None;
        for records in records.chunks(8192) {
            decoder.serialize(records)?;
            if let Some(batch) = decoder.flush()? {
                if output.is_none() {
                    output = Some(ArrowIpcOutput::try_new(&batch)?);
                }
                output
                    .as_mut()
                    .expect("Arrow IPC output initialized")
                    .write(&batch)?;
            }
        }
        let mut output = output.ok_or_else(|| anyhow::anyhow!("No data"))?;
        output.finish()?;
        Ok(output)
    }

    fn read_json<R: BufRead + Seek>(&self, mut schema_reader: R) -> Result<ArrowIpcOutput> {
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

fn json_lines(document: &serde_json::Value) -> Result<Vec<u8>> {
    let records = match document {
        serde_json::Value::Array(records) => records.as_slice(),
        record => std::slice::from_ref(record),
    };
    let mut bytes = Vec::new();
    for record in records {
        serde_json::to_writer(&mut bytes, record)?;
        bytes.push(b'\n');
    }
    Ok(bytes)
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
