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

use std::{
    fs::File,
    io::{BufRead, BufReader},
    path::PathBuf,
    sync::Arc,
};

use anyhow::Result;
use arrow::{
    array::{ArrayRef, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use reqwest::blocking::Client;

use crate::{
    ckan_resource::CkanResource,
    parquet_output::ParquetOutput,
    readers::{
        ckan_reader::{download_to_temp, CkanReader, FailedResult, ReadResult, SuccessResult},
        temp_file_cleanup::TempFileCleanup,
    },
};

pub const LOG_FORMAT: &str = "LOG";
const BATCH_SIZE: usize = 8192;

pub struct LogfileReader {
    client: Client,
    supported_formats: Vec<String>,
}

impl LogfileReader {
    pub fn new() -> Self {
        Self::with_client(Client::new())
    }

    pub fn with_client(client: Client) -> Self {
        Self {
            client,
            supported_formats: vec![LOG_FORMAT.to_string()],
        }
    }

    fn read_batches(&self, resource: &CkanResource) -> ReadResult {
        let is_remote = resource.url.starts_with("http://") || resource.url.starts_with("https://");
        let path = if is_remote {
            download_to_temp(&self.client, &resource.url, ".log")?
        } else {
            resource.url.clone()
        };
        let mut cleanup = TempFileCleanup::from_path(PathBuf::from(&path));
        if !is_remote {
            cleanup.commit();
        }

        let mut reader = BufReader::new(File::open(path)?);
        let mut output = None;
        let mut messages = Vec::with_capacity(BATCH_SIZE);
        let mut order = 1_i64;

        loop {
            let mut message = String::new();
            if reader.read_line(&mut message)? == 0 {
                break;
            }
            trim_line_ending(&mut message);
            messages.push(message);

            if messages.len() == BATCH_SIZE {
                write_batch(&mut output, &mut messages, order)?;
                order += BATCH_SIZE as i64;
            }
        }
        if !messages.is_empty() {
            write_batch(&mut output, &mut messages, order)?;
        }

        let mut output = output
            .ok_or_else(|| FailedResult::from_string("No data", self.reader_name().to_string()))?;
        output.finish()?;
        Ok(SuccessResult::new(output, self.reader_name().to_string()))
    }
}

fn trim_line_ending(message: &mut String) {
    if message.ends_with('\n') {
        message.pop();
        if message.ends_with('\r') {
            message.pop();
        }
    }
}

fn write_batch(
    output: &mut Option<ParquetOutput>,
    messages: &mut Vec<String>,
    first_order: i64,
) -> Result<()> {
    let orders = (first_order..first_order + messages.len() as i64).collect::<Vec<_>>();
    let schema = Arc::new(Schema::new(vec![
        Field::new("order", DataType::Int64, false),
        Field::new("message", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(orders)) as ArrayRef,
            Arc::new(StringArray::from(std::mem::take(messages))) as ArrayRef,
        ],
    )?;
    if output.is_none() {
        *output = Some(ParquetOutput::try_new(&batch)?);
    }
    output
        .as_mut()
        .expect("Parquet output initialized")
        .write(&batch)?;
    Ok(())
}

impl Default for LogfileReader {
    fn default() -> Self {
        Self::new()
    }
}

impl CkanReader for LogfileReader {
    fn supported_formats(&self) -> &[String] {
        &self.supported_formats
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        self.read_batches(resource)
    }
}
