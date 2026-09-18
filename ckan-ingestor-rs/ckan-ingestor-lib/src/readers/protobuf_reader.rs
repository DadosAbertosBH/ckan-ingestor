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
use arrow::{error::ArrowError, record_batch::RecordBatch};
use arrow_json::reader::{infer_json_schema_from_iterator, ReaderBuilder};
use reqwest::blocking::Client;
use serde_json::Value;
use std::sync::Arc;

use crate::{
    ckan_resource::CkanResource,
    parquet_output::ParquetOutput,
    readers::{
        ckan_reader::{download_to_temp, CkanReader, FailedResult, ReadResult, SuccessResult},
        gtfs_realtime::decode_feed_entities,
        temp_file_cleanup::TempFileCleanup,
    },
};

/// CKAN format token handled by the binary protobuf reader.
pub const PROTOBUF_FORMAT: &str = "PROTOBUF";

const BATCH_SIZE: usize = 8192;

/// Reads binary protobuf resources.
///
/// A single protobuf payload is supported today: [GTFS Realtime][gtfs]
/// feeds, converted into one table row per feed entity.
///
/// [gtfs]: https://developers.google.com/transit/gtfs-realtime
pub struct ProtobufReader {
    client: Client,
    supported_formats: Vec<String>,
}

impl ProtobufReader {
    pub fn new() -> Self {
        Self::with_client(Client::new())
    }

    pub fn with_client(client: Client) -> Self {
        Self {
            client,
            supported_formats: vec![PROTOBUF_FORMAT.to_string()],
        }
    }

    fn read_batches(&self, resource: &CkanResource) -> ReadResult {
        let is_remote = resource.url.starts_with("http://") || resource.url.starts_with("https://");
        let path = if is_remote {
            download_to_temp(&self.client, &resource.url, ".binpb")?
        } else {
            resource.url.clone()
        };
        let mut cleanup = TempFileCleanup::from_path(path.clone().into());
        if !is_remote {
            cleanup.commit();
        }
        let bytes = std::fs::read(&path)?;
        let entities = decode_feed_entities(&bytes)?;
        let output = entities_to_parquet(entities)?;
        if output.rows == 0 {
            return Err(FailedResult::from_string(
                "No data",
                self.reader_name().to_string(),
            ));
        }
        Ok(SuccessResult::new(output, self.reader_name().to_string()))
    }
}

fn entities_to_parquet(entities: Vec<Value>) -> Result<ParquetOutput> {
    let schema = infer_json_schema_from_iterator(entities.iter().map(Ok::<_, ArrowError>))?;
    let mut decoder = ReaderBuilder::new(Arc::new(schema))
        .with_batch_size(BATCH_SIZE)
        .build_decoder()?;
    let mut output = None;
    for entities in entities.chunks(BATCH_SIZE) {
        decoder.serialize(entities)?;
        if let Some(batch) = decoder.flush()? {
            write_batch(&mut output, &batch)?;
        }
    }
    let mut output = output.ok_or_else(|| anyhow::anyhow!("No data"))?;
    output.finish()?;
    Ok(output)
}

fn write_batch(output: &mut Option<ParquetOutput>, batch: &RecordBatch) -> Result<()> {
    if output.is_none() {
        *output = Some(ParquetOutput::try_new(batch)?);
    }
    output
        .as_mut()
        .expect("Parquet output initialized")
        .write(batch)?;
    Ok(())
}

impl Default for ProtobufReader {
    fn default() -> Self {
        Self::new()
    }
}

impl CkanReader for ProtobufReader {
    fn supported_formats(&self) -> &[String] {
        &self.supported_formats
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        self.read_batches(resource)
    }
}
