// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use arrow_ipc::convert::IpcSchemaEncoder;
use base64::Engine;
use ckan_ingestor_lib::{config::S3Settings, parquet_output::ParquetOutput};
use ckan_ingestor_worker_lib::{ParquetArtifact, ParquetColumnStatistics};
use object_store::{ObjectStoreExt, WriteMultipart, parse_url_opts};
use tokio::io::AsyncReadExt;
use url::Url;

#[derive(Clone)]
pub struct ParquetUploader {
    s3: S3Settings,
}

impl ParquetUploader {
    pub fn new(s3: S3Settings) -> Self {
        Self { s3 }
    }

    pub async fn upload(
        &self,
        resource_id: &str,
        job_id: &str,
        parquet: &ParquetOutput,
    ) -> Result<ParquetArtifact> {
        let uri = self.destination_uri(resource_id, job_id);
        let url = Url::parse(&uri)?;
        let (store, path) = parse_url_opts(&url, self.s3.object_store_options())?;
        let upload = store.put_multipart(&path).await?;
        let mut writer = WriteMultipart::new(upload);
        let mut input = tokio::fs::File::open(parquet.path()).await?;
        let mut buffer = vec![0; 5 * 1024 * 1024];
        loop {
            let bytes_read = input.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            writer.write(&buffer[..bytes_read]);
            writer.wait_for_capacity(4).await?;
        }
        writer.finish().await?;
        artifact_from_parquet(&uri, parquet)
    }

    fn destination_uri(&self, resource_id: &str, job_id: &str) -> String {
        let resource =
            url::form_urlencoded::byte_serialize(resource_id.as_bytes()).collect::<String>();
        let job = url::form_urlencoded::byte_serialize(job_id.as_bytes()).collect::<String>();
        format!("s3://{}/{resource}/ducklake-{job}.parquet", self.s3.bucket)
    }
}

pub fn artifact_from_parquet(uri: &str, parquet: &ParquetOutput) -> Result<ParquetArtifact> {
    let metadata = parquet
        .metadata()
        .context("Parquet output must be finished before it is published")?;
    let encoded_schema = IpcSchemaEncoder::new().schema_to_fb(parquet.schema.as_ref());
    let mut stats: BTreeMap<i64, ParquetColumnStatistics> = BTreeMap::new();
    for row_group in metadata.row_groups() {
        for column in row_group.columns() {
            let primitive = column.column_descr().self_type().get_basic_info();
            if !primitive.has_id() {
                continue;
            }
            let entry =
                stats
                    .entry(i64::from(primitive.id()))
                    .or_insert_with(|| ParquetColumnStatistics {
                        field_id: i64::from(primitive.id()),
                        size_bytes: Some(0),
                        min_value: None,
                        max_value: None,
                        null_count: Some(0),
                        contains_nan: None,
                    });
            entry.size_bytes = entry.size_bytes.and_then(|size| {
                u64::try_from(column.compressed_size())
                    .ok()
                    .map(|value| size + value)
            });
            entry.null_count = match (
                entry.null_count,
                column.statistics().and_then(|value| value.null_count_opt()),
            ) {
                (Some(total), Some(value)) => Some(total + value),
                _ => None,
            };
        }
    }
    Ok(ParquetArtifact {
        uri: uri.to_owned(),
        schema_ipc_base64: base64::engine::general_purpose::STANDARD
            .encode(encoded_schema.finished_data()),
        num_rows: u64::try_from(metadata.file_metadata().num_rows())?,
        file_size_bytes: parquet.file_size()?,
        footer_size_bytes: None,
        column_statistics: stats.into_values().collect(),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::Int32Array,
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::artifact_from_parquet;
    use ckan_ingestor_lib::parquet_output::ParquetOutput;

    #[test]
    fn descriptor_is_complete_without_reading_the_uploaded_object() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, true)])),
            vec![Arc::new(Int32Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let mut parquet = ParquetOutput::try_new(&batch).unwrap();
        parquet.write(&batch).unwrap();
        parquet.finish().unwrap();

        let artifact = artifact_from_parquet("s3://warehouse/r/job.parquet", &parquet).unwrap();

        assert_eq!(artifact.uri, "s3://warehouse/r/job.parquet");
        assert_eq!(artifact.num_rows, 2);
        assert!(artifact.file_size_bytes > 0);
        assert!(!artifact.schema_ipc_base64.is_empty());
        assert_eq!(artifact.column_statistics[0].field_id, 1);
        assert_eq!(artifact.column_statistics[0].null_count, Some(1));
    }
}
