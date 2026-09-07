// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::collections::HashMap;

use anyhow::{Context, Result};
use arrow::datatypes::Schema;
use arrow_ipc::convert::fb_to_schema;
use base64::Engine;
use ckan_ingestor_lib::ducklake_factory::DucklakeFactory;
use ckan_ingestor_worker_lib::ParquetArtifact;
use ducklake::{
    Column, DataFileStatistics, FileColumnStats, IfExistsStrategy, TableName, Tag, Value,
    WriteDataFile,
};

use crate::ducklake_data_writer::{append_last_update, initialize_last_update_table};

#[derive(Clone)]
pub struct ParquetRegistrar {
    factory: DucklakeFactory,
}

impl ParquetRegistrar {
    pub fn new(factory: DucklakeFactory) -> Self {
        Self { factory }
    }

    pub async fn initialize(&self) -> Result<()> {
        let client = self.factory.client().await?;
        initialize_last_update_table(&client).await
    }

    /// Registers a pre-uploaded file. `write_data_files` receives complete
    /// statistics, so the SDK never opens the S3 object to inspect its footer.
    pub async fn register(
        &self,
        resource_id: &str,
        job_id: &str,
        artifact: &ParquetArtifact,
    ) -> Result<()> {
        let schema = decode_schema(&artifact.schema_ipc_base64)?;
        let columns = schema
            .fields()
            .iter()
            .map(|field| Column::try_from(field.as_ref()))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let client = self.factory.client().await?;
        let table_name = TableName {
            schema: "main".into(),
            name: resource_id.into(),
        };
        if client.table_exists(table_name.clone()).await? {
            let current = client.table(table_name.clone()).await?;
            if current
                .tags()
                .await?
                .iter()
                .any(|tag| tag.key == "source_job_id" && tag.value == job_id)
            {
                return Ok(());
            }
        }
        let mut transaction = client.transaction().await?;
        if client.table_exists(table_name.clone()).await? {
            transaction.table(table_name.clone())?.delete()?;
        }
        let mut table = transaction.create_table(
            table_name,
            columns,
            None,
            None,
            Some(vec![Tag {
                key: "source_job_id".into(),
                value: job_id.into(),
            }]),
            IfExistsStrategy::Fail,
        )?;
        table
            .write_data_files(vec![WriteDataFile {
                path: artifact.uri.clone(),
                statistics: Some(statistics(artifact)?),
                partition_values: None,
            }])
            .await
            .context("registering pre-uploaded parquet file")?;
        drop(table);
        append_last_update(&mut transaction, resource_id)?;
        transaction
            .commit()
            .await
            .context("committing parquet registration")?;
        Ok(())
    }
}

fn decode_schema(encoded: &str) -> Result<Schema> {
    let bytes = base64::engine::general_purpose::STANDARD.decode(encoded)?;
    let flatbuffer = arrow_ipc::root_as_schema(&bytes)?;
    Ok(fb_to_schema(flatbuffer))
}

fn statistics(artifact: &ParquetArtifact) -> Result<DataFileStatistics> {
    Ok(DataFileStatistics {
        num_rows: usize::try_from(artifact.num_rows)?,
        file_size_bytes: Some(usize::try_from(artifact.file_size_bytes)?),
        footer_size_bytes: artifact
            .footer_size_bytes
            .map(usize::try_from)
            .transpose()?,
        column_stats: artifact
            .column_statistics
            .iter()
            .map(|column| {
                Ok((
                    column.field_id,
                    FileColumnStats {
                        size_bytes: column.size_bytes.map(usize::try_from).transpose()?,
                        // The producer intentionally sends exact structural statistics and
                        // leaves typed extrema optional. This avoids a lossy text-to-type
                        // conversion in the coordinator.
                        min_value: None::<Value>,
                        max_value: None::<Value>,
                        null_count: column.null_count.map(usize::try_from).transpose()?,
                        contains_nan: column.contains_nan,
                    },
                ))
            })
            .collect::<Result<HashMap<_, _>>>()?,
    })
}

#[cfg(test)]
mod tests {
    use super::ParquetRegistrar;
    use super::statistics;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_ipc::convert::IpcSchemaEncoder;
    use base64::Engine;
    use ckan_ingestor_lib::ducklake_factory::DucklakeFactory;
    use ckan_ingestor_worker_lib::{ParquetArtifact, ParquetColumnStatistics};
    use tempfile::tempdir;

    #[test]
    fn registration_statistics_are_available_without_a_remote_parquet_read() {
        let stats = statistics(&ParquetArtifact {
            uri: "s3://warehouse/does-not-exist.parquet".into(),
            schema_ipc_base64: "".into(),
            num_rows: 2,
            file_size_bytes: 100,
            footer_size_bytes: Some(20),
            column_statistics: vec![ParquetColumnStatistics {
                field_id: 1,
                size_bytes: Some(80),
                min_value: None,
                max_value: None,
                null_count: Some(1),
                contains_nan: None,
            }],
        })
        .unwrap();
        assert_eq!(stats.num_rows, 2);
        assert_eq!(stats.column_stats[&1].null_count, Some(1));
    }

    #[tokio::test]
    async fn registers_an_unreachable_s3_uri_without_reading_its_footer() {
        let temp = tempdir().unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &temp.path().join("catalog.sqlite"),
            &temp.path().join("data"),
        );
        let registrar = ParquetRegistrar::new(factory);
        registrar.initialize().await.unwrap();
        let client = registrar.factory.client().await.unwrap();
        assert!(
            client
                .table_exists(crate::ducklake_data_writer::LAST_UPDATE_TABLE)
                .await
                .unwrap()
        );
        let schema = Schema::new(vec![Field::new("id", DataType::Int32, true)]);
        let schema_ipc_base64 = base64::engine::general_purpose::STANDARD.encode(
            IpcSchemaEncoder::new()
                .schema_to_fb(&schema)
                .finished_data(),
        );
        let artifact = ParquetArtifact {
            // No object exists at this URI. A footer read would fail this test.
            uri: "s3://warehouse/not-uploaded.parquet".into(),
            schema_ipc_base64,
            num_rows: 2,
            file_size_bytes: 100,
            footer_size_bytes: Some(20),
            column_statistics: vec![ParquetColumnStatistics {
                field_id: 1,
                size_bytes: Some(80),
                min_value: None,
                max_value: None,
                null_count: Some(1),
                contains_nan: None,
            }],
        };
        registrar
            .register("resource", "job", &artifact)
            .await
            .unwrap();
    }
}
