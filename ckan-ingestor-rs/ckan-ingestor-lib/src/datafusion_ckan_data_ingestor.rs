// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::Result;
use datafusion::execution::options::ArrowReadOptions;
use datafusion_ducklake::metadata_writer::WriteMode;
use futures::StreamExt;

use crate::{
    ckan_resource::CkanResource,
    datafusion_ducklake_factory::DatafusionDucklakeFactory,
    ingestor_outcome::{IngestionOutcome, IngestionStatus},
    readers::{
        ckan_reader::{CkanReader, SuccessResult},
        multiple_reader::MultipleReader,
    },
};

pub struct DatafusionCkanDataIngestor<'a> {
    factory: &'a DatafusionDucklakeFactory,
    reader: &'a MultipleReader<'a>,
}

impl<'a> DatafusionCkanDataIngestor<'a> {
    pub fn new(factory: &'a DatafusionDucklakeFactory, reader: &'a MultipleReader<'a>) -> Self {
        Self { factory, reader }
    }

    pub async fn ingest_ckan_data(&self, resource: &CkanResource) -> IngestionOutcome {
        match self.reader.read(resource) {
            Ok(result) => match self
                .persist_successful_ingestion(&resource.id, &result)
                .await
            {
                Ok(()) => Self::success_outcome(resource, result),
                Err(error) => Self::failed_outcome(resource, result, error.to_string()),
            },
            Err(failed) => IngestionOutcome {
                reader: failed.reader,
                rows_processed: 0,
                preview: vec![],
                expected_rows: failed.expected_rows,
                encoding: None,
                csv_strict_mode: None,
                csv_delimiter: None,
                datastore_active: resource.datastore_active,
                expected_columns: failed.expected_columns,
                error_message: Some(failed.error.to_string()),
                status: IngestionStatus::Failed,
            },
        }
    }

    async fn persist_successful_ingestion(
        &self,
        resource_id: &str,
        result: &SuccessResult,
    ) -> Result<()> {
        let ctx = self.factory.session().await?;
        let input = ctx
            .read_arrow(
                result.arrow_ipc.path().to_string_lossy().into_owned(),
                ArrowReadOptions::default(),
            )
            .await?;
        let mut batches = input.execute_stream().await?;
        let writer = self.factory.table_writer().await?;
        let mut write = writer.begin_write(
            "main",
            resource_id,
            batches.schema().as_ref(),
            WriteMode::Replace,
        )?;
        while let Some(batch) = batches.next().await {
            write.write_batch(&batch?)?;
        }
        write.finish().await?;

        let resource_id = resource_id.replace('\'', "''");
        let metadata_table = self.factory.table("ckan_resource_last_update");
        let delete = self.factory.session().await?;
        delete
            .sql(&format!(
                "DELETE FROM {metadata_table} WHERE ckan_resource_id = '{resource_id}'"
            ))
            .await?
            .collect()
            .await?;
        let insert = self.factory.session().await?;
        insert.sql(&format!("INSERT INTO {metadata_table} (ckan_resource_id, last_modified) VALUES ('{resource_id}', CURRENT_TIMESTAMP)")).await?.collect().await?;
        Ok(())
    }

    fn success_outcome(resource: &CkanResource, result: SuccessResult) -> IngestionOutcome {
        IngestionOutcome {
            reader: result.reader,
            rows_processed: result.rows_processed,
            preview: result.preview,
            expected_rows: result.expected_rows,
            encoding: result.encoding,
            csv_strict_mode: result.csv_strict_mode,
            csv_delimiter: result.csv_delimiter,
            datastore_active: resource.datastore_active,
            expected_columns: result.expected_columns,
            error_message: None,
            status: IngestionStatus::Success,
        }
    }

    fn failed_outcome(
        resource: &CkanResource,
        result: SuccessResult,
        error_message: String,
    ) -> IngestionOutcome {
        IngestionOutcome {
            reader: result.reader,
            rows_processed: 0,
            preview: vec![],
            expected_rows: result.expected_rows,
            encoding: None,
            csv_strict_mode: None,
            csv_delimiter: None,
            datastore_active: resource.datastore_active,
            expected_columns: result.expected_columns,
            error_message: Some(error_message),
            status: IngestionStatus::Failed,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        arrow_ipc_output::ArrowIpcOutput,
        readers::{
            ckan_reader::{FailedResult, ReadResult},
            json_reader::JsonReader,
        },
    };
    use arrow::{
        array::{ArrayRef, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use std::{sync::Arc, time::Instant};
    use tempfile::TempDir;
    use testcontainers::{
        core::{IntoContainerPort, WaitFor},
        runners::AsyncRunner,
        ContainerAsync, GenericImage, ImageExt,
    };

    struct BatchReader {
        batches: Vec<RecordBatch>,
        formats: Vec<String>,
    }
    impl CkanReader for BatchReader {
        fn supported_formats(&self) -> &[String] {
            &self.formats
        }
        fn do_read(&self, _: &CkanResource) -> ReadResult {
            let mut output = ArrowIpcOutput::try_new(&self.batches[0]).unwrap();
            for batch in &self.batches {
                output.write(batch).unwrap();
            }
            output.finish().unwrap();
            Ok(SuccessResult::new(output, "BatchReader".into()))
        }
    }
    struct FailingReader {
        formats: Vec<String>,
    }
    impl CkanReader for FailingReader {
        fn supported_formats(&self) -> &[String] {
            &self.formats
        }
        fn do_read(&self, _: &CkanResource) -> ReadResult {
            Err(FailedResult::from_string(
                "No data to create table from",
                self.reader_name().to_string(),
            ))
        }
    }
    fn resource() -> CkanResource {
        CkanResource {
            id: "resource_table".into(),
            url: "https://example.test/resource.csv".into(),
            format: "CSV".into(),
            datastore_active: false,
        }
    }
    struct PostgresFixture {
        factory: DatafusionDucklakeFactory,
        _container: ContainerAsync<GenericImage>,
        _data_dir: TempDir,
    }

    async fn postgres_fixture() -> PostgresFixture {
        let container = GenericImage::new("postgres", "17-alpine")
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_exposed_port(5432.tcp())
            .with_env_var("POSTGRES_USER", "postgres")
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .with_env_var("POSTGRES_DB", "ducklake")
            .start()
            .await
            .expect("PostgreSQL test container starts");
        let host = container.get_host().await.unwrap();
        let port = container.get_host_port_ipv4(5432.tcp()).await.unwrap();
        let data_dir = TempDir::new().unwrap();
        let factory = DatafusionDucklakeFactory::for_postgres(
            format!("postgresql://postgres:postgres@{host}:{port}/ducklake"),
            data_dir.path().to_string_lossy(),
        );
        PostgresFixture {
            factory,
            _container: container,
            _data_dir: data_dir,
        }
    }
    async fn initialize_metadata_schema(factory: &DatafusionDucklakeFactory) {
        factory
            .session()
            .await
            .unwrap()
            .sql(&format!(
                "CREATE TABLE {} AS SELECT \
                 CAST(NULL AS VARCHAR) AS ckan_resource_id, \
                 CAST(NULL AS TIMESTAMP) AS last_modified WHERE FALSE",
                factory.table("ckan_resource_last_update")
            ))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
    }

    async fn row_count(factory: &DatafusionDucklakeFactory, table: &str) -> i64 {
        let batches = factory
            .session()
            .await
            .unwrap()
            .sql(&format!("SELECT COUNT(*) FROM {}", factory.table(table)))
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn persists_batches_and_returns_a_success_outcome() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batches = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec!["Ana"])) as ArrayRef],
            )
            .unwrap(),
            RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(vec!["Bia"])) as ArrayRef],
            )
            .unwrap(),
        ];
        let fixture = postgres_fixture().await;
        initialize_metadata_schema(&fixture.factory).await;
        let reader = MultipleReader::new(vec![Box::new(BatchReader {
            batches,
            formats: vec!["CSV".into()],
        })]);
        let ingestor = DatafusionCkanDataIngestor::new(&fixture.factory, &reader);
        let outcome = ingestor.ingest_ckan_data(&resource()).await;
        assert_eq!(outcome.status, IngestionStatus::Success);
        assert_eq!(outcome.rows_processed, 2);
        assert_eq!(row_count(&fixture.factory, "resource_table").await, 2);
        ingestor.ingest_ckan_data(&resource()).await;
        assert_eq!(row_count(&fixture.factory, "resource_table").await, 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn preserves_reader_error_in_failed_outcome() {
        let factory = DatafusionDucklakeFactory::for_postgres(
            "postgresql://postgres:postgres@localhost/unused",
            "/tmp/unused",
        );
        let reader = MultipleReader::new(vec![Box::new(FailingReader {
            formats: vec!["CSV".into()],
        })]);
        let outcome = DatafusionCkanDataIngestor::new(&factory, &reader)
            .ingest_ckan_data(&resource())
            .await;
        assert_eq!(outcome.status, IngestionStatus::Failed);
        assert_eq!(
            outcome.error_message.as_deref(),
            Some("No data to create table from")
        );
    }

    #[test]
    fn persists_synthetic_nested_json_with_datafusion_factory() {
        let reader = MultipleReader::new(vec![Box::new(JsonReader::new())]);
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
        let json_path =
            std::env::temp_dir().join(format!("datafusion-synthetic-{}.json", std::process::id()));
        let errors = (0..55_003).map(|index| serde_json::json!({"code":"constraint-error", "message":"x".repeat(128), "description":"y".repeat(32), "rowNumber":index, "fieldName":format!("field_{}", index % 889)})).collect::<Vec<_>>();
        let fields = (0..889)
            .map(|index| serde_json::json!({"name":format!("field_{index}"), "type":"string"}))
            .collect::<Vec<_>>();
        let json = serde_json::json!({"resources":[{"name":"synthetic-resource", "schema":{"fields":fields}, "validation":{"tasks":[{"errors":errors}]}}]});
        std::fs::write(&json_path, serde_json::to_vec(&json).unwrap()).unwrap();
        let fixture = postgres_fixture().await;
        initialize_metadata_schema(&fixture.factory).await;
        let resource = CkanResource {
            id: format!("synthetic-{}", std::process::id()),
            url: json_path.to_string_lossy().to_string(),
            format: "JSON".into(),
            datastore_active: false,
        };
        let started = Instant::now();
        let outcome = DatafusionCkanDataIngestor::new(&fixture.factory, &reader)
            .ingest_ckan_data(&resource)
            .await;
        assert_eq!(outcome.status, IngestionStatus::Success);
        assert_eq!(outcome.rows_processed, 1);
        assert!(started.elapsed() < std::time::Duration::from_secs(3));
        std::fs::remove_file(json_path).ok();
        });
    }
}
