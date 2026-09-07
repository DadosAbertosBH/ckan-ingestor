// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::Result;

use crate::{
    ckan_resource::CkanResource,
    data_writer::DataWriter,
    ingestor_outcome::{IngestionOutcome, IngestionStatus},
    readers::{
        ckan_reader::{CkanReader, SuccessResult},
        multiple_reader::MultipleReader,
    },
};

pub struct DataIngestor<'a, T: DataWriter> {
    writer: T,
    reader: &'a MultipleReader<'a>,
}

impl<'a, T: DataWriter> DataIngestor<'a, T> {
    pub fn new(writer: T, reader: &'a MultipleReader<'a>) -> Self {
        Self { writer, reader }
    }

    pub async fn ingest_ckan_data(&self, resource: &CkanResource) -> IngestionOutcome {
        match self.reader.read(resource) {
            Ok(result) => match self
                .persist_successful_ingestion(&resource.id, &result)
                .await
            {
                Ok(()) => Self::success_outcome(resource, result),
                Err(error) => Self::failed_outcome(resource, result, format!("{error:#}")),
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
        self.writer.ingest(resource_id, &result.parquet).await
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
    use std::sync::Arc;
    use std::time::Instant;

    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use tempfile::tempdir;

    use super::*;
    use crate::{
        ducklake_factory::DucklakeFactory,
        parquet_output::ParquetOutput,
        readers::ckan_reader::{FailedResult, ReadResult},
        readers::json_reader::JsonReader,
    };

    struct BatchReader {
        batches: Vec<RecordBatch>,
        formats: Vec<String>,
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

    impl CkanReader for BatchReader {
        fn supported_formats(&self) -> &[String] {
            &self.formats
        }

        fn do_read(&self, _: &CkanResource) -> ReadResult {
            let mut output = ParquetOutput::try_new(&self.batches[0])?;
            for batch in &self.batches {
                output.write(batch)?;
            }
            output.finish()?;
            Ok(SuccessResult::new(output, "BatchReader".into()))
        }
    }

    #[tokio::test]
    async fn persists_batches_and_returns_a_success_outcome() {
        let _memory_guard = crate::test_alloc::memory_intensive_test_guard();
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
        let batches = vec![RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["Ana", "Bia"])) as ArrayRef],
        )
        .unwrap()];
        let temp = tempdir().unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &temp.path().join("catalog.sqlite"),
            &temp.path().join("data"),
        );
        factory.initialize().await.unwrap();
        let reader = MultipleReader::new(vec![Box::new(BatchReader {
            batches,
            formats: vec!["CSV".into()],
        })]);
        let resource = CkanResource {
            id: "resource_table".into(),
            url: "https://example.test/resource.csv".into(),
            format: "CSV".into(),
            datastore_active: false,
        };

        let outcome = DataIngestor::new(factory.writer().await.unwrap(), &reader)
            .ingest_ckan_data(&resource)
            .await;

        assert_eq!(
            outcome.status,
            IngestionStatus::Success,
            "{:?}",
            outcome.error_message
        );
        assert_eq!(outcome.rows_processed, 2);

        let table = factory
            .client()
            .await
            .unwrap()
            .table("resource_table")
            .await
            .unwrap();
        let scan = table.scan().await.unwrap();
        assert_eq!(scan.data_files.len(), 1);
        assert_eq!(scan.data_files[0].statistics.num_rows, 2);
        let updates = factory
            .client()
            .await
            .unwrap()
            .table(crate::ducklake_data_writer::LAST_UPDATE_TABLE)
            .await
            .unwrap()
            .scan()
            .await
            .unwrap();
        assert_eq!(
            updates
                .inline_data
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            1
        );

        let outcome = DataIngestor::new(factory.writer().await.unwrap(), &reader)
            .ingest_ckan_data(&resource)
            .await;
        assert_eq!(
            outcome.status,
            IngestionStatus::Success,
            "{:?}",
            outcome.error_message
        );
        let scan = factory
            .client()
            .await
            .unwrap()
            .table("resource_table")
            .await
            .unwrap()
            .scan()
            .await
            .unwrap();
        assert_eq!(scan.data_files.len(), 1);
        assert_eq!(scan.data_files[0].statistics.num_rows, 2);
        let updates = factory
            .client()
            .await
            .unwrap()
            .table(crate::ducklake_data_writer::LAST_UPDATE_TABLE)
            .await
            .unwrap()
            .scan()
            .await
            .unwrap();
        assert_eq!(
            updates
                .inline_data
                .iter()
                .map(RecordBatch::num_rows)
                .sum::<usize>(),
            2
        );
    }

    #[tokio::test]
    async fn preserves_reader_error_in_failed_outcome() {
        let _memory_guard = crate::test_alloc::memory_intensive_test_guard();
        let temp = tempdir().unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &temp.path().join("catalog.sqlite"),
            &temp.path().join("data"),
        );
        factory.initialize().await.unwrap();
        let reader = MultipleReader::new(vec![Box::new(FailingReader {
            formats: vec!["CSV".into()],
        })]);
        let resource = CkanResource {
            id: "failed-resource".into(),
            url: "https://example.test/resource.csv".into(),
            format: "CSV".into(),
            datastore_active: false,
        };

        let outcome = DataIngestor::new(factory.writer().await.unwrap(), &reader)
            .ingest_ckan_data(&resource)
            .await;

        assert_eq!(outcome.status, IngestionStatus::Failed);
        assert_eq!(
            outcome.error_message.as_deref(),
            Some("No data to create table from")
        );
    }

    #[test]
    fn persists_synthetic_nested_json_with_ducklake_factory() {
        let _memory_guard = crate::test_alloc::memory_intensive_test_guard();
        let temp = tempdir().unwrap();
        let json_path = temp.path().join("synthetic.json");
        let errors = (0..55_003)
            .map(|index| {
                serde_json::json!({
                    "code": "constraint-error",
                    "message": "x".repeat(128),
                    "description": "y".repeat(32),
                    "rowNumber": index,
                    "fieldName": format!("field_{}", index % 889)
                })
            })
            .collect::<Vec<_>>();
        let fields = (0..889)
            .map(|index| {
                serde_json::json!({
                    "name": format!("field_{index}"),
                    "type": "string"
                })
            })
            .collect::<Vec<_>>();
        let json = serde_json::json!({"resources": [{
            "name": "synthetic-resource",
            "schema": {"fields": fields},
            "validation": {"tasks": [{"errors": errors}]}
        }]});
        std::fs::write(&json_path, serde_json::to_vec(&json).unwrap()).unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &temp.path().join("catalog.sqlite"),
            &temp.path().join("data"),
        );
        let reader = MultipleReader::new(vec![Box::new(JsonReader::new())]);
        let resource = CkanResource {
            id: "synthetic-resource".into(),
            url: json_path.to_string_lossy().into_owned(),
            format: "JSON".into(),
            datastore_active: false,
        };
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(factory.initialize()).unwrap();
        runtime.block_on(async {
            let started = Instant::now();
            let outcome = DataIngestor::new(factory.writer().await.unwrap(), &reader)
                .ingest_ckan_data(&resource)
                .await;

            assert_eq!(
                outcome.status,
                IngestionStatus::Success,
                "{:?}",
                outcome.error_message
            );
            assert_eq!(outcome.rows_processed, 1);
            assert!(started.elapsed() < std::time::Duration::from_secs(3));
        });
    }
}
