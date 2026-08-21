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

use crate::ingestor_outcome::{IngestionOutcome, IngestionStatus};
use crate::{
    ckan_resource::CkanResource,
    readers::ckan_reader::CkanReader,
    readers::{ckan_reader::SuccessResult, multiple_reader::MultipleReader},
};
use anyhow::Result;
use arrow_ipc::writer::FileWriter;
use log::debug;

pub struct DuckdbCkanDataIngestor<'a> {
    conn: &'a duckdb::Connection,
    reader: &'a MultipleReader<'a>,
}

impl<'a> DuckdbCkanDataIngestor<'a> {
    pub fn new(conn: &'a duckdb::Connection, reader: &'a MultipleReader<'a>) -> Self {
        Self { conn, reader }
    }

    pub fn ingest_ckan_data(&self, resource: &CkanResource) -> IngestionOutcome {
        let resource_id = &resource.id;

        debug!("updating {} from resource {}", resource_id, resource_id);

        match self.reader.read(resource) {
            Ok(result) => match self.persist_successful_ingestion(resource_id, &result) {
                Ok(()) => IngestionOutcome {
                    reader: result.reader,
                    rows_processed: result.rows_processed,
                    preview: result.preview,
                    expected_rows: result.expected_rows,
                    encoding: result.encoding,
                    datastore_active: resource.datastore_active,
                    expected_columns: result.expected_columns,
                    error_message: None,
                    status: IngestionStatus::Success,
                },
                Err(error) => Self::failed_outcome(
                    result.reader,
                    resource,
                    error.to_string(),
                    result.expected_rows,
                    result.expected_columns,
                ),
            },
            Err(failed) => Self::failed_outcome(
                failed.reader,
                resource,
                failed.error.to_string(),
                failed.expected_rows,
                failed.expected_columns,
            ),
        }
    }

    fn persist_successful_ingestion(
        &self,
        resource_id: &str,
        result: &SuccessResult,
    ) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS ckan_resource_last_update \
              (ckan_resource_id VARCHAR, last_modified TIMESTAMP)",
        )?;
        self.create_table_from_batches(resource_id, &result.data)?;
        self.update_last_modified(resource_id)?;
        Ok(())
    }

    fn failed_outcome(
        reader: String,
        resource: &CkanResource,
        error_message: String,
        expected_rows: Option<usize>,
        expected_columns: Option<usize>,
    ) -> IngestionOutcome {
        IngestionOutcome {
            reader,
            rows_processed: 0,
            preview: vec![],
            expected_rows,
            encoding: None,
            datastore_active: resource.datastore_active,
            expected_columns,
            error_message: Some(error_message),
            status: IngestionStatus::Failed,
        }
    }

    fn create_table_from_batches(
        &self,
        resource_id: &str,
        batches: &[duckdb::arrow::array::RecordBatch],
    ) -> Result<()> {
        let temp_path = std::env::temp_dir().join(format!("{}.arrow", uuid::Uuid::new_v4()));
        {
            let mut file = std::fs::File::create(&temp_path)?;
            let mut writer = FileWriter::try_new(&mut file, &batches[0].schema())?;
            for batch in batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }

        let result = self.conn.execute_batch(&format!(
            "CREATE OR REPLACE TABLE \"{}\" AS SELECT * FROM read_arrow('{}')",
            resource_id,
            temp_path.to_string_lossy()
        ));
        std::fs::remove_file(&temp_path)?;
        result?;
        Ok(())
    }

    fn update_last_modified(&self, resource_id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM ckan_resource_last_update WHERE ckan_resource_id = ?",
            duckdb::params![resource_id],
        )?;
        self.conn.execute(
            "INSERT INTO ckan_resource_last_update (ckan_resource_id, last_modified) VALUES (?, NOW())",
            duckdb::params![resource_id],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use duckdb::arrow::{
        array::{ArrayRef, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use crate::{
        ckan_resource::CkanResource,
        ingestor_outcome::IngestionStatus,
        readers::{
            ckan_reader::{CkanReader, FailedResult, ReadResult, SuccessResult},
            multiple_reader::MultipleReader,
        },
    };

    use super::DuckdbCkanDataIngestor;

    struct StaticReader {
        formats: Vec<String>,
    }

    impl CkanReader for StaticReader {
        fn supported_formats(&self) -> &[String] {
            &self.formats
        }

        fn do_read(&self, _resource: &CkanResource) -> ReadResult {
            let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));
            let first = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from(vec!["Ana"])) as ArrayRef],
            )
            .expect("valid batch");
            let second = RecordBatch::try_new(
                schema,
                vec![Arc::new(StringArray::from(vec!["Bia"])) as ArrayRef],
            )
            .expect("valid batch");
            Ok(SuccessResult::new(
                vec![first, second],
                self.reader_name().to_string(),
            ))
        }
    }

    struct FailingReader {
        formats: Vec<String>,
    }

    impl CkanReader for FailingReader {
        fn supported_formats(&self) -> &[String] {
            &self.formats
        }

        fn do_read(&self, _resource: &CkanResource) -> ReadResult {
            Err(FailedResult::from_string(
                "No data to create table from",
                self.reader_name().to_string(),
            ))
        }
    }

    #[test]
    fn persists_batches_and_returns_a_success_outcome() {
        let conn = duckdb::Connection::open_in_memory().expect("in-memory DuckDB");
        conn.execute_batch("INSTALL arrow FROM community; LOAD arrow;")
            .expect("DuckDB Arrow extension is available");
        let reader = MultipleReader::new(vec![Box::new(StaticReader {
            formats: vec!["CSV".to_string()],
        })]);
        let ingestor = DuckdbCkanDataIngestor::new(&conn, &reader);
        let resource = CkanResource {
            id: "resource_table".to_string(),
            url: "https://example.test/resource.csv".to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
        };

        let outcome = ingestor.ingest_ckan_data(&resource);

        assert_eq!(outcome.status, IngestionStatus::Success);
        assert_eq!(outcome.rows_processed, 2);
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM resource_table", [], |row| row.get(0))
            .expect("table was created");
        assert_eq!(rows, 2);

        ingestor.ingest_ckan_data(&resource);
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM resource_table", [], |row| row.get(0))
            .expect("table was replaced");
        assert_eq!(rows, 2);
    }

    #[test]
    fn preserves_reader_error_in_failed_outcome() {
        let conn = duckdb::Connection::open_in_memory().expect("in-memory DuckDB");
        let reader = MultipleReader::new(vec![Box::new(FailingReader {
            formats: vec!["CSV".to_string()],
        })]);
        let ingestor = DuckdbCkanDataIngestor::new(&conn, &reader);
        let resource = CkanResource {
            id: "failed-resource".to_string(),
            url: "https://example.test/resource.csv".to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
        };

        let outcome = ingestor.ingest_ckan_data(&resource);

        assert_eq!(outcome.status, IngestionStatus::Failed);
        assert_eq!(
            outcome.error_message.as_deref(),
            Some("No data to create table from")
        );
    }
}
