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
use arrow::{
    array::{Array, Int64Array, StringArray},
    datatypes::DataType,
};
use ckan_ingestor_lib::{
    ckan_resource::CkanResource,
    readers::{
        ckan_reader::CkanReader,
        logfile_reader::{LogfileReader, LOG_FORMAT},
    },
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs::File;

const LOG_FIXTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/data/log_fixture.txt"
);

fn resource(url: &str, format: &str) -> CkanResource {
    CkanResource {
        id: "log-resource".to_string(),
        package_id: String::new(),
        url: url.to_string(),
        format: format.to_string(),
        datastore_active: false,
        last_modified: String::new(),
    }
}

#[test]
fn exposes_the_log_format() {
    let reader = LogfileReader::new();

    assert_eq!(reader.supported_formats(), &[LOG_FORMAT.to_string()]);
    assert!(reader.can_read(&resource(LOG_FIXTURE, "log")));
    assert!(reader.can_read(&resource(LOG_FIXTURE, "LOG")));
    assert!(!reader.can_read(&resource(LOG_FIXTURE, "CSV")));
}

#[test]
fn reads_each_fixture_line_as_an_ordered_message() -> Result<()> {
    let result = LogfileReader::new().read(&resource(LOG_FIXTURE, LOG_FORMAT))?;

    assert_eq!(result.rows_processed, 7);
    assert_eq!(result.number_of_columns, 2);
    assert_eq!(
        result.parquet.schema.field_with_name("order")?.data_type(),
        &DataType::Int64
    );
    assert_eq!(
        result
            .parquet
            .schema
            .field_with_name("message")?
            .data_type(),
        &DataType::Utf8
    );

    let batches = ParquetRecordBatchReaderBuilder::try_new(File::open(result.parquet.path())?)?
        .build()?
        .collect::<Result<Vec<_>, _>>()?;
    let batch = &batches[0];
    let order = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let message = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();

    assert_eq!(
        order.values().iter().copied().collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6, 7]
    );
    let expected_messages = std::fs::read_to_string(LOG_FIXTURE)?
        .lines()
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(
        message
            .iter()
            .map(|value| value.unwrap().to_owned())
            .collect::<Vec<_>>(),
        expected_messages
    );
    Ok(())
}
