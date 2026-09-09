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
use crate::ckan_resource::CkanResource;
use crate::readers::ckan_reader::{CkanReader, FailedResult, ReadResult};
use log::info;

/// Aggregate multiple types of a readers
/// into a single struct
pub struct MultipleReader<'a> {
    readers: Vec<Box<dyn CkanReader + 'a>>,
    supported_formarts: Vec<String>,
}

impl<'a> MultipleReader<'a> {
    pub fn new(readers: Vec<Box<dyn CkanReader + 'a>>) -> Self {
        let mut supported_formarts = Vec::new();
        for format in readers.iter().flat_map(|reader| reader.supported_formats()) {
            if !supported_formarts.contains(format) {
                supported_formarts.push(format.clone());
            }
        }
        Self {
            readers,
            supported_formarts,
        }
    }
}

impl CkanReader for MultipleReader<'_> {
    fn supported_formats(&self) -> &[String] {
        &self.supported_formarts
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        let mut last_failure = None;
        for reader in &self.readers {
            if !reader.can_read(resource) {
                continue;
            }
            match reader.read(resource) {
                Ok(result) => {
                    return Ok(result);
                }
                Err(error) => {
                    info!(
                        "reader {} failed to read CKAN resource {}: {}",
                        reader.reader_name(),
                        resource.id,
                        error.error
                    );
                    last_failure = Some(error);
                }
            };
        }
        if let Some(failure) = last_failure {
            return Err(failure);
        }
        let error = format!(
            "no reader could read CKAN resource {} (unsupported format)",
            resource.id
        );
        Err(FailedResult::from_string(
            &error,
            self.reader_name().to_string(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::parquet_output::ParquetOutput;
    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use super::*;

    struct TestReader {
        formats: Vec<String>,
        fails: bool,
        returns_data: bool,
    }

    fn output(batches: &[RecordBatch]) -> ParquetOutput {
        let mut output = ParquetOutput::try_new(&batches[0]).expect("valid Parquet output");
        for batch in batches {
            output.write(batch).expect("write Parquet batch");
        }
        output.finish().expect("finish Parquet output");
        output
    }

    impl TestReader {
        fn new(formats: &[&str], fails: bool) -> Self {
            Self {
                formats: formats.iter().map(|format| (*format).to_string()).collect(),
                fails,
                returns_data: false,
            }
        }

        fn with_data(formats: &[&str]) -> Self {
            Self {
                formats: formats.iter().map(|format| (*format).to_string()).collect(),
                fails: false,
                returns_data: true,
            }
        }
    }

    impl CkanReader for TestReader {
        fn supported_formats(&self) -> &[String] {
            &self.formats
        }

        fn do_read(&self, _resource: &CkanResource) -> ReadResult {
            if self.fails {
                Err(FailedResult::from_string(
                    "test reader failed",
                    self.reader_name().to_string(),
                ))
            } else {
                let data = if self.returns_data {
                    let schema = Arc::new(Schema::new(vec![Field::new(
                        "value",
                        DataType::Utf8,
                        false,
                    )]));
                    vec![RecordBatch::try_new(
                        schema,
                        vec![Arc::new(StringArray::from(vec!["value"])) as ArrayRef],
                    )
                    .expect("valid test batch")]
                } else {
                    let schema =
                        Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
                    vec![RecordBatch::try_new(
                        schema,
                        vec![Arc::new(StringArray::from(Vec::<String>::new())) as ArrayRef],
                    )
                    .expect("valid empty test batch")]
                };
                Ok(crate::readers::ckan_reader::SuccessResult::new(
                    output(&data),
                    self.reader_name().to_string(),
                ))
            }
        }
    }

    struct CannotReadReader;

    impl CkanReader for CannotReadReader {
        fn supported_formats(&self) -> &[String] {
            static FORMATS: [String; 1] = [String::new()];
            &FORMATS
        }

        fn can_read(&self, _resource: &CkanResource) -> bool {
            false
        }

        fn do_read(&self, _resource: &CkanResource) -> ReadResult {
            panic!("do_read must not be called when can_read returns false");
        }
    }

    fn resource(format: &str) -> CkanResource {
        CkanResource {
            id: "resource-id".to_string(),
            package_id: String::new(),
            url: "https://example.test/resource".to_string(),
            format: format.to_string(),
            datastore_active: false,
        }
    }

    #[test]
    fn supported_formats_are_unique_in_first_seen_order() {
        let reader = MultipleReader::new(vec![
            Box::new(TestReader::new(&["CSV", "JSON"], false)),
            Box::new(TestReader::new(&["CSV", "PDF"], false)),
        ]);

        assert_eq!(reader.supported_formats(), &["CSV", "JSON", "PDF"]);
    }

    #[test]
    fn falls_back_to_the_next_reader_after_a_failure() {
        let reader = MultipleReader::new(vec![
            Box::new(TestReader::new(&["CSV"], true)),
            Box::new(TestReader::with_data(&["CSV"])),
        ]);

        assert!(reader.read(&resource("CSV")).is_ok());
    }

    #[test]
    fn falls_back_to_the_next_reader_after_empty_data() {
        let reader = MultipleReader::new(vec![
            Box::new(TestReader::new(&["CSV"], false)),
            Box::new(TestReader::with_data(&["CSV"])),
        ]);

        assert!(reader.read(&resource("CSV")).is_ok());
    }

    #[test]
    fn treats_an_empty_success_as_a_failure_for_reader_fallback() {
        let reader = TestReader::new(&["CSV"], false);

        let result = reader.read(&resource("CSV"));

        match result {
            Err(error) => assert_eq!(error.to_string(), "No data"),
            Ok(_) => panic!("an empty reader result must be treated as a failure"),
        }
    }

    #[test]
    fn skips_readers_that_cannot_read_the_resource() {
        let reader = MultipleReader::new(vec![
            Box::new(CannotReadReader),
            Box::new(TestReader::with_data(&["CSV"])),
        ]);

        assert!(reader.read(&resource("CSV")).is_ok());
    }

    #[test]
    fn rejects_an_unsupported_format_before_trying_readers() {
        let reader = MultipleReader::new(vec![Box::new(TestReader::new(&["CSV"], false))]);

        assert!(reader.read(&resource("PDF")).is_err());
    }
}
