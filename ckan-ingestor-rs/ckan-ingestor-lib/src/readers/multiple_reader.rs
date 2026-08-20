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
            supported_formarts: supported_formarts,
        }
    }
}

impl CkanReader for MultipleReader<'_> {
    fn supported_formats(&self) -> &[String] {
        return &self.supported_formarts;
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
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
                        error
                    );
                }
            };
        }
        let error = format!(
            "no reader could read CKAN resource {} (unsupported format)",
            resource.id
        );
        Err(FailedResult::from_string(&error, self.reader_name().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestReader {
        formats: Vec<String>,
        fails: bool,
    }

    impl TestReader {
        fn new(formats: &[&str], fails: bool) -> Self {
            Self {
                formats: formats.iter().map(|format| (*format).to_string()).collect(),
                fails,
            }
        }
    }

    impl CkanReader for TestReader {
        fn supported_formats(&self) -> &[String] {
            &self.formats
        }

        fn do_read(&self, _resource: &CkanResource) -> ReadResult {
            if self.fails {
                Err(FailedResult::from_string("test reader failed", self.reader_name().to_string()))
            } else {
                Ok(crate::readers::ckan_reader::SuccessResult::new(Vec::new(), self.reader_name().to_string()))
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
            Box::new(TestReader::new(&["CSV"], false)),
        ]);

        assert!(reader.read(&resource("CSV")).is_ok());
    }

    #[test]
    fn skips_readers_that_cannot_read_the_resource() {
        let reader = MultipleReader::new(vec![
            Box::new(CannotReadReader),
            Box::new(TestReader::new(&["CSV"], false)),
        ]);

        assert!(reader.read(&resource("CSV")).is_ok());
    }

    #[test]
    fn rejects_an_unsupported_format_before_trying_readers() {
        let reader = MultipleReader::new(vec![Box::new(TestReader::new(&["CSV"], false))]);

        assert!(reader.read(&resource("PDF")).is_err());
    }
}
