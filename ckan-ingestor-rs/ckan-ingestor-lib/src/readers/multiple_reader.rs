use crate::ckan_resource::CkanResource;
use crate::readers::ckan_reader::{CkanReader, FailedResult, ReadResult};
use log::info;

/// Aggregate multiple types of a readers
/// into a single struct
pub struct MultipleReader {
    readers: Vec<Box<dyn CkanReader>>,
    supported_formarts: Vec<String>,
}

impl MultipleReader {
    pub fn new(readers: Vec<Box<dyn CkanReader>>) -> Self {
        let mut supported_formarts = readers
            .iter()
            .flat_map(|b| b.supported_formats())
            .cloned()
            .collect::<Vec<String>>();
        supported_formarts.dedup();
        Self {
            readers,
            supported_formarts: supported_formarts,
        }
    }
}

impl CkanReader for MultipleReader {
    fn supported_formats(&self) -> &[String] {
        return &self.supported_formarts;
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        for reader in &self.readers {
            match reader.read(resource) {
                Ok(result) => {
                    return Ok(result);
                }
                Err(e) => {
                    info!(
                        "reader {} failed to read CKAN resource {}: {}",
                        reader.reader_name(),
                        resource.id,
                        e
                    );
                }
            };
        }
        let error = format!(
            "no reader could read CKAN resource {} (unsupported format)",
            resource.id
        );
        Err(FailedResult::from_string(&error))
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
                Err(FailedResult::from_string("test reader failed"))
            } else {
                Ok(crate::readers::ckan_reader::SuccessResult::new(Vec::new()))
            }
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
    fn rejects_an_unsupported_format_before_trying_readers() {
        let reader = MultipleReader::new(vec![Box::new(TestReader::new(&["CSV"], false))]);

        assert!(reader.read(&resource("PDF")).is_err());
    }
}
