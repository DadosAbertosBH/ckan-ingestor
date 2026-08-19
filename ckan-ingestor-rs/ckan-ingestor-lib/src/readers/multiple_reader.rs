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
