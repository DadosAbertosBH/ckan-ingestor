use crate::ckan_resource::CkanResource;
use duckdb::arrow::array::RecordBatch;

pub trait CkanReader {
    fn supported_formats(&self) -> Vec<String>;
    fn do_read(&self, resource: &CkanResource) -> anyhow::Result<Vec<RecordBatch>>;

    fn read(&self, resource: &CkanResource) -> anyhow::Result<Vec<RecordBatch>> {
        if !self.can_read(resource) {
            return Err(anyhow::anyhow!("Unsupported format"));
        }
        self.do_read(resource)
    }

    fn can_read(&self, resource: &CkanResource) -> bool {
        self.supported_formats()
            .iter()
            .any(|e| resource.format.contains(e))
    }
}
