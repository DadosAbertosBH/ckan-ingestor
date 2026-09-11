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
use crate::parquet_output::ParquetOutput;
use crate::readers::ckan_reader::{CkanReader, SuccessResult};
use crate::s3_document_ingestor::S3DocumentIngestor;
use crate::{ckan_resource::CkanResource, readers::ckan_reader::ReadResult};

use arrow::{
    array::StringArray,
    datatypes::{Field, Schema},
    record_batch::RecordBatch,
};
use std::sync::Arc;
/// Stores opaque source files in S3 and exposes their download URL as a one-row dataset.
pub struct FileReferenceReader<'a> {
    ingestor: &'a S3DocumentIngestor,
    supported_formats: Vec<String>,
}

impl<'a> FileReferenceReader<'a> {
    pub fn new(ingestor: &'a S3DocumentIngestor) -> Self {
        Self {
            ingestor,
            supported_formats: vec![
                "DOCX".to_string(),
                "MAP".to_string(),
                "PDF".to_string(),
                "TAB".to_string(),
            ],
        }
    }
}

impl CkanReader for FileReferenceReader<'_> {
    fn supported_formats(&self) -> &[String] {
        &self.supported_formats
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        let download_url = self.ingestor.ingest(
            [
                resource.id.as_str(),
                resource.format.to_lowercase().as_str(),
            ]
            .join(".")
            .as_str(),
            &resource.url,
            &resource.format,
        )?;

        // Criar o schema com uma coluna 'url'
        let schema = Arc::new(Schema::new(vec![Field::new(
            "url",
            arrow::datatypes::DataType::Utf8,
            false,
        )]));

        // Criar o array com o valor do download_url
        let url_array = StringArray::from(vec![download_url.as_str()]);

        // Criar o RecordBatch
        let batch = RecordBatch::try_new(schema, vec![Arc::new(url_array)])?;

        let mut parquet = ParquetOutput::try_new(&batch)?;
        parquet.write(&batch)?;
        parquet.finish()?;

        Ok(SuccessResult::new(parquet, self.reader_name().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        ckan_resource::CkanResource, config::S3Settings, readers::ckan_reader::CkanReader,
        s3_document_ingestor::S3DocumentIngestor,
    };

    use super::FileReferenceReader;

    #[test]
    fn supports_file_reference_resources() {
        let ingestor = S3DocumentIngestor::new(S3Settings::default()).expect("valid S3 settings");
        let reader = FileReferenceReader::new(&ingestor);

        assert_eq!(reader.supported_formats(), &["DOCX", "MAP", "PDF", "TAB"]);
    }

    #[test]
    fn reads_lowercase_file_reference_formats() {
        let ingestor = S3DocumentIngestor::new(S3Settings::default()).expect("valid S3 settings");
        let reader = FileReferenceReader::new(&ingestor);
        let resource = CkanResource {
            id: "resource-id".to_string(),
            package_id: "package-id".to_string(),
            url: "https://example.test/geometry.map".to_string(),
            format: "map".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        assert!(reader.can_read(&resource));
    }
}
