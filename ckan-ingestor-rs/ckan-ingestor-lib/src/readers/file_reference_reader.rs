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
use std::{path::Path, sync::Arc};
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
                "HTML".to_string(),
                "DOCX".to_string(),
                "MAP".to_string(),
                "PDF".to_string(),
                "TAB".to_string(),
                "TIF".to_string(),
                "TIFF".to_string(),
            ],
        }
    }
}

impl CkanReader for FileReferenceReader<'_> {
    fn supported_formats(&self) -> &[String] {
        &self.supported_formats
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        let filename = [
            resource.id.as_str(),
            resource.format.to_lowercase().as_str(),
        ]
        .join(".");
        let local_path = Path::new(&resource.url);
        let download_url = if resource
            .url
            .starts_with(&format!("{}/crawled-files/", self.ingestor.public_url))
        {
            resource.url.clone()
        } else if local_path.is_file() {
            self.ingestor.ingest_path(
                &filename,
                local_path,
                &resource.format,
                Some(&resource.last_modified),
            )?
        } else {
            self.ingestor
                .ingest(&filename, &resource.url, &resource.format)?
        };
        let source_name = url::Url::parse(&resource.url)
            .ok()
            .filter(|url| matches!(url.scheme(), "http" | "https"))
            .and_then(|url| url.path_segments()?.next_back().map(str::to_owned))
            .or_else(|| {
                local_path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .map(str::to_owned)
            })
            .unwrap_or_else(|| filename.clone());

        let schema = Arc::new(Schema::new(vec![
            Field::new("url", arrow::datatypes::DataType::Utf8, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, false),
        ]));

        let url_array = StringArray::from(vec![download_url.as_str()]);
        let name_array = StringArray::from(vec![source_name]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(url_array), Arc::new(name_array)])?;

        let mut parquet = ParquetOutput::try_new(&batch)?;
        parquet.write(&batch)?;
        parquet.finish()?;

        Ok(SuccessResult::new(parquet, self.reader_name().to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::{
        ckan_resource::CkanResource, config::S3Settings, readers::ckan_reader::CkanReader,
        s3_document_ingestor::S3DocumentIngestor,
    };
    use object_store::memory::InMemory;
    use reqwest::blocking::Client;

    use super::FileReferenceReader;

    fn client() -> Client {
        Client::builder()
            .user_agent(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0",
            )
            .build()
            .expect("valid HTTP client")
    }

    #[test]
    fn supports_file_reference_resources() {
        let ingestor =
            S3DocumentIngestor::new(S3Settings::default(), client()).expect("valid S3 settings");
        let reader = FileReferenceReader::new(&ingestor);

        assert_eq!(
            reader.supported_formats(),
            &["HTML", "DOCX", "MAP", "PDF", "TAB", "TIF", "TIFF"]
        );
    }

    #[test]
    fn reads_lowercase_file_reference_formats() {
        let ingestor =
            S3DocumentIngestor::new(S3Settings::default(), client()).expect("valid S3 settings");
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

    #[test]
    fn reuses_a_crawled_file_url_without_downloading_it_again() {
        let ingestor = S3DocumentIngestor::new_with_store(
            "https://files.example/documents".into(),
            Arc::new(InMemory::new()),
            client(),
        );
        let reader = FileReferenceReader::new(&ingestor);
        let resource = CkanResource {
            id: "child-id".into(),
            package_id: "package-id".into(),
            url: "https://files.example/documents/crawled-files/hash/mapa.tif".into(),
            format: "TIF".into(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource);
        assert!(
            result.is_ok(),
            "cached R2 reference should be read directly"
        );
    }

    #[test]
    fn rejects_html_when_a_non_html_format_is_declared() {
        let server = httpmock::MockServer::start();
        let _page = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/raster.tif");
            then.status(200)
                .header("content-type", "text/html")
                .body("<html>links</html>");
        });
        let ingestor = S3DocumentIngestor::new_with_store(
            "https://files.example/documents".into(),
            Arc::new(InMemory::new()),
            client(),
        );
        let resource = CkanResource {
            id: "resource-id".into(),
            package_id: "package-id".into(),
            url: format!("{}/raster.tif", server.base_url()),
            format: "PDF".into(),
            datastore_active: false,
            last_modified: String::new(),
        };

        assert!(FileReferenceReader::new(&ingestor).read(&resource).is_err());
    }

    #[test]
    fn downloads_documents_with_the_configured_user_agent() {
        let server = httpmock::MockServer::start();
        let _document = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/document.pdf")
                .header(
                    "user-agent",
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0",
                );
            then.status(200)
                .header("content-type", "application/pdf")
                .body("PDF");
        });
        let ingestor = S3DocumentIngestor::new_with_store(
            "https://files.example/documents".into(),
            Arc::new(InMemory::new()),
            client(),
        );
        let resource = CkanResource {
            id: "resource-id".into(),
            package_id: "package-id".into(),
            url: format!("{}/document.pdf", server.base_url()),
            format: "PDF".into(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = FileReferenceReader::new(&ingestor)
            .read(&resource)
            .expect("the configured user agent downloads the document");

        assert_eq!(result.rows_processed, 1);
        assert_eq!(
            result.preview[0]["url"],
            "https://files.example/documents/docs/resource-id.pdf"
        );
    }

    #[test]
    fn ingests_a_local_file_discovered_by_the_crawler() {
        let source = tempfile::NamedTempFile::with_suffix(".tif").expect("temporary TIFF");
        std::fs::write(source.path(), b"TIFF").expect("write TIFF");
        let ingestor = S3DocumentIngestor::new_with_store(
            "https://files.example/documents".into(),
            Arc::new(InMemory::new()),
            client(),
        );
        let resource = CkanResource {
            id: "drive-file-id".into(),
            package_id: "package-id".into(),
            url: source.path().to_string_lossy().into_owned(),
            format: "TIF".into(),
            datastore_active: false,
            last_modified: "2026-01-01T00:00:00Z".into(),
        };

        let result = FileReferenceReader::new(&ingestor)
            .read(&resource)
            .expect("local file is ingested");

        assert_eq!(result.rows_processed, 1);
        assert_eq!(result.number_of_columns, 2);
        assert_eq!(
            result.preview[0]["name"],
            source
                .path()
                .file_name()
                .unwrap()
                .to_string_lossy()
                .as_ref()
        );
    }
}
