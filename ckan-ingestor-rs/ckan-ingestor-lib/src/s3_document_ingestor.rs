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
use crate::config::S3Settings;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use object_store::{
    parse_url_opts, path::Path, Attribute, Attributes, Error as StoreError, GetOptions,
    ObjectStore, PutMultipartOptions, PutOptions, WriteMultipart,
};
use reqwest::blocking::Client as HttpClient;
use std::io::Read;
use std::sync::Arc;
use url::Url;

#[derive(Clone)]
pub struct S3DocumentIngestor {
    pub public_url: String,
    store: Arc<dyn ObjectStore>,
}

impl S3DocumentIngestor {
    pub fn new(settings: S3Settings) -> Result<Self> {
        let protocol = if settings.use_ssl { "https" } else { "http" };
        let public_url = format!("{}://{}/{}", protocol, settings.endpoint, settings.bucket);
        let uri = Url::parse(&format!("s3://{}", settings.bucket))?;
        let (store, _) = parse_url_opts(&uri, settings.object_store_options())?;
        Ok(Self {
            public_url,
            store: store.into(),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_with_store(public_url: String, store: Arc<dyn ObjectStore>) -> Self {
        Self { public_url, store }
    }

    pub fn ingest(&self, filename: &str, download_url: &str, content_type: &str) -> Result<String> {
        let client = HttpClient::new();
        let resp = client.get(download_url).send()?;
        anyhow::ensure!(
            resp.status().is_success(),
            "document download failed: {}",
            resp.status()
        );
        if !content_type.eq_ignore_ascii_case("HTML") {
            anyhow::ensure!(
                !resp
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
                    .is_some_and(is_html_content_type),
                "resource declared as {content_type} returned HTML"
            );
        }
        let bytes = resp.bytes()?;
        let object_url = format!("docs/{filename}");
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::ContentType, content_type.to_owned().into());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime
            .block_on(self.store.put_opts(
                &Path::from(object_url.clone()),
                bytes.into(),
                PutOptions {
                    attributes,
                    ..Default::default()
                },
            ))
            .with_context(|| {
                format!(
                    "error uploading document to S3 endpoint {}",
                    self.public_url
                )
            })?;
        Ok(format!("{}/{}", self.public_url, object_url))
    }

    /// Stores a Drive file under a key shared by every CKAN resource that links to it.
    pub fn ingest_drive_file<F, R>(
        &self,
        file_id: &str,
        modified_time: &str,
        content_type: &str,
        download: F,
    ) -> Result<String>
    where
        F: FnOnce() -> Result<R>,
        R: Read,
    {
        anyhow::ensure!(
            !file_id.is_empty()
                && file_id
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
            "invalid Drive file ID"
        );
        anyhow::ensure!(
            !modified_time.is_empty()
                && modified_time
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | ':' | '.' | '+')),
            "invalid Drive modification time"
        );
        let source_modified = DateTime::parse_from_rfc3339(modified_time)
            .context("Drive modifiedTime must be RFC 3339")?
            .with_timezone(&Utc);
        let key = format!("docs/google-drive/{file_id}");
        let path = Path::from(key.clone());
        let public_url = format!("{}/{}", self.public_url, key);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        match runtime.block_on(self.store.get_opts(
            &path,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )) {
            Ok(object) if source_modified <= object.meta.last_modified => return Ok(public_url),
            Ok(_) | Err(StoreError::NotFound { .. }) => {}
            Err(error) => return Err(error.into()),
        }

        let mut input = download()?;
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::ContentType, content_type.to_owned().into());
        attributes.insert(
            Attribute::Metadata("drive-modified-time".into()),
            modified_time.to_owned().into(),
        );
        let upload = runtime.block_on(self.store.put_multipart_opts(
            &path,
            PutMultipartOptions {
                attributes,
                ..Default::default()
            },
        ))?;
        let mut writer = WriteMultipart::new(upload);
        let mut buffer = vec![0; 5 * 1024 * 1024];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            writer.write(&buffer[..read]);
            runtime.block_on(writer.wait_for_capacity(2))?;
        }
        runtime.block_on(writer.finish())?;
        Ok(public_url)
    }
}

fn is_html_content_type(content_type: &str) -> bool {
    let media_type = content_type.split(';').next().unwrap_or_default().trim();
    media_type.eq_ignore_ascii_case("text/html")
        || media_type.eq_ignore_ascii_case("application/xhtml+xml")
}

#[cfg(test)]
mod tests {
    use std::{
        io::Cursor,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use crate::config::S3Settings;
    use object_store::{memory::InMemory, path::Path, Attribute, ObjectStoreExt};

    use super::S3DocumentIngestor;

    #[test]
    fn new_builds_the_public_document_base_url() {
        let settings = S3Settings {
            endpoint: "rustfs.local:9000".to_string(),
            bucket: "documents".to_string(),
            use_ssl: false,
            ..S3Settings::default()
        };

        let ingestor = S3DocumentIngestor::new(settings).expect("valid S3 settings");

        assert_eq!(ingestor.public_url, "http://rustfs.local:9000/documents");
    }

    #[test]
    fn uploads_documents_with_the_object_store_client() {
        let store = Arc::new(InMemory::new());
        let ingestor = S3DocumentIngestor::new_with_store(
            "https://files.example/documents".into(),
            store.clone(),
        );
        let server = httpmock::MockServer::start();
        let _download = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/document.pdf");
            then.status(200).body("document");
        });

        let url = ingestor
            .ingest(
                "document.pdf",
                &format!("{}/document.pdf", server.base_url()),
                "application/pdf",
            )
            .unwrap();

        assert_eq!(url, "https://files.example/documents/docs/document.pdf");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (stored, attributes) = runtime
            .block_on(async {
                let result = store.get(&Path::from("docs/document.pdf")).await?;
                let attributes = result.attributes.clone();
                Ok::<_, object_store::Error>((result.bytes().await?, attributes))
            })
            .unwrap();
        assert_eq!(stored, "document");
        assert_eq!(
            attributes.get(&Attribute::ContentType).map(AsRef::as_ref),
            Some("application/pdf")
        );
    }

    #[test]
    fn reuses_the_same_drive_revision_across_resources() {
        let store = Arc::new(InMemory::new());
        let ingestor =
            S3DocumentIngestor::new_with_store("https://files.example/bucket".into(), store);
        let downloads = AtomicUsize::new(0);
        let download = || {
            downloads.fetch_add(1, Ordering::SeqCst);
            Ok(Cursor::new(b"tiff content".to_vec()))
        };
        let first = ingestor
            .ingest_drive_file("drive-id", "2026-01-01T00:00:00Z", "image/tiff", download)
            .unwrap();
        let second = ingestor
            .ingest_drive_file("drive-id", "2026-01-01T00:00:00Z", "image/tiff", download)
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(downloads.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn replaces_a_drive_object_when_the_source_is_newer_than_its_upload() {
        let store = Arc::new(InMemory::new());
        let ingestor =
            S3DocumentIngestor::new_with_store("https://files.example/bucket".into(), store);
        let downloads = AtomicUsize::new(0);
        let download = || {
            downloads.fetch_add(1, Ordering::SeqCst);
            Ok(Cursor::new(b"tiff content".to_vec()))
        };
        let first = ingestor
            .ingest_drive_file("drive-id", "2020-01-01T00:00:00Z", "image/tiff", download)
            .unwrap();
        let second = ingestor
            .ingest_drive_file("drive-id", "2099-01-01T00:00:00Z", "image/tiff", download)
            .unwrap();
        assert_eq!(first, second);
        assert_eq!(downloads.load(Ordering::SeqCst), 2);
    }
}
