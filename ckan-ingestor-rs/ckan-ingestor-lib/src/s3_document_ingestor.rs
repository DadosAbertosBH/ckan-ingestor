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
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path as FsPath;
use std::sync::Arc;
use std::time::Duration;
use url::Url;

const MULTIPART_UPLOAD_ATTEMPTS: usize = 3;
const MULTIPART_UPLOAD_CONCURRENCY: usize = 2;
const MULTIPART_BUFFER_BYTES: usize = 5 * 1024 * 1024;
const SINGLE_PART_UPLOAD_MAX_BYTES: u64 = 100 * 1024 * 1024;

#[derive(Debug, PartialEq, Eq)]
enum UploadStrategy {
    SinglePart,
    Multipart,
}

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
        let mut resp = client.get(download_url).send()?;
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
        let object_url = format!("docs/{filename}");
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::ContentType, content_type.to_owned().into());
        let mut staged = tempfile::NamedTempFile::new()?;
        std::io::copy(&mut resp, &mut staged)?;
        staged.flush()?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        self.upload_file_with_retry(
            &runtime,
            &Path::from(object_url.clone()),
            attributes,
            staged.path(),
        )
        .with_context(|| {
            format!(
                "error uploading document to S3 endpoint {}",
                self.public_url
            )
        })?;
        Ok(format!("{}/{}", self.public_url, object_url))
    }

    /// Stores a local file materialized by a crawler. When the upstream
    /// modification timestamp is available, an existing newer object is reused.
    pub fn ingest_path(
        &self,
        filename: &str,
        source_path: &FsPath,
        content_type: &str,
        source_modified: Option<&str>,
    ) -> Result<String> {
        let key = format!("docs/{filename}");
        self.ingest_path_at_key(&key, source_path, content_type, source_modified)
    }

    /// Returns the public URL when a crawled object is at least as recent as
    /// the upstream file. Sources without a modification timestamp are refreshed.
    pub fn current_crawled_file_url(
        &self,
        source_identity: &str,
        filename: &str,
        source_modified: Option<&str>,
    ) -> Result<Option<String>> {
        let Some(source_modified) = source_modified.filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        let key = crawled_file_key(source_identity, filename);
        let path = Path::from(key.clone());
        let public_url = format!("{}/{}", self.public_url, key);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        let source_modified = DateTime::parse_from_rfc3339(source_modified)
            .context("source modification time must be RFC 3339")?
            .with_timezone(&Utc);
        match runtime.block_on(self.store.get_opts(
            &path,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )) {
            Ok(object) if source_modified <= object.meta.last_modified => Ok(Some(public_url)),
            Ok(_) | Err(StoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Streams a materialized crawler file to the shared `crawled-files/` namespace.
    pub fn ingest_crawled_path(
        &self,
        source_identity: &str,
        filename: &str,
        source_path: &FsPath,
        content_type: &str,
        source_modified: Option<&str>,
    ) -> Result<String> {
        let key = crawled_file_key(source_identity, filename);
        self.ingest_path_at_key(&key, source_path, content_type, source_modified)
    }

    fn ingest_path_at_key(
        &self,
        key: &str,
        source_path: &FsPath,
        content_type: &str,
        source_modified: Option<&str>,
    ) -> Result<String> {
        let path = Path::from(key);
        let public_url = format!("{}/{}", self.public_url, key);
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        if let Some(url) =
            self.current_url_for_path(&runtime, &path, &public_url, source_modified)?
        {
            return Ok(url);
        }

        let mut attributes = Attributes::new();
        attributes.insert(Attribute::ContentType, content_type.to_owned().into());
        if let Some(modified) = source_modified.filter(|value| !value.is_empty()) {
            attributes.insert(
                Attribute::Metadata("source-modified-time".into()),
                modified.to_owned().into(),
            );
        }
        self.upload_file_with_retry(&runtime, &path, attributes, source_path)?;
        Ok(public_url)
    }

    fn current_url_for_path(
        &self,
        runtime: &tokio::runtime::Runtime,
        path: &Path,
        public_url: &str,
        source_modified: Option<&str>,
    ) -> Result<Option<String>> {
        let Some(modified) = source_modified.filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        let source_modified = DateTime::parse_from_rfc3339(modified)
            .context("source modification time must be RFC 3339")?
            .with_timezone(&Utc);
        match runtime.block_on(self.store.get_opts(
            path,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )) {
            Ok(object) if source_modified <= object.meta.last_modified => {
                Ok(Some(public_url.to_owned()))
            }
            Ok(_) | Err(StoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Stores a Drive file under a key shared by every CKAN resource that links to it.
    pub fn ingest_drive_file<F, R>(
        &self,
        file_id: &str,
        modified_time: &str,
        content_type: &str,
        mut download: F,
    ) -> Result<String>
    where
        F: FnMut() -> Result<R>,
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

        let mut attributes = Attributes::new();
        attributes.insert(Attribute::ContentType, content_type.to_owned().into());
        attributes.insert(
            Attribute::Metadata("drive-modified-time".into()),
            modified_time.to_owned().into(),
        );
        let mut staged = tempfile::NamedTempFile::new()?;
        std::io::copy(&mut download()?, &mut staged)?;
        staged.flush()?;
        self.upload_file_with_retry(&runtime, &path, attributes, staged.path())?;
        Ok(public_url)
    }

    fn upload_file_with_retry(
        &self,
        runtime: &tokio::runtime::Runtime,
        path: &Path,
        attributes: Attributes,
        source_path: &FsPath,
    ) -> Result<()> {
        match upload_strategy(source_path.metadata()?.len()) {
            UploadStrategy::SinglePart => {
                let payload: bytes::Bytes = std::fs::read(source_path)?.into();
                retry_operation(
                    MULTIPART_UPLOAD_ATTEMPTS,
                    || {
                        runtime
                            .block_on(self.store.put_opts(
                                path,
                                payload.clone().into(),
                                PutOptions {
                                    attributes: attributes.clone(),
                                    ..Default::default()
                                },
                            ))
                            .map(|_| ())
                            .map_err(Into::into)
                    },
                    std::thread::sleep,
                )
            }
            UploadStrategy::Multipart => retry_operation(
                MULTIPART_UPLOAD_ATTEMPTS,
                || {
                    self.upload_multipart_once(
                        runtime,
                        path,
                        attributes.clone(),
                        File::open(source_path)?,
                    )
                },
                std::thread::sleep,
            ),
        }
    }

    fn upload_multipart_once<R: Read>(
        &self,
        runtime: &tokio::runtime::Runtime,
        path: &Path,
        attributes: Attributes,
        mut input: R,
    ) -> Result<()> {
        let upload = runtime.block_on(self.store.put_multipart_opts(
            path,
            PutMultipartOptions {
                attributes,
                ..Default::default()
            },
        ))?;
        let mut writer = WriteMultipart::new(upload);
        let mut buffer = vec![0; MULTIPART_BUFFER_BYTES];
        loop {
            let read = match input.read(&mut buffer) {
                Ok(read) => read,
                Err(error) => {
                    abort_upload(runtime, writer);
                    return Err(error.into());
                }
            };
            if read == 0 {
                break;
            }
            writer.write(&buffer[..read]);
            if let Err(error) =
                runtime.block_on(writer.wait_for_capacity(MULTIPART_UPLOAD_CONCURRENCY))
            {
                abort_upload(runtime, writer);
                return Err(error.into());
            }
        }
        if let Err(error) = runtime.block_on(writer.wait_for_capacity(0)) {
            abort_upload(runtime, writer);
            return Err(error.into());
        }
        runtime.block_on(writer.finish())?;
        Ok(())
    }
}

fn upload_strategy(file_size: u64) -> UploadStrategy {
    if file_size < SINGLE_PART_UPLOAD_MAX_BYTES {
        UploadStrategy::SinglePart
    } else {
        UploadStrategy::Multipart
    }
}

fn crawled_file_key(source_identity: &str, filename: &str) -> String {
    let source_id = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, source_identity.as_bytes());
    let filename = FsPath::new(filename)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("source");
    let filename: String = filename
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '.' | '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect();
    format!("crawled-files/{source_id}/{filename}")
}

fn abort_upload(runtime: &tokio::runtime::Runtime, writer: WriteMultipart) {
    if let Err(error) = runtime.block_on(writer.abort()) {
        log::warn!("Failed to abort multipart upload after an error: {error:#}");
    }
}

fn retry_operation<T, F, S>(attempts: usize, mut operation: F, mut sleep: S) -> Result<T>
where
    F: FnMut() -> Result<T>,
    S: FnMut(Duration),
{
    anyhow::ensure!(attempts > 0, "retry attempts must be positive");
    for attempt in 1..=attempts {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if attempt < attempts => {
                let delay = Duration::from_secs(1 << (attempt - 1));
                log::warn!(
                    "Object upload attempt {attempt}/{attempts} failed; retrying in {}s: {error:#}",
                    delay.as_secs()
                );
                sleep(delay);
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!("positive attempt count always returns")
}

fn is_html_content_type(content_type: &str) -> bool {
    let media_type = content_type.split(';').next().unwrap_or_default().trim();
    media_type.eq_ignore_ascii_case("text/html")
        || media_type.eq_ignore_ascii_case("application/xhtml+xml")
}

#[cfg(test)]
mod tests {
    use std::{
        cell::{Cell, RefCell},
        io::Cursor,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use crate::config::S3Settings;
    use object_store::{memory::InMemory, path::Path, Attribute, ObjectStoreExt};

    use super::{
        retry_operation, upload_strategy, S3DocumentIngestor, UploadStrategy,
        SINGLE_PART_UPLOAD_MAX_BYTES,
    };

    #[test]
    fn uses_single_part_below_one_hundred_mebibytes() {
        assert_eq!(upload_strategy(0), UploadStrategy::SinglePart);
        assert_eq!(
            upload_strategy(SINGLE_PART_UPLOAD_MAX_BYTES - 1),
            UploadStrategy::SinglePart
        );
        assert_eq!(
            upload_strategy(SINGLE_PART_UPLOAD_MAX_BYTES),
            UploadStrategy::Multipart
        );
    }

    #[test]
    fn retries_a_transient_multipart_failure_with_exponential_backoff() {
        let attempts = Cell::new(0);
        let delays = RefCell::new(Vec::new());

        let result = retry_operation(
            3,
            || {
                let attempt = attempts.get() + 1;
                attempts.set(attempt);
                if attempt < 3 {
                    anyhow::bail!("transient connection reset");
                }
                Ok("uploaded")
            },
            |delay| delays.borrow_mut().push(delay),
        )
        .unwrap();

        assert_eq!(result, "uploaded");
        assert_eq!(attempts.get(), 3);
        assert_eq!(
            delays.into_inner(),
            [
                std::time::Duration::from_secs(1),
                std::time::Duration::from_secs(2)
            ]
        );
    }

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

    #[test]
    fn reuses_a_crawled_local_file_until_its_source_revision_advances() {
        let store = Arc::new(InMemory::new());
        let ingestor = S3DocumentIngestor::new_with_store(
            "https://files.example/bucket".into(),
            store.clone(),
        );
        let source = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(source.path(), b"first").unwrap();
        ingestor
            .ingest_path(
                "stable-id.tif",
                source.path(),
                "image/tiff",
                Some("2020-01-01T00:00:00Z"),
            )
            .unwrap();
        std::fs::write(source.path(), b"second").unwrap();
        ingestor
            .ingest_path(
                "stable-id.tif",
                source.path(),
                "image/tiff",
                Some("2020-01-01T00:00:00Z"),
            )
            .unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let stored = runtime
            .block_on(store.get(&Path::from("docs/stable-id.tif")))
            .unwrap();
        assert_eq!(runtime.block_on(stored.bytes()).unwrap(), "first");

        ingestor
            .ingest_path(
                "stable-id.tif",
                source.path(),
                "image/tiff",
                Some("2099-01-01T00:00:00Z"),
            )
            .unwrap();
        let stored = runtime
            .block_on(store.get(&Path::from("docs/stable-id.tif")))
            .unwrap();
        assert_eq!(runtime.block_on(stored.bytes()).unwrap(), "second");
    }
}
