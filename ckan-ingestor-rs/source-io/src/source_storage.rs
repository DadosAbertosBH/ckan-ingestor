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
use std::{
    fs::File,
    io::{self, Read, Write},
    path::Path as FsPath,
    sync::Arc,
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use object_store::{
    Attribute, Attributes, Error as StoreError, GetOptions, ObjectStore, PutMultipartOptions,
    PutOptions, WriteMultipart, parse_url_opts, path::Path,
};
use reqwest::{
    blocking::Client,
    header::{CONTENT_ENCODING, CONTENT_TYPE, LAST_MODIFIED},
};
use url::Url;

use crate::{DownloadedSource, HttpStatusError, MediaType, S3Settings, Source, SourceVersion};

const RETRIES: usize = 3;
const MULTIPART_CONCURRENCY: usize = 2;
const BUFFER_BYTES: usize = 5 * 1024 * 1024;
const SINGLE_PART_MAX_BYTES: u64 = 100 * 1024 * 1024;

#[derive(Clone)]
pub struct SourceStorage {
    client: Client,
    pub public_url: String,
    store: Arc<dyn ObjectStore>,
}

impl SourceStorage {
    pub fn new(client: Client, settings: S3Settings) -> Result<Self> {
        let public_url = format!(
            "{}://{}/{}",
            if settings.use_ssl { "https" } else { "http" },
            settings.endpoint,
            settings.bucket
        );
        let (store, _) = parse_url_opts(
            &Url::parse(&format!("s3://{}", settings.bucket))?,
            settings.object_store_options(),
        )?;
        Ok(Self {
            client,
            public_url,
            store: store.into(),
        })
    }

    pub fn new_with_store(client: Client, public_url: String, store: Arc<dyn ObjectStore>) -> Self {
        Self {
            client,
            public_url,
            store,
        }
    }

    /// Materialises `source` using the client owned by this storage instance.
    pub fn download(&self, source: Source) -> Result<DownloadedSource> {
        download_to_temp(&self.client, source)
    }

    /// Enriches a HTTP source with its advertised modification timestamp.
    /// Sources that do not answer HEAD remain usable with their current version.
    pub fn inspect(&self, source: Source) -> Source {
        let version = self
            .client
            .head(&source.url)
            .send()
            .ok()
            .and_then(|response| response.error_for_status().ok())
            .and_then(|response| {
                response
                    .headers()
                    .get(LAST_MODIFIED)
                    .and_then(|value| value.to_str().ok())
                    .and_then(http_date_as_rfc3339)
            })
            .map(SourceVersion::known)
            .unwrap_or(source.version.clone());
        Source { version, ..source }
    }
}

pub(crate) fn download_to_temp(client: &Client, source: Source) -> Result<DownloadedSource> {
    let temp_path = temporary_path(&source.filename);
    if !is_remote(&source.url) {
        std::fs::copy(&source.url, &temp_path)?;
        return Ok(DownloadedSource::from_path(
            source,
            temp_path,
            MediaType::Unknown,
        ));
    }
    let mut response = client.get(&source.url).send()?;
    let status = response.status();
    if !status.is_success() {
        let content_type = response
            .headers()
            .get(CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<missing or invalid>");
        let content_encoding = response
            .headers()
            .get(CONTENT_ENCODING)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("<missing or invalid>");
        return Err(anyhow::Error::new(HttpStatusError::new(
            status,
            format!(
                "resource download failed with HTTP status {status} (Content-Type: {content_type}, Content-Encoding: {content_encoding}): {}",
                source.url
            ),
        )));
    }
    let response_version = response
        .headers()
        .get(LAST_MODIFIED)
        .and_then(|v| v.to_str().ok())
        .and_then(http_date_as_rfc3339)
        .map(SourceVersion::Known);
    let media_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| MediaType::known(v.to_owned()))
        .unwrap_or(MediaType::Unknown);
    let mut output = File::create(&temp_path)?;
    io::copy(&mut response, &mut output)?;
    output.flush()?;
    Ok(DownloadedSource::from_path(
        Source {
            version: response_version.unwrap_or(source.version),
            ..source
        },
        temp_path,
        media_type,
    ))
}

impl SourceStorage {
    pub fn store_document(&self, source: &DownloadedSource) -> Result<String> {
        self.store_at_key(&format!("docs/{}", source.source.filename), source)
    }

    pub fn cached_crawled_url(&self, source: &Source) -> Result<Option<String>> {
        self.current_url(&Path::from(crawled_file_key(source)), &source.version)
    }

    pub fn crawled_url(&self, source: &Source) -> String {
        format!("{}/{}", self.public_url, crawled_file_key(source))
    }

    pub fn store_crawled(&self, source: &DownloadedSource) -> Result<String> {
        self.store_at_key(&crawled_file_key(&source.source), source)
    }

    fn store_at_key(&self, key: &str, source: &DownloadedSource) -> Result<String> {
        let path = Path::from(key);
        if let Some(url) = self.current_url(&path, &source.source.version)? {
            return Ok(url);
        }
        let mut attributes = Attributes::new();
        attributes.insert(
            Attribute::ContentType,
            source
                .media_type
                .as_str()
                .unwrap_or("application/octet-stream")
                .to_owned()
                .into(),
        );
        if let Some(modified) = source
            .source
            .version
            .as_str()
            .filter(|value| !value.is_empty())
        {
            attributes.insert(
                Attribute::Metadata("source-modified-time".into()),
                modified.to_owned().into(),
            );
        }
        self.upload_path(&path, attributes, &source.path)?;
        Ok(format!("{}/{}", self.public_url, key))
    }

    fn current_url(&self, path: &Path, version: &SourceVersion) -> Result<Option<String>> {
        let Some(modified) = version.as_str().filter(|value| !value.is_empty()) else {
            return Ok(None);
        };
        let modified = DateTime::parse_from_rfc3339(modified)
            .context("source modification time must be RFC 3339")?
            .with_timezone(&Utc);
        let runtime = runtime()?;
        match runtime.block_on(self.store.get_opts(
            path,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )) {
            Ok(object) if modified <= object.meta.last_modified => {
                Ok(Some(format!("{}/{}", self.public_url, path)))
            }
            Ok(_) | Err(StoreError::NotFound { .. }) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    fn upload_path(&self, path: &Path, attributes: Attributes, source: &FsPath) -> Result<()> {
        let runtime = runtime()?;
        let size = source.metadata()?.len();
        retry(RETRIES, || {
            if size < SINGLE_PART_MAX_BYTES {
                let payload = std::fs::read(source)?;
                runtime.block_on(self.store.put_opts(
                    path,
                    payload.into(),
                    PutOptions {
                        attributes: attributes.clone(),
                        ..Default::default()
                    },
                ))?;
            } else {
                self.upload_multipart(&runtime, path, attributes.clone(), File::open(source)?)?;
            }
            Ok(())
        })
    }

    fn upload_multipart<R: Read>(
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
        let mut buffer = vec![0; BUFFER_BYTES];
        loop {
            let read = input.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            writer.write(&buffer[..read]);
            runtime.block_on(writer.wait_for_capacity(MULTIPART_CONCURRENCY))?;
        }
        runtime.block_on(writer.finish())?;
        Ok(())
    }
}

fn temporary_path(filename: &str) -> std::path::PathBuf {
    let suffix = FsPath::new(filename)
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| format!(".{value}"))
        .unwrap_or_default();
    std::env::temp_dir().join(format!("{}{}", uuid::Uuid::new_v4(), suffix))
}

fn is_remote(url: &str) -> bool {
    url.starts_with("http://") || url.starts_with("https://")
}

fn http_date_as_rfc3339(value: &str) -> Option<String> {
    chrono::DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|time| time.to_rfc3339())
}

fn runtime() -> Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}

fn retry<T, F>(attempts: usize, mut operation: F) -> Result<T>
where
    F: FnMut() -> Result<T>,
{
    for attempt in 1..=attempts {
        match operation() {
            Ok(value) => return Ok(value),
            Err(error) if attempt < attempts => {
                let delay = Duration::from_secs(1 << (attempt - 1));
                log::warn!(
                    "source upload attempt {attempt}/{attempts} failed; retrying in {}s: {error:#}",
                    delay.as_secs()
                );
                std::thread::sleep(delay);
            }
            Err(error) => return Err(error),
        }
    }
    unreachable!()
}

fn crawled_file_key(source: &Source) -> String {
    let id = uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, source.identity.as_bytes());
    let name = FsPath::new(&source.filename)
        .file_name()
        .and_then(|value| value.to_str())
        .filter(|value| !value.is_empty())
        .unwrap_or("source");
    format!("crawled-files/{id}/{name}")
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;
    use reqwest::blocking::Client;

    use crate::{Source, SourceVersion};

    use super::SourceStorage;

    #[test]
    fn crawled_cache_location_is_derived_only_from_the_source() {
        let storage = SourceStorage::new_with_store(
            Client::new(),
            "https://files.example/documents".into(),
            Arc::new(InMemory::new()),
        );
        let source = Source::new(
            "google-drive:file-1",
            "https://drive.example/download/file-1",
            "contagem.json",
            SourceVersion::known("2026-01-01T00:00:00Z"),
        );

        let url = storage.crawled_url(&source);

        assert!(url.ends_with("/contagem.json"));
        assert!(url.contains("crawled-files/"));
    }
}
