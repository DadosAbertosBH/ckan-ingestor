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
    collections::HashSet,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use futures::{Stream, StreamExt};
use regex::Regex;
use reqwest::{blocking::Client, header::CONTENT_TYPE};
use serde::Deserialize;
use url::Url;

use crate::{
    ckan_resource::CkanResource,
    readers::ckan_reader::{CkanReader, ReadResult, SuccessResult},
    readers::{
        crawler_aggregate::SchemaAccumulator,
        crawler_archive::{matches_format, visit_zip, ArchiveLimits},
    },
};
use source_io::{DownloadedSource, MediaType, Source, SourceStorage, SourceVersion, TemporaryPath};

const MAX_PAGE_BYTES: u64 = 8 * 1024 * 1024;
const FOLDER_MIME: &str = "application/vnd.google-apps.folder";
const MAX_HTML_DEPTH: usize = 4;
const MAX_HTML_PAGES: usize = 10_000;

#[derive(Debug)]
pub struct CrawlError(pub anyhow::Error);

#[derive(Debug, Clone)]
pub struct DiscoveredChild {
    pub resource_id: String,
    pub url: String,
    pub source_label: String,
    pub format: String,
    pub last_modified: String,
}

#[derive(Debug)]
pub struct CrawlFanout(pub Vec<DiscoveredChild>);

impl std::fmt::Display for CrawlFanout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} child jobs discovered", self.0.len())
    }
}
impl std::error::Error for CrawlFanout {}

impl std::fmt::Display for CrawlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "external resource crawl failed: {:#}", self.0)
    }
}
impl std::error::Error for CrawlError {}

#[derive(Clone)]
struct DriveLink {
    id: String,
    resource_key: Option<String>,
    folder: bool,
}

#[derive(Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DriveFile {
    id: String,
    name: String,
    mime_type: String,
    modified_time: Option<String>,
    resource_key: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DrivePage {
    files: Vec<DriveFile>,
    next_page_token: Option<String>,
    #[serde(default)]
    incomplete_search: bool,
}

struct CopiedSource {
    resource: CkanResource,
    source_label: String,
    public_url: String,
    _downloaded: Option<DownloadedSource>,
}

#[derive(Clone)]
enum CrawledSource {
    Http { url: String },
    Zip { url: String },
    Drive(DriveFile),
}

impl CrawledSource {
    fn log_label(&self) -> String {
        match self {
            Self::Http { url } => format!("http:{url}"),
            Self::Zip { url } => format!("zip:{url}"),
            Self::Drive(file) => format!("drive:{}/{}", file.id, file.name),
        }
    }
}

/// Discovers files linked by HTML pages, ZIP archives, and public Drive folders,
/// then delegates each materialized file to the configured readers.
pub struct ExternalResourceCrawlerReader<'a> {
    client: Client,
    storage: &'a SourceStorage,
    readers: Vec<Box<dyn CkanReader + 'a>>,
    api_key: String,
    api_base: String,
    formats: Vec<String>,
}

impl<'a> ExternalResourceCrawlerReader<'a> {
    pub fn new(
        client: Client,
        storage: &'a SourceStorage,
        readers: Vec<Box<dyn CkanReader + 'a>>,
        api_key: String,
    ) -> Self {
        let mut formats = Vec::new();
        for format in readers.iter().flat_map(|reader| reader.supported_formats()) {
            if !format.eq_ignore_ascii_case("HTML") && !formats.contains(format) {
                formats.push(format.clone());
            }
        }
        Self {
            client,
            storage,
            readers,
            api_key,
            api_base: "https://www.googleapis.com/drive/v3".into(),
            formats,
        }
    }

    #[cfg(test)]
    fn with_api_base(mut self, base: String) -> Self {
        self.api_base = base;
        self
    }

    fn get_file(&self, link: &DriveLink) -> Result<DriveFile> {
        let mut request = self
            .client
            .get(format!("{}/files/{}", self.api_base, link.id))
            .header("x-goog-api-key", &self.api_key)
            .query(&[
                ("fields", "id,name,mimeType,modifiedTime,resourceKey"),
                ("supportsAllDrives", "true"),
            ]);
        if let Some(key) = &link.resource_key {
            request = request.header("x-goog-drive-resource-keys", format!("{}/{}", link.id, key));
        }
        Ok(request.send()?.error_for_status()?.json()?)
    }

    fn list_folder(&self, folder: &DriveLink) -> Result<Vec<DriveFile>> {
        let mut files = Vec::new();
        let mut token = None::<String>;
        loop {
            let mut request = self.client.get(format!("{}/files", self.api_base))
                .header("x-goog-api-key", &self.api_key)
                .query(&[
                    ("q", format!("'{}' in parents and trashed = false", folder.id)),
                    ("fields", "nextPageToken,incompleteSearch,files(id,name,mimeType,modifiedTime,resourceKey)".into()),
                    ("pageSize", "1000".into()),
                    ("supportsAllDrives", "true".into()),
                    ("includeItemsFromAllDrives", "true".into()),
                ]);
            if let Some(value) = &token {
                request = request.query(&[("pageToken", value)]);
            }
            if let Some(key) = &folder.resource_key {
                request = request.header(
                    "x-goog-drive-resource-keys",
                    format!("{}/{}", folder.id, key),
                );
            }
            let page: DrivePage = request.send()?.error_for_status()?.json()?;
            anyhow::ensure!(
                !page.incomplete_search,
                "incomplete Drive folder listing: {}",
                folder.id
            );
            files.extend(page.files);
            token = page.next_page_token;
            if token.is_none() {
                return Ok(files);
            }
        }
    }

    fn crawl_delegated(
        &self,
        resource: &CkanResource,
        response: impl Read,
    ) -> Result<SuccessResult> {
        let mut bytes = Vec::new();
        response.take(MAX_PAGE_BYTES + 1).read_to_end(&mut bytes)?;
        anyhow::ensure!(
            bytes.len() as u64 <= MAX_PAGE_BYTES,
            "HTML page exceeds crawl limit"
        );
        let html = String::from_utf8(bytes).context("HTML page is not UTF-8")?;
        let sources = self.discover_sources(&resource.url, &resource.format, &html)?;
        log::info!(
            "External crawler discovered {} source(s) for resource {}: {}",
            sources.len(),
            resource.id,
            sources
                .iter()
                .map(CrawledSource::log_label)
                .collect::<Vec<_>>()
                .join(", ")
        );
        anyhow::ensure!(
            !sources.is_empty(),
            "no matching files or ZIP archives found"
        );
        let mut materialized = Vec::new();
        let mut copied =
            Box::pin(self.copy_sources(resource, &sources, ArchiveLimits::from_env()?));
        while let Some(source) = futures::executor::block_on(copied.next()) {
            materialized.push(source?);
        }
        anyhow::ensure!(!materialized.is_empty(), "no matching files found");
        if materialized.len() > 1 {
            materialized.sort_by(|left, right| left.resource.id.cmp(&right.resource.id));
            return Err(CrawlFanout(
                materialized
                    .iter()
                    .map(|source| DiscoveredChild {
                        resource_id: source.resource.id.clone(),
                        url: source.public_url.clone(),
                        source_label: source.source_label.clone(),
                        format: source.resource.format.clone(),
                        last_modified: source.resource.last_modified.clone(),
                    })
                    .collect(),
            )
            .into());
        }
        let mut schemas = SchemaAccumulator::new();
        let mut results = Vec::new();
        for source in &materialized {
            let result = self.read_child(&source.resource)?;
            schemas.add(&result)?;
            results.push((result, source.source_label.clone()));
        }
        let format = resource.format.to_ascii_uppercase();
        let tabular = format.contains("CSV") || format.contains("JSON");
        let mut output = schemas.build(tabular)?;
        for (result, source) in &results {
            output.append(result, source)?;
        }
        let parquet = output.finish()?;
        Ok(SuccessResult::new(parquet, self.reader_name().into()))
    }

    fn discover_sources(
        &self,
        page_url: &str,
        format: &str,
        html: &str,
    ) -> Result<Vec<CrawledSource>> {
        let root = Url::parse(page_url)?;
        let root_prefix = directory_prefix(root.path());
        let mut pages = vec![(root.clone(), Some(html.to_owned()), 0_usize)];
        let mut visited_pages = HashSet::new();
        let mut visited_sources = HashSet::new();
        let mut sources = Vec::new();
        while let Some((page, document, depth)) = pages.pop() {
            if !visited_pages.insert(page.as_str().to_owned()) {
                continue;
            }
            anyhow::ensure!(
                visited_pages.len() <= MAX_HTML_PAGES,
                "HTML page count exceeds crawl limit"
            );
            let document = match document {
                Some(document) => document,
                None => {
                    let response = self.client.get(page.clone()).send()?.error_for_status()?;
                    if !is_html(
                        response
                            .headers()
                            .get(CONTENT_TYPE)
                            .and_then(|value| value.to_str().ok()),
                    ) {
                        continue;
                    }
                    limited_html(response)?
                }
            };
            for url in extract_http_links(&document, &page)? {
                if url.host_str() == Some("drive.google.com") {
                    continue;
                }
                let name = url
                    .path_segments()
                    .and_then(Iterator::last)
                    .unwrap_or_default()
                    .to_owned();
                let lower = name.to_ascii_lowercase();
                if lower.ends_with(".zip") {
                    if visited_sources.insert(url.as_str().to_owned()) {
                        sources.push(CrawledSource::Zip { url: url.into() });
                    }
                } else if matches_format(&name, format) {
                    if visited_sources.insert(url.as_str().to_owned()) {
                        sources.push(CrawledSource::Http { url: url.into() });
                    }
                } else if depth < MAX_HTML_DEPTH
                    && url.host_str() == root.host_str()
                    && within_root_path(url.path(), root_prefix)
                    && looks_like_directory(&url)
                {
                    pages.push((url, None, depth + 1));
                }
            }
            for file in self.collect_drive_files(&document, page.as_str(), format)? {
                let key = format!("drive:{}", file.id);
                if visited_sources.insert(key) {
                    sources.push(CrawledSource::Drive(file));
                }
            }
        }
        Ok(sources)
    }

    fn collect_drive_files(
        &self,
        html: &str,
        page_url: &str,
        format: &str,
    ) -> Result<Vec<DriveFile>> {
        let mut pending = extract_drive_links(html, page_url)?;
        if pending.is_empty() {
            return Ok(Vec::new());
        }
        anyhow::ensure!(!self.api_key.is_empty(), "GOOGLE_DRIVE_API_KEY is required");
        let mut folders = HashSet::new();
        let mut files = HashSet::new();
        let mut found = Vec::new();
        while let Some(link) = pending.pop() {
            if link.folder {
                if !folders.insert(link.id.clone()) {
                    continue;
                }
                for child in self.list_folder(&link)? {
                    if child.mime_type == FOLDER_MIME {
                        pending.push(DriveLink {
                            id: child.id,
                            resource_key: child.resource_key,
                            folder: true,
                        });
                    } else if matches_format(&child.name, format) && files.insert(child.id.clone())
                    {
                        found.push(child);
                    }
                }
            } else {
                let file = self.get_file(&link)?;
                if file.mime_type == FOLDER_MIME {
                    pending.push(DriveLink {
                        folder: true,
                        ..link
                    });
                } else if matches_format(&file.name, format) && files.insert(file.id.clone()) {
                    found.push(file);
                }
            }
        }
        Ok(found)
    }

    fn copy_sources<'b>(
        &'b self,
        resource: &'b CkanResource,
        sources: &'b [CrawledSource],
        limits: ArchiveLimits,
    ) -> impl Stream<Item = Result<CopiedSource>> + 'b {
        async_stream::stream! {
            for source in sources {
                match self.copy_source(resource, source, limits) {
                    Ok(copied) => {
                        for source in copied {
                            yield Ok(source);
                        }
                    }
                    Err(error) => {
                        yield Err(error);
                        return;
                    }
                }
            }
        }
    }

    fn copy_source(
        &self,
        resource: &CkanResource,
        source: &CrawledSource,
        limits: ArchiveLimits,
    ) -> Result<Vec<CopiedSource>> {
        let mut copied = Vec::new();
        match source {
            CrawledSource::Http { url } => {
                let source = self
                    .storage
                    .inspect(Source::from_url_with_format(url, &resource.format));
                if let Some(cached_url) = self.storage.cached_crawled_url(&source)? {
                    copied.push(CopiedSource {
                        resource: child_resource(
                            resource,
                            cached_url.clone(),
                            &source.identity,
                            source.version.as_str(),
                        ),
                        source_label: url.clone(),
                        public_url: cached_url,
                        _downloaded: None,
                    });
                    return Ok(copied);
                }
                let downloaded = self.storage.download(source)?;
                let public_url = self.storage.store_crawled(&downloaded)?;
                copied.push(CopiedSource {
                    resource: child_resource(
                        resource,
                        downloaded.path.to_string_lossy().into_owned(),
                        &downloaded.source.identity,
                        downloaded.source.version.as_str(),
                    ),
                    source_label: url.clone(),
                    public_url,
                    _downloaded: Some(downloaded),
                });
            }
            CrawledSource::Drive(file) => {
                let modified_time = file
                    .modified_time
                    .as_deref()
                    .context("Drive file missing modifiedTime")?;
                let source = Source::new(
                    format!("drive://{}", file.id),
                    format!("{}/files/{}?alt=media", self.api_base, file.id),
                    &file.name,
                    SourceVersion::known(modified_time),
                );
                if let Some(cached_url) = self.storage.cached_crawled_url(&source)? {
                    copied.push(CopiedSource {
                        resource: child_resource(
                            resource,
                            cached_url.clone(),
                            &source.identity,
                            source.version.as_str(),
                        ),
                        source_label: format!("drive://{}/{}", file.id, file.name),
                        public_url: cached_url,
                        _downloaded: None,
                    });
                    return Ok(copied);
                }
                let downloaded = self.download_drive_file(file, source)?;
                let public_url = self.storage.store_crawled(&downloaded)?;
                copied.push(CopiedSource {
                    resource: child_resource(
                        resource,
                        downloaded.path.to_string_lossy().into_owned(),
                        &downloaded.source.identity,
                        downloaded.source.version.as_str(),
                    ),
                    source_label: format!("drive://{}/{}", file.id, file.name),
                    public_url,
                    _downloaded: Some(downloaded),
                });
            }
            CrawledSource::Zip { url } => {
                let path = download_zip_to_temp(&self.client, url, limits.max_zip_bytes)?;
                let _cleanup = TemporaryPath::from_path(path.clone());
                visit_zip(
                    Path::new(&path),
                    url,
                    &resource.format,
                    limits,
                    &mut |member, source_name, modified| {
                        let mut source =
                            Source::from_url_with_format(source_name, &resource.format);
                        source.version = source_version(modified.map(str::to_owned));
                        if let Some(cached_url) = self.storage.cached_crawled_url(&source)? {
                            copied.push(CopiedSource {
                                resource: child_resource(
                                    resource,
                                    cached_url.clone(),
                                    &source.identity,
                                    source.version.as_str(),
                                ),
                                source_label: source_name.to_owned(),
                                public_url: cached_url,
                                _downloaded: None,
                            });
                            return Ok(());
                        }
                        let retained = source.temporary_path();
                        std::fs::rename(member, &retained)?;
                        let downloaded = DownloadedSource::from_path(
                            source,
                            retained,
                            MediaType::known(content_type_for_format(&resource.format)),
                        );
                        let public_url = self.storage.store_crawled(&downloaded)?;
                        copied.push(CopiedSource {
                            resource: child_resource(
                                resource,
                                downloaded.path.to_string_lossy().into_owned(),
                                &downloaded.source.identity,
                                downloaded.source.version.as_str(),
                            ),
                            source_label: source_name.to_owned(),
                            public_url,
                            _downloaded: Some(downloaded),
                        });
                        Ok(())
                    },
                )?;
            }
        }
        Ok(copied)
    }

    fn read_child(&self, resource: &CkanResource) -> ReadResult {
        let mut last_failure = None;
        for reader in &self.readers {
            if !reader.can_read(resource) {
                continue;
            }
            match reader.read(resource) {
                Ok(result) => return Ok(result),
                Err(error) => last_failure = Some(error),
            }
        }
        Err(last_failure.unwrap_or_else(|| {
            crate::readers::ckan_reader::FailedResult::from_string(
                &format!("no delegated reader supports {}", resource.format),
                self.reader_name().into(),
            )
        }))
    }

    fn download_drive_file(&self, file: &DriveFile, source: Source) -> Result<DownloadedSource> {
        let path = source.temporary_path();
        let mut request = self
            .client
            .get(format!("{}/files/{}", self.api_base, file.id))
            .header("x-goog-api-key", &self.api_key)
            .query(&[("alt", "media")]);
        if let Some(key) = &file.resource_key {
            request = request.header("x-goog-drive-resource-keys", format!("{}/{}", file.id, key));
        }
        let mut response = request.send()?.error_for_status()?;
        anyhow::ensure!(
            !is_html(
                response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok())
            ),
            "Drive returned HTML for {}",
            file.id
        );
        let mut output = std::fs::File::create(&path)?;
        std::io::copy(&mut response, &mut output)?;
        Ok(DownloadedSource::from_path(
            source,
            path,
            MediaType::known(file.mime_type.clone()),
        ))
    }
}

fn child_resource(
    parent: &CkanResource,
    url: String,
    source_identity: &str,
    last_modified: Option<&str>,
) -> CkanResource {
    CkanResource {
        id: uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, source_identity.as_bytes()).to_string(),
        url,
        last_modified: last_modified.unwrap_or_default().to_owned(),
        ..parent.clone()
    }
}

fn source_version(value: Option<String>) -> SourceVersion {
    value
        .map(SourceVersion::known)
        .unwrap_or(SourceVersion::Unknown)
}

fn content_type_for_format(format: &str) -> &'static str {
    let format = format.to_ascii_uppercase();
    if format.contains("CSV") {
        "text/csv"
    } else if format.contains("JSON") {
        "application/json"
    } else if format.contains("TIFF") || format.contains("TIF") {
        "image/tiff"
    } else if format.contains("PDF") {
        "application/pdf"
    } else {
        "application/octet-stream"
    }
}

fn download_zip_to_temp(client: &Client, url: &str, max_bytes: u64) -> Result<PathBuf> {
    let mut response = client.get(url).send()?.error_for_status()?;
    if let Some(length) = response.content_length() {
        anyhow::ensure!(length <= max_bytes, "ZIP exceeds configured size");
    }
    let path = std::env::temp_dir().join(format!("{}.zip", uuid::Uuid::new_v4()));
    let mut output = File::create(&path)?;
    let copied = std::io::copy(&mut response.by_ref().take(max_bytes + 1), &mut output)?;
    anyhow::ensure!(copied <= max_bytes, "ZIP exceeds configured size");
    output.flush()?;
    Ok(path)
}

fn is_html(content_type: Option<&str>) -> bool {
    content_type.is_some_and(|value| {
        let media_type = value.split(';').next().unwrap_or_default().trim();
        media_type.eq_ignore_ascii_case("text/html")
            || media_type.eq_ignore_ascii_case("application/xhtml+xml")
    })
}

fn limited_html(mut response: reqwest::blocking::Response) -> Result<String> {
    let mut bytes = Vec::new();
    response
        .by_ref()
        .take(MAX_PAGE_BYTES + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(
        bytes.len() as u64 <= MAX_PAGE_BYTES,
        "HTML page exceeds crawl limit"
    );
    String::from_utf8(bytes).context("HTML page is not UTF-8")
}

fn directory_prefix(path: &str) -> &str {
    if path.ends_with('/') {
        path
    } else {
        path.rsplit_once('/')
            .map(|(prefix, _)| prefix)
            .unwrap_or("")
    }
}

fn within_root_path(path: &str, prefix: &str) -> bool {
    prefix == "/"
        || path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| prefix.ends_with('/') || suffix.starts_with('/'))
}

fn looks_like_directory(url: &Url) -> bool {
    url.path().ends_with('/') || Path::new(url.path()).extension().is_none()
}

fn extract_http_links(html: &str, base: &Url) -> Result<Vec<Url>> {
    let hrefs = Regex::new(r#"(?is)<a\b[^>]*\bhref\s*=\s*["']([^"']+)["']"#)?;
    let mut links = Vec::new();
    for href in hrefs.captures_iter(html) {
        let value = href[1].replace("&amp;", "&");
        let Ok(mut url) = base.join(&value) else {
            continue;
        };
        if !matches!(url.scheme(), "http" | "https") {
            continue;
        }
        url.set_fragment(None);
        links.push(url);
    }
    Ok(links)
}

fn extract_drive_links(html: &str, base: &str) -> Result<Vec<DriveLink>> {
    let base = Url::parse(base)?;
    let hrefs = Regex::new(r#"(?is)<a\b[^>]*\bhref\s*=\s*["']([^"']+)["']"#)?;
    let mut result = Vec::new();
    for href in hrefs.captures_iter(html) {
        let value = href[1].replace("&amp;", "&");
        let Ok(url) = base.join(&value) else { continue };
        if url.host_str() != Some("drive.google.com") {
            continue;
        }
        let segments: Vec<_> = url.path_segments().map(|s| s.collect()).unwrap_or_default();
        let (id, folder) = match segments.as_slice() {
            ["drive", "folders", id, ..] | ["drive", "u", _, "folders", id, ..] => (*id, true),
            ["file", "d", id, ..] => (*id, false),
            _ => {
                let Some(id) = url
                    .query_pairs()
                    .find(|(k, _)| k == "id")
                    .map(|(_, v)| v.into_owned())
                else {
                    continue;
                };
                result.push(DriveLink {
                    id,
                    folder: false,
                    resource_key: url
                        .query_pairs()
                        .find(|(k, _)| k == "resourcekey")
                        .map(|(_, v)| v.into_owned()),
                });
                continue;
            }
        };
        if id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            result.push(DriveLink {
                id: id.into(),
                folder,
                resource_key: url
                    .query_pairs()
                    .find(|(k, _)| k == "resourcekey")
                    .map(|(_, v)| v.into_owned()),
            });
        }
    }
    Ok(result)
}

impl CkanReader for ExternalResourceCrawlerReader<'_> {
    fn supported_formats(&self) -> &[String] {
        &self.formats
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        let result = (|| -> Result<SuccessResult> {
            let response = self.client.get(&resource.url).send()?.error_for_status()?;
            let is_html = is_html(
                response
                    .headers()
                    .get(CONTENT_TYPE)
                    .and_then(|value| value.to_str().ok()),
            );
            anyhow::ensure!(is_html, "resource content is not HTML");
            self.crawl_delegated(resource, response)
        })();
        result.map_err(|error| CrawlError(error).into())
    }
}

#[cfg(test)]
mod tests {
    use futures::TryStreamExt;
    use std::{
        io::{Cursor, Write},
        sync::Arc,
    };

    use object_store::{memory::InMemory, ObjectStore};

    use crate::{
        ckan_resource::CkanResource,
        readers::{
            ckan_reader::CkanReader, csv_reader::CsvReader,
            file_reference_reader::FileReferenceReader, json_reader::JsonReader,
        },
        s3_document_ingestor::S3DocumentIngestor,
    };

    use super::*;

    fn test_ingestor() -> SourceStorage {
        SourceStorage::new_with_store(
            Client::new(),
            "https://files.example/documents".into(),
            Arc::new(InMemory::new()),
        )
    }

    fn zip_bytes(entries: &[(&str, &str)]) -> Vec<u8> {
        let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
        for (name, body) in entries {
            archive
                .start_file(*name, zip::write::SimpleFileOptions::default())
                .expect("start ZIP member");
            archive
                .write_all(body.as_bytes())
                .expect("write ZIP member");
        }
        archive.finish().expect("finish ZIP").into_inner()
    }

    #[test]
    fn extracts_only_google_drive_links_and_resource_keys() {
        let html = r#"<a href="https://drive.google.com/drive/folders/folder_1?resourcekey=key1">folder</a>
            <a href='/local'>local</a><a href="https://drive.google.com/file/d/file_1/view">file</a>"#;
        let links = extract_drive_links(html, "https://example.test/page").unwrap();
        assert_eq!(links.len(), 2);
        assert_eq!(links[0].id, "folder_1");
        assert_eq!(links[0].resource_key.as_deref(), Some("key1"));
        assert!(links[0].folder);
        assert_eq!(links[1].id, "file_1");
        assert!(within_root_path("/bhgeo/files/", "/bhgeo"));
        assert!(!within_root_path("/bhgeo-other/", "/bhgeo"));
    }

    #[test]
    fn labels_every_discovered_source_kind_for_logging() {
        let sources = [
            CrawledSource::Http {
                url: "https://example.test/data.json".into(),
            },
            CrawledSource::Zip {
                url: "https://example.test/data.zip".into(),
            },
            CrawledSource::Drive(DriveFile {
                id: "drive-id".into(),
                name: "data.json".into(),
                mime_type: "application/json".into(),
                modified_time: Some("2026-01-01T00:00:00Z".into()),
                resource_key: Some("secret-resource-key".into()),
            }),
        ];

        assert_eq!(
            sources.map(|source| source.log_label()),
            [
                "http:https://example.test/data.json",
                "zip:https://example.test/data.zip",
                "drive:drive-id/data.json",
            ]
        );
    }

    #[test]
    fn crawl_error_displays_its_complete_cause_chain() {
        let error = anyhow::anyhow!("socket timed out").context("multipart upload failed");

        let message = CrawlError(error).to_string();

        assert!(message.contains("multipart upload failed"));
        assert!(message.contains("socket timed out"));
    }

    #[test]
    fn accepts_json_and_csv_resources_for_crawling() {
        let client = Client::new();
        let ingestor = test_ingestor();
        let reader = ExternalResourceCrawlerReader::new(
            client.clone(),
            &ingestor,
            vec![
                Box::new(JsonReader::with_client(client.clone())),
                Box::new(CsvReader::new(client)),
            ],
            String::new(),
        );
        for format in ["JSON", "CSV"] {
            let resource = CkanResource {
                id: "resource-id".into(),
                package_id: "package-id".into(),
                url: "https://example.test/listing/".into(),
                format: format.into(),
                datastore_active: false,
                last_modified: String::new(),
            };
            assert!(reader.can_read(&resource), "{format} should be crawled");
        }
    }

    #[test]
    fn emits_a_temporary_source_only_after_uploading_it_to_crawled_files() {
        let server = httpmock::MockServer::start();
        let _file = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/records.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"[{"id":1}]"#);
        });
        let store = Arc::new(InMemory::new());
        let ingestor = SourceStorage::new_with_store(
            Client::new(),
            "https://files.example/documents".into(),
            store.clone(),
        );
        let reader = ExternalResourceCrawlerReader::new(
            Client::new(),
            &ingestor,
            vec![Box::new(JsonReader::with_client(Client::new()))],
            String::new(),
        );
        let resource = CkanResource {
            id: "resource-id".into(),
            package_id: "package-id".into(),
            url: format!("{}/listing", server.base_url()),
            format: "JSON".into(),
            datastore_active: false,
            last_modified: String::new(),
        };
        let sources = [CrawledSource::Http {
            url: format!("{}/records.json", server.base_url()),
        }];
        let mut copied =
            Box::pin(reader.copy_sources(&resource, &sources, ArchiveLimits::default()));

        let source = futures::executor::block_on(copied.next())
            .expect("one source")
            .expect("source copy succeeds");

        assert!(Path::new(&source.resource.url).exists());
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let objects = runtime
            .block_on(store.list(None).try_collect::<Vec<_>>())
            .unwrap();
        assert_eq!(objects.len(), 1);
        assert!(objects[0].location.as_ref().starts_with("crawled-files/"));
    }

    #[test]
    fn extracts_zip_members_again_but_emits_the_cached_r2_url_when_current() {
        let server = httpmock::MockServer::start();
        let archive = zip_bytes(&[("records.json", r#"[{"id":1}]"#)]);
        let _archive = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/records.zip");
            then.status(200)
                .header("content-type", "application/zip")
                .body(archive);
        });
        let ingestor = test_ingestor();
        let reader = ExternalResourceCrawlerReader::new(
            Client::new(),
            &ingestor,
            vec![Box::new(JsonReader::with_client(Client::new()))],
            String::new(),
        );
        let resource = CkanResource {
            id: "resource-id".into(),
            package_id: "package-id".into(),
            url: format!("{}/listing", server.base_url()),
            format: "JSON".into(),
            datastore_active: false,
            last_modified: String::new(),
        };
        let sources = [CrawledSource::Zip {
            url: format!("{}/records.zip", server.base_url()),
        }];

        let first = futures::executor::block_on(
            Box::pin(reader.copy_sources(&resource, &sources, ArchiveLimits::default())).next(),
        )
        .expect("first member")
        .expect("first copy succeeds");
        let second = futures::executor::block_on(
            Box::pin(reader.copy_sources(&resource, &sources, ArchiveLimits::default())).next(),
        )
        .expect("second member")
        .expect("cached copy succeeds");

        assert!(Path::new(&first.resource.url).exists());
        assert!(second.resource.url.contains("/crawled-files/"));
        assert_eq!(_archive.hits(), 2, "ZIP is always extracted again");
    }

    #[test]
    fn returns_multiple_json_links_as_child_jobs() {
        let server = httpmock::MockServer::start();
        let _page = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/listing/");
            then.status(200)
                .header("content-type", "text/html")
                .body(r#"<a href="one.json">one</a><a href="sub/">subdirectory</a>"#);
        });
        let _subdirectory = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/listing/sub/");
            then.status(200)
                .header("content-type", "text/html")
                .body(r#"<a href="two.json">two</a>"#);
        });
        let _one = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/listing/one.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"[{"shared":1,"left":"a"}]"#);
        });
        let _two = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/listing/sub/two.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"[{"shared":"two","right":true}]"#);
        });
        let client = Client::new();
        let ingestor = test_ingestor();
        let reader = ExternalResourceCrawlerReader::new(
            client.clone(),
            &ingestor,
            vec![Box::new(JsonReader::with_client(client))],
            String::new(),
        );
        let resource = CkanResource {
            id: "resource-id".into(),
            package_id: "package-id".into(),
            url: format!("{}/listing/", server.base_url()),
            format: "JSON".into(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let failure = match reader.read(&resource) {
            Ok(_) => panic!("multiple files create child jobs"),
            Err(failure) => failure,
        };
        assert!(failure.error.to_string().contains("child jobs"));
        assert_eq!(_one.hits(), 1, "one.json must be downloaded once");
        assert_eq!(_two.hits(), 1, "two.json must be downloaded once");
    }

    #[test]
    fn extracts_every_matching_file_in_a_zip_as_child_jobs() {
        let server = httpmock::MockServer::start();
        let _page = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/listing/");
            then.status(200)
                .header("content-type", "text/html")
                .body(r#"<a href="records.zip">records</a>"#);
        });
        let body = zip_bytes(&[
            ("one.json", r#"[{"id":1}]"#),
            ("nested/two.json", r#"[{"id":2}]"#),
            ("ignored.txt", "ignored"),
        ]);
        let _archive = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/listing/records.zip");
            then.status(200)
                .header("content-type", "application/zip")
                .body(body);
        });
        let client = Client::new();
        let ingestor = test_ingestor();
        let reader = ExternalResourceCrawlerReader::new(
            client.clone(),
            &ingestor,
            vec![Box::new(JsonReader::with_client(client))],
            String::new(),
        );
        let resource = CkanResource {
            id: "resource-id".into(),
            package_id: "package-id".into(),
            url: format!("{}/listing/", server.base_url()),
            format: "JSON".into(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let failure = match reader.read(&resource) {
            Ok(_) => panic!("ZIP members become child jobs"),
            Err(failure) => failure,
        };
        let crawl = failure.error.downcast_ref::<CrawlError>().unwrap();
        let fanout = crawl.0.downcast_ref::<CrawlFanout>().unwrap();
        assert_eq!(fanout.0.len(), 2);
        assert!(fanout
            .0
            .iter()
            .all(|child| child.url.contains("/crawled-files/")));
    }

    #[test]
    fn discovers_plain_json_and_geojson_as_separate_child_jobs() {
        let server = httpmock::MockServer::start();
        let _page = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/listing/");
            then.status(200)
                .header("content-type", "text/html")
                .body(r#"<a href="plain.json">plain</a><a href="geo.geojson">geo</a>"#);
        });
        let _plain = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/listing/plain.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"[{"id":1}]"#);
        });
        let _geo = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/listing/geo.geojson");
            then.status(200)
                .header("content-type", "application/geo+json")
                .body(
                    r#"{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"id":2},"geometry":{"type":"Point","coordinates":[-43.9,-19.9]}}]}"#,
                );
        });
        let client = Client::new();
        let ingestor = test_ingestor();
        let reader = ExternalResourceCrawlerReader::new(
            client.clone(),
            &ingestor,
            vec![Box::new(JsonReader::with_client(client))],
            String::new(),
        );
        let resource = CkanResource {
            id: "resource-id".into(),
            package_id: "package-id".into(),
            url: format!("{}/listing/", server.base_url()),
            format: "JSON".into(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let error = match reader.read(&resource) {
            Ok(_) => panic!("mixed JSON kinds must fail"),
            Err(error) => error,
        };

        let crawl = error.error.downcast_ref::<CrawlError>().unwrap();
        let fanout = crawl.0.downcast_ref::<CrawlFanout>().unwrap();
        assert_eq!(fanout.0.len(), 2);
    }

    #[test]
    fn crawls_a_drive_folder_and_emits_one_tif_reference() {
        let server = httpmock::MockServer::start();
        let _page = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/page");
            then.status(200)
                .header("content-type", "text/html")
                .body(r#"<a href="https://drive.google.com/drive/folders/folder_1">folder</a>"#);
        });
        let _listing = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/drive/v3/files");
            then.status(200).json_body(serde_json::json!({
                "files": [{
                    "id": "file_1", "name": "mapa.tif", "mimeType": "image/tiff",
                    "modifiedTime": "2026-01-01T00:00:00Z"
                }]
            }));
        });
        let _download = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/drive/v3/files/file_1")
                .query_param("alt", "media");
            then.status(200)
                .header("content-type", "image/tiff")
                .body("TIFF");
        });
        let _metadata = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/drive/v3/files/file_1");
            then.status(200).json_body(serde_json::json!({
                "id": "file_1", "name": "mapa.tif", "mimeType": "image/tiff",
                "modifiedTime": "2026-01-01T00:00:00Z"
            }));
        });
        let ingestor = SourceStorage::new_with_store(
            Client::new(),
            "https://files.example/documents".into(),
            Arc::new(InMemory::new()),
        );
        let file_reference_ingestor = S3DocumentIngestor::new_with_store(
            "https://files.example/documents".into(),
            Arc::new(InMemory::new()),
        );
        let reader = ExternalResourceCrawlerReader::new(
            Client::new(),
            &ingestor,
            vec![Box::new(FileReferenceReader::new(&file_reference_ingestor))],
            "api-key".into(),
        )
        .with_api_base(format!("{}/drive/v3", server.base_url()));
        let resource = CkanResource {
            id: "resource-id".into(),
            package_id: "package-id".into(),
            url: format!("{}/page", server.base_url()),
            format: "TIF".into(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource).expect("crawl succeeds");
        assert_eq!(result.rows_processed, 1);
        assert_eq!(result.number_of_columns, 2);
        assert_eq!(result.preview[0]["name"], "mapa.tif");
    }
}
