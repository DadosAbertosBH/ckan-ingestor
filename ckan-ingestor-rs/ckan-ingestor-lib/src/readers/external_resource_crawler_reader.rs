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
    collections::{HashMap, HashSet, VecDeque},
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use flate2::{write::GzEncoder, Compression};
use futures::{Stream, StreamExt};
use regex::Regex;
use reqwest::{
    blocking::{Client, RequestBuilder, Response},
    header::{CONTENT_TYPE, LAST_MODIFIED},
};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::{
    ckan_resource::CkanResource,
    readers::ckan_reader::{CkanReader, ReadResult, SuccessResult},
    readers::{
        crawler_aggregate::SchemaAccumulator,
        crawler_archive::{matches_format, visit_zip, ArchiveLimits},
    },
};
use source_io::{
    CopiedSource, DownloadedSource, MediaType, Source, SourceStorage, SourceVersion, TemporaryPath,
};

const MAX_PAGE_BYTES: u64 = 8 * 1024 * 1024;
const FOLDER_MIME: &str = "application/vnd.google-apps.folder";
const MAX_HTML_DEPTH: usize = 4;
const MAX_HTML_PAGES: usize = 10_000;
const MAX_DRIVE_ERROR_BODY_BYTES: u64 = 64 * 1024;
const DISCOVERY_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Debug)]
pub struct CrawlError(pub anyhow::Error);

#[derive(Debug)]
struct DriveHttpError(String);

impl std::fmt::Display for DriveHttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for DriveHttpError {}

#[derive(Debug)]
struct WarcArchiveError(anyhow::Error);

impl std::fmt::Display for WarcArchiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WARC archive failed: {:#}", self.0)
    }
}

impl std::error::Error for WarcArchiveError {}

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

#[derive(Clone, Deserialize, Serialize)]
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

#[derive(Clone)]
struct ChildSource {
    resource: CkanResource,
    source_label: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "source_type", rename_all = "lowercase")]
enum CrawledSource {
    Http { url: String },
    Zip { url: String },
    Drive(DriveFile),
}

#[derive(Serialize, Deserialize)]
struct DiscoveryManifest {
    generation: String,
    order: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct MaterializedEntry {
    id: String,
    url: String,
    source_label: String,
    last_modified: String,
}

#[derive(Serialize, Deserialize)]
struct MaterializedSnapshot {
    entries: Vec<MaterializedEntry>,
    origin_modified: Option<String>,
    origin_etag: Option<String>,
}

struct CachedMaterialization {
    children: Vec<ChildSource>,
    snapshot: MaterializedSnapshot,
    uploaded_at: DateTime<Utc>,
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

    fn cached_discovery(
        &self,
        page_url: &str,
        format: &str,
    ) -> Result<Option<(Vec<CrawledSource>, DateTime<Utc>)>> {
        let base = discovery_key(page_url, format);
        let Some(manifest) = self
            .storage
            .get_cached_object(&format!("discovered-sources/manifest/{base}.json"))?
        else {
            return Ok(None);
        };
        let Ok(manifest_value) = serde_json::from_slice::<DiscoveryManifest>(&manifest.bytes)
        else {
            log::warn!("Invalid discovery manifest for {page_url}");
            return Ok(None);
        };
        let mut groups = HashMap::<&str, VecDeque<CrawledSource>>::new();
        for kind in ["http", "zip", "drive"] {
            let key = format!(
                "discovered-sources/{kind}/{base}/{}.json",
                manifest_value.generation
            );
            let Some(object) = self.storage.get_cached_object(&key)? else {
                log::warn!("Incomplete discovery cache for {page_url}: {key}");
                return Ok(None);
            };
            let Ok(mut group) = serde_json::from_slice::<Vec<CrawledSource>>(&object.bytes) else {
                log::warn!("Invalid discovery cache for {page_url}: {key}");
                return Ok(None);
            };
            groups.insert(kind, group.drain(..).collect());
        }
        let mut sources = Vec::new();
        for kind in &manifest_value.order {
            let Some(source) = groups.get_mut(kind.as_str()).and_then(VecDeque::pop_front) else {
                log::warn!("Invalid discovery ordering for {page_url}");
                return Ok(None);
            };
            sources.push(source);
        }
        if groups.values().any(|group| !group.is_empty()) {
            log::warn!("Incomplete discovery ordering for {page_url}");
            return Ok(None);
        }
        Ok(Some((sources, manifest.uploaded_at)))
    }

    fn store_discovery(
        &self,
        page_url: &str,
        format: &str,
        sources: &[CrawledSource],
    ) -> Result<()> {
        let base = discovery_key(page_url, format);
        let generation = uuid::Uuid::new_v4().to_string();
        for kind in ["http", "zip", "drive"] {
            let group = sources
                .iter()
                .filter(|source| source_kind(source) == kind)
                .collect::<Vec<_>>();
            self.storage.put_cached_object(
                &format!("discovered-sources/{kind}/{base}/{generation}.json"),
                serde_json::to_vec(&group)?,
                "application/json",
            )?;
        }
        self.storage.put_cached_object(
            &format!("discovered-sources/manifest/{base}.json"),
            serde_json::to_vec(&DiscoveryManifest {
                generation,
                order: sources
                    .iter()
                    .map(|source| source_kind(source).to_owned())
                    .collect(),
            })?,
            "application/json",
        )
    }

    fn load_or_discover(&self, resource: &CkanResource) -> Result<Vec<CrawledSource>> {
        let cached = self.cached_discovery(&resource.url, &resource.format)?;
        if let Some((sources, uploaded_at)) = &cached {
            match self.client.head(&resource.url).send() {
                Ok(response) if response.status().is_success() => {
                    let modified = response
                        .headers()
                        .get(LAST_MODIFIED)
                        .and_then(|value| value.to_str().ok())
                        .and_then(http_modified);
                    if modified.is_some_and(|value| value <= *uploaded_at)
                        || (modified.is_none() && cache_fresh(*uploaded_at))
                    {
                        log::debug!("Reusing discovery cache for {}", resource.url);
                        return Ok(sources.clone());
                    }
                }
                Ok(response) => {
                    log::warn!(
                        "HTML page HEAD returned {}; using cached discovery for {}",
                        response.status(),
                        resource.url
                    );
                    return Ok(sources.clone());
                }
                Err(error) => {
                    log::warn!(
                        "HTML page HEAD failed: {error}; using cached discovery for {}",
                        resource.url
                    );
                    return Ok(sources.clone());
                }
            }
        }
        let discovered = (|| -> Result<Vec<CrawledSource>> {
            let response = self.client.get(&resource.url).send()?.error_for_status()?;
            anyhow::ensure!(
                is_html(
                    response
                        .headers()
                        .get(CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok())
                ),
                "resource content is not HTML"
            );
            let html = self.archive_html(response).map_err(WarcArchiveError)?;
            let sources = self.discover_sources(&resource.url, &resource.format, &html)?;
            self.store_discovery(&resource.url, &resource.format, &sources)?;
            Ok(sources)
        })();
        match (discovered, cached) {
            (Ok(sources), _) => Ok(sources),
            (Err(error), Some((sources, _))) if is_origin_error(&error) => {
                log::warn!(
                    "Discovery origin failed: {error:#}; using cached discovery for {}",
                    resource.url
                );
                Ok(sources)
            }
            (Err(error), _) => Err(error),
        }
    }

    fn archive_html(&self, mut response: Response) -> Result<String> {
        let url = response.url().to_string();
        let status = response.status();
        let headers = response.headers().clone();
        let mut bytes = Vec::new();
        response
            .by_ref()
            .take(MAX_PAGE_BYTES + 1)
            .read_to_end(&mut bytes)?;
        anyhow::ensure!(
            bytes.len() as u64 <= MAX_PAGE_BYTES,
            "HTML page exceeds crawl limit"
        );
        let html = String::from_utf8(bytes.clone()).context("HTML page is not UTF-8")?;
        let mut http = format!("HTTP/1.1 {}\r\n", status).into_bytes();
        for (name, value) in &headers {
            if name == reqwest::header::CONTENT_LENGTH
                || name == reqwest::header::TRANSFER_ENCODING
                || name == reqwest::header::CONTENT_ENCODING
            {
                continue;
            }
            http.extend_from_slice(name.as_str().as_bytes());
            http.extend_from_slice(b": ");
            http.extend_from_slice(value.as_bytes());
            http.extend_from_slice(b"\r\n");
        }
        http.extend_from_slice(format!("content-length: {}\r\n\r\n", bytes.len()).as_bytes());
        http.extend_from_slice(&bytes);
        let record = format!(
            "WARC/1.1\r\nWARC-Type: response\r\nWARC-Target-URI: {url}\r\nWARC-Date: {}\r\nWARC-Record-ID: <urn:uuid:{}>\r\nContent-Type: application/http; msgtype=response\r\nContent-Length: {}\r\n\r\n",
            Utc::now().to_rfc3339(),
            uuid::Uuid::new_v4(),
            http.len()
        );
        let mut gzip = GzEncoder::new(Vec::new(), Compression::default());
        gzip.write_all(record.as_bytes())?;
        gzip.write_all(&http)?;
        gzip.write_all(b"\r\n\r\n")?;
        let key = format!(
            "crawled-pages/{}/{}-{}.warc.gz",
            stable_id(&url),
            Utc::now().format("%Y%m%dT%H%M%SZ"),
            uuid::Uuid::new_v4()
        );
        self.storage
            .put_cached_object(&key, gzip.finish()?, "application/warc")?;
        Ok(html)
    }

    fn cached_materialized(
        &self,
        parent: &CkanResource,
        source: &CrawledSource,
    ) -> Result<Option<CachedMaterialization>> {
        let key = materialized_key(source, &parent.format);
        let Some(object) = self.storage.get_cached_object(&key)? else {
            return Ok(None);
        };
        let Ok(snapshot) = serde_json::from_slice::<MaterializedSnapshot>(&object.bytes) else {
            log::warn!("Invalid materialized cache at {key}");
            return Ok(None);
        };
        let mut children = Vec::new();
        for entry in &snapshot.entries {
            let Some(storage_key) = entry
                .url
                .strip_prefix(&format!("{}/", self.storage.public_url))
            else {
                log::warn!("Invalid materialized object URL in {key}");
                return Ok(None);
            };
            if !self.storage.cached_object_exists(storage_key)? {
                log::warn!("Missing materialized object {storage_key}");
                return Ok(None);
            }
            children.push(ChildSource {
                resource: CkanResource {
                    id: entry.id.clone(),
                    url: entry.url.clone(),
                    last_modified: entry.last_modified.clone(),
                    ..parent.clone()
                },
                source_label: entry.source_label.clone(),
            });
        }
        Ok(Some(CachedMaterialization {
            children,
            snapshot,
            uploaded_at: object.uploaded_at,
        }))
    }

    fn store_materialized(
        &self,
        parent: &CkanResource,
        source: &CrawledSource,
        copied: &[ChildSource],
        origin_modified: Option<String>,
        origin_etag: Option<String>,
    ) -> Result<()> {
        let snapshot = MaterializedSnapshot {
            entries: copied
                .iter()
                .map(|child| MaterializedEntry {
                    id: child.resource.id.clone(),
                    url: child.resource.url.clone(),
                    source_label: child.source_label.clone(),
                    last_modified: child.resource.last_modified.clone(),
                })
                .collect(),
            origin_modified,
            origin_etag,
        };
        self.storage.put_cached_object(
            &materialized_key(source, &parent.format),
            serde_json::to_vec(&snapshot)?,
            "application/json",
        )
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
        Ok(drive_response(request)?.json()?)
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
            let page: DrivePage = drive_response(request)?.json()?;
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
        sources: &[CrawledSource],
    ) -> Result<SuccessResult> {
        log::info!(
            "External crawler discovered {} source(s) for resource {}",
            sources.len(),
            resource.id
        );
        anyhow::ensure!(
            !sources.is_empty(),
            "no matching files or ZIP archives found"
        );
        let mut materialized = Vec::new();
        let mut copied = Box::pin(self.copy_sources(resource, sources, ArchiveLimits::from_env()?));
        while let Some(source) = futures::executor::block_on(copied.next()) {
            materialized.push(source?);
        }
        log::info!(
            "External crawler materialized {} source(s) for resource {}",
            materialized.len(),
            resource.id
        );
        anyhow::ensure!(!materialized.is_empty(), "no matching files found");
        if materialized.len() > 1 {
            materialized.sort_by(|left, right| left.resource.id.cmp(&right.resource.id));
            return Err(CrawlFanout(
                materialized
                    .iter()
                    .map(|source| DiscoveredChild {
                        resource_id: source.resource.id.clone(),
                        url: source.resource.url.clone(),
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
                    self.archive_html(response).map_err(WarcArchiveError)?
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
    ) -> impl Stream<Item = Result<ChildSource>> + 'b {
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
    ) -> Result<Vec<ChildSource>> {
        let mut copied = Vec::new();
        match source {
            CrawledSource::Http { url } => {
                let source = self
                    .storage
                    .inspect(Source::from_url_with_format(url, &resource.format));
                if let Some(copied_source) = self.storage.cached_crawled(source.clone())? {
                    copied.push(child_source(resource, copied_source, url.clone()));
                    return Ok(copied);
                }
                let copied_source = self.storage.copy_crawled(self.storage.download(source)?)?;
                copied.push(child_source(resource, copied_source, url.clone()));
            }
            CrawledSource::Drive(file) => {
                let cached = self.cached_materialized(resource, source)?;
                if let Some(cached) = &cached {
                    if cache_fresh(cached.uploaded_at)
                        && cached.snapshot.origin_modified == file.modified_time
                    {
                        log::debug!(
                            "Reusing materialized Drive file {} for resource {}",
                            file.id,
                            resource.id
                        );
                        return Ok(cached.children.clone());
                    }
                }
                let fresh_file = if cached.is_some() {
                    match self.get_file(&DriveLink {
                        id: file.id.clone(),
                        resource_key: file.resource_key.clone(),
                        folder: false,
                    }) {
                        Ok(file) => file,
                        Err(error) => {
                            if let Some(cached) = cached {
                                log::warn!(
                                    "Drive metadata failed: {error:#}; using cached file {}",
                                    file.id
                                );
                                return Ok(cached.children);
                            }
                            return Err(error);
                        }
                    }
                } else {
                    file.clone()
                };
                if let Some(cached) = &cached {
                    if cached.snapshot.origin_modified == fresh_file.modified_time {
                        self.store_materialized(
                            resource,
                            source,
                            &cached.children,
                            fresh_file.modified_time.clone(),
                            None,
                        )?;
                        return Ok(cached.children.clone());
                    }
                }
                let force_copy = cached.is_some();
                let modified_time = fresh_file
                    .modified_time
                    .as_deref()
                    .context("Drive file missing modifiedTime")?;
                let source = Source::new(
                    format!("drive://{}", fresh_file.id),
                    format!("{}/files/{}?alt=media", self.api_base, fresh_file.id),
                    &fresh_file.name,
                    SourceVersion::known(modified_time),
                );
                let source_label = format!("drive://{}/{}", fresh_file.id, fresh_file.name);
                if !force_copy {
                    if let Some(copied_source) = self.storage.cached_crawled(source.clone())? {
                        log::debug!(
                            "Reusing cached google drive member {} from {}",
                            source_label,
                            copied_source.url,
                        );
                        copied.push(child_source(resource, copied_source, source_label));
                        self.store_materialized(
                            resource,
                            &CrawledSource::Drive(file.clone()),
                            &copied,
                            fresh_file.modified_time,
                            None,
                        )?;
                        return Ok(copied);
                    }
                }
                let downloaded = self.download_drive_file(&fresh_file, source);
                let downloaded = match downloaded {
                    Ok(downloaded) => downloaded,
                    Err(error) => {
                        if let Some(cached) = cached {
                            log::warn!(
                                "Drive download failed: {error:#}; using cached file {}",
                                file.id
                            );
                            return Ok(cached.children);
                        }
                        return Err(error);
                    }
                };
                let copied_source = if force_copy {
                    self.storage.copy_crawled_versioned(downloaded)?
                } else {
                    self.storage.copy_crawled(downloaded)?
                };
                log::debug!(
                    "Materialized google drive member {} from {}",
                    source_label,
                    copied_source.url,
                );
                copied.push(child_source(resource, copied_source, source_label));
                self.store_materialized(
                    resource,
                    &CrawledSource::Drive(file.clone()),
                    &copied,
                    fresh_file.modified_time,
                    None,
                )?;
            }
            CrawledSource::Zip { url } => {
                let cached = self.cached_materialized(resource, source)?;
                if let Some(cached) = &cached {
                    match self.client.head(url).send() {
                        Ok(response) if response.status().is_success() => {
                            let modified = response
                                .headers()
                                .get(LAST_MODIFIED)
                                .and_then(|value| value.to_str().ok())
                                .and_then(http_modified)
                                .map(|value| value.to_rfc3339());
                            let etag = response
                                .headers()
                                .get(reqwest::header::ETAG)
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_owned);
                            let same_version = (modified.is_some() || etag.is_some())
                                && modified.as_ref().is_none_or(|value| {
                                    cached.snapshot.origin_modified.as_ref() == Some(value)
                                })
                                && etag.as_ref().is_none_or(|value| {
                                    cached.snapshot.origin_etag.as_ref() == Some(value)
                                });
                            if same_version
                                || (modified.is_none()
                                    && etag.is_none()
                                    && cache_fresh(cached.uploaded_at))
                            {
                                log::debug!("Reusing materialized ZIP {url}");
                                return Ok(cached.children.clone());
                            }
                        }
                        Ok(response) => {
                            log::warn!(
                                "ZIP HEAD returned {}; using cached materialization for {url}",
                                response.status()
                            );
                            return Ok(cached.children.clone());
                        }
                        Err(error) => {
                            log::warn!(
                                "ZIP HEAD failed: {error}; using cached materialization for {url}"
                            );
                            return Ok(cached.children.clone());
                        }
                    }
                }
                let downloaded = match download_zip_to_temp(&self.client, url, limits.max_zip_bytes)
                {
                    Ok(downloaded) => downloaded,
                    Err(error) => {
                        if let Some(cached) = cached {
                            log::warn!("ZIP download failed: {error:#}; using cached materialization for {url}");
                            return Ok(cached.children);
                        }
                        return Err(error);
                    }
                };
                let (path, origin_modified, origin_etag) = downloaded;
                let _cleanup = TemporaryPath::from_path(path.clone());
                let zip_revision = Utc::now().to_rfc3339();
                visit_zip(
                    Path::new(&path),
                    url,
                    &resource.format,
                    limits,
                    &mut |member, source_name, _modified| {
                        let mut source =
                            Source::from_url_with_format(source_name, &resource.format);
                        source.version = SourceVersion::known(&zip_revision);
                        let retained = source.temporary_path();
                        std::fs::rename(member, &retained)?;
                        let downloaded = DownloadedSource::from_path(
                            source,
                            retained,
                            MediaType::known(content_type_for_format(&resource.format)),
                        );
                        let copied_source = self.storage.copy_crawled_versioned(downloaded)?;
                        log::debug!(
                            "Materialized ZIP member {} at {}",
                            source_name,
                            copied_source.url,
                        );
                        copied.push(child_source(
                            resource,
                            copied_source,
                            source_name.to_owned(),
                        ));
                        Ok(())
                    },
                )?;
                self.store_materialized(resource, source, &copied, origin_modified, origin_etag)?;
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
            .query(&[("alt", "media"), ("supportsAllDrives", "true")]);
        if let Some(key) = &file.resource_key {
            request = request.header("x-goog-drive-resource-keys", format!("{}/{}", file.id, key));
        }
        let mut response = drive_response(request)?;
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

fn child_source(parent: &CkanResource, copied: CopiedSource, source_label: String) -> ChildSource {
    ChildSource {
        resource: child_resource(
            parent,
            copied.url,
            &copied.source.identity,
            copied.source.version.as_str(),
        ),
        source_label,
    }
}

fn stable_id(value: &str) -> String {
    uuid::Uuid::new_v5(&uuid::Uuid::NAMESPACE_URL, value.as_bytes()).to_string()
}

fn discovery_key(page_url: &str, format: &str) -> String {
    stable_id(&format!("{page_url}\n{}", format.to_ascii_uppercase()))
}

fn source_kind(source: &CrawledSource) -> &'static str {
    match source {
        CrawledSource::Http { .. } => "http",
        CrawledSource::Zip { .. } => "zip",
        CrawledSource::Drive(_) => "drive",
    }
}

fn materialized_key(source: &CrawledSource, format: &str) -> String {
    let identity = match source {
        CrawledSource::Zip { url } => url.as_str(),
        CrawledSource::Drive(file) => file.id.as_str(),
        CrawledSource::Http { .. } => unreachable!("direct HTTP sources use the existing cache"),
    };
    format!(
        "discovered-resources/{}/{}/{}.json",
        source_kind(source),
        stable_id(format),
        stable_id(identity)
    )
}

fn cache_fresh(uploaded_at: DateTime<Utc>) -> bool {
    Utc::now().signed_duration_since(uploaded_at)
        < chrono::Duration::from_std(DISCOVERY_TTL).expect("valid cache TTL")
}

fn http_modified(value: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc2822(value)
        .ok()
        .map(|value| value.with_timezone(&Utc))
}

fn is_origin_error(error: &anyhow::Error) -> bool {
    if error.chain().any(|cause| cause.is::<WarcArchiveError>()) {
        return false;
    }
    error
        .chain()
        .any(|cause| cause.is::<reqwest::Error>() || cause.is::<DriveHttpError>())
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

fn download_zip_to_temp(
    client: &Client,
    url: &str,
    max_bytes: u64,
) -> Result<(PathBuf, Option<String>, Option<String>)> {
    let mut response = client.get(url).send()?.error_for_status()?;
    let modified = response
        .headers()
        .get(LAST_MODIFIED)
        .and_then(|value| value.to_str().ok())
        .and_then(http_modified)
        .map(|value| value.to_rfc3339());
    let etag = response
        .headers()
        .get(reqwest::header::ETAG)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    if let Some(length) = response.content_length() {
        anyhow::ensure!(length <= max_bytes, "ZIP exceeds configured size");
    }
    let path = std::env::temp_dir().join(format!("{}.zip", uuid::Uuid::new_v4()));
    let mut output = File::create(&path)?;
    let copied = std::io::copy(&mut response.by_ref().take(max_bytes + 1), &mut output)?;
    anyhow::ensure!(copied <= max_bytes, "ZIP exceeds configured size");
    output.flush()?;
    Ok((path, modified, etag))
}

fn is_html(content_type: Option<&str>) -> bool {
    content_type.is_some_and(|value| {
        let media_type = value.split(';').next().unwrap_or_default().trim();
        media_type.eq_ignore_ascii_case("text/html")
            || media_type.eq_ignore_ascii_case("application/xhtml+xml")
    })
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

fn drive_response(request: RequestBuilder) -> Result<Response> {
    let mut response = request.send()?;
    let status = response.status();
    if status.is_success() {
        return Ok(response);
    }

    let url = response.url().clone();
    let mut body = Vec::new();
    response
        .by_ref()
        .take(MAX_DRIVE_ERROR_BODY_BYTES + 1)
        .read_to_end(&mut body)
        .context("read Google Drive error response body")?;
    let truncated = body.len() as u64 > MAX_DRIVE_ERROR_BODY_BYTES;
    body.truncate(MAX_DRIVE_ERROR_BODY_BYTES as usize);
    let suffix = if truncated { " (truncated)" } else { "" };

    Err(DriveHttpError(format!(
        "Google Drive request failed with HTTP status {status} for {url}; response body{suffix}: {}",
        String::from_utf8_lossy(&body)
    ))
    .into())
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
            let sources = self.load_or_discover(resource)?;
            self.crawl_delegated(resource, &sources)
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

    use object_store::{memory::InMemory, ObjectStore, ObjectStoreExt};

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
    fn emits_an_r2_source_after_uploading_it_to_crawled_files() {
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

        assert!(source.resource.url.contains("/crawled-files/"));
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

        assert!(first.resource.url.contains("/crawled-files/"));
        assert!(second.resource.url.contains("/crawled-files/"));
        assert_eq!(_archive.hits(), 1, "ZIP materialization should be cached");
    }

    #[test]
    fn changed_zip_replaces_cached_member_bytes() {
        let server = httpmock::MockServer::start();
        let mut first_archive = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/records.zip");
            then.status(200)
                .header("etag", "v1")
                .body(zip_bytes(&[("records.json", r#"[{"id":1}]"#)]));
        });
        let _head = server.mock(|when, then| {
            when.method(httpmock::Method::HEAD).path("/records.zip");
            then.status(200).header("etag", "v2");
        });
        let store = Arc::new(InMemory::new());
        let storage = SourceStorage::new_with_store(
            Client::new(),
            "https://files.example/documents".into(),
            store.clone(),
        );
        let reader =
            ExternalResourceCrawlerReader::new(Client::new(), &storage, Vec::new(), String::new());
        let resource = CkanResource {
            id: "resource-id".into(),
            package_id: "package-id".into(),
            url: format!("{}/listing", server.base_url()),
            format: "JSON".into(),
            datastore_active: false,
            last_modified: String::new(),
        };
        let source = CrawledSource::Zip {
            url: format!("{}/records.zip", server.base_url()),
        };
        let previous = reader
            .copy_source(&resource, &source, ArchiveLimits::default())
            .unwrap();
        first_archive.delete();
        let second_archive = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/records.zip");
            then.status(200)
                .header("etag", "v2")
                .body(zip_bytes(&[("records.json", r#"[{"id":2}]"#)]));
        });

        let copied = reader
            .copy_source(&resource, &source, ArchiveLimits::default())
            .unwrap();

        assert_eq!(second_archive.hits(), 1);
        let key = copied[0]
            .resource
            .url
            .strip_prefix("https://files.example/documents/")
            .unwrap();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let object = runtime
            .block_on(store.get(&object_store::path::Path::from(key)))
            .unwrap();
        let bytes = runtime.block_on(object.bytes()).unwrap();
        assert_eq!(bytes.as_ref(), br#"[{"id":2}]"#);
        assert_ne!(previous[0].resource.url, copied[0].resource.url);
        let old_key = previous[0]
            .resource
            .url
            .strip_prefix("https://files.example/documents/")
            .unwrap();
        let old_object = runtime
            .block_on(store.get(&object_store::path::Path::from(old_key)))
            .unwrap();
        let old_bytes = runtime.block_on(old_object.bytes()).unwrap();
        assert_eq!(old_bytes.as_ref(), br#"[{"id":1}]"#);
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
    fn archives_each_fetched_html_page_as_warc_gzip() {
        let server = httpmock::MockServer::start();
        let _page = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/listing/");
            then.status(200)
                .header("content-type", "text/html")
                .body(r#"<a href="sub/">subdirectory</a>"#);
        });
        let _subpage = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/listing/sub/");
            then.status(200)
                .header("content-type", "text/html")
                .body(r#"<a href="file.json">file</a><a href="other.json">other</a>"#);
        });
        let _file = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/listing/sub/file.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"[{"id":1}]"#);
        });
        let _other = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/listing/sub/other.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"[{"id":2}]"#);
        });
        let store = Arc::new(InMemory::new());
        let storage = SourceStorage::new_with_store(
            Client::new(),
            "https://files.example/documents".into(),
            store.clone(),
        );
        let client = Client::new();
        let reader = ExternalResourceCrawlerReader::new(
            client.clone(),
            &storage,
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
            Ok(_) => panic!("two files create child jobs"),
            Err(failure) => failure,
        };
        let crawl = failure.error.downcast_ref::<CrawlError>().unwrap();
        assert!(crawl.0.downcast_ref::<CrawlFanout>().is_some());

        let runtime = tokio::runtime::Runtime::new().unwrap();
        let warcs = runtime
            .block_on(store.list(None).try_collect::<Vec<_>>())
            .unwrap()
            .into_iter()
            .filter(|meta| meta.location.as_ref().starts_with("crawled-pages/"))
            .collect::<Vec<_>>();
        assert_eq!(warcs.len(), 2);
        for meta in warcs {
            let bytes = runtime.block_on(store.get(&meta.location)).unwrap();
            let bytes = runtime.block_on(bytes.bytes()).unwrap();
            let mut content = String::new();
            flate2::read::GzDecoder::new(bytes.as_ref())
                .read_to_string(&mut content)
                .unwrap();
            assert!(content.starts_with("WARC/1.1\r\n"));
            assert!(content.contains("WARC-Type: response\r\n"));
            assert!(content.contains("HTTP/1.1 200 OK\r\n"));
        }
    }

    #[test]
    fn fails_discovery_when_warc_upload_fails() {
        let server = httpmock::MockServer::start();
        let _page = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/page");
            then.status(200)
                .header("content-type", "text/html")
                .body(r#"<a href="file.json">file</a>"#);
        });
        let directory = tempfile::tempdir().unwrap();
        let store =
            object_store::local::LocalFileSystem::new_with_prefix(directory.path()).unwrap();
        std::fs::write(directory.path().join("crawled-pages"), b"occupied").unwrap();
        let storage = SourceStorage::new_with_store(
            Client::new(),
            "https://files.example/documents".into(),
            Arc::new(store),
        );
        let reader =
            ExternalResourceCrawlerReader::new(Client::new(), &storage, Vec::new(), String::new());
        let resource = CkanResource {
            id: "resource-id".into(),
            package_id: "package-id".into(),
            url: format!("{}/page", server.base_url()),
            format: "JSON".into(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let error = match reader.load_or_discover(&resource) {
            Ok(_) => panic!("WARC upload failure must stop discovery"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("WARC archive failed"));
    }

    #[test]
    fn caches_discovery_in_r2_without_listing_drive_twice() {
        let server = httpmock::MockServer::start();
        let page = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/page");
            then.status(200)
                .header("content-type", "text/html")
                .body(r#"<a href="https://drive.google.com/drive/folders/folder_1">folder</a>"#);
        });
        let _head = server.mock(|when, then| {
            when.method(httpmock::Method::HEAD).path("/page");
            then.status(200);
        });
        let listing = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/drive/v3/files");
            then.status(200).json_body(serde_json::json!({
                "files": [{
                    "id": "file_1", "name": "mapa.tif", "mimeType": "image/tiff",
                    "modifiedTime": "2026-01-01T00:00:00Z"
                }]
            }));
        });
        let storage = test_ingestor();
        let reader = ExternalResourceCrawlerReader::new(
            Client::new(),
            &storage,
            Vec::new(),
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

        let first = reader.load_or_discover(&resource).unwrap();
        let second = reader.load_or_discover(&resource).unwrap();

        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert!(matches!(&second[0], CrawledSource::Drive(file) if file.id == "file_1"));
        assert_eq!(page.hits(), 1);
        assert_eq!(listing.hits(), 1);
    }

    #[test]
    fn discovery_manifest_preserves_mixed_source_order() {
        let storage = test_ingestor();
        let reader =
            ExternalResourceCrawlerReader::new(Client::new(), &storage, Vec::new(), String::new());
        let sources = vec![
            CrawledSource::Zip {
                url: "https://example.test/a.zip".into(),
            },
            CrawledSource::Http {
                url: "https://example.test/b.json".into(),
            },
            CrawledSource::Drive(DriveFile {
                id: "file_1".into(),
                name: "c.json".into(),
                mime_type: "application/json".into(),
                modified_time: Some("2026-01-01T00:00:00Z".into()),
                resource_key: None,
            }),
        ];

        reader
            .store_discovery("https://example.test/page", "JSON", &sources)
            .unwrap();
        let (cached, _) = reader
            .cached_discovery("https://example.test/page", "JSON")
            .unwrap()
            .unwrap();

        assert_eq!(cached.len(), 3);
        assert!(matches!(cached[0], CrawledSource::Zip { .. }));
        assert!(matches!(cached[1], CrawledSource::Http { .. }));
        assert!(matches!(cached[2], CrawledSource::Drive(_)));
    }

    #[test]
    fn incomplete_discovery_generation_is_ignored() {
        let storage = test_ingestor();
        let reader =
            ExternalResourceCrawlerReader::new(Client::new(), &storage, Vec::new(), String::new());
        let url = "https://example.test/page";
        let base = discovery_key(url, "JSON");
        storage
            .put_cached_object(
                &format!("discovered-sources/manifest/{base}.json"),
                serde_json::to_vec(&DiscoveryManifest {
                    generation: "incomplete".into(),
                    order: vec!["http".into()],
                })
                .unwrap(),
                "application/json",
            )
            .unwrap();

        assert!(reader.cached_discovery(url, "JSON").unwrap().is_none());
    }

    #[test]
    fn cache_ttl_is_seven_days() {
        assert!(cache_fresh(Utc::now() - chrono::Duration::days(6)));
        assert!(!cache_fresh(Utc::now() - chrono::Duration::days(8)));
    }

    #[test]
    fn uses_discovery_cache_when_page_head_fails() {
        let server = httpmock::MockServer::start();
        let page = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/page");
            then.status(200)
                .header("content-type", "text/html")
                .body(r#"<a href="file.json">file</a>"#);
        });
        let _head = server.mock(|when, then| {
            when.method(httpmock::Method::HEAD).path("/page");
            then.status(502);
        });
        let storage = test_ingestor();
        let reader =
            ExternalResourceCrawlerReader::new(Client::new(), &storage, Vec::new(), String::new());
        let resource = CkanResource {
            id: "resource-id".into(),
            package_id: "package-id".into(),
            url: format!("{}/page", server.base_url()),
            format: "JSON".into(),
            datastore_active: false,
            last_modified: String::new(),
        };

        assert_eq!(reader.load_or_discover(&resource).unwrap().len(), 1);
        assert_eq!(reader.load_or_discover(&resource).unwrap().len(), 1);
        assert_eq!(page.hits(), 1);
    }

    #[test]
    fn newer_html_last_modified_rebuilds_discovery() {
        let server = httpmock::MockServer::start();
        let page = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/page");
            then.status(200)
                .header("content-type", "text/html")
                .body(r#"<a href="file.json">file</a>"#);
        });
        let future_modified = (Utc::now() + chrono::Duration::days(1)).to_rfc2822();
        let _head = server.mock(|when, then| {
            when.method(httpmock::Method::HEAD).path("/page");
            then.status(200).header("last-modified", &future_modified);
        });
        let storage = test_ingestor();
        let reader =
            ExternalResourceCrawlerReader::new(Client::new(), &storage, Vec::new(), String::new());
        let resource = CkanResource {
            id: "resource-id".into(),
            package_id: "package-id".into(),
            url: format!("{}/page", server.base_url()),
            format: "JSON".into(),
            datastore_active: false,
            last_modified: String::new(),
        };

        reader.load_or_discover(&resource).unwrap();
        reader.load_or_discover(&resource).unwrap();

        assert_eq!(page.hits(), 2);
    }

    #[test]
    fn discovery_cache_falls_back_after_drive_listing_error() {
        let server = httpmock::MockServer::start();
        let _page = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/page");
            then.status(200)
                .header("content-type", "text/html")
                .body(r#"<a href="https://drive.google.com/drive/folders/folder_1">folder</a>"#);
        });
        let future_modified = (Utc::now() + chrono::Duration::days(1)).to_rfc2822();
        let _head = server.mock(|when, then| {
            when.method(httpmock::Method::HEAD).path("/page");
            then.status(200).header("last-modified", &future_modified);
        });
        let mut listing = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/drive/v3/files");
            then.status(200).json_body(serde_json::json!({
                "files": [{
                    "id": "file_1", "name": "mapa.tif", "mimeType": "image/tiff",
                    "modifiedTime": "2026-01-01T00:00:00Z"
                }]
            }));
        });
        let storage = test_ingestor();
        let reader = ExternalResourceCrawlerReader::new(
            Client::new(),
            &storage,
            Vec::new(),
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
        assert_eq!(reader.load_or_discover(&resource).unwrap().len(), 1);
        listing.delete();
        let _forbidden = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/drive/v3/files");
            then.status(403).body("Sorry, automated queries");
        });

        let fallback = reader.load_or_discover(&resource).unwrap();

        assert!(matches!(&fallback[0], CrawledSource::Drive(file) if file.id == "file_1"));
    }

    #[test]
    fn reuses_recent_materialized_drive_file_without_second_download() {
        let server = httpmock::MockServer::start();
        let download = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/drive/v3/files/file_1")
                .query_param("alt", "media");
            then.status(200)
                .header("content-type", "image/tiff")
                .body("TIFF");
        });
        let storage = test_ingestor();
        let reader = ExternalResourceCrawlerReader::new(
            Client::new(),
            &storage,
            Vec::new(),
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
        let source = CrawledSource::Drive(DriveFile {
            id: "file_1".into(),
            name: "mapa.tif".into(),
            mime_type: "image/tiff".into(),
            modified_time: Some("2026-01-01T00:00:00Z".into()),
            resource_key: None,
        });

        let first = reader
            .copy_source(&resource, &source, ArchiveLimits::default())
            .unwrap();
        let second = reader
            .copy_source(&resource, &source, ArchiveLimits::default())
            .unwrap();

        assert_eq!(first[0].resource.id, second[0].resource.id);
        assert_eq!(first[0].resource.url, second[0].resource.url);
        assert_eq!(download.hits(), 1);
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
                .query_param("alt", "media")
                .query_param("supportsAllDrives", "true");
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
            Client::new(),
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

    #[test]
    fn includes_drive_download_error_response_body() {
        let server = httpmock::MockServer::start();
        let _download = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/drive/v3/files/file_1")
                .query_param("alt", "media")
                .query_param("supportsAllDrives", "true");
            then.status(403).json_body(serde_json::json!({
                "error": {
                    "code": 403,
                    "message": "The user's Drive storage quota has been exceeded.",
                    "status": "RESOURCE_EXHAUSTED"
                }
            }));
        });
        let storage = test_ingestor();
        let reader = ExternalResourceCrawlerReader::new(
            Client::new(),
            &storage,
            Vec::new(),
            "api-key".into(),
        )
        .with_api_base(format!("{}/drive/v3", server.base_url()));
        let file = DriveFile {
            id: "file_1".into(),
            name: "mapa.tif".into(),
            mime_type: "image/tiff".into(),
            modified_time: Some("2026-01-01T00:00:00Z".into()),
            resource_key: None,
        };
        let source = Source::new(
            "drive://file_1",
            "https://example.test/file_1",
            "mapa.tif",
            SourceVersion::known("2026-01-01T00:00:00Z"),
        );

        let error = match reader.download_drive_file(&file, source) {
            Ok(_) => panic!("Drive download should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("HTTP status 403"));
        assert!(error.to_string().contains("RESOURCE_EXHAUSTED"));
        assert!(error.to_string().contains("storage quota"));
    }

    #[test]
    fn includes_drive_folder_listing_error_response_body() {
        let server = httpmock::MockServer::start();
        let _listing = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/drive/v3/files");
            then.status(429).json_body(serde_json::json!({
                "error": {
                    "code": 429,
                    "message": "Rate Limit Exceeded",
                    "status": "RESOURCE_EXHAUSTED"
                }
            }));
        });
        let storage = test_ingestor();
        let reader = ExternalResourceCrawlerReader::new(
            Client::new(),
            &storage,
            Vec::new(),
            "api-key".into(),
        )
        .with_api_base(format!("{}/drive/v3", server.base_url()));

        let error = match reader.list_folder(&DriveLink {
            id: "folder_1".into(),
            resource_key: None,
            folder: true,
        }) {
            Ok(_) => panic!("Drive folder listing should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("HTTP status 429"));
        assert!(error.to_string().contains("Rate Limit Exceeded"));
    }

    #[test]
    fn truncates_large_drive_error_response_bodies() {
        let server = httpmock::MockServer::start();
        let body = format!("{}tail", "x".repeat(MAX_DRIVE_ERROR_BODY_BYTES as usize));
        let _metadata = server.mock(|when, then| {
            when.method(httpmock::Method::GET)
                .path("/drive/v3/files/file_1");
            then.status(403).body(body);
        });
        let storage = test_ingestor();
        let reader = ExternalResourceCrawlerReader::new(
            Client::new(),
            &storage,
            Vec::new(),
            "api-key".into(),
        )
        .with_api_base(format!("{}/drive/v3", server.base_url()));

        let error = match reader.get_file(&DriveLink {
            id: "file_1".into(),
            resource_key: None,
            folder: false,
        }) {
            Ok(_) => panic!("Drive metadata request should fail"),
            Err(error) => error,
        };

        assert!(error.to_string().contains("response body (truncated)"));
        assert!(!error.to_string().contains("tail"));
    }
}
