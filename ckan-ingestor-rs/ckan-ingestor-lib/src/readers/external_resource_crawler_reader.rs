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

use std::{collections::HashSet, io::Read, sync::Arc};

use anyhow::{Context, Result};
use arrow::{
    array::StringArray,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use regex::Regex;
use reqwest::{blocking::Client, header::CONTENT_TYPE};
use serde::Deserialize;
use url::Url;

use crate::{
    ckan_resource::CkanResource,
    parquet_output::ParquetOutput,
    readers::ckan_reader::{CkanReader, ReadResult, SuccessResult},
    s3_document_ingestor::S3DocumentIngestor,
};

const MAX_PAGE_BYTES: u64 = 8 * 1024 * 1024;
const FOLDER_MIME: &str = "application/vnd.google-apps.folder";

#[derive(Debug)]
pub struct CrawlError(pub anyhow::Error);

impl std::fmt::Display for CrawlError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Drive crawl failed: {}", self.0)
    }
}
impl std::error::Error for CrawlError {}

#[derive(Clone)]
struct DriveLink {
    id: String,
    resource_key: Option<String>,
    folder: bool,
}

#[derive(Deserialize)]
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

/// Resolves public Google Drive links found on an HTML resource page.
pub struct ExternalResourceCrawlerReader<'a> {
    client: Client,
    ingestor: &'a S3DocumentIngestor,
    api_key: String,
    api_base: String,
    formats: Vec<String>,
}

impl<'a> ExternalResourceCrawlerReader<'a> {
    pub fn new(client: Client, ingestor: &'a S3DocumentIngestor, api_key: String) -> Self {
        Self {
            client,
            ingestor,
            api_key,
            api_base: "https://www.googleapis.com/drive/v3".into(),
            formats: vec![
                "DOCX".into(),
                "MAP".into(),
                "PDF".into(),
                "TAB".into(),
                "TIF".into(),
                "TIFF".into(),
            ],
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

    fn crawl(
        &self,
        page_url: &str,
        resource_format: &str,
        response: impl Read,
    ) -> Result<SuccessResult> {
        anyhow::ensure!(!self.api_key.is_empty(), "GOOGLE_DRIVE_API_KEY is required");
        let mut bytes = Vec::new();
        response.take(MAX_PAGE_BYTES + 1).read_to_end(&mut bytes)?;
        anyhow::ensure!(
            bytes.len() as u64 <= MAX_PAGE_BYTES,
            "HTML page exceeds crawl limit"
        );
        let html = String::from_utf8(bytes).context("HTML page is not UTF-8")?;
        let mut pending = extract_drive_links(&html, page_url)?;
        let mut visited_folders = HashSet::new();
        let mut visited_files = HashSet::new();
        let mut output = None::<ParquetOutput>;
        while let Some(link) = pending.pop() {
            if link.folder {
                if !visited_folders.insert(link.id.clone()) {
                    continue;
                }
                for child in self.list_folder(&link)? {
                    pending.push(DriveLink {
                        id: child.id.clone(),
                        resource_key: child.resource_key.clone(),
                        folder: child.mime_type == FOLDER_MIME,
                    });
                    if child.mime_type != FOLDER_MIME
                        && matches_resource_format(&child.name, resource_format)
                    {
                        self.write_file(&child, &mut visited_files, &mut output)?;
                    }
                }
            } else {
                let file = self.get_file(&link)?;
                if file.mime_type == FOLDER_MIME {
                    pending.push(DriveLink {
                        folder: true,
                        ..link
                    });
                } else if matches_resource_format(&file.name, resource_format) {
                    self.write_file(&file, &mut visited_files, &mut output)?;
                }
            }
        }
        let mut output = output.context("no accessible matching files found in Drive links")?;
        output.finish()?;
        Ok(SuccessResult::new(output, self.reader_name().into()))
    }

    fn write_file(
        &self,
        file: &DriveFile,
        visited: &mut HashSet<String>,
        output: &mut Option<ParquetOutput>,
    ) -> Result<()> {
        if !visited.insert(file.id.clone()) {
            return Ok(());
        }
        let modified = file
            .modified_time
            .as_deref()
            .context("Drive file missing modifiedTime")?;
        let url = self
            .ingestor
            .ingest_drive_file(&file.id, modified, &file.mime_type, || {
                let mut request = self
                    .client
                    .get(format!("{}/files/{}", self.api_base, file.id))
                    .header("x-goog-api-key", &self.api_key)
                    .query(&[("alt", "media")]);
                if let Some(key) = &file.resource_key {
                    request = request
                        .header("x-goog-drive-resource-keys", format!("{}/{}", file.id, key));
                }
                let response = request.send()?.error_for_status()?;
                anyhow::ensure!(
                    !response
                        .headers()
                        .get(CONTENT_TYPE)
                        .and_then(|v| v.to_str().ok())
                        .is_some_and(|v| v.to_ascii_lowercase().starts_with("text/html")),
                    "Drive returned HTML for {}",
                    file.id
                );
                Ok(response)
            })?;
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("url", DataType::Utf8, false),
                Field::new("name", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec![url])),
                Arc::new(StringArray::from(vec![file.name.clone()])),
            ],
        )?;
        if output.is_none() {
            *output = Some(ParquetOutput::try_new(&batch)?);
        }
        output.as_mut().expect("initialized output").write(&batch)?;
        Ok(())
    }
}

fn matches_resource_format(name: &str, format: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    match format.to_ascii_uppercase().as_str() {
        "TIF" | "TIFF" => lower.ends_with(".tif") || lower.ends_with(".tiff"),
        "DOCX" => lower.ends_with(".docx"),
        "MAP" => lower.ends_with(".map"),
        "PDF" => lower.ends_with(".pdf"),
        "TAB" => lower.ends_with(".tab"),
        _ => false,
    }
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
            let is_html = response
                .headers()
                .get(CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .is_some_and(|v| {
                    v.to_ascii_lowercase().starts_with("text/html")
                        || v.to_ascii_lowercase().starts_with("application/xhtml+xml")
                });
            anyhow::ensure!(is_html, "resource content is not HTML");
            self.crawl(&resource.url, &resource.format, response)
        })();
        result.map_err(|error| CrawlError(error).into())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use object_store::memory::InMemory;

    use crate::{
        ckan_resource::CkanResource, readers::ckan_reader::CkanReader,
        s3_document_ingestor::S3DocumentIngestor,
    };

    use super::*;

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
        let ingestor = S3DocumentIngestor::new_with_store(
            "https://files.example/documents".into(),
            Arc::new(InMemory::new()),
        );
        let reader = ExternalResourceCrawlerReader::new(Client::new(), &ingestor, "api-key".into())
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
    }
}
