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
use std::path::{Path, PathBuf};

mod s3_settings;
mod source_storage;
mod temporary_path;

pub use s3_settings::S3Settings;
pub use source_storage::SourceStorage;
pub use temporary_path::TemporaryPath;

/// Materialises a source with a caller-owned HTTP client.
///
/// Readers that do not interact with object storage use this helper. Storage
/// operations use the client held by `SourceStorage`.
pub fn download_to_temp(
    client: &reqwest::blocking::Client,
    source: Source,
) -> anyhow::Result<DownloadedSource> {
    source_storage::download_to_temp(client, source)
}

/// Identifies a source before any bytes have been fetched.
///
/// `identity` is stable across downloads and is used as the crawler cache key.
/// `filename` is the name the source carries into storage; callers never pass it
/// separately to upload or cache operations.
#[derive(Clone, Debug)]
pub struct Source {
    pub identity: String,
    pub url: String,
    pub filename: String,
    pub version: SourceVersion,
}

impl Source {
    pub fn new(
        identity: impl Into<String>,
        url: impl Into<String>,
        filename: impl Into<String>,
        version: SourceVersion,
    ) -> Self {
        Self {
            identity: identity.into(),
            url: url.into(),
            filename: filename.into(),
            version,
        }
    }

    pub fn from_url(url: impl Into<String>, filename: impl Into<String>) -> Self {
        let url = url.into();
        Self::new(url.clone(), url, filename, SourceVersion::Unknown)
    }

    pub fn from_url_with_format(url: impl Into<String>, format: &str) -> Self {
        let url = url.into();
        let filename = url::Url::parse(&url)
            .ok()
            .and_then(|url| url.path_segments()?.next_back().map(str::to_owned))
            .filter(|name| !name.is_empty())
            .or_else(|| Path::new(&url).file_name()?.to_str().map(str::to_owned))
            .unwrap_or_else(|| "source".into());
        let filename = if Path::new(&filename).extension().is_some() {
            filename
        } else {
            format!("{filename}.{}", extension_for_format(format))
        };
        Self::from_url(url, filename)
    }

    pub fn temporary_path(&self) -> PathBuf {
        let suffix = Path::new(&self.filename)
            .extension()
            .and_then(|value| value.to_str())
            .map(|value| format!(".{value}"))
            .unwrap_or_default();
        std::env::temp_dir().join(format!("{}{}", uuid::Uuid::new_v4(), suffix))
    }
}

fn extension_for_format(format: &str) -> &'static str {
    let format = format.to_ascii_uppercase();
    if format.contains("CSV") {
        "csv"
    } else if format.contains("JSON") {
        "json"
    } else if format.contains("TIFF") || format.contains("TIF") {
        "tif"
    } else if format.contains("PDF") {
        "pdf"
    } else {
        "dat"
    }
}

/// The version supplied by a source system, used to determine cache freshness.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SourceVersion {
    Known(String),
    Unknown,
}

impl SourceVersion {
    pub fn known(value: impl Into<String>) -> Self {
        Self::Known(value.into())
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Known(value) => Some(value),
            Self::Unknown => None,
        }
    }
}

/// The media type observed while materialising source bytes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MediaType {
    Known(String),
    Unknown,
}

impl MediaType {
    pub fn known(value: impl Into<String>) -> Self {
        Self::Known(value.into())
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Known(value) => Some(value),
            Self::Unknown => None,
        }
    }

    pub fn is_html(&self) -> bool {
        self.as_str().is_some_and(|value| {
            matches!(
                value.split(';').next().unwrap_or_default().trim(),
                value if value.eq_ignore_ascii_case("text/html")
                    || value.eq_ignore_ascii_case("application/xhtml+xml")
            )
        })
    }
}

/// A source that has been materialised in a temporary local file.
///
/// Dropping this value removes the file. It is deliberately distinct from
/// `Source`: cache identity belongs to the source, while the local path and the
/// observed media type exist only after download/extraction.
pub struct DownloadedSource {
    pub source: Source,
    pub path: PathBuf,
    pub media_type: MediaType,
}

impl DownloadedSource {
    pub fn from_path(source: Source, path: PathBuf, media_type: MediaType) -> Self {
        Self {
            source,
            path,
            media_type,
        }
    }

    pub fn path_as_str(&self) -> anyhow::Result<&str> {
        self.path
            .to_str()
            .ok_or_else(|| anyhow::anyhow!("temporary source path is not UTF-8"))
    }
}

impl Drop for DownloadedSource {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// A non-success HTTP status returned while materialising a source.
#[derive(Debug)]
pub struct HttpStatusError {
    status: reqwest::StatusCode,
    message: String,
}

impl HttpStatusError {
    pub fn new(status: reqwest::StatusCode, message: impl Into<String>) -> Self {
        Self {
            status,
            message: message.into(),
        }
    }

    pub fn status(&self) -> reqwest::StatusCode {
        self.status
    }

    pub fn is_not_found(&self) -> bool {
        self.status == reqwest::StatusCode::NOT_FOUND
    }
}

impl std::fmt::Display for HttpStatusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for HttpStatusError {}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{DownloadedSource, MediaType, Source, SourceVersion};

    #[test]
    fn downloaded_source_adds_local_artifact_metadata_to_a_source() {
        let path = std::env::temp_dir().join(format!("source-io-{}.json", uuid::Uuid::new_v4()));
        fs::write(&path, "{} ").expect("temporary fixture");
        let source = Source::new(
            "drive:file-1",
            "https://drive.example/files/file-1",
            "data.json",
            SourceVersion::known("2026-01-01T00:00:00Z"),
        );

        let downloaded =
            DownloadedSource::from_path(source, path.clone(), MediaType::known("application/json"));

        assert_eq!(downloaded.source.filename, "data.json");
        assert_eq!(
            downloaded.source.version.as_str(),
            Some("2026-01-01T00:00:00Z")
        );
        assert_eq!(downloaded.media_type.as_str(), Some("application/json"));
        drop(downloaded);
        assert!(!path.exists());
    }
}
