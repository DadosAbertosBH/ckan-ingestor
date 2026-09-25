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
    path::Path,
};

use anyhow::{Context, Result};
use zip::ZipArchive;

use source_io::TemporaryPath;

#[derive(Clone, Copy)]
pub(crate) struct ArchiveLimits {
    pub max_zip_bytes: u64,
    pub max_expanded_bytes: u64,
    pub max_depth: usize,
    pub max_entries: usize,
}

impl Default for ArchiveLimits {
    fn default() -> Self {
        Self {
            max_zip_bytes: 8 * 1024 * 1024 * 1024,
            max_expanded_bytes: 64 * 1024 * 1024 * 1024,
            max_depth: 4,
            max_entries: 100_000,
        }
    }
}

impl ArchiveLimits {
    pub fn from_env() -> Result<Self> {
        let defaults = Self::default();
        Ok(Self {
            max_zip_bytes: setting("CRAWLER_MAX_ZIP_BYTES", defaults.max_zip_bytes)?,
            max_expanded_bytes: setting("CRAWLER_MAX_EXPANDED_BYTES", defaults.max_expanded_bytes)?,
            max_depth: setting("CRAWLER_MAX_ARCHIVE_DEPTH", defaults.max_depth)?,
            max_entries: setting("CRAWLER_MAX_ARCHIVE_ENTRIES", defaults.max_entries)?,
        })
    }
}

fn setting<T>(name: &str, default: T) -> Result<T>
where
    T: std::str::FromStr + PartialOrd + From<u8> + Copy,
    T::Err: std::error::Error + Send + Sync + 'static,
{
    match std::env::var(name) {
        Ok(value) => {
            let parsed: T = value.parse().with_context(|| format!("invalid {name}"))?;
            anyhow::ensure!(parsed > T::from(0), "{name} must be positive");
            Ok(parsed)
        }
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn visit_zip<F>(
    path: &Path,
    source: &str,
    format: &str,
    limits: ArchiveLimits,
    visit: &mut F,
) -> Result<usize>
where
    F: FnMut(&Path, &str, Option<&str>) -> Result<()>,
{
    let mut entries = 0;
    let mut expanded = 0;
    visit_zip_inner(
        path,
        source,
        format,
        limits,
        1,
        &mut entries,
        &mut expanded,
        visit,
    )
}

fn visit_zip_inner<F>(
    path: &Path,
    source: &str,
    format: &str,
    limits: ArchiveLimits,
    depth: usize,
    entries: &mut usize,
    expanded: &mut u64,
    visit: &mut F,
) -> Result<usize>
where
    F: FnMut(&Path, &str, Option<&str>) -> Result<()>,
{
    anyhow::ensure!(
        depth <= limits.max_depth,
        "ZIP nesting exceeds configured depth"
    );
    anyhow::ensure!(
        path.metadata()?.len() <= limits.max_zip_bytes,
        "ZIP exceeds configured size"
    );
    let mut archive = ZipArchive::new(File::open(path)?)?;
    let mut found = 0;
    for index in 0..archive.len() {
        *entries += 1;
        anyhow::ensure!(
            *entries <= limits.max_entries,
            "ZIP entry count exceeds configured limit"
        );
        let mut member = archive.by_index(index)?;
        if member.is_dir() {
            continue;
        }
        let member_size = member.size();
        *expanded = expanded
            .checked_add(member_size)
            .context("ZIP size overflow")?;
        anyhow::ensure!(
            *expanded <= limits.max_expanded_bytes,
            "ZIP expanded size exceeds configured limit"
        );
        let name = member.name().to_owned();
        let modified = member.last_modified().map(|value| {
            format!(
                "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                value.year(),
                value.month(),
                value.day(),
                value.hour(),
                value.minute(),
                value.second()
            )
        });
        let lower = name.to_ascii_lowercase();
        let nested = lower.ends_with(".zip");
        if !nested && !matches_format(&lower, format) {
            continue;
        }
        let suffix = if nested { ".zip" } else { extension(format) };
        let temp_path = std::env::temp_dir().join(format!("{}{}", uuid::Uuid::new_v4(), suffix));
        let _cleanup = TemporaryPath::from_path(temp_path.clone());
        let mut output = File::create(&temp_path)?;
        let copied = io::copy(&mut member.by_ref().take(member_size + 1), &mut output)?;
        anyhow::ensure!(
            copied <= member_size,
            "ZIP member exceeds its declared size"
        );
        output.flush()?;
        drop(output);
        let member_source = format!("{source}!/{name}");
        if nested {
            found += visit_zip_inner(
                &temp_path,
                &member_source,
                format,
                limits,
                depth + 1,
                entries,
                expanded,
                visit,
            )?;
        } else {
            visit(&temp_path, &member_source, modified.as_deref())?;
            found += 1;
        }
    }
    Ok(found)
}

pub(crate) fn matches_format(name: &str, format: &str) -> bool {
    let lower = name.to_ascii_lowercase();
    let format = format.to_ascii_uppercase();
    if format.contains("TIFF") || format.contains("TIF") {
        lower.ends_with(".tif") || lower.ends_with(".tiff")
    } else if format.contains("CSV") {
        lower.ends_with(".csv")
    } else if format.contains("JSON") {
        lower.ends_with(".json") || lower.ends_with(".geojson")
    } else if format.contains("DOCX") {
        lower.ends_with(".docx")
    } else if format.contains("MAP") {
        lower.ends_with(".map")
    } else if format.contains("PDF") {
        lower.ends_with(".pdf")
    } else if format.contains("TAB") {
        lower.ends_with(".tab")
    } else {
        false
    }
}

fn extension(format: &str) -> &'static str {
    let format = format.to_ascii_uppercase();
    if format.contains("CSV") {
        ".csv"
    } else if format.contains("JSON") {
        ".json"
    } else if format.contains("TIFF") || format.contains("TIF") {
        ".tif"
    } else if format.contains("PDF") {
        ".pdf"
    } else if format.contains("DOCX") {
        ".docx"
    } else if format.contains("MAP") {
        ".map"
    } else if format.contains("TAB") {
        ".tab"
    } else {
        ".dat"
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Cursor, Write};

    use super::*;

    fn zip(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut archive = zip::ZipWriter::new(Cursor::new(Vec::new()));
        for (name, body) in entries {
            archive
                .start_file(*name, zip::write::SimpleFileOptions::default())
                .unwrap();
            archive.write_all(body).unwrap();
        }
        archive.finish().unwrap().into_inner()
    }

    #[test]
    fn applies_the_expansion_limit_across_nested_archives() {
        let inner = zip(&[("records.json", br#"[{"id":1234567890}]"#)]);
        let outer = zip(&[("nested.zip", &inner)]);
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(&outer).unwrap();
        let limit_between_each_and_their_sum = inner.len() as u64 + 1;
        assert!(
            (inner.len() + br#"[{"id":1234567890}]"#.len()) as u64
                > limit_between_each_and_their_sum
        );
        let limits = ArchiveLimits {
            max_expanded_bytes: limit_between_each_and_their_sum,
            ..ArchiveLimits::default()
        };

        let error = visit_zip(file.path(), "source.zip", "JSON", limits, &mut |_, _, _| {
            Ok(())
        })
        .expect_err("cumulative expansion must exceed the limit");

        assert!(error.to_string().contains("expanded size"));
    }

    #[test]
    fn matches_geojson_and_composite_ckan_format_labels() {
        assert!(matches_format("features.geojson", "GEOJSON"));
        assert!(matches_format("records.csv", "CSV / ZIP"));
        assert!(matches_format("raster.tiff", "GeoTIFF"));
    }
}
