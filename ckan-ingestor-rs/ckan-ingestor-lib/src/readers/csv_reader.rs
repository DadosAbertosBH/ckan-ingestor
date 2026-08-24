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
use crate::ckan_resource::CkanResource;
use crate::readers::ckan_reader::{CkanReader, ReadResult, SuccessResult};
use anyhow::Result;
use csv_nose::{Metadata, Quote, Sniffer};
use duckdb::arrow::array::RecordBatch;
use duckdb::Connection;
use flate2::read::GzDecoder;
use std::fs::File;
use std::io;

pub struct CsvReader<'a> {
    conn: &'a Connection,
    client: reqwest::blocking::Client,
    supported_formats: Vec<String>,
}

impl<'a> CsvReader<'a> {
    pub fn new(conn: &'a Connection, client: reqwest::blocking::Client) -> Self {
        Self {
            conn,
            client,
            supported_formats: vec!["CSV".to_string()],
        }
    }

    /// Detect the CSV metadata once, then read it with the detected settings.
    /// Downloads the file first (if remote), matching Swift's approach.
    pub fn read_batches(&self, resource: &CkanResource) -> ReadResult {
        let is_remote = resource.url.starts_with("http://") || resource.url.starts_with("https://");
        let csv_path = if is_remote {
            self.download_to_temp(&resource.url)?
        } else {
            resource.url.clone()
        };

        // Cleanup temp file when done
        struct Cleanup(Option<String>);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                if let Some(ref path) = self.0 {
                    let _ = std::fs::remove_file(path);
                }
            }
        }
        let _cleanup = Cleanup(if is_remote {
            Some(csv_path.clone())
        } else {
            None
        });

        let metadata = sniff_metadata(&csv_path)?;
        let encoding = metadata.encoding.name.to_string();
        let batches = match self.try_read_csv(&csv_path, &metadata, true) {
            Ok(batches) => (batches, true),
            Err(error) if is_csv_sniffing_error(&error) => {
                (self.try_read_csv(&csv_path, &metadata, false)?, false)
            }
            Err(error) => return Err(error.into()),
        };

        Ok(SuccessResult::from_csv(
            batches.0,
            encoding,
            batches.1,
            self.reader_name().to_string(),
        ))
    }

    /// Download a remote file to a temporary location.
    fn download_to_temp(&self, url: &str) -> Result<String> {
        let response = self.client.get(url).send()?;
        let bytes = response.bytes()?;
        let temp_path = std::env::temp_dir().join(format!(
            "{}{}",
            uuid::Uuid::new_v4(),
            downloaded_csv_suffix(url)
        ));
        std::fs::write(&temp_path, &bytes)?;
        Ok(temp_path.to_string_lossy().to_string())
    }

    /// Try DuckDB's `read_csv` with a specific encoding.
    fn try_read_csv(
        &self,
        path: &str,
        metadata: &Metadata,
        strict_mode: bool,
    ) -> Result<Vec<RecordBatch>> {
        let dialect = &metadata.dialect;
        let quote = match dialect.quote {
            Quote::None => String::new(),
            Quote::Some(value) => (value as char).to_string(),
        };
        let mut stmt = self.conn.prepare(&format!(
            "SELECT * FROM read_csv('{}', sample_size=900000, encoding='{}', delim='{}', quote='{}', header={}, skip={}, strict_mode={}, nullstr=['', '-', ' - ', ' -   '])",
            escape_sql_literal(path),
            escape_sql_literal(duckdb_encoding(metadata.encoding.name)),
            escape_sql_literal((dialect.delimiter as char).to_string()),
            escape_sql_literal(&quote),
            dialect.header.has_header_row,
            dialect.header.num_preamble_rows,
            strict_mode,
        ))?;
        let arrow_iter = stmt.query_arrow([])?;
        let batches: Vec<RecordBatch> = arrow_iter.collect();
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            anyhow::bail!("No data");
        }
        Ok(batches)
    }
}

fn escape_sql_literal(value: impl AsRef<str>) -> String {
    value.as_ref().replace('\'', "''")
}

fn duckdb_encoding(name: &str) -> &str {
    match name {
        "UTF-8" => "utf-8",
        "UTF-16LE" | "UTF-16BE" => "utf-16",
        "windows-1252" => "CP1252",
        "windows-1251" => "CP1251",
        "ISO-8859-1" => "latin-1",
        other => other,
    }
}

fn sniff_metadata(path: &str) -> Result<Metadata> {
    if !path.to_ascii_lowercase().ends_with(".gz") {
        return Ok(Sniffer::new().sniff_path(path)?);
    }

    let sniff_path =
        std::env::temp_dir().join(format!("csv-nose-sniff-{}.csv", uuid::Uuid::new_v4()));
    let result = (|| -> Result<Metadata> {
        let input = File::open(path)?;
        let mut decoder = GzDecoder::new(input);
        let mut output = File::create(&sniff_path)?;
        io::copy(&mut decoder, &mut output)?;
        Ok(Sniffer::new().sniff_path(&sniff_path)?)
    })();
    let _ = std::fs::remove_file(sniff_path);
    result
}

fn is_csv_sniffing_error(error: &anyhow::Error) -> bool {
    error.to_string().contains("Error when sniffing file")
}

fn downloaded_csv_suffix(url: &str) -> &'static str {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    if path.to_ascii_lowercase().ends_with(".gz") {
        ".csv.gz"
    } else {
        ".csv"
    }
}

impl CkanReader for CsvReader<'_> {
    fn supported_formats(&self) -> &[String] {
        &self.supported_formats
    }

    fn do_read(&self, resource: &CkanResource) -> ReadResult {
        self.read_batches(resource)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use std::time::Duration;

    fn fixture_path(file: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("data")
            .join(file)
    }

    fn test_client() -> reqwest::blocking::Client {
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .unwrap()
    }

    #[test]
    fn maps_csv_nose_encoding_names_to_duckdb_aliases() {
        assert_eq!(duckdb_encoding("UTF-8"), "utf-8");
        assert_eq!(duckdb_encoding("windows-1252"), "CP1252");
        assert_eq!(duckdb_encoding("ISO-8859-1"), "latin-1");
        assert_eq!(duckdb_encoding("custom-encoding"), "custom-encoding");
    }

    /// A semicolon-delimited CSV containing quoted fields with embedded
    /// semicolons must still be parsed into a table with all records.
    #[test]
    fn do_read_parses_windows1152_enconde_semicolon_delimited_csv() -> Result<()> {
        let conn = Connection::open_in_memory()?;
        let reader = CsvReader::new(&conn, test_client());

        let csv_path = fixture_path("renuncia-fiscal-informacoes-conceituais-2024.csv");
        let resource = CkanResource {
            id: "5d16743c-0f6b-411d-aa7c-734a24b02812".to_string(),
            url: csv_path.to_str().unwrap().to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
        };

        let result = reader.do_read(&resource)?;
        let total: usize = result.data.iter().map(|batch| batch.num_rows()).sum();
        assert_eq!(total, 31, "should read all 31 data records");

        Ok(())
    }
}
