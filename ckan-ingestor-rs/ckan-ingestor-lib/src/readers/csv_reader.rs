use crate::arrow_ipc_output::ArrowIpcOutput;
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
use crate::readers::temp_file_cleanup::TempFileCleanup;
use anyhow::Result;
use arrow_csv::reader::{Format, ReaderBuilder};
use csv_nose::{Metadata, Quote, SampleSize, Sniffer};
use encoding_rs::Encoding;
use encoding_rs_io::DecodeReaderBytesBuilder;
use flate2::read::GzDecoder;
use regex::Regex;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read, Write};
use std::path::PathBuf;

pub struct CsvReader {
    client: reqwest::blocking::Client,
    supported_formats: Vec<String>,
}

impl CsvReader {
    pub fn new(client: reqwest::blocking::Client) -> Self {
        Self {
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

        let mut cleanup = TempFileCleanup::from_path(PathBuf::from(&csv_path));
        if !is_remote {
            cleanup.commit();
        }

        let metadata = sniff_metadata(&csv_path)?;
        let encoding = metadata.encoding.name.to_string();
        let has_mixed_line_endings = has_mixed_line_endings(
            &csv_path,
            metadata.encoding.name,
            metadata.dialect.header.num_preamble_rows,
        )?;
        let (arrow_ipc, strict_mode) = match try_read_csv(&csv_path, &metadata, true) {
            Ok(batches) => (batches, !has_mixed_line_endings),
            Err(_) => (try_read_csv(&csv_path, &metadata, false)?, false),
        };

        Ok(SuccessResult::from_csv(
            arrow_ipc,
            encoding,
            strict_mode,
            self.reader_name().to_string(),
        ))
    }

    /// Download a remote file to a temporary location.
    fn download_to_temp(&self, url: &str) -> Result<String> {
        let mut response = self.client.get(url).send()?;
        let status = response.status();
        let content_type = response_header(&response, reqwest::header::CONTENT_TYPE);
        let content_encoding = response_header(&response, reqwest::header::CONTENT_ENCODING);
        if !status.is_success() {
            anyhow::bail!(
                "CSV download failed with HTTP status {status}: {url} (Content-Type: {content_type}, Content-Encoding: {content_encoding})"
            );
        }
        let temp_path = std::env::temp_dir().join(format!(
            "{}{}",
            uuid::Uuid::new_v4(),
            downloaded_csv_suffix(url)
        ));
        let mut cleanup = TempFileCleanup::from_path(temp_path.clone());
        let mut output = File::create(&temp_path)?;
        stream_download(&mut response, &mut output).map_err(|error| {
            download_body_error(url, status, &content_type, &content_encoding, error)
        })?;
        cleanup.commit();
        Ok(temp_path.to_string_lossy().to_string())
    }
}

fn response_header(
    resp: &reqwest::blocking::Response,
    name: reqwest::header::HeaderName,
) -> String {
    resp.headers()
        .get(name)
        .and_then(|value| value.to_str().ok())
        .unwrap_or("<missing or invalid>")
        .to_string()
}

fn download_body_error(
    url: &str,
    status: reqwest::StatusCode,
    content_type: &str,
    content_encoding: &str,
    error: io::Error,
) -> anyhow::Error {
    let message = format!(
        "error reading CSV response body from {url} (HTTP status {status}, Content-Type: {content_type}, Content-Encoding: {content_encoding}): {error}"
    );
    anyhow::Error::new(error).context(message)
}

fn stream_download(reader: &mut impl Read, writer: &mut impl Write) -> io::Result<u64> {
    io::copy(reader, writer)
}

fn try_read_csv(path: &str, metadata: &Metadata, strict_mode: bool) -> Result<ArrowIpcOutput> {
    let dialect = &metadata.dialect;
    let format = Format::default()
        .with_header(dialect.header.has_header_row)
        .with_delimiter(dialect.delimiter)
        .with_truncated_rows(!strict_mode)
        .with_null_regex(Regex::new(r"^(| - | -   | -)$")?);
    let format = match dialect.quote {
        Quote::None => format.with_quote(0),
        Quote::Some(quote) => format.with_quote(quote),
    };

    let mut schema_reader = decoded_reader(
        path,
        metadata.encoding.name,
        dialect.header.num_preamble_rows,
    )?;
    let (schema, _) = format.infer_schema(&mut schema_reader, Some(50_000))?;
    let reader = decoded_reader(
        path,
        metadata.encoding.name,
        dialect.header.num_preamble_rows,
    )?;
    let csv_reader = ReaderBuilder::new(std::sync::Arc::new(schema))
        .with_format(format)
        .with_batch_size(32_768)
        .build(reader)?;

    let mut arrow_ipc = None;
    for batch_result in csv_reader {
        let batch = batch_result?;
        if arrow_ipc.is_none() {
            arrow_ipc = Some(ArrowIpcOutput::try_new(&batch)?);
        }

        arrow_ipc
            .as_mut()
            .expect("Arrow IPC output initialized")
            .write(&batch)?;
    }
    let mut arrow_ipc = arrow_ipc.ok_or_else(|| anyhow::anyhow!("No data"))?;
    arrow_ipc.finish()?;
    Ok(arrow_ipc)
}

fn sniff_metadata(path: &str) -> Result<Metadata> {
    let mut sniffer = Sniffer::new();
    sniffer.sample_size(SampleSize::Records(900_000));

    if !path.to_ascii_lowercase().ends_with(".gz") {
        return Ok(sniffer.sniff_path(path)?);
    }

    let sniff_path =
        std::env::temp_dir().join(format!("csv-nose-sniff-{}.csv", uuid::Uuid::new_v4()));
    let result = (|| -> Result<Metadata> {
        let input = File::open(path)?;
        let mut decoder = GzDecoder::new(input);
        let mut output = File::create(&sniff_path)?;
        io::copy(&mut decoder, &mut output)?;
        Ok(sniffer.sniff_path(&sniff_path)?)
    })();
    let _ = std::fs::remove_file(sniff_path);
    result
}

fn encoding(name: &str) -> Result<&'static Encoding> {
    Encoding::for_label(name.as_bytes())
        .ok_or_else(|| anyhow::anyhow!("unsupported CSV encoding detected: {name}"))
}

fn decode_reader<R: Read>(reader: R, name: &str) -> Result<impl Read> {
    let mut builder = DecodeReaderBytesBuilder::new();
    builder.encoding(Some(encoding(name)?));
    Ok(builder.build(reader))
}

fn decoded_reader(path: &str, encoding_name: &str, preamble_rows: usize) -> Result<Box<dyn Read>> {
    let input: Box<dyn Read> = if path.to_ascii_lowercase().ends_with(".gz") {
        Box::new(GzDecoder::new(File::open(path)?))
    } else {
        Box::new(File::open(path)?)
    };
    let mut reader = BufReader::new(decode_reader(input, encoding_name)?);
    let mut line = String::new();
    for _ in 0..preamble_rows {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
    }
    Ok(Box::new(reader))
}

fn has_mixed_line_endings(path: &str, encoding_name: &str, preamble_rows: usize) -> Result<bool> {
    let mut reader = decoded_reader(path, encoding_name, preamble_rows)?;
    let mut sample = Vec::new();
    reader.by_ref().take(1024 * 1024).read_to_end(&mut sample)?;

    let mut has_crlf = false;
    let mut has_bare_lf = false;
    for (index, byte) in sample.iter().enumerate() {
        if *byte != b'\n' {
            continue;
        }
        if index > 0 && sample[index - 1] == b'\r' {
            has_crlf = true;
        } else {
            has_bare_lf = true;
        }
    }
    Ok(has_crlf && has_bare_lf)
}

fn downloaded_csv_suffix(url: &str) -> &'static str {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    if path.to_ascii_lowercase().ends_with(".gz") {
        ".csv.gz"
    } else {
        ".csv"
    }
}

impl CkanReader for CsvReader {
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
    use std::cell::Cell;
    use std::io::{Read, Write};
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::time::Duration;

    struct WriteAwareReader {
        write_started: Rc<Cell<bool>>,
        reads: usize,
    }

    impl Read for WriteAwareReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if self.reads == 1 && !self.write_started.get() {
                return Err(io::Error::other(
                    "the next download chunk was read before the first was written",
                ));
            }
            if self.reads >= 2 {
                return Ok(0);
            }

            let chunk: &[u8] = if self.reads == 0 { b"first" } else { b"second" };
            buffer[..chunk.len()].copy_from_slice(chunk);
            self.reads += 1;
            Ok(chunk.len())
        }
    }

    struct WriteObserver {
        write_started: Rc<Cell<bool>>,
        bytes: Vec<u8>,
    }

    impl Write for WriteObserver {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.write_started.set(true);
            self.bytes.extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

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
    fn supports_csv_nose_encoding_names() {
        assert!(encoding("UTF-8").is_ok());
        assert!(encoding("windows-1252").is_ok());
        assert!(encoding("UTF-16LE").is_ok());
    }

    #[test]
    fn decodes_windows_1252_as_utf8_while_streaming() -> Result<()> {
        let bytes = b"name\nCaf\xe9\n";
        let mut reader = decode_reader(bytes.as_slice(), "windows-1252")?;
        let mut decoded = String::new();
        reader.read_to_string(&mut decoded)?;

        assert_eq!(decoded, "name\nCafé\n");
        Ok(())
    }

    #[test]
    fn writes_each_download_chunk_before_reading_the_next_one() -> Result<()> {
        let write_started = Rc::new(Cell::new(false));
        let mut reader = WriteAwareReader {
            write_started: Rc::clone(&write_started),
            reads: 0,
        };
        let mut writer = WriteObserver {
            write_started,
            bytes: Vec::new(),
        };

        stream_download(&mut reader, &mut writer)?;

        assert_eq!(writer.bytes, b"firstsecond");
        Ok(())
    }

    #[test]
    fn download_body_error_includes_original_error_message() {
        let error = download_body_error(
            "https://example.test/data.csv",
            reqwest::StatusCode::OK,
            "text/csv",
            "gzip",
            io::Error::other("error decoding response body"),
        );

        let message = error.to_string();
        assert!(message.contains("error reading CSV response body"));
        assert!(message.contains("Content-Encoding: gzip"));
        assert!(message.contains("error decoding response body"));
    }

    /// A semicolon-delimited CSV containing quoted fields with embedded
    /// semicolons must still be parsed into a table with all records.
    #[test]
    fn do_read_parses_windows1152_enconde_semicolon_delimited_csv() -> Result<()> {
        let reader = CsvReader::new(test_client());

        let csv_path = fixture_path("renuncia-fiscal-informacoes-conceituais-2024.csv");
        let resource = CkanResource {
            id: "5d16743c-0f6b-411d-aa7c-734a24b02812".to_string(),
            url: csv_path.to_str().unwrap().to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
        };

        let result = reader.do_read(&resource)?;
        assert_eq!(result.rows_processed, 31, "should read all 31 data records");

        Ok(())
    }
}
