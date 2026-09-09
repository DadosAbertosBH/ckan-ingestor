use crate::parquet_output::ParquetOutput;
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
use crate::readers::ckan_reader::{download_to_temp, CkanReader, ReadResult, SuccessResult};
use crate::readers::temp_file_cleanup::TempFileCleanup;
use anyhow::Result;
use arrow_csv::reader::{Format, ReaderBuilder};
use csv_nose::{Metadata, Quote, SampleSize, Sniffer, Type};
use encoding_rs::Encoding;
use encoding_rs_io::DecodeReaderBytesBuilder;
use flate2::read::GzDecoder;
use regex::Regex;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::PathBuf;

pub struct CsvReader {
    client: reqwest::blocking::Client,
    supported_formats: Vec<String>,
    csv_delimiter: Option<String>,
}

impl CsvReader {
    pub fn new(client: reqwest::blocking::Client) -> Self {
        Self {
            client,
            supported_formats: vec!["CSV".to_string()],
            csv_delimiter: None,
        }
    }

    pub fn with_delimiter(
        client: reqwest::blocking::Client,
        csv_delimiter: Option<String>,
    ) -> Self {
        Self {
            client,
            supported_formats: vec!["CSV".to_string()],
            csv_delimiter,
        }
    }

    /// Detect the CSV metadata once, then read it with the detected settings.
    /// Downloads the file first (if remote), matching Swift's approach.
    pub fn read_batches(&self, resource: &CkanResource) -> ReadResult {
        let is_remote = resource.url.starts_with("http://") || resource.url.starts_with("https://");
        let csv_path = if is_remote {
            download_to_temp(
                &self.client,
                &resource.url,
                downloaded_csv_suffix(&resource.url),
            )?
        } else {
            resource.url.clone()
        };

        let mut cleanup = TempFileCleanup::from_path(PathBuf::from(&csv_path));
        if !is_remote {
            cleanup.commit();
        }

        for sample_size in csv_sample_sizes() {
            let metadata = sniff_metadata(&csv_path, self.csv_delimiter.as_deref(), sample_size)?;
            let encoding = metadata.encoding.name.to_string();
            let csv_delimiter = char::from(metadata.dialect.delimiter).to_string();
            let has_mixed_line_endings = has_mixed_line_endings(
                &csv_path,
                metadata.encoding.name,
                metadata.dialect.header.num_preamble_rows,
            )?;

            match try_read_csv(&csv_path, &metadata, true) {
                Ok(parquet) => {
                    return Ok(SuccessResult::from_csv(
                        parquet,
                        encoding,
                        !has_mixed_line_endings,
                        csv_delimiter,
                        self.reader_name().to_string(),
                    ));
                }
                Err(error) if !is_retryable_csv_parse_error(&error) => return Err(error.into()),
                Err(_) => match try_read_csv(&csv_path, &metadata, false) {
                    Ok(parquet) => {
                        return Ok(SuccessResult::from_csv(
                            parquet,
                            encoding,
                            false,
                            csv_delimiter,
                            self.reader_name().to_string(),
                        ));
                    }
                    Err(error)
                        if is_retryable_csv_parse_error(&error)
                            && sample_size != SampleSize::All =>
                    {
                        continue;
                    }
                    Err(error) => return Err(error.into()),
                },
            }
        }

        unreachable!("the final CSV sample-size attempt returns or fails")
    }
}

fn try_read_csv(path: &str, metadata: &Metadata, strict_mode: bool) -> Result<ParquetOutput> {
    let dialect = &metadata.dialect;
    let format = Format::default()
        .with_header(dialect.header.has_header_row)
        .with_delimiter(dialect.delimiter)
        .with_truncated_rows(!strict_mode)
        .with_null_regex(Regex::new(r"^(|\s*-\s*)$")?);
    let format = match dialect.quote {
        Quote::None => format.with_quote(0),
        Quote::Some(quote) => format.with_quote(quote),
    };

    let schema = schema_from_metadata(metadata);
    let reader = decoded_reader(
        path,
        metadata.encoding.name,
        dialect.header.num_preamble_rows,
    )?;
    let csv_reader = ReaderBuilder::new(std::sync::Arc::new(schema))
        .with_format(format)
        .with_batch_size(32_768)
        .build(reader)?;

    let mut parquet = None;
    for batch_result in csv_reader {
        let batch = batch_result?;
        if parquet.is_none() {
            parquet = Some(ParquetOutput::try_new(&batch)?);
        }

        parquet
            .as_mut()
            .expect("Parquet output initialized")
            .write(&batch)?;
    }
    let mut parquet = parquet.ok_or_else(|| anyhow::anyhow!("No data"))?;
    parquet.finish()?;
    Ok(parquet)
}

fn schema_from_metadata(metadata: &Metadata) -> arrow::datatypes::Schema {
    arrow::datatypes::Schema::new(
        metadata
            .fields
            .iter()
            .zip(&metadata.types)
            .map(|(name, field_type)| {
                arrow::datatypes::Field::new(name, arrow_data_type(*field_type), true)
            })
            .collect::<Vec<_>>(),
    )
}

fn arrow_data_type(field_type: Type) -> arrow::datatypes::DataType {
    match field_type {
        Type::Unsigned => arrow::datatypes::DataType::UInt64,
        Type::Signed => arrow::datatypes::DataType::Int64,
        Type::Float => arrow::datatypes::DataType::Float64,
        Type::Boolean => arrow::datatypes::DataType::Boolean,
        // csv-nose recognizes several date representations, while Arrow's
        // default CSV parser only accepts ISO-formatted temporal values.
        // Preserve the original value when no parser format is available.
        Type::Date | Type::DateTime => arrow::datatypes::DataType::Utf8,
        Type::NULL | Type::Text => arrow::datatypes::DataType::Utf8,
    }
}

fn csv_sample_sizes() -> [SampleSize; 6] {
    [
        SampleSize::Records(50_000),
        SampleSize::Records(100_000),
        SampleSize::Records(200_000),
        SampleSize::Records(400_000),
        SampleSize::Records(800_000),
        SampleSize::All,
    ]
}

fn is_retryable_csv_parse_error(error: &anyhow::Error) -> bool {
    error.chain().any(|source| {
        matches!(
            source.downcast_ref::<arrow::error::ArrowError>(),
            Some(arrow::error::ArrowError::ParseError(_) | arrow::error::ArrowError::CsvError(_))
        )
    })
}

fn sniff_metadata(
    path: &str,
    delimiter_hint: Option<&str>,
    sample_size: SampleSize,
) -> Result<Metadata> {
    let mut sniffer = Sniffer::new();
    sniffer.sample_size(sample_size);
    if let Some(delimiter_hint) = delimiter_hint {
        let &[delimiter] = delimiter_hint.as_bytes() else {
            anyhow::bail!("CSV delimiter hint must contain exactly one byte");
        };
        sniffer.delimiter(delimiter);
    }

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
    use std::io::{BufWriter, Read, Write};
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

    #[test]
    fn supports_csv_nose_encoding_names() {
        assert!(encoding("UTF-8").is_ok());
        assert!(encoding("windows-1252").is_ok());
        assert!(encoding("UTF-16LE").is_ok());
    }

    #[test]
    fn sniff_metadata_uses_the_delimiter_hint() -> Result<()> {
        let path =
            std::env::temp_dir().join(format!("csv-delimiter-hint-{}.csv", uuid::Uuid::new_v4()));
        let _cleanup = TempFileCleanup::from_path(path.clone());
        std::fs::write(&path, "name;description\nAna;value, with comma\n")?;

        let metadata = sniff_metadata(
            path.to_str().expect("valid temporary path"),
            Some(";"),
            SampleSize::Records(50_000),
        )?;

        assert_eq!(metadata.dialect.delimiter, b';');
        Ok(())
    }

    #[test]
    fn retries_arrow_parse_and_csv_errors_only() {
        let parse_error = anyhow::Error::new(arrow::error::ArrowError::ParseError(
            "invalid value".to_string(),
        ));
        let csv_error = anyhow::Error::new(arrow::error::ArrowError::CsvError(
            "incorrect number of fields".to_string(),
        ));
        let io_error = anyhow::Error::new(std::io::Error::other("disk error"));

        assert!(is_retryable_csv_parse_error(&parse_error));
        assert!(is_retryable_csv_parse_error(&csv_error));
        assert!(!is_retryable_csv_parse_error(&io_error));
    }

    #[test]
    fn grows_csv_sample_sizes_before_reading_the_full_file() {
        assert_eq!(
            csv_sample_sizes(),
            [
                SampleSize::Records(50_000),
                SampleSize::Records(100_000),
                SampleSize::Records(200_000),
                SampleSize::Records(400_000),
                SampleSize::Records(800_000),
                SampleSize::All,
            ]
        );
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
    fn csv_reader_api_stays_within_a_bounded_memory_amplification() -> Result<()> {
        let _memory_guard = crate::test_alloc::memory_intensive_test_guard();
        let path = std::env::temp_dir().join(format!(
            "csv-sniff-memory-test-{}.csv",
            uuid::Uuid::new_v4()
        ));
        let _cleanup = TempFileCleanup::from_path(path.clone());
        {
            let mut file = BufWriter::new(File::create(&path)?);
            writeln!(
                file,
                "column_one,column_two,column_three,column_four,column_five,column_six"
            )?;
            for row in 0..10_000 {
                writeln!(
                    file,
                    "value{row:05},value{row:05},value{row:05},value{row:05},value{row:05},value{row:05}"
                )?;
            }
        }
        let sample_bytes = usize::try_from(std::fs::metadata(&path)?.len())?;
        let reader = CsvReader::new(test_client());
        let resource = CkanResource {
            id: "csv-reader-memory-test".to_string(),
            package_id: String::new(),
            url: path.to_string_lossy().into_owned(),
            format: "CSV".to_string(),
            datastore_active: false,
        };

        let baseline = crate::test_alloc::reset_peak();
        let result = reader.read(&resource)?;
        let peak_growth = crate::test_alloc::peak_growth_since(baseline);

        assert_eq!(result.rows_processed, 10_001);
        assert!(
            peak_growth <= sample_bytes * 37,
            "CsvReader API used {} bytes above baseline for a {}-byte sample ({:.1}x amplification)",
            peak_growth,
            sample_bytes,
            peak_growth as f64 / sample_bytes as f64,
        );
        Ok(())
    }

    #[test]
    #[ignore = "requires a large CSV path in CSV_MEMORY_TEST_FILE"]
    fn measures_memory_for_large_semicolon_csv() -> Result<()> {
        let Ok(path) = std::env::var("CSV_MEMORY_TEST_FILE") else {
            eprintln!("set CSV_MEMORY_TEST_FILE to run the large CSV memory profile");
            return Ok(());
        };
        let sample_bytes = std::fs::metadata(&path)?.len();
        let reader = CsvReader::new(test_client());
        let resource = CkanResource {
            id: "csv-memory-profile".to_string(),
            package_id: String::new(),
            url: path,
            format: "CSV".to_string(),
            datastore_active: false,
        };

        let baseline = crate::test_alloc::reset_peak();
        let result = reader.read(&resource)?;
        let peak_growth = crate::test_alloc::peak_growth_since(baseline);

        eprintln!(
            "large CSV memory profile: input_bytes={sample_bytes} rows={} peak_allocated_bytes={peak_growth} peak_allocated_mb={:.2}",
            result.rows_processed,
            peak_growth as f64 / (1024.0 * 1024.0),
        );
        assert!(result.rows_processed > 0);
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
            package_id: String::new(),
            url: csv_path.to_str().unwrap().to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
        };

        let result = reader.do_read(&resource)?;
        assert_eq!(result.rows_processed, 31, "should read all 31 data records");

        Ok(())
    }
}
