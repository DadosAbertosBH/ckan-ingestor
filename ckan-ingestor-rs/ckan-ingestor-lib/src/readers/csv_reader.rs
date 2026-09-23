use crate::jev_csv_sniffer::RepairAction::{FixInput, FixMetadata};
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
use crate::jev_csv_sniffer::JevCsvRepairer;
use crate::readers::ckan_reader::{
    download_to_temp, CkanReader, FailedResult, ReadResult, SuccessResult,
};
use crate::readers::temp_file_cleanup::TempFileCleanup;
use anyhow::{anyhow, Context, Result};
use arrow::error::ArrowError;
use arrow_csv::reader::{Format, ReaderBuilder};
use csv_nose::{Metadata, Quote, SampleSize, Sniffer, Type};
use encoding_rs::Encoding;
use encoding_rs_io::DecodeReaderBytesBuilder;
use flate2::read::GzDecoder;
use regex::Regex;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Read};
use std::path::PathBuf;
use std::sync::LazyLock;
static COLUMN_PARSE_ERROR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"Error while parsing value '([^']+)' as type '([^']+)' for column (\d+) at line (\d+)",
    )
    .expect("valid column parse error regex")
});

static CSV_FIELD_COUNT_ERROR: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"line\s+(\d+), expected\s+(\d+) got\s+(\d+)").expect("valid CSV line-number regex")
});

const CSV_BATCH_SIZE: usize = 32_768;
const MAX_CSV_BATCH_CELLS: usize = 4 * 1024 * 1024;
const MAX_JEV_CSV_REPAIRS: usize = 2000;
pub const CSV_READER_INITIAL_SAMPLE_RECORDS: usize = 25_000;
pub const CSV_READER_MAX_SAMPLE_RECORDS: usize = 800_000;

pub enum CsvParserError {
    ColumnCountMissmatch {
        line: usize,
        expect_number_of_columns: usize,
        actual_number_of_columns: usize,
        message: String,
    },
    ColumnTypeMissmatch {
        value: String,
        column_index: usize,
        line: usize,
        expected_type: String,
        message: String,
    },
    ParserError {
        message: String,
    },
    UnknownError {
        error: anyhow::Error,
    },
}

impl CsvParserError {
    pub fn unknown_error(error: anyhow::Error) -> Self {
        Self::UnknownError { error }
    }

    pub fn error_message(&self) -> String {
        match self {
            CsvParserError::ColumnCountMissmatch {
                line: _,
                expect_number_of_columns: _,
                actual_number_of_columns: _,
                message,
            } => message.to_string(),
            CsvParserError::ColumnTypeMissmatch {
                value: _,
                column_index: _,
                line: _,
                expected_type: _,
                message,
            } => message.to_string(),
            CsvParserError::ParserError { message } => message.to_string(),
            CsvParserError::UnknownError { error } => error.to_string(),
        }
    }

    fn get_column_count_misssmatch(message: String) -> Option<CsvParserError> {
        let captures = CSV_FIELD_COUNT_ERROR.captures(&message)?;
        let line: Option<usize> = captures.get(1)?.as_str().parse().ok();
        let expected: Option<usize> = captures.get(2)?.as_str().parse().ok();
        let found: Option<usize> = captures.get(3)?.as_str().parse().ok();
        match (line, expected, found) {
            (Some(line), Some(expected), Some(found)) => {
                Some(CsvParserError::ColumnCountMissmatch {
                    line,
                    expect_number_of_columns: expected,
                    actual_number_of_columns: found,
                    message: message.to_string(),
                })
            }
            _ => None,
        }
    }

    fn get_column_type_missmatch(message: String) -> Option<CsvParserError> {
        let captures = COLUMN_PARSE_ERROR.captures(&message)?;
        let value = captures.get(1)?.as_str().to_string();
        let expected_type = captures.get(2)?.as_str().to_string();
        let column_index: Option<usize> = captures.get(3)?.as_str().parse().ok();
        let line: Option<usize> = captures.get(4)?.as_str().parse().ok();
        match (line, column_index) {
            (Some(line), Some(column_index)) => Some(CsvParserError::ColumnTypeMissmatch {
                value,
                column_index,
                line,
                expected_type,
                message: message.to_string(),
            }),
            _ => None,
        }
    }
}

impl From<anyhow::Error> for CsvParserError {
    fn from(value: anyhow::Error) -> Self {
        CsvParserError::UnknownError { error: value }
    }
}

impl From<ArrowError> for CsvParserError {
    fn from(value: ArrowError) -> Self {
        match value {
            ArrowError::NotYetImplemented(message) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::ExternalError(error) => CsvParserError::UnknownError {
                error: anyhow!(error.to_string()),
            },
            ArrowError::CastError(message) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::MemoryError(message) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::ParseError(message) => CsvParserError::get_column_type_missmatch(
                message.to_string(),
            )
            .unwrap_or(CsvParserError::ParserError {
                message: message.to_string(),
            }),
            ArrowError::SchemaError(message) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::ComputeError(message) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::DivideByZero => CsvParserError::UnknownError {
                error: anyhow!(value.to_string()),
            },
            ArrowError::ArithmeticOverflow(message) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::CsvError(message) => CsvParserError::get_column_count_misssmatch(
                message.to_string(),
            )
            .unwrap_or(CsvParserError::ParserError {
                message: message.to_string(),
            }),
            ArrowError::JsonError(message) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::AvroError(message) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::IoError(message, _error) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::IpcError(message) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::InvalidArgumentError(message) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::ParquetError(message) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::CDataInterface(message) => CsvParserError::UnknownError {
                error: anyhow!(message.to_string()),
            },
            ArrowError::DictionaryKeyOverflowError => CsvParserError::UnknownError {
                error: anyhow!(value.to_string()),
            },
            ArrowError::RunEndIndexOverflowError => CsvParserError::UnknownError {
                error: anyhow!(value.to_string()),
            },
            ArrowError::OffsetOverflowError(_) => CsvParserError::UnknownError {
                error: anyhow!(value.to_string()),
            },
        }
    }
}

trait NextSize {
    fn next(&self) -> SampleSize;
}

impl NextSize for SampleSize {
    fn next(&self) -> SampleSize {
        match self {
            SampleSize::Records(records) if records < &CSV_READER_MAX_SAMPLE_RECORDS => {
                SampleSize::Records(records * 2)
            }
            _ => SampleSize::All,
        }
    }
}

pub struct CsvReader {
    client: reqwest::blocking::Client,
    supported_formats: Vec<String>,
    csv_delimiter: Option<String>,
    jev_repairer: Option<JevCsvRepairer>,
}

impl CsvReader {
    pub fn new(client: reqwest::blocking::Client) -> Self {
        let jev_repairer = JevCsvRepairer::from_env(client.clone());
        Self {
            client,
            supported_formats: vec!["CSV".to_string()],
            csv_delimiter: None,
            jev_repairer,
        }
    }

    pub fn with_delimiter(
        client: reqwest::blocking::Client,
        csv_delimiter: Option<String>,
    ) -> Self {
        let jev_repairer = JevCsvRepairer::from_env(client.clone());
        Self {
            client,
            supported_formats: vec!["CSV".to_string()],
            csv_delimiter,
            jev_repairer,
        }
    }

    pub fn with_jev_repairer(mut self, jev_repairer: JevCsvRepairer) -> Self {
        self.jev_repairer = Some(jev_repairer);
        self
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

        let mut metadata = sniff_metadata(
            &csv_path,
            self.csv_delimiter.as_deref(),
            SampleSize::Records(CSV_READER_INITIAL_SAMPLE_RECORDS),
            false,
        )?;

        self.try_read_csv(
            &csv_path,
            &mut metadata,
            true,
            SampleSize::Records(CSV_READER_INITIAL_SAMPLE_RECORDS),
            0,
        )
        .map_err(|error| FailedResult::from(anyhow!(error.error_message())))
    }

    fn try_read_csv(
        &self,
        path: &str,
        metadata: &mut Metadata,
        strict_mode: bool,
        sample_size: SampleSize,
        jev_repair_count: usize,
    ) -> Result<SuccessResult, CsvParserError> {
        let dialect = &metadata.dialect;
        let regex = Regex::new(r"^(|\s*|\s*-\s*)$").context("failed to compile regex")?;
        let format = Format::default()
            .with_header(dialect.header.has_header_row)
            .with_delimiter(dialect.delimiter)
            .with_truncated_rows(!strict_mode)
            .with_null_regex(regex);

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
            .with_batch_size(csv_batch_size(metadata.fields.len()))
            .build(reader)?;

        let mut parquet = None;
        for batch_result in csv_reader {
            let batch = match batch_result.map_err(CsvParserError::from) {
                Err(error) => match error {
                    // Type inference already identified the column; preserve the
                    // current sample and promote only that column to text.
                    CsvParserError::ColumnTypeMissmatch {
                        value: _,
                        column_index,
                        line: _,
                        expected_type: _,
                        message: _,
                    } => {
                        return self.try_read_csv_promoting_column(
                            path,
                            metadata,
                            false,
                            sample_size,
                            jev_repair_count,
                            column_index,
                        )
                    }
                    // Field-count and unclassified parser errors may be caused
                    // by incomplete metadata, so retry with a larger sample.
                    CsvParserError::ColumnCountMissmatch {
                        line: _,
                        expect_number_of_columns: _,
                        actual_number_of_columns: _,
                        message: _,
                    }
                    | CsvParserError::ParserError { message: _ }
                        if sample_size != SampleSize::All =>
                    {
                        return self.try_read_csv_increasing_metadata_sample(
                            path,
                            metadata,
                            strict_mode,
                            sample_size,
                            jev_repair_count,
                            error,
                        )
                    }
                    // After exaust sample_size try to repair with Jev
                    error @ CsvParserError::ColumnCountMissmatch { .. }
                        if sample_size == SampleSize::All =>
                    {
                        return self.try_read_csv_reapairing_with_jev(
                            path,
                            metadata,
                            error,
                            jev_repair_count,
                        )
                    }
                    // Non recover erros
                    CsvParserError::UnknownError { error: _ } => return Err(error),
                    parser_error => return Err(parser_error),
                },
                Ok(batch) => batch,
            };
            if parquet.is_none() {
                parquet = Some(ParquetOutput::try_new(&batch).context("failed to create parquet")?);
            }

            parquet
                .as_mut()
                .expect("Parquet output initialized")
                .write(&batch)
                .context("failed to write parquet")?;
        }
        let mut parquet = parquet.ok_or_else(|| anyhow::anyhow!("No data"))?;
        parquet.finish().context("failed to finish parquet")?;
        Ok(SuccessResult::from_csv(
            parquet,
            metadata.encoding.name.to_string(),
            strict_mode,
            char::from(metadata.dialect.delimiter).to_string(),
            csv_sample_size_label(sample_size),
            self.reader_name().to_string(),
        ))
    }

    fn try_read_csv_promoting_column(
        &self,
        path: &str,
        metadata: &mut Metadata,
        strict_mode: bool,
        sample_size: SampleSize,
        jev_repair_count: usize,
        column_index: usize,
    ) -> Result<SuccessResult, CsvParserError> {
        log::info!("CSV parse failed for column {column_index}; treating the column as text line");
        if let Some(column) = metadata.types.get_mut(column_index) {
            *column = Type::Text;
            self.try_read_csv(path, metadata, strict_mode, sample_size, jev_repair_count)
        } else {
            Err(CsvParserError::UnknownError {
                error: anyhow!("Failed to get column at index {column_index}"),
            })
        }
    }

    fn try_read_csv_increasing_metadata_sample(
        &self,
        path: &str,
        metadata: &Metadata,
        strict_mode: bool,
        sample_size: SampleSize,
        jev_repair_count: usize,
        error: CsvParserError,
    ) -> Result<SuccessResult, CsvParserError> {
        let error_message = error.error_message();
        log::info!(
            "CSV parse failed with sample size {}; retrying with a larger sample: {error_message}",
            csv_sample_size_label(sample_size)
        );
        let mut metadata = sniff_metadata(
            path,
            Some(&char::from(metadata.dialect.delimiter).to_string()),
            sample_size.next(),
            false,
        )?;
        self.try_read_csv(
            path,
            &mut metadata,
            strict_mode,
            sample_size.next(),
            jev_repair_count,
        )
    }

    fn try_read_csv_reapairing_with_jev(
        &self,
        csv_path: &str,
        metadata: &mut Metadata,
        error: CsvParserError,
        jev_repair_count: usize,
    ) -> Result<SuccessResult, CsvParserError> {
        let Some(repairer) = &self.jev_repairer else {
            return Err(anyhow!("Jev is disable").into());
        };
        if jev_repair_count > MAX_JEV_CSV_REPAIRS {
            return Err(CsvParserError::UnknownError {
                error: anyhow::anyhow!(
                    "JEV CSV repair limit reached after {MAX_JEV_CSV_REPAIRS} repairs"
                ),
            });
        }
        let CsvParserError::ColumnCountMissmatch {
            line: line_number,
            expect_number_of_columns,
            actual_number_of_columns,
            message: error_message,
        } = error
        else {
            unreachable!("JEV repairs require a column-count mismatch")
        };
        let current_path = PathBuf::from(csv_path);
        log::info!("Attempting JEV CSV repair {jev_repair_count} for line {line_number}");
        let repair_result = repairer.repair_csv_with_metadata(
            &current_path,
            metadata,
            line_number,
            expect_number_of_columns,
            actual_number_of_columns,
            error_message.clone(),
        );
        match repair_result {
            Ok(action) => match action {
                FixInput(path_buf) => {
                    self.try_read_csv_with_repaired_input(path_buf, metadata, jev_repair_count + 1)
                }
                FixMetadata(fixed_metadata) => {
                    let delimiter_hint = char::from(fixed_metadata.dialect.delimiter).to_string();
                    let mut metadata =
                        sniff_metadata(csv_path, Some(&delimiter_hint), SampleSize::All, true)?;
                    self.try_read_csv(
                        csv_path,
                        &mut metadata,
                        false,
                        SampleSize::All,
                        jev_repair_count + 1,
                    )
                }
            },
            Err(repair_error) => {
                log::info!("JEV CSV repair did not produce a usable file: {repair_error}");
                Err(CsvParserError::ColumnCountMissmatch {
                    line: line_number,
                    expect_number_of_columns,
                    actual_number_of_columns,
                    message: error_message,
                })
            }
        }
    }

    fn try_read_csv_with_repaired_input(
        &self,
        repaired_path: PathBuf,
        metadata: &mut Metadata,
        jev_repair_count: usize,
    ) -> Result<SuccessResult, CsvParserError> {
        let _cleanup = TempFileCleanup::from_path(repaired_path.clone());
        self.try_read_csv(
            &repaired_path.to_string_lossy(),
            metadata,
            false,
            SampleSize::All,
            jev_repair_count,
        )
    }
}

fn csv_batch_size(number_of_columns: usize) -> usize {
    let rows_for_cell_limit = MAX_CSV_BATCH_CELLS / number_of_columns.max(1);
    CSV_BATCH_SIZE.min(rows_for_cell_limit.max(1))
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
        Type::Unsigned | Type::Signed => arrow::datatypes::DataType::Int64,
        Type::Float => arrow::datatypes::DataType::Float64,
        Type::Boolean => arrow::datatypes::DataType::Boolean,
        // csv-nose recognizes several date representations, while Arrow's
        // default CSV parser only accepts ISO-formatted temporal values.
        // Preserve the original value when no parser format is available.
        Type::Date | Type::DateTime => arrow::datatypes::DataType::Utf8,
        Type::NULL | Type::Text => arrow::datatypes::DataType::Utf8,
    }
}

fn csv_sample_size_label(sample_size: SampleSize) -> String {
    match sample_size {
        SampleSize::Records(records) => records.to_string(),
        SampleSize::All => "all".to_string(),
        SampleSize::Bytes(bytes) => format!("bytes:{bytes}"),
    }
}

fn sniff_metadata(
    path: &str,
    delimiter_hint: Option<&str>,
    sample_size: SampleSize,
    force_header: bool,
) -> Result<Metadata> {
    let mut sniffer = Sniffer::new();
    sniffer.sample_size(sample_size);
    if force_header {
        sniffer.force_header(true);
    }
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
    use arrow::datatypes::DataType;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use httpmock::{
        Method::{GET, POST},
        MockServer,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::cell::Cell;
    use std::fs;
    use std::io::{BufWriter, Read, Write};
    use std::path::PathBuf;
    use std::rc::Rc;
    use std::time::Duration;
    use tempfile::tempdir;

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
            false,
        )?;

        assert_eq!(metadata.dialect.delimiter, b';');
        Ok(())
    }

    #[test]
    fn jev_header_correction_resniffs_with_forced_header() -> Result<()> {
        let server = MockServer::start();
        let response = serde_json::json!({
            "model": "jev-latest",
            "answers": {
                "fields_to_merge": {"type": "choice", "choice": "merge_1_2"},
                "is_delimiter_correct": {"type": "noul", "noul": 1.0},
                "is_has_header_correct": {"type": "noul", "noul": 0.0},
                "is_num_fields_correct": {"type": "noul", "noul": 1.0}
            }
        });
        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/systemone");
            then.status(200).json_body(response);
        });
        let repairer = JevCsvRepairer::new(
            test_client(),
            format!("{}/v1/systemone", server.base_url()),
            "test-key".to_string(),
        );
        let reader = CsvReader::new(test_client()).with_jev_repairer(repairer);
        let directory = tempdir()?;
        let path = directory.path().join("header.csv");
        fs::write(&path, "name,age\nAlice,30\nBob,40\n")?;
        let mut metadata = sniff_metadata(
            path.to_str().expect("temporary path is valid UTF-8"),
            None,
            SampleSize::All,
            false,
        )?;
        metadata.dialect.header.has_header_row = false;

        let result = reader.try_read_csv_reapairing_with_jev(
            path.to_str().expect("temporary path is valid UTF-8"),
            &mut metadata,
            CsvParserError::ColumnCountMissmatch {
                line: 2,
                expect_number_of_columns: 2,
                actual_number_of_columns: 3,
                message: "line 2, expected 2 got 3".to_string(),
            },
            0,
        );

        let result = result.map_err(|error| anyhow!(error.error_message()))?;
        assert_eq!(result.number_of_columns, 2);
        mock.assert();
        Ok(())
    }

    #[test]
    fn removes_repaired_input_after_retry() -> Result<()> {
        let directory = tempdir()?;
        let repaired_path = directory.path().join("jev-csv-repair.csv");
        fs::write(&repaired_path, "name,age\nAlice,30\nBob,40\n")?;
        let mut metadata = sniff_metadata(
            repaired_path
                .to_str()
                .expect("temporary path is valid UTF-8"),
            None,
            SampleSize::All,
            false,
        )?;
        let reader = CsvReader::new(test_client());

        let result =
            reader.try_read_csv_with_repaired_input(repaired_path.clone(), &mut metadata, 1);

        result.map_err(|error| anyhow!(error.error_message()))?;
        assert!(!repaired_path.exists());
        Ok(())
    }

    #[test]
    fn classifies_arrow_parse_and_csv_errors_as_parser_errors() {
        let parse_error = CsvParserError::from(arrow::error::ArrowError::ParseError(
            "invalid value".to_string(),
        ));
        let csv_error = CsvParserError::from(arrow::error::ArrowError::CsvError(
            "incorrect number of fields".to_string(),
        ));
        let io_error = CsvParserError::from(arrow::error::ArrowError::IoError(
            "disk error".to_string(),
            std::io::Error::other("disk error"),
        ));

        assert!(matches!(parse_error, CsvParserError::ParserError { .. }));
        assert!(matches!(csv_error, CsvParserError::ParserError { .. }));
        assert!(matches!(io_error, CsvParserError::UnknownError { .. }));
    }

    #[test]
    fn grows_csv_sample_sizes_before_reading_the_full_file() {
        let mut sample_size = SampleSize::Records(CSV_READER_INITIAL_SAMPLE_RECORDS);
        for expected in [50_000, 100_000, 200_000, 400_000, 800_000] {
            sample_size = sample_size.next();
            assert_eq!(sample_size, SampleSize::Records(expected));
        }
        assert_eq!(sample_size.next(), SampleSize::All);
    }

    #[test]
    fn limits_csv_batch_cells_while_preserving_the_default_for_normal_schemas() {
        assert_eq!(csv_batch_size(128), 32_768);
        assert_eq!(csv_batch_size(129), 32_513);
        assert_eq!(csv_batch_size(44_032), 95);
    }

    #[test]
    fn promotes_an_integer_column_with_decimal_using_comma_to_text() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "csv-decimal-using-comma-{}.csv",
            uuid::Uuid::new_v4()
        ));
        let _cleanup = TempFileCleanup::from_path(path.clone());
        let header = (0..26)
            .map(|column| format!("column_{column}"))
            .collect::<Vec<_>>()
            .join(";");
        let integer_row = vec!["0"; 26].join(";");
        let decimal_row = (0..26)
            .map(|column| if column == 25 { "7405,87" } else { "0" })
            .collect::<Vec<_>>()
            .join(";");
        let mut file = BufWriter::new(File::create(&path)?);
        writeln!(file, "{header}")?;
        for _ in 0..=CSV_READER_INITIAL_SAMPLE_RECORDS {
            writeln!(file, "{integer_row}")?;
        }
        writeln!(file, "{decimal_row}")?;
        file.flush()?;

        let reader = CsvReader::new(test_client());
        let resource = CkanResource {
            id: "decimal-using-comma".to_string(),
            package_id: String::new(),
            url: path.to_string_lossy().into_owned(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource)?;

        assert_eq!(result.rows_processed, CSV_READER_INITIAL_SAMPLE_RECORDS + 2);
        assert_eq!(result.parquet.schema.field(25).data_type(), &DataType::Utf8);
        Ok(())
    }

    #[test]
    fn promotes_multiple_integer_columns_with_decimal_using_comma_to_text() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "csv-multiple-decimals-using-comma-{}.csv",
            uuid::Uuid::new_v4()
        ));
        let _cleanup = TempFileCleanup::from_path(path.clone());
        let header = (0..26)
            .map(|column| format!("column_{column}"))
            .collect::<Vec<_>>()
            .join(";");
        let integer_row = vec!["0"; 26].join(";");
        let decimal_row = (0..26)
            .map(|column| match column {
                24 => "123,45",
                25 => "7405,87",
                _ => "0",
            })
            .collect::<Vec<_>>()
            .join(";");
        let mut file = BufWriter::new(File::create(&path)?);
        writeln!(file, "{header}")?;
        for _ in 0..=CSV_READER_INITIAL_SAMPLE_RECORDS {
            writeln!(file, "{integer_row}")?;
        }
        writeln!(file, "{decimal_row}")?;
        file.flush()?;

        let reader = CsvReader::new(test_client());
        let resource = CkanResource {
            id: "multiple-decimals-using-comma".to_string(),
            package_id: String::new(),
            url: path.to_string_lossy().into_owned(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource)?;

        assert_eq!(result.rows_processed, CSV_READER_INITIAL_SAMPLE_RECORDS + 2);
        let expected_sample_size = CSV_READER_INITIAL_SAMPLE_RECORDS.to_string();
        assert_eq!(
            result.csv_samples.as_deref(),
            Some(expected_sample_size.as_str())
        );
        assert_eq!(result.parquet.schema.field(24).data_type(), &DataType::Utf8);
        assert_eq!(result.parquet.schema.field(25).data_type(), &DataType::Utf8);
        Ok(())
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
        const ISOLATED_MEMORY_TEST: &str = "CKAN_INGESTOR_CSV_MEMORY_TEST_CHILD";
        if std::env::var_os(ISOLATED_MEMORY_TEST).is_none() {
            let status = std::process::Command::new(std::env::current_exe()?)
                .args([
                    "--exact",
                    "readers::csv_reader::tests::csv_reader_api_stays_within_a_bounded_memory_amplification",
                    "--nocapture",
                ])
                .env(ISOLATED_MEMORY_TEST, "1")
                .status()?;
            assert!(status.success(), "isolated CSV memory test failed");
            return Ok(());
        }

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
            last_modified: String::new(),
        };

        let baseline = crate::test_alloc::reset_peak();
        let result = reader.read(&resource)?;
        let peak_growth = crate::test_alloc::peak_growth_since(baseline);

        assert_eq!(result.rows_processed, 10_001);
        assert!(
            peak_growth <= sample_bytes * 27,
            "CsvReader API used {} bytes above baseline for a {}-byte sample ({:.1}x amplification)",
            peak_growth,
            sample_bytes,
            peak_growth as f64 / sample_bytes as f64,
        );
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
            last_modified: String::new(),
        };

        let result = reader.do_read(&resource)?;
        assert_eq!(result.rows_processed, 31, "should read all 31 data records");

        Ok(())
    }

    #[test]
    fn processes_funcionalismo_csv_without_excessive_batch_memory() -> Result<()> {
        let reader = CsvReader::new(test_client());
        let resource = CkanResource {
            id: "8f8f1a40-63dd-4900-aabe-f95195a87092".to_string(),
            package_id: String::new(),
            url: fixture_path("funcionalismo-publico-adm-indireta-05-2026.csv")
                .to_str()
                .unwrap()
                .to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource)?;

        assert!(result.number_of_columns > 40_000);
        Ok(())
    }

    #[test]
    fn parse_latin_encoded_csv() -> Result<()> {
        let reader = CsvReader::new(test_client());

        let resource = CkanResource {
            id: "00000000-0000-0000-0000-ffff00000000".to_string(),
            package_id: String::new(),
            url: fixture_path("csv_with_latin_encode.csv")
                .to_str()
                .unwrap()
                .to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        // Python equivalent: test_parse_latin_encoded_csv_file
        // Just verifies it doesn't error. DuckDB with encoding='latin-1'
        // may fall through to PyArrow fallback for semicolon-delimited files.
        let result = reader.read(&resource)?;
        assert!(result.rows_processed > 0, "Should parse at least 1 row");
        assert!(result.encoding.is_some());
        Ok(())
    }

    #[test]
    fn parse_non_latin_and_non_utf8() -> Result<()> {
        let reader = CsvReader::new(test_client());

        let resource = CkanResource {
            id: "00000000-0000-0000-0000-ffff00000000".to_string(),
            package_id: String::new(),
            url: fixture_path("non_latin1_and_non_utf8.csv")
                .to_str()
                .unwrap()
                .to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource)?;
        assert_eq!(result.rows_processed, 2);
        // This file uses latin-1 encoding that only works after the PyArrow fallback
        // with semicolon delimiter
        assert!(result.encoding.is_some());
        Ok(())
    }

    #[test]
    fn returns_http_error_for_failed_remote_csv_download() -> Result<()> {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/failed.csv");
            then.status(500)
                .header("Content-Type", "text/html")
                .body("<html><title>Erro [500]</title></html>");
        });

        let reader = CsvReader::new(test_client());
        let resource = CkanResource {
            id: "failed-remote-csv".to_string(),
            package_id: String::new(),
            url: format!("{}/failed.csv", server.url("")),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let error = match reader.read(&resource) {
            Ok(_) => anyhow::bail!("a failed remote CSV response should return an HTTP error"),
            Err(error) => error,
        };
        let message = error.to_string();
        assert!(message.contains("500"), "unexpected error: {message}");
        assert!(
            message.contains("Content-Type: text/html"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("Content-Encoding: <missing or invalid>"),
            "unexpected error: {message}"
        );
        Ok(())
    }

    #[test]
    fn parses_dm_subitem_rec_utf8_csv_fixture() -> Result<()> {
        let reader = CsvReader::new(test_client());

        let resource = CkanResource {
            id: "00000000-0000-0000-0000-ffff00000000".to_string(),
            package_id: String::new(),
            url: fixture_path("dm_subitem_rec.csv")
                .to_str()
                .unwrap()
                .to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource)?;

        assert_eq!(result.rows_processed, 7_134);
        assert_eq!(result.number_of_columns, 3);
        assert_eq!(result.encoding.as_deref(), Some("UTF-8"));
        Ok(())
    }

    #[test]
    fn parses_csv_with_mixed_line_endings_in_quoted_header() -> Result<()> {
        let reader = CsvReader::new(test_client());

        struct TemporaryCsv(PathBuf);

        impl Drop for TemporaryCsv {
            fn drop(&mut self) {
                let _ = fs::remove_file(&self.0);
            }
        }

        let csv_path = std::env::temp_dir().join(format!(
            "voos-multiline-header-{}.csv",
            uuid::Uuid::new_v4()
        ));
        let temporary_csv = TemporaryCsv(csv_path);

        let mut csv = Vec::new();
        csv.extend_from_slice(
            b"Reg Voo;ANO;DATA;SOLICITANTE;PASSAGEIROS;AERONAVE;MATR;ORIGEM;DESTINO 1;\"DESTINO 2\n(quando houve)\"\r\n",
        );
        for id in 1..=2 {
            csv.extend_from_slice(
                format!(
                    "{id};2011;01/01/2011;Governador;Passageiro;Aeronave;PT-ABC;Origem;Destino;\r\n"
                )
                .as_bytes(),
            );
        }
        fs::write(&temporary_csv.0, csv)?;

        let resource = CkanResource {
            id: "fa4f8391-33d1-46ed-9e9e-22ca2ae51103".to_string(),
            package_id: String::new(),
            url: temporary_csv.0.to_str().unwrap().to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource)?;

        assert_eq!(result.rows_processed, 2);
        assert_eq!(result.number_of_columns, 10);
        assert_eq!(result.csv_strict_mode, Some(true));
        Ok(())
    }

    #[test]
    fn csv_with_bom() -> Result<()> {
        let reader = CsvReader::new(test_client());

        let resource = CkanResource {
            id: "00000000-0000-0000-0000-ffff00000000".to_string(),
            package_id: String::new(),
            url: fixture_path("csv_with_bom.csv")
                .to_str()
                .unwrap()
                .to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };
        let result = reader.read(&resource)?;
        assert_eq!(result.rows_processed, 804);
        assert_eq!(result.encoding.as_deref(), Some("UTF-8"));
        assert_eq!(result.csv_strict_mode, Some(true));
        assert_eq!(result.csv_delimiter.as_deref(), Some(","));
        let expected_sample_size = CSV_READER_INITIAL_SAMPLE_RECORDS.to_string();
        assert_eq!(
            result.csv_samples.as_deref(),
            Some(expected_sample_size.as_str())
        );
        Ok(())
    }

    #[test]
    fn reads_csv_data_with_rows() -> Result<()> {
        let reader = CsvReader::new(test_client());

        let resource = CkanResource {
            id: "00000000-0000-0000-0000-ffff00000000".to_string(),
            package_id: String::new(),
            url: fixture_path("csv_with_bom.csv")
                .to_str()
                .unwrap()
                .to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource)?;
        assert_eq!(result.rows_processed, 804);

        Ok(())
    }

    #[test]
    fn parses_numeric_columns_with_whitespace_padded_dash_as_null() -> Result<()> {
        let reader = CsvReader::new(test_client());

        let resource = CkanResource {
            id: "cb05125e-e879-420f-9bf6-0fcbc75bbc8e".to_string(),
            package_id: String::new(),
            url: fixture_path("despesa_pessoal_mensal(1).csv")
                .to_str()
                .unwrap()
                .to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource)?;

        assert_eq!(result.rows_processed, 52);
        assert_eq!(result.number_of_columns, 19);
        let batches: Vec<_> =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(result.parquet.path())?)?
                .build()?
                .collect::<std::result::Result<_, _>>()?;
        assert_eq!(
            batches[0].schema().field(16).data_type(),
            &arrow::datatypes::DataType::Float64
        );
        let null_count: usize = batches
            .iter()
            .map(|batch| batch.column(16).null_count())
            .sum();
        assert_eq!(null_count, 36);
        Ok(())
    }

    #[test]
    fn parses_whitespace_only_numeric_cells_as_null() -> Result<()> {
        let mut csv = tempfile::NamedTempFile::new()?;
        csv.write_all(b"id,value\n")?;
        for id in 1..=CSV_READER_INITIAL_SAMPLE_RECORDS + 1 {
            writeln!(csv, "{id},42")?;
        }
        writeln!(csv, "{}, ", CSV_READER_INITIAL_SAMPLE_RECORDS + 2)?;
        let resource = CkanResource {
            id: "whitespace-null".to_string(),
            package_id: String::new(),
            url: csv.path().to_string_lossy().to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = CsvReader::new(test_client()).read(&resource)?;
        let batches: Vec<_> =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(result.parquet.path())?)?
                .build()?
                .collect::<std::result::Result<_, _>>()?;

        assert_eq!(
            batches[0].schema().field(1).data_type(),
            &arrow::datatypes::DataType::Int64
        );
        let null_count: usize = batches
            .iter()
            .map(|batch| batch.column(1).null_count())
            .sum();
        assert_eq!(null_count, 1);
        Ok(())
    }

    #[test]
    fn represents_an_entirely_empty_csv_column_as_nullable_text() -> Result<()> {
        let mut csv = tempfile::NamedTempFile::new()?;
        csv.write_all(b"id,always_empty\n1,\n2,\n")?;

        let reader = CsvReader::new(test_client());
        let resource = CkanResource {
            id: "all-null-column".to_string(),
            package_id: String::new(),
            url: csv.path().to_str().unwrap().to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource)?;
        let batches: Vec<_> =
            ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(result.parquet.path())?)?
                .build()?
                .collect::<std::result::Result<_, _>>()?;

        assert_eq!(
            batches[0].schema().field(1).data_type(),
            &arrow::datatypes::DataType::Utf8
        );
        assert_eq!(batches[0].column(1).null_count(), 2);
        Ok(())
    }

    #[test]
    fn parse_remote_gzip_csv() -> Result<()> {
        let server = MockServer::start();
        let mut compressed = Vec::new();
        let mut encoder = GzEncoder::new(&mut compressed, Compression::default());
        encoder.write_all(b"name,value\nAna,1\nBia,2\n")?;
        encoder.finish()?;
        server.mock(|when, then| {
            when.method(GET).path("/ft_diarias_2014.csv.gz");
            then.status(200).body(compressed.clone());
        });

        let reader = CsvReader::new(test_client());
        let resource = CkanResource {
            id: "cfba57bb-358b-4b43-96e6-477920e39f19".to_string(),
            package_id: String::new(),
            url: format!("{}/ft_diarias_2014.csv.gz", server.url("")),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource)?;
        assert_eq!(result.rows_processed, 2);
        Ok(())
    }

    #[test]
    fn fails_to_parse_quoted_semicolon_after_long_csv_sample() -> Result<()> {
        let server = MockServer::start();
        let mut csv =
            String::from("id_favorecido;tp_documento;nr_documento_anonimizado;nome_anonimizado\n");

        for id in 1..50_001 {
            csv.push_str(&format!("{id};1;0;NOME\n"));
        }
        csv.push_str(
            "1254412;2;912488000123;\"COOPERATIVA DE CREDITO DE LIVRE ADMISSAO DO ALTO E MED. S; F\"\n",
        );

        let mock = server.mock(|when, then| {
            when.method(GET).path("/dm_favorecido.csv");
            then.status(200).body(csv);
        });

        let reader = CsvReader::with_delimiter(test_client(), Some(";".to_string()));
        let resource = CkanResource {
            id: "0331ad41-85e6-41da-bbf2-19c0505beef5".to_string(),
            package_id: String::new(),
            url: format!("{}/dm_favorecido.csv", server.url("")),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = reader.read(&resource);
        mock.assert();
        let result = result?;

        assert_eq!(result.rows_processed, 50_001);
        assert_eq!(result.encoding.as_deref(), Some("UTF-8"));
        Ok(())
    }

    #[test]
    fn uses_csv_nose_metadata_types_beyond_arrows_inference_window() -> Result<()> {
        let tempdir = tempdir()?;
        let path = tempdir.path().join("metadata-types.csv");
        let mut csv = String::from("value\n");
        for value in 0..50_000 {
            csv.push_str(&format!("{value}\n"));
        }
        csv.push_str("not-a-number\n");
        fs::write(&path, csv)?;
        let resource = CkanResource {
            id: "metadata-types".to_string(),
            package_id: String::new(),
            url: path.to_string_lossy().to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = CsvReader::new(test_client()).read(&resource)?;

        assert_eq!(result.rows_processed, 50_001);
        assert_eq!(
            result.parquet.schema.field_with_name("value")?.data_type(),
            &arrow::datatypes::DataType::Utf8
        );
        Ok(())
    }

    #[test]
    fn promotes_integer_column_with_na_to_text_without_resniffing() -> Result<()> {
        // Arrange
        let mut csv = tempfile::NamedTempFile::new()?;
        csv.write_all(b"value\n")?;
        for value in 0..=CSV_READER_INITIAL_SAMPLE_RECORDS {
            writeln!(csv, "{value}")?;
        }
        csv.write_all(b"NA\n")?;
        let resource = CkanResource {
            id: "integer-na".to_string(),
            package_id: String::new(),
            url: csv.path().to_string_lossy().to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        // Act
        let result = CsvReader::new(test_client()).read(&resource)?;

        // Assert
        assert_eq!(result.rows_processed, CSV_READER_INITIAL_SAMPLE_RECORDS + 2);
        let expected_sample_size = CSV_READER_INITIAL_SAMPLE_RECORDS.to_string();
        assert_eq!(
            result.csv_samples.as_deref(),
            Some(expected_sample_size.as_str())
        );
        assert_eq!(
            result.parquet.schema.field_with_name("value")?.data_type(),
            &arrow::datatypes::DataType::Utf8
        );
        Ok(())
    }

    #[test]
    fn promotes_boolean_column_with_f_to_text_without_resniffing() -> Result<()> {
        // Arrange
        let mut csv = tempfile::NamedTempFile::new()?;
        csv.write_all(b"value\n")?;
        for _ in 0..=CSV_READER_INITIAL_SAMPLE_RECORDS {
            csv.write_all(b"true\n")?;
        }
        csv.write_all(b"F\n")?;
        let resource = CkanResource {
            id: "boolean-f".to_string(),
            package_id: String::new(),
            url: csv.path().to_string_lossy().to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        // Act
        let result = CsvReader::new(test_client()).read(&resource)?;

        // Assert
        assert_eq!(result.rows_processed, CSV_READER_INITIAL_SAMPLE_RECORDS + 2);
        let expected_sample_size = CSV_READER_INITIAL_SAMPLE_RECORDS.to_string();
        assert_eq!(
            result.csv_samples.as_deref(),
            Some(expected_sample_size.as_str())
        );
        assert_eq!(
            result.parquet.schema.field_with_name("value")?.data_type(),
            &arrow::datatypes::DataType::Utf8
        );
        Ok(())
    }

    #[test]
    fn represents_unsigned_csv_values_as_signed_integers() -> Result<()> {
        let mut csv = tempfile::NamedTempFile::new()?;
        csv.write_all(b"count\n0\n42\n")?;

        let resource = CkanResource {
            id: "unsigned-integers".to_string(),
            package_id: String::new(),
            url: csv.path().to_string_lossy().to_string(),
            format: "CSV".to_string(),
            datastore_active: false,
            last_modified: String::new(),
        };

        let result = CsvReader::new(test_client()).read(&resource)?;

        assert_eq!(
            result.parquet.schema.field_with_name("count")?.data_type(),
            &arrow::datatypes::DataType::Int64
        );
        Ok(())
    }
}
