use std::any;

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

use crate::readers::temp_file_cleanup::TempFileCleanup;
use crate::{arrow_ipc_output::ArrowIpcOutput, ckan_resource::CkanResource};
use anyhow::Result;
use reqwest::blocking::Client;
use std::fs::File;
use std::io::{self, Write};

/// Download a remote resource to a temporary file.
///
/// The returned path is owned by the caller. Keep a `TempFileCleanup` guard
/// alive for as long as the downloaded file is needed.
pub fn download_to_temp(client: &Client, url: &str, suffix: &str) -> Result<String> {
    let mut response = client.get(url).send()?;
    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("resource download failed with HTTP status {status}: {url}");
    }

    let temp_path = std::env::temp_dir().join(format!("{}{}", uuid::Uuid::new_v4(), suffix));
    let mut cleanup = TempFileCleanup::from_path(temp_path.clone());
    let mut output = File::create(&temp_path)?;
    io::copy(&mut response, &mut output)?;
    output.flush()?;
    cleanup.commit();
    Ok(temp_path.to_string_lossy().into_owned())
}

pub struct SuccessResult {
    pub arrow_ipc: ArrowIpcOutput,
    pub preview: Vec<serde_json::Value>,
    pub rows_processed: usize,
    pub number_of_columns: usize,
    pub reader: String,
    pub encoding: Option<String>,
    pub csv_strict_mode: Option<bool>,
    pub csv_delimiter: Option<String>,
    pub expected_rows: Option<usize>,
    pub expected_columns: Option<usize>,
}

#[derive(Debug)]
pub struct FailedResult {
    pub error: anyhow::Error,
    pub reader: String,
    pub expected_rows: Option<usize>,
    pub expected_columns: Option<usize>,
}

impl SuccessResult {
    pub fn new(arrow_ipc: ArrowIpcOutput, reader: String) -> Self {
        Self {
            preview: arrow_ipc.preview.clone().unwrap_or_default(),
            rows_processed: arrow_ipc.rows,
            number_of_columns: arrow_ipc.columns,
            arrow_ipc,
            reader,
            encoding: None,
            csv_strict_mode: None,
            csv_delimiter: None,
            expected_rows: None,
            expected_columns: None,
        }
    }

    pub fn from_csv(
        arrow_ipc: ArrowIpcOutput,
        encoding: String,
        csv_strict_mode: bool,
        csv_delimiter: String,
        reader: String,
    ) -> Self {
        Self {
            encoding: Some(encoding),
            csv_strict_mode: Some(csv_strict_mode),
            csv_delimiter: Some(csv_delimiter),
            ..Self::new(arrow_ipc, reader)
        }
    }

    pub fn from_datastore(
        arrow_ipc: ArrowIpcOutput,
        expected_rows: usize,
        expected_columns: usize,
        reader: String,
    ) -> Self {
        Self {
            expected_rows: Some(expected_rows),
            expected_columns: Some(expected_columns),
            ..Self::new(arrow_ipc, reader)
        }
    }
}

impl FailedResult {
    pub fn from_string(error: &str, reader: String) -> Self {
        Self {
            error: anyhow::anyhow!(error.to_string()),
            reader,
            expected_rows: None,
            expected_columns: None,
        }
    }
}

impl<E> From<E> for FailedResult
where
    E: Into<anyhow::Error>,
    Result<(), E>: anyhow::Context<(), E>,
{
    fn from(error: E) -> Self {
        Self {
            error: error.into(),
            reader: String::new(),
            expected_rows: None,
            expected_columns: None,
        }
    }
}

impl From<FailedResult> for anyhow::Error {
    fn from(error: FailedResult) -> Self {
        error.error
    }
}

impl std::fmt::Display for FailedResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.error)
    }
}

pub type ReadResult = anyhow::Result<SuccessResult, FailedResult>;

pub trait CkanReader {
    fn supported_formats(&self) -> &[String];
    fn do_read(&self, resource: &CkanResource) -> ReadResult;

    fn reader_name(&self) -> &'static str {
        any::type_name::<Self>()
            .rsplit("::")
            .next()
            .unwrap_or_else(|| any::type_name::<Self>())
    }

    fn read(&self, resource: &CkanResource) -> ReadResult {
        if !self.can_read(resource) {
            return Err(FailedResult::from_string(
                "Unsupported format",
                self.reader_name().to_string(),
            ));
        }

        match self.do_read(resource) {
            Ok(result) if result.rows_processed == 0 => {
                Err(FailedResult::from_string("No data", result.reader))
            }
            result => result,
        }
    }

    fn can_read(&self, resource: &CkanResource) -> bool {
        self.supported_formats()
            .iter()
            .any(|format| resource.format.contains(format))
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::File, sync::Arc};

    use arrow::{
        array::{ArrayRef, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };

    use crate::arrow_ipc_output::ArrowIpcOutput;

    use super::SuccessResult;

    fn output() -> ArrowIpcOutput {
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec!["value"])) as ArrayRef],
        )
        .unwrap();
        let mut output = ArrowIpcOutput::try_new(&batch).unwrap();
        output.write(&batch).unwrap();
        output.finish().unwrap();
        output
    }

    #[test]
    fn new_copies_output_metadata() {
        let result = SuccessResult::new(output(), "test".to_string());

        assert_eq!(result.rows_processed, 1);
        assert_eq!(result.number_of_columns, 1);
        assert_eq!(result.preview, vec![serde_json::json!({"value": "value"})]);
        assert_eq!(result.encoding, None);
        assert_eq!(result.csv_strict_mode, None);
        assert_eq!(result.csv_delimiter, None);
    }

    #[test]
    fn from_csv_keeps_csv_metadata() {
        let result = SuccessResult::from_csv(
            output(),
            "UTF-8".to_string(),
            true,
            ";".to_string(),
            "test".to_string(),
        );

        assert_eq!(result.encoding.as_deref(), Some("UTF-8"));
        assert_eq!(result.csv_strict_mode, Some(true));
        assert_eq!(result.csv_delimiter.as_deref(), Some(";"));
    }

    #[test]
    fn from_datastore_keeps_expected_metadata() {
        let result = SuccessResult::from_datastore(output(), 42, 3, "test".to_string());

        assert_eq!(result.expected_rows, Some(42));
        assert_eq!(result.expected_columns, Some(3));
    }

    #[test]
    fn output_file_is_removed_when_result_is_dropped() {
        let path = {
            let result = SuccessResult::new(output(), "test".to_string());
            result.arrow_ipc.path().to_path_buf()
        };

        assert!(!path.exists());
    }

    #[test]
    fn ipc_output_can_be_opened_as_a_file() {
        let result = SuccessResult::new(output(), "test".to_string());

        assert!(File::open(result.arrow_ipc.path()).is_ok());
    }
}
