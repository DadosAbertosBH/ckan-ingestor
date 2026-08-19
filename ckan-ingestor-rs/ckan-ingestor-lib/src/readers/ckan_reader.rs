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
use crate::ckan_resource::CkanResource;
use duckdb::arrow::array::RecordBatch;

const PREVIEW_MAX_VALUE_LEN: usize = 1000;

pub struct SuccessResult {
    pub data: Vec<RecordBatch>,
    pub preview: Vec<serde_json::Value>,
    pub rows_processed: usize,
    pub number_of_columns: usize,
    pub encoding: Option<String>,
    pub expected_rows: Option<usize>,
    pub expected_columns: Option<usize>,
}

#[derive(Debug)]
pub struct FailedResult {
    pub error: anyhow::Error,
    pub expected_rows: Option<usize>,
    pub expected_columns: Option<usize>,
}

impl SuccessResult {
    pub fn new(data: Vec<RecordBatch>) -> Self {
        let rows_processed = data.iter().map(|batch| batch.num_rows()).sum();
        let number_of_columns = data
            .first()
            .map(|batch| batch.num_columns())
            .unwrap_or_default();
        let preview = Self::generate_preview(&data).unwrap_or_default();

        Self {
            data,
            preview,
            rows_processed,
            number_of_columns,
            encoding: None,
            expected_rows: None,
            expected_columns: None,
        }
    }

    pub fn from_csv(data: Vec<RecordBatch>, encoding: String) -> Self {
        Self {
            encoding: Some(encoding),
            ..Self::new(data)
        }
    }

    pub fn from_datastore(
        data: Vec<RecordBatch>,
        expected_rows: usize,
        expected_columns: usize,
    ) -> Self {
        Self {
            expected_rows: Some(expected_rows),
            expected_columns: Some(expected_columns),
            ..Self::new(data)
        }
    }

    fn truncate_preview_value(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::String(s) if s.len() > PREVIEW_MAX_VALUE_LEN => {
                serde_json::Value::String(format!(
                    "[TRUNCATED: value too large for preview ({} bytes)]",
                    s.len()
                ))
            }
            other => other,
        }
    }

    fn generate_preview(data: &Vec<RecordBatch>) -> Option<Vec<serde_json::Value>> {
        let batch = data.first()?;
        let mut rows = Vec::new();
        for row_idx in 0..batch.num_rows() {
            let mut row_map = serde_json::Map::new();
            let schema = batch.schema();
            for col_idx in 0..batch.num_columns() {
                let name = schema.field(col_idx).name();
                let val = if batch.column(col_idx).is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    let s = duckdb::arrow::util::display::array_value_to_string(
                        batch.column(col_idx),
                        row_idx,
                    )
                    .unwrap_or_default();
                    Self::truncate_preview_value(serde_json::Value::String(s))
                };
                row_map.insert(name.clone(), val);
            }
            rows.push(serde_json::Value::Object(row_map));
        }

        return Some(rows);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use duckdb::arrow::{
        array::{ArrayRef, RecordBatch, StringArray},
        datatypes::{DataType, Field, Schema},
    };

    use super::{SuccessResult, PREVIEW_MAX_VALUE_LEN};

    fn string_batch(names: Vec<&str>, ages: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("name", DataType::Utf8, true),
            Field::new("age", DataType::Utf8, true),
        ]));
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(StringArray::from(names)),
            Arc::new(StringArray::from(ages)),
        ];

        RecordBatch::try_new(schema, arrays).expect("valid string batch")
    }

    #[test]
    fn from_datastore_keeps_expected_metadata() {
        let result = SuccessResult::from_datastore(Vec::new(), 42, 3);

        assert_eq!(result.expected_rows, Some(42));
        assert_eq!(result.expected_columns, Some(3));
        assert_eq!(result.encoding, None);
    }

    #[test]
    fn new_has_no_reader_specific_metadata() {
        let result = SuccessResult::new(Vec::new());

        assert_eq!(result.encoding, None);
        assert_eq!(result.expected_rows, None);
        assert_eq!(result.expected_columns, None);
    }

    #[test]
    fn new_calculates_rows_columns_and_preview() {
        let batch = string_batch(vec!["Ana", "Bruno"], vec!["30", "40"]);

        let result = SuccessResult::new(vec![batch]);

        assert_eq!(result.rows_processed, 2);
        assert_eq!(result.number_of_columns, 2);
        assert_eq!(
            result.preview,
            vec![
                serde_json::json!({"name": "Ana", "age": "30"}),
                serde_json::json!({"name": "Bruno", "age": "40"}),
            ]
        );
    }

    #[test]
    fn new_returns_empty_preview_for_an_empty_batch() {
        let batch = string_batch(vec![], vec![]);

        let result = SuccessResult::new(vec![batch]);

        assert_eq!(result.rows_processed, 0);
        assert_eq!(result.number_of_columns, 2);
        assert!(result.preview.is_empty());
    }

    #[test]
    fn new_truncates_long_preview_values() {
        let long_value = "a".repeat(PREVIEW_MAX_VALUE_LEN + 1);
        let batch = string_batch(vec![long_value.as_str()], vec!["30"]);

        let result = SuccessResult::new(vec![batch]);

        assert_eq!(
            result.preview,
            vec![serde_json::json!({
                "name": format!(
                    "[TRUNCATED: value too large for preview ({} bytes)]",
                    PREVIEW_MAX_VALUE_LEN + 1
                ),
                "age": "30",
            })]
        );
    }
}

impl FailedResult {
    pub fn from_string(error: &str) -> Self {
        Self {
            error: anyhow::anyhow!(error.to_string()),
            expected_rows: Option::None,
            expected_columns: Option::None,
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

    /// A short human-readable name for this reader, used in log messages.
    fn reader_name(&self) -> &'static str {
        any::type_name::<Self>()
    }

    fn read(&self, resource: &CkanResource) -> ReadResult {
        if !self.can_read(resource) {
            return Err(FailedResult {
                error: anyhow::anyhow!("Unsupported format"),
                expected_columns: Option::None,
                expected_rows: Option::None,
            });
        }
        self.do_read(resource)
    }

    fn can_read(&self, resource: &CkanResource) -> bool {
        self.supported_formats()
            .iter()
            .any(|e| resource.format.contains(e))
    }
}
