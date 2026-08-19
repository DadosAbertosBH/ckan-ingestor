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

use serde::Serialize;

/// Label constants — mirrors Python's `_apply_ingestion_labels`.
pub const LABEL_EMPTY: &str = "empty";
pub const LABEL_SINGLE_ROW: &str = "single-row";
pub const LABEL_SINGLE_COLUMN: &str = "single-column";
pub const LABEL_ROW_COUNT_MISMATCH: &str = "row-count-mismatch";
pub const LABEL_COLUMN_COUNT_MISMATCH: &str = "column-count-mismatch";
pub const LABEL_SIZE_SMALL: &str = "size:small";
pub const LABEL_SIZE_MEDIUM: &str = "size:medium";
pub const LABEL_SIZE_LARGE: &str = "size:large";
pub const LABEL_DATASTORE: &str = "datastore";

const ONE_MB: i64 = 1_000_000;
const ONE_GB: i64 = 1_000_000_000;

/// The result of an ingestion, published to `ckan.ingest.jobs_result`.
///
#[derive(Debug, Serialize)]
pub struct IngestionOutcome {
    pub rows_processed: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub preview: Vec<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_rows: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    pub datastore_active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_columns: Option<usize>,
    /// Derived status: "success" or "failed" (emptiness is a label)
    pub status: String,
}

/// Compute labels exactly as Python's `_apply_ingestion_labels`.
///
/// Note: column-related labels (`single-column`, `column-count-mismatch`)
/// are computed separately via `compute_column_labels` since they depend
/// on the preview data.
pub fn compute_labels(
    rows_processed: i64,
    datastore_active: bool,
    encoding: &Option<String>,
    expected_rows: Option<i64>,
    resource_size: Option<i64>,
) -> Vec<String> {
    let mut labels: Vec<String> = Vec::new();

    // empty
    if rows_processed == 0 {
        labels.push(LABEL_EMPTY.to_string());
        return labels;
    }

    // single-row
    if rows_processed == 1 {
        labels.push(LABEL_SINGLE_ROW.to_string());
    }

    // row-count-mismatch
    if let Some(expected) = expected_rows {
        if rows_processed != expected {
            labels.push(LABEL_ROW_COUNT_MISMATCH.to_string());
        }
    }

    // size labels
    if let Some(size) = resource_size {
        if size < ONE_MB {
            labels.push(LABEL_SIZE_SMALL.to_string());
        } else if size < ONE_GB {
            labels.push(LABEL_SIZE_MEDIUM.to_string());
        } else {
            labels.push(LABEL_SIZE_LARGE.to_string());
        }
    }

    // encoding (skip utf-8, the default)
    if let Some(enc) = encoding {
        if enc != "utf-8" {
            labels.push(format!("encoding:{}", enc));
        }
    }

    // datastore
    if datastore_active {
        labels.push(LABEL_DATASTORE.to_string());
    }

    // column-count-mismatch and single-column need column_count from preview
    // These are computed by the caller once preview is available

    labels
}

/// Compute labels that depend on column count from the preview.
pub fn compute_column_labels(
    labels: &mut Vec<String>,
    column_count: usize,
    expected_columns: Option<i64>,
) {
    // single-column
    if column_count == 1 {
        labels.push(LABEL_SINGLE_COLUMN.to_string());
    }

    // column-count-mismatch
    if let Some(expected) = expected_columns {
        if column_count > 0 && column_count as i64 != expected {
            labels.push(LABEL_COLUMN_COUNT_MISMATCH.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::IngestionOutcome;

    #[test]
    fn serializes_the_current_ingestion_contract() {
        let outcome = IngestionOutcome {
            rows_processed: 42,
            preview: vec![serde_json::json!({"name": "Ana"})],
            expected_rows: Some(50),
            encoding: Some("latin-1".to_string()),
            datastore_active: true,
            expected_columns: Some(3),
            status: "success".to_string(),
        };

        let json = serde_json::to_value(outcome).expect("outcome serializes");

        assert_eq!(json["rows_processed"], 42);
        assert_eq!(json["preview"], serde_json::json!([{"name": "Ana"}]));
        assert_eq!(json["expected_rows"], 50);
        assert_eq!(json["encoding"], "latin-1");
        assert_eq!(json["datastore_active"], true);
        assert_eq!(json["expected_columns"], 3);
        assert_eq!(json["status"], "success");
    }
}
