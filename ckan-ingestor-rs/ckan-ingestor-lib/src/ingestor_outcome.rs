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

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum IngestionStatus {
    Success,
    Failed,
}

/// The result of an ingestion, published to `ckan.ingest.jobs_result`.
///
#[derive(Debug, Serialize)]
pub struct IngestionOutcome {
    pub reader: String,
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
    pub status: IngestionStatus,
}

#[cfg(test)]
mod tests {
    use super::{IngestionOutcome, IngestionStatus};

    #[test]
    fn serializes_the_current_ingestion_contract() {
        let outcome = IngestionOutcome {
            reader: "test-reader".to_string(),
            rows_processed: 42,
            preview: vec![serde_json::json!({"name": "Ana"})],
            expected_rows: Some(50),
            encoding: Some("latin-1".to_string()),
            datastore_active: true,
            expected_columns: Some(3),
            status: IngestionStatus::Success,
        };

        let json = serde_json::to_value(outcome).expect("outcome serializes");

        assert_eq!(json["rows_processed"], 42);
        assert_eq!(json["preview"], serde_json::json!([{"name": "Ana"}]));
        assert_eq!(json["expected_rows"], 50);
        assert_eq!(json["encoding"], "latin-1");
        assert_eq!(json["datastore_active"], true);
        assert_eq!(json["expected_columns"], 3);
        assert_eq!(json["status"], "SUCCESS");
    }
}
