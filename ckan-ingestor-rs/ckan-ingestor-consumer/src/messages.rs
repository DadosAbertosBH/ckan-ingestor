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

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum JobStatus {
    Processing,
    Success,
    Failed,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Processing => "PROCESSING",
            Self::Success => "SUCCESS",
            Self::Failed => "FAILED",
        };
        f.write_str(value)
    }
}

/// Message consumed from `ckan.ingest.jobs` / `ckan.ingest.jobs.retry`.
///
/// All fields are guaranteed present by the producer.
#[derive(Debug, Deserialize, Serialize)]
pub struct JobMessage {
    pub job_id: String,
    pub resource_id: String,
    pub ckan_url: String,
    #[serde(default)]
    pub resource_url: String,
    #[serde(default)]
    pub resource_format: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub csv_delimiter: Option<String>,
    #[serde(default)]
    pub datastore_active: bool,
}

/// The result message published to `ckan.ingest.jobs_result`.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobResultMessage {
    pub job_id: String,
    pub reader: String,
    pub status: JobStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rows_processed: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_rows: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub csv_strict_mode: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub csv_delimiter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_columns: Option<i64>,
    pub datastore_active: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<Vec<serde_json::Value>>,
}
