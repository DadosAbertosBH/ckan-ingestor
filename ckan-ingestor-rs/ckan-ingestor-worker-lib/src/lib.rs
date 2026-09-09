// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use message_processor::OutgoingMessage;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum JobStatus {
    Pending,
    Processing,
    Success,
    Failed,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Pending => "PENDING",
            Self::Processing => "PROCESSING",
            Self::Success => "SUCCESS",
            Self::Failed => "FAILED",
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct JobMessage {
    pub job_id: String,
    pub resource_id: String,
    #[serde(default)]
    pub package_id: String,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResultMessage {
    pub job_id: String,
    pub status: JobStatus,
    pub resource_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dataset_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resource_format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ckan_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub datastore_active: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reader: Option<String>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preview: Option<Vec<serde_json::Value>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub artifact: Option<ParquetArtifact>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParquetArtifact {
    pub uri: String,
    pub schema_ipc_base64: String,
    pub num_rows: u64,
    pub file_size_bytes: u64,
    pub footer_size_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub column_statistics: Vec<ParquetColumnStatistics>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ParquetColumnStatistics {
    pub field_id: i64,
    pub size_bytes: Option<u64>,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub null_count: Option<u64>,
    pub contains_nan: Option<bool>,
}

impl JobResultMessage {
    pub fn pending(
        job_id: impl Into<String>,
        resource_id: impl Into<String>,
        dataset_name: impl Into<String>,
        instance_id: impl Into<String>,
    ) -> Self {
        Self {
            job_id: job_id.into(),
            status: JobStatus::Pending,
            resource_id: resource_id.into(),
            dataset_name: Some(dataset_name.into()),
            resource_name: None,
            resource_url: None,
            resource_format: None,
            instance_id: Some(instance_id.into()),
            ckan_url: None,
            datastore_active: None,
            reader: None,
            rows_processed: None,
            expected_rows: None,
            encoding: None,
            csv_strict_mode: None,
            csv_delimiter: None,
            expected_columns: None,
            error_message: None,
            preview: None,
            artifact: None,
        }
    }
}

impl OutgoingMessage for JobResultMessage {
    fn partition_key(&self) -> &str {
        &self.resource_id
    }
}

#[cfg(test)]
mod artifact_tests {
    use super::{JobResultMessage, JobStatus, ParquetArtifact, ParquetColumnStatistics};

    #[test]
    fn parquet_artifact_round_trips_through_the_job_result_contract() {
        let message = JobResultMessage {
            job_id: "job-1".into(),
            status: JobStatus::Success,
            resource_id: "resource-1".into(),
            dataset_name: None,
            resource_name: None,
            resource_url: None,
            resource_format: None,
            instance_id: None,
            ckan_url: None,
            datastore_active: None,
            reader: Some("CsvReader".into()),
            rows_processed: Some(2),
            expected_rows: None,
            encoding: None,
            csv_strict_mode: None,
            csv_delimiter: None,
            expected_columns: None,
            error_message: None,
            preview: None,
            artifact: Some(ParquetArtifact {
                uri: "s3://warehouse/resource-1/job-1.parquet".into(),
                schema_ipc_base64: "schema".into(),
                num_rows: 2,
                file_size_bytes: 128,
                footer_size_bytes: Some(32),
                column_statistics: vec![ParquetColumnStatistics {
                    field_id: 1,
                    size_bytes: Some(64),
                    min_value: Some("a".into()),
                    max_value: Some("z".into()),
                    null_count: Some(0),
                    contains_nan: None,
                }],
            }),
        };

        let encoded = serde_json::to_vec(&message).unwrap();
        let decoded: JobResultMessage = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(decoded.artifact, message.artifact);
    }
}
