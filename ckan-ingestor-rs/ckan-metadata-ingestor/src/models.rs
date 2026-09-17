// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataSyncCommand {
    pub sync_id: String,
    pub instance_id: String,
    pub instance_name: String,
    pub instance_url: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MetadataSyncStatus {
    Success,
    Failure,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataSyncResult {
    pub sync_id: String,
    pub instance_id: String,
    pub instance_name: String,
    pub status: MetadataSyncStatus,
    pub total_packages: i64,
    pub new_datasets: i64,
    pub new_resources: i64,
    pub updated_datasets: i64,
    pub updated_resources: i64,
    pub dataset_count: i64,
    pub resource_count: i64,
    #[serde(default)]
    pub outdated_resources: i64,
    #[serde(default)]
    pub deleted_datasets: i64,
    #[serde(default)]
    pub deleted_resources: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{MetadataSyncResult, MetadataSyncStatus};

    #[test]
    fn metadata_sync_status_uses_lowercase_json_values() {
        assert_eq!(
            serde_json::to_value(MetadataSyncStatus::Success).unwrap(),
            json!("success")
        );
        assert_eq!(
            serde_json::to_value(MetadataSyncStatus::Failure).unwrap(),
            json!("failure")
        );
    }

    #[test]
    fn metadata_sync_result_rejects_unknown_status() {
        let result = serde_json::from_value::<MetadataSyncResult>(json!({
            "sync_id": "sync",
            "instance_id": "instance",
            "instance_name": "Example",
            "status": "unknown",
            "total_packages": 0,
            "new_datasets": 0,
            "new_resources": 0,
            "updated_datasets": 0,
            "updated_resources": 0,
            "dataset_count": 0,
            "resource_count": 0,
            "deleted_datasets": 0,
            "deleted_resources": 0,
            "error_message": null
        }));

        assert!(result.is_err());
    }
}
