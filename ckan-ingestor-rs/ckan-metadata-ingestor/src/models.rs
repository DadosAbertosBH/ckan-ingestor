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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataSyncResult {
    pub sync_id: String,
    pub instance_id: String,
    pub instance_name: String,
    pub status: String,
    pub total_packages: i64,
    pub new_datasets: i64,
    pub new_resources: i64,
    pub updated_datasets: i64,
    pub updated_resources: i64,
    pub dataset_count: i64,
    pub resource_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}
