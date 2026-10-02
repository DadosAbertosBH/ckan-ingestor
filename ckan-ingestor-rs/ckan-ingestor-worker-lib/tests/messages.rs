// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_ingestor_worker_lib::{JobDiscoveryMetadata, JobResultMessage, JobStatus};

#[test]
fn namespaced_job_preserves_ckan_datastore_identity() {
    let job: ckan_ingestor_worker_lib::JobMessage = serde_json::from_value(serde_json::json!({
        "job_id":"job", "resource_id":"namespaced", "external_resource_id":"original-ckan-id", "ckan_url":"https://portal.test", "datastore_active":true
    })).unwrap();
    let encoded = serde_json::to_value(job).unwrap();
    assert_eq!(encoded["external_resource_id"], "original-ckan-id");
}

#[test]
fn pending_result_carries_the_mysql_job_fields() {
    let message = JobResultMessage::pending("job-1", "resource-1", "dataset", "instance-1");
    assert_eq!(message.status, JobStatus::Pending);
    assert_eq!(message.job_id, "job-1");
    assert_eq!(message.resource_id, "resource-1");
}

#[test]
fn deleted_result_is_constructed_with_discovery_metadata() {
    let message = JobResultMessage::deleted(
        "job-1",
        "resource-1",
        "dataset",
        "instance-1",
        JobDiscoveryMetadata {
            resource_name: Some("Resource".into()),
            resource_url: Some("https://example.test/resource.csv".into()),
            resource_format: Some("CSV".into()),
            ckan_url: Some("https://example.test".into()),
            datastore_active: Some(true),
        },
    );

    assert_eq!(message.status, JobStatus::Deleted);
    assert_eq!(message.resource_name.as_deref(), Some("Resource"));
    assert_eq!(message.datastore_active, Some(true));
}

#[test]
fn result_messages_require_a_resource_id() {
    let result = serde_json::from_value::<JobResultMessage>(serde_json::json!({
        "status": "PROCESSING"
    }));

    assert!(result.is_err());
}

#[test]
fn result_messages_require_a_job_id() {
    let result = serde_json::from_value::<JobResultMessage>(serde_json::json!({
        "status": "PROCESSING",
        "resource_id": "resource-1"
    }));

    assert!(result.is_err());
}
