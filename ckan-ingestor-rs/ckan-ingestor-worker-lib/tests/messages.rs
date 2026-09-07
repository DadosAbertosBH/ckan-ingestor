// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_ingestor_worker_lib::{JobResultMessage, JobStatus};

#[test]
fn pending_result_carries_the_mysql_job_fields() {
    let message = JobResultMessage::pending("job-1", "resource-1", "dataset", "instance-1");
    assert_eq!(message.status, JobStatus::Pending);
    assert_eq!(message.job_id.as_deref(), Some("job-1"));
    assert_eq!(message.resource_id, "resource-1");
}

#[test]
fn result_messages_require_a_resource_id() {
    let result = serde_json::from_value::<JobResultMessage>(serde_json::json!({
        "status": "PROCESSING"
    }));

    assert!(result.is_err());
}
