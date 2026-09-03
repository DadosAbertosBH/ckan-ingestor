// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_worker_coordinator::IggySettings;

#[test]
fn defaults_cover_the_complete_iggy_topology() {
    let settings = IggySettings::default();

    assert_eq!(settings.stream, "ckan-ingestor");
    assert_eq!(settings.job_topic, "jobs");
    assert_eq!(settings.retry_topic, "jobs-retry");
    assert_eq!(settings.result_topic, "job-results");
    assert_eq!(settings.metadata_sync_topic, "ckan_metadata_sync");
    assert_eq!(
        settings.metadata_sync_result_topic,
        "ckan_metadata_sync_result"
    );
    assert_eq!(
        settings.metadata_consumer_group,
        "ckan-metadata-sync-worker"
    );
    assert_eq!(settings.partitions, 10);
}
