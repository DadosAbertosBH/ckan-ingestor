// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_ingestor_consumer::IggySettings;

#[test]
fn defaults_use_generic_source_and_result_topics() {
    let settings = IggySettings::default();

    assert_eq!(settings.address, "localhost:8090");
    assert_eq!(settings.username, "iggy");
    assert_eq!(settings.password, "iggy");
    assert_eq!(
        settings.connection_string(),
        "iggy://iggy:iggy@localhost:8090"
    );
    assert_eq!(settings.stream, "ckan-ingestor");
    assert_eq!(settings.source_topic, "jobs");
    assert_eq!(settings.retry_topic, "jobs-retry");
    assert_eq!(settings.result_topic, "parquet-results");
    assert_eq!(settings.consumer_group, "ckan-worker");
    assert_eq!(settings.partitions, 10);
}

#[test]
fn connection_string_preserves_non_delimiter_credentials() {
    let settings = IggySettings {
        username: "iggy".into(),
        password: "password*with*asterisk".into(),
        ..IggySettings::default()
    };

    assert_eq!(
        settings.connection_string(),
        "iggy://iggy:password*with*asterisk@localhost:8090"
    );
}
