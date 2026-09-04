// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_metadata_ingestor::{fetcher::CkanDatasetFetcher, models::MetadataSyncCommand};

#[test]
fn exposes_domain_components_from_their_modules() {
    let _ = CkanDatasetFetcher;
    let _ = MetadataSyncCommand {
        sync_id: "sync-1".into(),
        instance_id: "instance-1".into(),
        instance_name: "Test".into(),
        instance_url: "https://ckan.example".into(),
    };
}
