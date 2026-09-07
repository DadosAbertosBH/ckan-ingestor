// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_metadata_ingestor::{StructuredIpc, fetcher::PAGE_SIZE};
use httpmock::MockServer;
use serde_json::json;

#[test]
fn fetches_all_pages_and_separates_packages_from_resources() {
    let server = MockServer::start();
    let first_page = server.mock(|when, then| {
        when.method("GET")
            .path("/api/action/current_package_list_with_resources")
            .query_param("limit", PAGE_SIZE.to_string())
            .query_param("offset", "0");
        then.status(200).json_body(json!({"result": (0..PAGE_SIZE)
            .map(|index| json!({
                "id": format!("dataset-{index}"),
                "resources": [{
                    "id": format!("resource-{index}"),
                    "url": "https://example.test/1.csv"
                }]
            }))
            .collect::<Vec<_>>()
        }));
    });
    let final_page = server.mock(|when, then| {
        when.method("GET")
            .path("/api/action/current_package_list_with_resources")
            .query_param("limit", PAGE_SIZE.to_string())
            .query_param("offset", PAGE_SIZE.to_string());
        then.status(200).json_body(json!({"result": []}));
    });

    let ipc = StructuredIpc::fetch(&server.url("")).unwrap();

    first_page.assert();
    final_page.assert();
    assert_eq!(ipc.package_rows(), PAGE_SIZE);
    assert_eq!(ipc.resource_rows(), PAGE_SIZE);
}
