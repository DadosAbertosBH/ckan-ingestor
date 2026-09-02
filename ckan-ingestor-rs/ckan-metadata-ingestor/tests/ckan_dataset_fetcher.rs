// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_metadata_ingestor::CkanDatasetFetcher;
use httpmock::{Method::GET, MockServer};
use serde_json::json;

const ACTION_PATH: &str = "/api/action/current_package_list_with_resources";

#[test]
fn fetches_all_pages_until_a_short_page() {
    let server = MockServer::start();
    let first_page = (0..100)
        .map(|index| json!({"id": format!("dataset-{index}")}))
        .collect::<Vec<_>>();
    let first = server.mock(|when, then| {
        when.method(GET)
            .path(ACTION_PATH)
            .query_param("limit", "100")
            .query_param("offset", "0");
        then.status(200).json_body(json!({"result": first_page}));
    });
    let second = server.mock(|when, then| {
        when.method(GET)
            .path(ACTION_PATH)
            .query_param("limit", "100")
            .query_param("offset", "100");
        then.status(200)
            .json_body(json!({"result": [{"id": "dataset-100"}]}));
    });

    let packages = CkanDatasetFetcher::fetch(&format!("{}/", server.base_url())).unwrap();

    first.assert();
    second.assert();
    assert_eq!(packages.len(), 101);
    assert_eq!(packages[100]["id"], "dataset-100");
}

#[test]
fn sends_a_user_agent_accepted_by_protected_ckan_instances() {
    let server = MockServer::start();
    let request = server.mock(|when, then| {
        when.method(GET).path(ACTION_PATH).header(
            "user-agent",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15; rv:132.0) Gecko/20100101 Firefox/132.0",
        );
        then.status(200)
            .json_body(json!({"result": [{"id": "dataset-1"}]}));
    });

    CkanDatasetFetcher::fetch(&server.base_url()).unwrap();

    request.assert();
}

#[test]
fn rejects_an_empty_dataset_list() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path(ACTION_PATH);
        then.status(200).json_body(json!({"result": []}));
    });

    let error = CkanDatasetFetcher::fetch(&server.base_url()).unwrap_err();

    assert!(error.to_string().contains("No packages returned"));
}

#[test]
fn rejects_a_non_array_result() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path(ACTION_PATH);
        then.status(200)
            .json_body(json!({"result": {"id": "dataset-1"}}));
    });

    let error = CkanDatasetFetcher::fetch(&server.base_url()).unwrap_err();

    assert!(error.to_string().contains("result must be an array"));
}

#[test]
fn propagates_http_failures() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path(ACTION_PATH);
        then.status(503);
    });

    let error = CkanDatasetFetcher::fetch(&server.base_url()).unwrap_err();

    assert!(error.to_string().contains("503"));
}

#[test]
fn rejects_invalid_json() {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path(ACTION_PATH);
        then.status(200)
            .header("content-type", "application/json")
            .body("not-json");
    });

    let error = CkanDatasetFetcher::fetch(&server.base_url()).unwrap_err();

    assert!(error.to_string().contains("invalid CKAN JSON response"));
}
