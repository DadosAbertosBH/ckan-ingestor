// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// ckan-ingestor-rs is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with ckan-ingestor-rs.  If not, see <https://www.gnu.org/licenses/>.

mod common;

use anyhow::Result;
use ckan_ingestor_lib::ckan_resource::CkanResource;
use ckan_ingestor_lib::jev_csv_sniffer::{build_jev_csv_sniffer_request, JevCsvRepairer};
use ckan_ingestor_lib::readers::ckan_reader::CkanReader;
use ckan_ingestor_lib::readers::csv_reader::CsvReader;
use common::fixture_path;
use httpmock::{Method::POST, MockServer};
use reqwest::blocking::Client;
use std::fs;
use tempfile::tempdir;

#[test]
fn builds_a_complete_jev_request_for_an_extra_csv_field() -> Result<()> {
    let request = build_jev_csv_sniffer_request(
        &fixture_path("inventario.csv"),
        7,
        "Csv error: incorrect number of fields for line 7, expected 6 got 7",
    )?;
    assert!(request["questions"].get("affected_column").is_none());
    assert_eq!(request["questions"]["fields_to_merge"]["type"], "choice");
    assert!(request["questions"]["fields_to_merge"]["criteria"].is_object());
    assert!(request["questions"].get("repair_action").is_none());
    assert_eq!(request["questions"]["is_delimiter_correct"]["type"], "noul");
    assert_eq!(
        request["questions"]["is_has_header_correct"]["type"],
        "noul"
    );
    assert_eq!(
        request["questions"]["is_num_fields_correct"]["type"],
        "noul"
    );
    let expected: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(fixture_path("inventario.jev.json"))?)?;

    assert_json_key_eq(&request, &expected, "model");
    for key in ["raw_first_line", "first_ten_lines", "last_ten_lines"] {
        assert_json_key_eq(&request["state"], &expected["state"], key);
    }
    for key in [
        "encoding",
        "dialect",
        "avg_record_len",
        "num_fields",
        "fields",
        "types",
    ] {
        assert_json_key_eq(
            &request["state"]["inferred_metadata"],
            &expected["state"]["inferred_metadata"],
            key,
        );
    }
    for key in [
        "line_number",
        "raw",
        "parsed_fields",
        "error",
        "expected_fields",
        "actual_fields",
    ] {
        assert_json_key_eq(
            &request["state"]["problematic_line"],
            &expected["state"]["problematic_line"],
            key,
        );
    }
    for key in [
        "fields_to_merge",
        "is_delimiter_correct",
        "is_has_header_correct",
        "is_num_fields_correct",
    ] {
        assert_json_key_eq(&request["questions"], &expected["questions"], key);
    }
    Ok(())
}

fn assert_json_key_eq(actual: &serde_json::Value, expected: &serde_json::Value, key: &str) {
    assert_eq!(actual.get(key), expected.get(key), "JSON mismatch at {key}");
}

#[test]
fn repairs_the_problematic_record_from_a_jev_choice() -> Result<()> {
    let server = MockServer::start();
    let response = serde_json::json!({
        "model": "jev-latest",
        "answers": {
            "fields_to_merge": {
                "type": "choice",
                "choice": "merge_2_3",
                "probabilities": {"merge_2_3": 1.0},
                "confidence": 1.0
            },
            "is_delimiter_correct": {"type": "noul", "noul": 1.0},
            "is_has_header_correct": {"type": "noul", "noul": 1.0},
            "is_num_fields_correct": {"type": "noul", "noul": 1.0}
        },
        "usage": {"input_tokens": 1, "output_tokens": 1}
    });
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .header("authorization", "Bearer test-key")
            .body_contains("\"line_number\":7");
        then.status(200).json_body(response);
    });
    let repairer = JevCsvRepairer::new(
        Client::new(),
        format!("{}/v1/systemone", server.base_url()),
        "test-key".to_string(),
    );

    let repaired = repairer.repair_csv(
        &fixture_path("inventario.csv"),
        7,
        "Csv error: incorrect number of fields for line 7, expected 6 got 7",
    )?;

    mock.assert();
    let repaired_contents = fs::read_to_string(&repaired)?;
    assert!(repaired_contents
        .lines()
        .nth(6)
        .unwrap()
        .contains("\"Descrição"));
    assert!(repaired_contents
        .lines()
        .nth(6)
        .unwrap()
        .contains("ZEIS-1; assim"));
    fs::remove_file(repaired)?;
    Ok(())
}

#[test]
fn csv_reader_applies_five_inventario_jev_responses_before_reaching_line_92() -> Result<()> {
    let server = MockServer::start();
    let first_response = serde_json::json!({
        "model": "jev-1.13.0",
        "answers": {
            "fields_to_merge": {"type": "choice", "choice": "merge_2_3", "confidence": 0.84, "probabilities": {"merge_2_3": 0.86}, "stats": {}},
            "is_delimiter_correct": {"type": "noul", "noul": 0.88, "stats": {}},
            "is_has_header_correct": {"type": "noul", "noul": 0.93, "stats": {}},
            "is_num_fields_correct": {"type": "noul", "noul": 0.88, "stats": {}}
        },
        "usage": {"input_tokens": 4986, "output_tokens": 148}
    });
    let second_response = serde_json::json!({
        "model": "jev-1.13.0",
        "answers": {
            "fields_to_merge": {"type": "choice", "choice": "merge_2_3", "confidence": 0.68, "probabilities": {"merge_2_3": 0.73, "merge_3_4": 0.23}, "stats": {}},
            "is_delimiter_correct": {"type": "noul", "noul": 0.75, "stats": {}},
            "is_has_header_correct": {"type": "noul", "noul": 0.94, "stats": {}},
            "is_num_fields_correct": {"type": "noul", "noul": 0.86, "stats": {}}
        },
        "usage": {"input_tokens": 4806, "output_tokens": 148},
        "request_id": "playground_1831bfb932af4d54f2b946ed100fd956b63",
        "evaluation_time_ms": 124.02982099956716
    });
    let third_response = serde_json::json!({
        "model": "jev-1.13.0",
        "answers": {
            "fields_to_merge": {
                "type": "choice",
                "choice": "merge_2_3",
                "confidence": 0.88,
                "probabilities": {
                    "merge_2_3": 0.89,
                    "merge_5_6": 0.01,
                    "merge_3_4": 0.04,
                    "merge_6_7": 0.05,
                    "merge_4_5": 0,
                    "merge_1_2": 0.01
                },
                "stats": {}
            },
            "is_delimiter_correct": {"type": "noul", "noul": 0.68, "stats": {}},
            "is_has_header_correct": {"type": "noul", "noul": 0.94, "stats": {}},
            "is_num_fields_correct": {"type": "noul", "noul": 0.81, "stats": {}}
        },
        "usage": {"input_tokens": 1869, "output_tokens": 148},
        "request_id": "playground_183097c89bf739f488caaec117b03dad8e0",
        "evaluation_time_ms": 90.06480800599093
    });
    let fourth_response = serde_json::json!({
        "model": "jev-1.13.0",
        "answers": {
            "fields_to_merge": {
                "type": "choice",
                "choice": "merge_3_4",
                "confidence": 0.55,
                "probabilities": {
                    "merge_4_5": 0.01,
                    "merge_1_2": 0.03,
                    "merge_2_3": 0.31,
                    "merge_6_7": 0.01,
                    "merge_3_4": 0.62,
                    "merge_5_6": 0,
                    "merge_7_8": 0.02
                },
                "stats": {}
            },
            "is_delimiter_correct": {"type": "noul", "noul": 0.83, "stats": {}},
            "is_has_header_correct": {"type": "noul", "noul": 0.93, "stats": {}},
            "is_num_fields_correct": {"type": "noul", "noul": 0.83, "stats": {}}
        },
        "usage": {"input_tokens": 4888, "output_tokens": 159},
        "request_id": "playground_1834f68cb8f4af049f4bfa11551117b0597",
        "evaluation_time_ms": 140.03539900022588
    });
    let fifth_response = serde_json::json!({
        "model": "jev-1.13.0",
        "answers": {
            "fields_to_merge": {
                "type": "choice",
                "choice": "merge_2_3",
                "confidence": 0.92,
                "probabilities": {
                    "merge_4_5": 0,
                    "merge_2_3": 0.94,
                    "merge_3_4": 0,
                    "merge_5_6": 0.01,
                    "merge_1_2": 0.01,
                    "merge_6_7": 0.04
                },
                "stats": {}
            },
            "is_delimiter_correct": {"type": "noul", "noul": 0.68, "stats": {}},
            "is_has_header_correct": {"type": "noul", "noul": 0.93, "stats": {}},
            "is_num_fields_correct": {"type": "noul", "noul": 0.85, "stats": {}}
        },
        "usage": {"input_tokens": 4803, "output_tokens": 148},
        "request_id": "playground_1830c6f1b585e8048bb9110164886d46fb5",
        "evaluation_time_ms": 129.32364299922483
    });
    let first_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .body_contains("\"line_number\":7");
        then.status(200).json_body(first_response);
    });
    let second_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .body_contains("\"line_number\":60")
            .body_contains("\"actual_fields\":8");
        then.status(200).json_body(second_response);
    });
    let third_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .body_contains("\"line_number\":60")
            .body_contains("\"actual_fields\":7");
        then.status(200).json_body(third_response);
    });
    let fourth_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .body_contains("\"line_number\":90")
            .body_contains("\"actual_fields\":8");
        then.status(200).json_body(fourth_response);
    });
    let fifth_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .body_contains("\"line_number\":90")
            .body_contains("\"actual_fields\":7");
        then.status(200).json_body(fifth_response);
    });
    let repairer = JevCsvRepairer::new(
        Client::new(),
        format!("{}/v1/systemone", server.base_url()),
        "test-key".to_string(),
    );
    let reader = CsvReader::new(Client::new()).with_jev_repairer(repairer);
    let resource = CkanResource {
        id: "inventario".to_string(),
        package_id: String::new(),
        url: fixture_path("inventario.csv")
            .to_string_lossy()
            .into_owned(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let error = match reader.read(&resource) {
        Ok(_) => panic!("line 92 remains malformed"),
        Err(error) => error,
    };

    first_mock.assert();
    second_mock.assert();
    third_mock.assert();
    fourth_mock.assert();
    fifth_mock.assert();
    assert!(error.to_string().contains("line 92, expected 6 got 12"));
    Ok(())
}

#[test]
fn csv_reader_retries_the_normal_parse_with_the_jev_repaired_file() -> Result<()> {
    let server = MockServer::start();
    let response = serde_json::json!({
        "model": "jev-latest",
        "answers": {
            "fields_to_merge": {
                "type": "choice",
                "choice": "merge_2_3",
                "probabilities": {"merge_2_3": 1.0},
                "confidence": 1.0
            },
            "is_delimiter_correct": {"type": "noul", "noul": 1.0},
            "is_has_header_correct": {"type": "noul", "noul": 1.0},
            "is_num_fields_correct": {"type": "noul", "noul": 1.0}
        },
        "usage": {"input_tokens": 1, "output_tokens": 1}
    });
    let mock = server.mock(|when, then| {
        when.method(POST).path("/v1/systemone");
        then.status(200).json_body(response);
    });
    let repairer = JevCsvRepairer::new(
        Client::new(),
        format!("{}/v1/systemone", server.base_url()),
        "test-key".to_string(),
    );
    let reader = CsvReader::new(Client::new()).with_jev_repairer(repairer);
    let temp_dir = tempdir()?;
    let csv_path = temp_dir.path().join("inventario.csv");
    let fixture = fs::read_to_string(fixture_path("inventario.csv"))?;
    fs::write(
        &csv_path,
        fixture.lines().take(10).collect::<Vec<_>>().join("\n"),
    )?;
    let resource = CkanResource {
        id: "inventario".to_string(),
        package_id: String::new(),
        url: csv_path.to_string_lossy().into_owned(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = reader.read(&resource)?;

    mock.assert();
    assert_eq!(result.number_of_columns, 6);
    Ok(())
}

#[test]
fn csv_reader_repairs_the_first_inventario_error_before_reporting_the_next_one() -> Result<()> {
    let server = MockServer::start();
    let response = serde_json::json!({
        "model": "jev-1.13.0",
        "answers": {
            "fields_to_merge": {
                "type": "choice",
                "choice": "merge_2_3",
                "confidence": 0.84,
                "probabilities": {
                    "merge_5_6": 0.02,
                    "merge_1_2": 0.04,
                    "merge_6_7": 0.07,
                    "merge_2_3": 0.86,
                    "merge_3_4": 0.01,
                    "merge_4_5": 0
                },
                "stats": {}
            },
            "is_delimiter_correct": {"type": "noul", "noul": 0.88, "stats": {}},
            "is_has_header_correct": {"type": "noul", "noul": 0.93, "stats": {}},
            "is_num_fields_correct": {"type": "noul", "noul": 0.88, "stats": {}}
        },
        "usage": {"input_tokens": 4986, "output_tokens": 148},
        "request_id": "playground_183b26b4a7bf56c4129af26787443889729",
        "evaluation_time_ms": 79.90030399923853
    });
    let mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .header("authorization", "Bearer test-key")
            .body_contains("\"line_number\":7");
        then.status(200).json_body(response);
    });
    let repairer = JevCsvRepairer::new(
        Client::new(),
        format!("{}/v1/systemone", server.base_url()),
        "test-key".to_string(),
    );
    let reader = CsvReader::new(Client::new()).with_jev_repairer(repairer);
    let resource = CkanResource {
        id: "inventario".to_string(),
        package_id: String::new(),
        url: fixture_path("inventario.csv")
            .to_string_lossy()
            .into_owned(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let error = match reader.read(&resource) {
        Ok(_) => panic!("line 60 remains malformed"),
        Err(error) => error,
    };

    mock.assert();
    assert!(error.to_string().contains("line 60, expected 6 got 8"));
    Ok(())
}
