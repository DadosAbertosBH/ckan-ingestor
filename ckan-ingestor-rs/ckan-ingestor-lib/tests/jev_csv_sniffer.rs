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
use std::ptr::metadata;
use tempfile::tempdir;

#[test]
fn builds_a_complete_jev_request_for_an_extra_csv_field() -> Result<()> {
    let metadata = sniff_metadata(&fixture_path("inventario.csv"))?;
    build_jev_csv_sniffer_request_with_metadata(metadata);
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
        "error_source",
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

    let metadata = sniff_metadata(&fixture_path("inventario.csv"))?;
    let repaired = repairer.repair_csv_with_metadata(
        &fixture_path("inventario.csv"),
        &metadata,
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
    let sixth_response = serde_json::json!({
        "model": "jev-1.13.0",
        "answers": {
            "fields_to_merge": {
                "type": "choice",
                "choice": "merge_2_3",
                "confidence": 0.38,
                "probabilities": {
                    "merge_7_8": 0.13,
                    "merge_5_6": 0.01,
                    "merge_1_2": 0.1,
                    "merge_4_5": 0.01,
                    "merge_11_12": 0.06,
                    "merge_6_7": 0.02,
                    "merge_10_11": 0.03,
                    "merge_8_9": 0.15,
                    "merge_3_4": 0.04,
                    "merge_2_3": 0.44,
                    "merge_9_10": 0.01
                },
                "stats": {}
            },
            "is_delimiter_correct": {"type": "noul", "noul": 0.76, "stats": {}},
            "is_has_header_correct": {"type": "noul", "noul": 0.93, "stats": {}},
            "is_num_fields_correct": {"type": "noul", "noul": 0.83, "stats": {}}
        },
        "usage": {"input_tokens": 5340, "output_tokens": 208},
        "request_id": "playground_183fa4a859869d54a1bbded6a291db0a27d",
        "evaluation_time_ms": 130.6539039942436
    });
    let sixth_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .body_contains("\"line_number\":92")
            .body_contains("\"actual_fields\":12")
            .matches(|request| {
                let body = request.body.as_deref().expect("JEV request body");
                let payload: serde_json::Value =
                    serde_json::from_slice(body).expect("valid JEV request JSON");
                println!(
                    "Sixth JEV request:\n{}",
                    serde_json::to_string_pretty(&payload).expect("serializable JEV request")
                );
                true
            });
        then.status(200).json_body(sixth_response);
    });
    let seventh_response = serde_json::json!({
        "model": "jev-1.13.0",
        "answers": {
            "fields_to_merge": {
                "type": "choice",
                "choice": "merge_2_3",
                "confidence": 0.8,
                "probabilities": {
                    "merge_4_5": 0.01,
                    "merge_7_8": 0.04,
                    "merge_6_7": 0.06,
                    "merge_3_4": 0.02,
                    "merge_8_9": 0,
                    "merge_10_11": 0.01,
                    "merge_5_6": 0,
                    "merge_2_3": 0.83,
                    "merge_9_10": 0,
                    "merge_1_2": 0.03
                },
                "stats": {}
            },
            "is_delimiter_correct": {"type": "noul", "noul": 0.72, "stats": {}},
            "is_has_header_correct": {"type": "noul", "noul": 0.97, "stats": {}},
            "is_num_fields_correct": {"type": "noul", "noul": 0.86, "stats": {}}
        },
        "usage": {"input_tokens": 5283, "output_tokens": 195},
        "request_id": "playground_1837cfbf2f3964f44998eff7edc66bb7d56",
        "evaluation_time_ms": 106.13544999796432
    });
    let seventh_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .body_contains("\"line_number\":92")
            .body_contains("\"actual_fields\":11")
            .matches(|request| {
                let body = request.body.as_deref().expect("JEV request body");
                let payload: serde_json::Value =
                    serde_json::from_slice(body).expect("valid JEV request JSON");
                println!(
                    "Seventh JEV request:\n{}",
                    serde_json::to_string_pretty(&payload).expect("serializable JEV request")
                );
                true
            });
        then.status(200).json_body(seventh_response);
    });
    let eighth_response = serde_json::json!({
        "model": "jev-1.13.0",
        "answers": {
            "fields_to_merge": {
                "type": "choice",
                "choice": "merge_2_3",
                "confidence": 0.33,
                "probabilities": {
                    "merge_2_3": 0.41,
                    "merge_4_5": 0.01,
                    "merge_5_6": 0.25,
                    "merge_3_4": 0.17,
                    "merge_7_8": 0,
                    "merge_6_7": 0.12,
                    "merge_1_2": 0.03,
                    "merge_8_9": 0,
                    "merge_9_10": 0.01
                },
                "stats": {}
            },
            "is_delimiter_correct": {"type": "noul", "noul": 0.71, "stats": {}},
            "is_has_header_correct": {"type": "noul", "noul": 0.97, "stats": {}},
            "is_num_fields_correct": {"type": "noul", "noul": 0.83, "stats": {}}
        },
        "usage": {"input_tokens": 5191, "output_tokens": 182},
        "request_id": "playground_183cf0745182dae4088a3cc5d01a46df398",
        "evaluation_time_ms": 132.8395989985438
    });
    let eighth_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .body_contains("\"line_number\":92")
            .body_contains("\"actual_fields\":10")
            .matches(|request| {
                let body = request.body.as_deref().expect("JEV request body");
                let payload: serde_json::Value =
                    serde_json::from_slice(body).expect("valid JEV request JSON");
                println!(
                    "Eighth JEV request:\n{}",
                    serde_json::to_string_pretty(&payload).expect("serializable JEV request")
                );
                true
            });
        then.status(200).json_body(eighth_response);
    });
    let ninth_response = serde_json::json!({
        "model": "jev-1.13.0",
        "answers": {
            "fields_to_merge": {
                "type": "choice",
                "choice": "merge_2_3",
                "confidence": 0.22,
                "probabilities": {
                    "merge_7_8": 0,
                    "merge_6_7": 0.01,
                    "merge_3_4": 0.21,
                    "merge_8_9": 0.01,
                    "merge_1_2": 0.03,
                    "merge_2_3": 0.32,
                    "merge_5_6": 0.13,
                    "merge_4_5": 0.29
                },
                "stats": {}
            },
            "is_delimiter_correct": {"type": "noul", "noul": 0.68, "stats": {}},
            "is_has_header_correct": {"type": "noul", "noul": 0.97, "stats": {}},
            "is_num_fields_correct": {"type": "noul", "noul": 0.84, "stats": {}}
        },
        "usage": {"input_tokens": 5102, "output_tokens": 170},
        "request_id": "playground_183dc033277e80e4f288ff57527e056f473",
        "evaluation_time_ms": 122.48423999699298
    });
    let ninth_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .body_contains("\"line_number\":92")
            .body_contains("\"actual_fields\":9")
            .matches(|request| {
                let body = request.body.as_deref().expect("JEV request body");
                let payload: serde_json::Value =
                    serde_json::from_slice(body).expect("valid JEV request JSON");
                println!(
                    "Ninth JEV request:\n{}",
                    serde_json::to_string_pretty(&payload).expect("serializable JEV request")
                );
                true
            });
        then.status(200).json_body(ninth_response);
    });
    let tenth_response = serde_json::json!({
        "model": "jev-1.13.0",
        "answers": {
            "fields_to_merge": {
                "type": "choice",
                "choice": "merge_3_4",
                "confidence": 0.87,
                "probabilities": {
                    "merge_3_4": 0.9,
                    "merge_4_5": 0,
                    "merge_5_6": 0,
                    "merge_2_3": 0.08,
                    "merge_1_2": 0.02,
                    "merge_7_8": 0,
                    "merge_6_7": 0
                },
                "stats": {}
            },
            "is_delimiter_correct": {"type": "noul", "noul": 0.72, "stats": {}},
            "is_has_header_correct": {"type": "noul", "noul": 0.97, "stats": {}},
            "is_num_fields_correct": {"type": "noul", "noul": 0.84, "stats": {}}
        },
        "usage": {"input_tokens": 5017, "output_tokens": 159},
        "request_id": "playground_183054dd7bd231b469cbc0b17052c64438d",
        "evaluation_time_ms": 169.12725499423686
    });
    let tenth_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .body_contains("\"line_number\":92")
            .body_contains("\"actual_fields\":8")
            .matches(|request| {
                let body = request.body.as_deref().expect("JEV request body");
                let payload: serde_json::Value =
                    serde_json::from_slice(body).expect("valid JEV request JSON");
                println!(
                    "Tenth JEV request:\n{}",
                    serde_json::to_string_pretty(&payload).expect("serializable JEV request")
                );
                true
            });
        then.status(200).json_body(tenth_response);
    });
    let eleventh_mock = server.mock(|when, then| {
        when.method(POST)
            .path("/v1/systemone")
            .body_contains("\"line_number\":92")
            .body_contains("\"actual_fields\":7")
            .matches(|request| {
                let body = request.body.as_deref().expect("JEV request body");
                let payload: serde_json::Value =
                    serde_json::from_slice(body).expect("valid JEV request JSON");
                println!(
                    "Eleventh JEV request:\n{}",
                    serde_json::to_string_pretty(&payload).expect("serializable JEV request")
                );
                true
            });
        then.status(200).json_body(serde_json::json!({
            "model": "jev-1.13.0",
            "answers": {
                "fields_to_merge": {
                    "type": "choice",
                    "choice": "merge_2_3",
                    "confidence": 0.88,
                    "probabilities": {
                        "merge_4_5": 0,
                        "merge_2_3": 0.91,
                        "merge_6_7": 0.08,
                        "merge_3_4": 0,
                        "merge_5_6": 0.01,
                        "merge_1_2": 0
                    },
                    "stats": {}
                },
                "is_delimiter_correct": {"type": "noul", "noul": 0.77, "stats": {}},
                "is_has_header_correct": {"type": "noul", "noul": 0.97, "stats": {}},
                "is_num_fields_correct": {"type": "noul", "noul": 0.84, "stats": {}}
            },
            "usage": {"input_tokens": 4933, "output_tokens": 148},
            "request_id": "playground_183c3b76b07b71747deb04780a510e80e8a",
            "evaluation_time_ms": 187.68362600530963
        }));
    });
    let twelfth_mock = server.mock(|when, then| {
        when.method(POST).path("/v1/systemone").matches(|request| {
            let body = request.body.as_deref().expect("JEV request body");
            let payload: serde_json::Value =
                serde_json::from_slice(body).expect("valid JEV request JSON");
            println!(
                "Twelfth JEV request:\n{}",
                serde_json::to_string_pretty(&payload).expect("serializable JEV request")
            );
            true
        });
        then.status(500);
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
    sixth_mock.assert();
    seventh_mock.assert();
    eighth_mock.assert();
    ninth_mock.assert();
    tenth_mock.assert();
    eleventh_mock.assert();
    twelfth_mock.assert();
    assert!(error.to_string().contains("line 138, expected 6 got 9"));
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

#[test]
fn logs_jev_request_for_relatorio_nominal_metadata_inference_error() -> Result<()> {
    let metadata = sniff_metadata(&fixture_path(
        "relatorio_nominal_servidores_adm_direta_10-2025.csv",
    ))?;
    build_jev_csv_sniffer_request_with_metadata(metadata);

    println!(
        "Relatorio nominal JEV request:\n{}",
        serde_json::to_string_pretty(&request)?
    );

    assert_eq!(request["state"]["problematic_line"]["line_number"], 225);
    assert_eq!(request["state"]["problematic_line"]["expected_fields"], 9);
    assert_eq!(
        request["state"]["problematic_line"]["error"],
        "Csv error: incorrect number of fields for line 225, expected 9 got 10"
    );
    Ok(())
}
