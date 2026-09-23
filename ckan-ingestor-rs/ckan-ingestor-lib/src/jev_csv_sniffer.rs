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

use crate::jev_csv_sniffer::CsvNoulQuestion::{
    IsDelimiterCorrect, IsHasHeaderCorrect, IsNumFieldsCorrect,
};
use anyhow::{anyhow, Context, Result};
use csv_nose::{Metadata, Quote};
use encoding_rs::Encoding;
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

const JEV_SYSTEM_ONE_URL: &str = "https://api.typesafe.ai/v1/systemone";

pub enum RepairAction {
    FixInput(PathBuf),
    FixMetadata(Metadata),
}

pub struct JevCsvRepairer {
    client: reqwest::blocking::Client,
    endpoint: String,
    api_key: String,
}

impl JevCsvRepairer {
    pub fn new(client: reqwest::blocking::Client, endpoint: String, api_key: String) -> Self {
        Self {
            client,
            endpoint,
            api_key,
        }
    }

    pub fn from_env(client: reqwest::blocking::Client) -> Option<Self> {
        let api_key = std::env::var("JEV_API_KEY").ok()?;
        let endpoint = std::env::var("JEV_API_URL").unwrap_or_else(|_| JEV_SYSTEM_ONE_URL.into());
        Some(Self::new(client, endpoint, api_key))
    }

    pub fn repair_csv_with_metadata(
        &self,
        csv_path: &Path,
        metadata: &Metadata,
        line_number: usize,
        expect_number_of_columns: usize,
        actual_number_of_columns: usize,
        error_message: String,
    ) -> Result<RepairAction> {
        log::info!(
            "Requesting JEV CSV repair decision for line {}",
            line_number
        );
        let request = build_jev_csv_sniffer_request_with_metadata(
            csv_path,
            metadata,
            line_number,
            expect_number_of_columns,
            actual_number_of_columns,
            error_message,
        )?;
        let response: JevResponse = self
            .client
            .post(&self.endpoint)
            .bearer_auth(&self.api_key)
            .json(&request)
            .send()?
            .error_for_status()?
            .json()?;
        let choice = response
            .answers
            .get("fields_to_merge")
            .and_then(|answer| answer.choice.as_deref())
            .context("JEV response has no fields_to_merge choice")?;

        return match ensure_metadata_answers_are_true(&response) {
            Ok(_) => {
                let (left, right) = merge_choice(choice)?;
                log::info!("JEV approved CSV repair for line {line_number}: choice={choice}");
                let repaired_path = repair_line(csv_path, metadata, line_number, left, right)?;
                log::info!("Created temporary JEV-repaired CSV for line {line_number}");
                Ok(RepairAction::FixInput(repaired_path))
            }
            Err(error) => {
                let is_has_header_correct =
                    response.get_noul_answer_as_bool(&CsvNoulQuestion::IsHasHeaderCorrect)?;
                if !(is_has_header_correct) {
                    let mut new_metadata = metadata.clone();
                    new_metadata.dialect.header.has_header_row = true;
                    Ok(RepairAction::FixMetadata(new_metadata))
                } else {
                    Err(error)
                }
            }
        };
    }
}

#[derive(Deserialize)]
struct JevResponse {
    answers: HashMap<String, JevAnswer>,
}

#[derive(Deserialize)]
struct JevAnswer {
    choice: Option<String>,
    noul: Option<f64>,
}

impl JevResponse {
    fn get_noul_answer_as_bool(&self, question: &CsvNoulQuestion) -> Result<bool> {
        let answer = self.answers.get(question.as_str()).ok_or(anyhow!(
            "JEV response has no question: {}",
            question.as_str()
        ))?;
        answer.get_noul_as_boolean()
    }
}

enum CsvNoulQuestion {
    IsDelimiterCorrect,
    IsHasHeaderCorrect,
    IsNumFieldsCorrect,
}

impl CsvNoulQuestion {
    fn as_str(&self) -> &'static str {
        match self {
            CsvNoulQuestion::IsDelimiterCorrect => "is_delimiter_correct",
            CsvNoulQuestion::IsHasHeaderCorrect => "is_has_header_correct",
            CsvNoulQuestion::IsNumFieldsCorrect => "is_num_fields_correct",
        }
    }
}

impl JevAnswer {
    fn get_noul_as_boolean(&self) -> Result<bool> {
        let value = self.noul.ok_or(anyhow!("JEV response has no noul"))?;
        Ok(value > 0.5)
    }
}

fn ensure_metadata_answers_are_true(answers: &JevResponse) -> Result<()> {
    for question in [IsDelimiterCorrect, IsHasHeaderCorrect, IsNumFieldsCorrect] {
        let answer = answers.get_noul_answer_as_bool(&question);
        anyhow::ensure!(answer?, "JEV did not confirm {}", question.as_str());
    }
    Ok(())
}

fn merge_choice(choice: &str) -> Result<(usize, usize)> {
    let captures = Regex::new(r"^merge_(\d+)_(\d+)$")?
        .captures(choice)
        .context("JEV fields_to_merge choice has an invalid format")?;
    let left = captures[1].parse::<usize>()?;
    let right = captures[2].parse::<usize>()?;
    anyhow::ensure!(
        right == left + 1 && left > 0,
        "JEV choice must merge adjacent fields"
    );
    Ok((left - 1, right - 1))
}

fn repair_line(
    csv_path: &Path,
    metadata: &Metadata,
    line_number: usize,
    left: usize,
    right: usize,
) -> Result<PathBuf> {
    let mut lines = read_lines(csv_path, metadata.encoding.name)?;
    let line = lines
        .get_mut(line_number.saturating_sub(1))
        .with_context(|| format!("CSV has no line {line_number}"))?;
    anyhow::ensure!(
        !line.contains(['\r', '\n']),
        "JEV repair only supports physical CSV lines"
    );

    let delimiter = char::from(metadata.dialect.delimiter);
    let quote = csv_quote(metadata);
    let mut fields = parse_csv_fields(line, delimiter, quote)?;
    anyhow::ensure!(
        right < fields.len(),
        "JEV choice references missing CSV fields"
    );
    let merged = format!("{}{}{}", fields[left], delimiter, fields[right]);
    fields.splice(left..=right, [merged]);
    *line = fields
        .iter()
        .map(|field| escape_csv_field(field, delimiter, quote))
        .collect::<Vec<_>>()
        .join(&delimiter.to_string());

    let output = std::env::temp_dir().join(format!("jev-csv-repair-{}.csv", uuid::Uuid::new_v4()));
    fs::write(&output, lines.join("\n"))?;
    Ok(output)
}

fn csv_quote(metadata: &Metadata) -> char {
    match metadata.dialect.quote {
        Quote::Some(quote) => char::from(quote),
        Quote::None => '"',
    }
}

fn parse_csv_fields(line: &str, delimiter: char, quote: char) -> Result<Vec<String>> {
    let mut fields = Vec::new();
    let mut field = String::new();
    let mut characters = line.chars().peekable();
    let mut quoted = false;

    while let Some(character) = characters.next() {
        if character == quote {
            if quoted && characters.peek() == Some(&quote) {
                field.push(quote);
                characters.next();
            } else {
                quoted = !quoted;
            }
        } else if character == delimiter && !quoted {
            fields.push(std::mem::take(&mut field));
        } else {
            field.push(character);
        }
    }
    anyhow::ensure!(!quoted, "CSV line has an unclosed quoted field");
    fields.push(field);
    Ok(fields)
}

fn escape_csv_field(field: &str, delimiter: char, quote: char) -> String {
    if field.contains(delimiter) || field.contains(quote) || field.contains(['\r', '\n']) {
        format!(
            "{quote}{}{quote}",
            field.replace(quote, &format!("{quote}{quote}"))
        )
    } else {
        field.to_owned()
    }
}

pub fn build_jev_csv_sniffer_request_with_metadata(
    csv_path: &Path,
    metadata: &Metadata,
    problematic_line_number: usize,
    expect_number_of_columns: usize,
    actual_number_of_columns: usize,
    error: String,
) -> Result<Value> {
    let lines = read_lines(csv_path, metadata.encoding.name)?;
    let problematic_line = lines
        .get(problematic_line_number.saturating_sub(1))
        .with_context(|| format!("CSV has no line {problematic_line_number}"))?;
    let parsed_fields = parse_csv_fields(
        problematic_line,
        char::from(metadata.dialect.delimiter),
        csv_quote(metadata),
    )?;
    let questions = questions_json(&parsed_fields, metadata.dialect.delimiter);

    let request = json!({
        "model": "jev-latest",
        "state": {
            "inferred_metadata": metadata_json(metadata),
            "raw_first_line": lines.first(),
            "first_ten_lines": &lines[..lines.len().min(10)],
            "last_ten_lines": &lines[lines.len().saturating_sub(10)..],
            "problematic_line": {
                "line_number": problematic_line_number,
                "raw": problematic_line,
                "parsed_fields": parsed_fields,
                "error": error,
                "expected_fields": expect_number_of_columns,
                "actual_fields": actual_number_of_columns
            }
        },
        "questions": questions
    });

    #[cfg(test)]
    persist_last_jev_test_request(&request)?;

    Ok(request)
}

#[cfg(test)]
fn persist_last_jev_test_request(request: &Value) -> Result<()> {
    use std::io::Write;

    let directory = std::env::temp_dir().join("jev-csv-sniffer-tests");
    fs::create_dir_all(&directory)?;
    let output = directory.join("last-request.json");
    let mut temporary = tempfile::NamedTempFile::new_in(&directory)?;
    serde_json::to_writer_pretty(temporary.as_file_mut(), request)?;
    temporary.write_all(b"\n")?;
    temporary.as_file_mut().sync_all()?;
    temporary.persist(output).map_err(|error| error.error)?;
    Ok(())
}

fn read_lines(csv_path: &Path, encoding_name: &str) -> Result<Vec<String>> {
    let bytes = fs::read(csv_path)?;
    let encoding = Encoding::for_label(encoding_name.as_bytes())
        .with_context(|| format!("unsupported CSV encoding detected: {encoding_name}"))?;
    let (decoded, _, _) = encoding.decode(&bytes);
    Ok(decoded.lines().map(str::to_owned).collect())
}

fn metadata_json(metadata: &Metadata) -> Value {
    let quote = match metadata.dialect.quote {
        Quote::None => Value::Null,
        Quote::Some(quote) => Value::String(char::from(quote).to_string()),
    };
    json!({
        "encoding": {
            "name": metadata.encoding.name,
            "is_utf8": metadata.encoding.is_utf8,
            "has_bom": metadata.encoding.has_bom
        },
        "dialect": {
            "delimiter": char::from(metadata.dialect.delimiter).to_string(),
            "quote": quote,
            "has_header": metadata.dialect.header.has_header_row,
            "preamble_rows": metadata.dialect.header.num_preamble_rows,
            "flexible": metadata.dialect.flexible,
            "is_utf8": metadata.dialect.is_utf8
        },
        "avg_record_len": metadata.avg_record_len,
        "num_fields": metadata.num_fields,
        "fields": metadata.fields,
        "types": metadata.types.iter().map(ToString::to_string).collect::<Vec<_>>()
    })
}

fn questions_json(parsed_fields: &[String], delimiter: u8) -> Value {
    json!({
        "fields_to_merge": {
            "type": "choice",
            "instructions": {
                "question": "Which adjacent fields were incorrectly split by an unquoted delimiter and must be merged to restore the CSV schema?",
                "parsed_fields": "problematic_line.parsed_fields",
                "expected_column_count": "problematic_line.expected_fields",
                "actual_column_count": "problematic_line.actual_fields"
            },
            "criteria": merge_criteria_json(parsed_fields, delimiter)
        },
        "error_source": {
            "type": "choice",
            "instructions": {
                "question": "Is the field-count mismatch caused by an unquoted delimiter in problematic_line or by incorrect inferred metadata?",
                "expected_column_count": "problematic_line.expected_fields",
                "observed_column_count": "problematic_line.actual_fields"
            },
            "criteria": {
                "malformed_line": {
                    "id": "malformed_line",
                    "description": "The inferred metadata is correct, and an unquoted delimiter in problematic_line caused the field-count mismatch."
                },
                "incorrect_metadata": {
                    "id": "incorrect_metadata",
                    "description": "The inferred delimiter, header setting, or expected field count is incorrect for this CSV."
                }
            }
        },
        "is_delimiter_correct": {
            "type": "noul",
            "instructions": "Is inferred_metadata.dialect.delimiter correct? Consider raw_first_line, first_ten_lines, and last_ten_lines.",
            "criteria": {
                "true": "The inferred delimiter correctly separates the CSV fields.",
                "false": "A different delimiter would better separate the CSV fields."
            }
        },
        "is_has_header_correct": {
            "type": "noul",
            "instructions": "Is inferred_metadata.dialect.has_header correct? Base your answer on raw_first_line.",
            "criteria": {
                "true": "The inferred_metadata.dialect.has_header setting is correct.",
                "false": "The inferred_metadata.dialect.has_header setting is incorrect."
            }
        },
        "is_num_fields_correct": {
            "type": "noul",
            "instructions": "Is inferred_metadata.num_fields correct?",
            "criteria": {
                "true": "The inferred number of fields matches the intended CSV schema.",
                "false": "The inferred number of fields does not match the intended CSV schema."
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ckan_resource::CkanResource;
    use crate::readers::ckan_reader::CkanReader;
    use crate::readers::csv_reader::CsvReader;
    use csv_nose::{SampleSize, Sniffer};
    use httpmock::{Method::POST, MockServer};
    use reqwest::blocking::Client;
    use tempfile::tempdir;

    fn fixture_path(file: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("fixtures")
            .join("data")
            .join(file)
    }

    fn sniff_metadata(path: &Path) -> Result<Metadata> {
        let mut sniffer = Sniffer::new();
        sniffer.sample_size(SampleSize::All);
        Ok(sniffer.sniff_path(path)?)
    }

    fn approved_repair_response(choice: &str) -> Value {
        json!({
            "model": "jev-latest",
            "answers": {
                "fields_to_merge": {"type": "choice", "choice": choice},
                "is_delimiter_correct": {"type": "noul", "noul": 1.0},
                "is_has_header_correct": {"type": "noul", "noul": 1.0},
                "is_num_fields_correct": {"type": "noul", "noul": 1.0}
            }
        })
    }

    #[test]
    fn builds_a_complete_jev_request_for_an_extra_csv_field() -> Result<()> {
        let csv_path = fixture_path("inventario.csv");
        let metadata = sniff_metadata(&csv_path)?;
        let request = build_jev_csv_sniffer_request_with_metadata(
            &csv_path,
            &metadata,
            7,
            6,
            7,
            "Csv error: incorrect number of fields for line 7, expected 6 got 7".to_string(),
        )?;

        assert_eq!(request["model"], "jev-latest");
        assert_eq!(request["state"]["problematic_line"]["line_number"], 7);
        assert_eq!(request["state"]["problematic_line"]["expected_fields"], 6);
        assert_eq!(request["state"]["problematic_line"]["actual_fields"], 7);
        assert_eq!(
            request["state"]["inferred_metadata"]["dialect"]["delimiter"],
            ";"
        );
        assert_eq!(request["questions"]["fields_to_merge"]["type"], "choice");
        assert_eq!(request["questions"]["is_delimiter_correct"]["type"], "noul");
        Ok(())
    }

    #[test]
    fn repairs_the_problematic_record_from_a_jev_choice() -> Result<()> {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST)
                .path("/v1/systemone")
                .header("authorization", "Bearer test-key")
                .body_contains("\"line_number\":7");
            then.status(200)
                .json_body(approved_repair_response("merge_2_3"));
        });
        let repairer = JevCsvRepairer::new(
            Client::new(),
            format!("{}/v1/systemone", server.base_url()),
            "test-key".to_string(),
        );
        let csv_path = fixture_path("inventario.csv");
        let metadata = sniff_metadata(&csv_path)?;

        let repaired = repairer.repair_csv_with_metadata(
            &csv_path,
            &metadata,
            7,
            6,
            7,
            "Csv error: incorrect number of fields for line 7, expected 6 got 7".to_string(),
        )?;
        mock.assert();
        let RepairAction::FixInput(repaired) = repaired else {
            panic!("JEV should repair the malformed input file");
        };
        let repaired_contents = fs::read_to_string(&repaired)?;
        assert!(repaired_contents
            .lines()
            .nth(6)
            .unwrap()
            .contains("\"Descrição"));
        fs::remove_file(repaired)?;
        Ok(())
    }

    #[test]
    fn csv_reader_retries_with_the_jev_repaired_file() -> Result<()> {
        let server = MockServer::start();
        let mock = server.mock(|when, then| {
            when.method(POST).path("/v1/systemone");
            then.status(200)
                .json_body(approved_repair_response("merge_2_3"));
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
}

fn merge_criteria_json(parsed_fields: &[String], delimiter: u8) -> Value {
    let mut criteria = serde_json::Map::new();
    for index in 0..parsed_fields.len().saturating_sub(1) {
        let id = format!("merge_{}_{}", index + 1, index + 2);
        criteria.insert(
            id.clone(),
            json!({
                "id": id,
                "left_field": format!("problematic_line.parsed_fields[{index}]"),
                "right_field": format!("problematic_line.parsed_fields[{}]", index + 1),
                "merged_value": format!(
                    "{}{}{}",
                    parsed_fields[index],
                    char::from(delimiter),
                    parsed_fields[index + 1]
                ),
                "action": "Merge these two adjacent parsed fields."
            }),
        );
    }
    Value::Object(criteria)
}
#[cfg(test)]
mod migrated_tests {
    use super::*;
    use crate::ckan_resource::CkanResource;
    use crate::readers::ckan_reader::CkanReader;
    use crate::readers::csv_reader::CsvReader;
    use csv_nose::{SampleSize, Sniffer};
    use httpmock::{Method::POST, MockServer};
    use reqwest::blocking::Client;
    use tempfile::tempdir;

    fn fixture_path(file: &str) -> PathBuf {
        PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/data")
            .join(file)
    }

    fn sniff_metadata(path: &Path) -> Result<Metadata> {
        let mut sniffer = Sniffer::new();
        sniffer.sample_size(SampleSize::All);
        Ok(sniffer.sniff_path(path)?)
    }

    #[test]
    fn builds_a_complete_jev_request_for_an_extra_csv_field() -> Result<()> {
        let csv_path = fixture_path("inventario.csv");
        let metadata = sniff_metadata(&csv_path)?;
        let request = build_jev_csv_sniffer_request_with_metadata(
            &csv_path,
            &metadata,
            7,
            6,
            7,
            "Csv error: incorrect number of fields for line 7, expected 6 got 7".to_string(),
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
    fn writes_and_replaces_the_last_jev_test_request() -> Result<()> {
        let csv_path = fixture_path("inventario.csv");
        let metadata = sniff_metadata(&csv_path)?;
        let request_path = std::env::temp_dir()
            .join("jev-csv-sniffer-tests")
            .join("last-request.json");

        let first_request = build_jev_csv_sniffer_request_with_metadata(
            &csv_path,
            &metadata,
            7,
            6,
            7,
            "first request".to_string(),
        )?;
        let persisted_first: Value = serde_json::from_slice(&fs::read(&request_path)?)?;
        assert_eq!(persisted_first, first_request);

        let last_request = build_jev_csv_sniffer_request_with_metadata(
            &csv_path,
            &metadata,
            8,
            6,
            7,
            "last request".to_string(),
        )?;
        let persisted_last: Value = serde_json::from_slice(&fs::read(request_path)?)?;
        assert_eq!(persisted_last, last_request);
        assert_ne!(persisted_first, persisted_last);
        Ok(())
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
            6,
            7,
            "Csv error: incorrect number of fields for line 7, expected 6 got 7".to_string(),
        )?;
        let RepairAction::FixInput(repaired) = repaired else {
            panic!("JEV should repair the malformed input file");
        };

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
    fn logs_jev_request_for_wrong_metadata() -> Result<()> {
        let csv_path = fixture_path("wrong_metadata_inference.csv");
        let metadata = sniff_metadata(&csv_path)?;
        let request = build_jev_csv_sniffer_request_with_metadata(
            &csv_path,
            &metadata,
            225,
            9,
            10,
            "Csv error: incorrect number of fields for line 225, expected 9 got 10".to_string(),
        )?;

        assert_eq!(request["state"]["problematic_line"]["line_number"], 225);
        assert_eq!(request["state"]["problematic_line"]["expected_fields"], 9);
        assert_eq!(
            request["state"]["problematic_line"]["error"],
            "Csv error: incorrect number of fields for line 225, expected 9 got 10"
        );
        Ok(())
    }
}
