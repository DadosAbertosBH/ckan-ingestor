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
use crate::readers::csv_reader::{
    ColumnCountMissmatchError, CsvParserError, CSV_READER_INITIAL_SAMPLE_RECORDS,
};
use anyhow::{anyhow, Context, Result};
use csv_nose::{Metadata, Quote, SampleSize, Sniffer};
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
        error: ColumnCountMissmatchError,
    ) -> Result<RepairAction> {
        log::info!("Requesting JEV CSV repair decision for line {}", error.line);
        let request = build_jev_csv_sniffer_request_with_metadata(
            csv_path,
            metadata,
            error.line,
            error.error,
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
                log::info!(
                    "JEV approved CSV repair for line {problematic_line_number}: choice={choice}"
                );
                let repaired_path =
                    repair_line(csv_path, metadata, problematic_line_number, left, right)?;
                log::info!("Created temporary JEV-repaired CSV for line {problematic_line_number}");
                Ok(RepairAction::FixInput(repaired_path))
            }
            Err(error) => {
                let is_has_header_correct =
                    response.getNoulAnswerAsBool(&CsvNoulQuestion::IsHasHeaderCorrect)?;
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
    fn getNoulAnswerAsBool(&self, question: &CsvNoulQuestion) -> Result<bool> {
        let answer = self.answers.get(question.as_str()).ok_or(anyhow!(
            "JEV response has no question: {}",
            question.as_str()
        ))?;
        answer.getNoulAsBoolean()
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
    fn getNoulAsBoolean(&self) -> Result<bool> {
        let value = self.noul.ok_or(anyhow!("JEV response has no noul"))?;
        Ok(value > 0.5)
    }
}

fn ensure_metadata_answers_are_true(answers: &JevResponse) -> Result<()> {
    for question in [IsDelimiterCorrect, IsHasHeaderCorrect, IsNumFieldsCorrect] {
        let answer = answers.getNoulAnswerAsBool(&question);
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
    let actual_fields = parsed_fields.len();
    let questions = questions_json(&parsed_fields, metadata.dialect.delimiter);

    Ok(json!({
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
                "expected_fields": metadata.num_fields,
                "actual_fields": actual_fields
            }
        },
        "questions": questions
    }))
}

fn sniff_metadata(csv_path: &Path) -> Result<Metadata> {
    let mut sniffer = Sniffer::new();
    sniffer.sample_size(SampleSize::Records(CSV_READER_INITIAL_SAMPLE_RECORDS));
    Ok(sniffer.sniff_path(csv_path)?)
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
