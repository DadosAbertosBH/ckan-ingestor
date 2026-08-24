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
use ckan_ingestor_lib::readers::ckan_reader::CkanReader;
use ckan_ingestor_lib::readers::csv_reader::CsvReader;
use common::fixture_path;
use flate2::{write::GzEncoder, Compression};
use httpmock::{Method::GET, MockServer};
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

fn test_client() -> reqwest::blocking::Client {
    reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(600))
        .user_agent(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0",
        )
        .build()
        .unwrap()
}

#[test]
fn parse_latin_encoded_csv() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("csv_with_latin_encode.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    // Python equivalent: test_parse_latin_encoded_csv_file
    // Just verifies it doesn't error. DuckDB with encoding='latin-1'
    // may fall through to PyArrow fallback for semicolon-delimited files.
    let result = reader.read(&resource)?;
    assert!(result.rows_processed > 0, "Should parse at least 1 row");
    assert!(result.encoding.is_some());
    Ok(())
}

#[test]
fn parse_non_latin_and_non_utf8() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("non_latin1_and_non_utf8.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let result = reader.read(&resource)?;
    assert_eq!(result.rows_processed, 2);
    // This file uses latin-1 encoding that only works after the PyArrow fallback
    // with semicolon delimiter
    assert!(result.encoding.is_some());
    Ok(())
}

#[test]
fn parses_csv_with_mixed_line_endings_in_quoted_header() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());

    struct TemporaryCsv(PathBuf);

    impl Drop for TemporaryCsv {
        fn drop(&mut self) {
            let _ = fs::remove_file(&self.0);
        }
    }

    let csv_path = std::env::temp_dir().join(format!(
        "voos-multiline-header-{}.csv",
        uuid::Uuid::new_v4()
    ));
    let temporary_csv = TemporaryCsv(csv_path);

    let mut csv = Vec::new();
    csv.extend_from_slice(
        b"Reg Voo;ANO;DATA;SOLICITANTE;PASSAGEIROS;AERONAVE;MATR;ORIGEM;DESTINO 1;\"DESTINO 2\n(quando houve)\"\r\n",
    );
    for id in 1..=2 {
        csv.extend_from_slice(
            format!(
                "{id};2011;01/01/2011;Governador;Passageiro;Aeronave;PT-ABC;Origem;Destino;\r\n"
            )
            .as_bytes(),
        );
    }
    fs::write(&temporary_csv.0, csv)?;

    let resource = CkanResource {
        id: "fa4f8391-33d1-46ed-9e9e-22ca2ae51103".to_string(),
        url: temporary_csv.0.to_str().unwrap().to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let result = reader.read(&resource)?;

    assert_eq!(result.rows_processed, 2);
    assert_eq!(result.number_of_columns, 10);
    assert_eq!(result.csv_strict_mode, Some(false));
    Ok(())
}

#[test]
fn csv_with_bom() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("csv_with_bom.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };
    let result = reader.read(&resource)?;
    assert_eq!(result.rows_processed, 804);
    assert_eq!(result.encoding.as_deref(), Some("UTF-8"));
    assert_eq!(result.csv_strict_mode, Some(true));
    Ok(())
}

#[test]
fn reads_csv_data_with_rows() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        url: fixture_path("csv_with_bom.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let result = reader.read(&resource)?;
    assert_eq!(result.rows_processed, 804);

    Ok(())
}

#[test]
fn parses_numeric_columns_with_whitespace_padded_dash_as_null() -> Result<()> {
    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());

    let resource = CkanResource {
        id: "cb05125e-e879-420f-9bf6-0fcbc75bbc8e".to_string(),
        url: fixture_path("despesa_pessoal_mensal(1).csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let result = reader.read(&resource)?;

    assert_eq!(result.rows_processed, 52);
    assert_eq!(result.number_of_columns, 19);
    assert_eq!(
        result.data[0].schema().field(16).data_type(),
        &duckdb::arrow::datatypes::DataType::Float64
    );
    let null_count: usize = result
        .data
        .iter()
        .map(|batch| batch.column(16).null_count())
        .sum();
    assert_eq!(null_count, 36);
    Ok(())
}

#[test]
fn parse_remote_gzip_csv() -> Result<()> {
    let server = MockServer::start();
    let mut compressed = Vec::new();
    let mut encoder = GzEncoder::new(&mut compressed, Compression::default());
    encoder.write_all(b"name,value\nAna,1\nBia,2\n")?;
    encoder.finish()?;
    server.mock(|when, then| {
        when.method(GET).path("/ft_diarias_2014.csv.gz");
        then.status(200).body(compressed.clone());
    });

    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());
    let resource = CkanResource {
        id: "cfba57bb-358b-4b43-96e6-477920e39f19".to_string(),
        url: format!("{}/ft_diarias_2014.csv.gz", server.url("")),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let result = reader.read(&resource)?;
    assert_eq!(result.rows_processed, 2);
    Ok(())
}

#[test]
fn fails_to_parse_quoted_semicolon_after_long_csv_sample() -> Result<()> {
    let server = MockServer::start();
    let mut csv =
        String::from("id_favorecido;tp_documento;nr_documento_anonimizado;nome_anonimizado\n");

    for id in 1..800_000 {
        csv.push_str(&format!("{id};1;0;NOME\n"));
    }
    csv.push_str(
        "1254412;2;912488000123;\"COOPERATIVA DE CREDITO DE LIVRE ADMISSAO DO ALTO E MED. S; F\"\n",
    );

    let mock = server.mock(|when, then| {
        when.method(GET).path("/dm_favorecido.csv");
        then.status(200).body(csv);
    });

    let conn = duckdb::Connection::open_in_memory()?;
    let reader = CsvReader::new(&conn, test_client());
    let resource = CkanResource {
        id: "0331ad41-85e6-41da-bbf2-19c0505beef5".to_string(),
        url: format!("{}/dm_favorecido.csv", server.url("")),
        format: "CSV".to_string(),
        datastore_active: false,
    };

    let result = reader.read(&resource);
    mock.assert();
    let result = result?;

    assert_eq!(result.rows_processed, 800_000);
    assert_eq!(result.encoding.as_deref(), Some("UTF-8"));
    Ok(())
}
