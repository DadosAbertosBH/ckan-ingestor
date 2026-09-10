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
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;
use tempfile::tempdir;

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
    let reader = CsvReader::new(test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        package_id: String::new(),
        url: fixture_path("csv_with_latin_encode.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
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
    let reader = CsvReader::new(test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        package_id: String::new(),
        url: fixture_path("non_latin1_and_non_utf8.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = reader.read(&resource)?;
    assert_eq!(result.rows_processed, 2);
    // This file uses latin-1 encoding that only works after the PyArrow fallback
    // with semicolon delimiter
    assert!(result.encoding.is_some());
    Ok(())
}

#[test]
fn returns_http_error_for_failed_remote_csv_download() -> Result<()> {
    let server = MockServer::start();
    server.mock(|when, then| {
        when.method(GET).path("/failed.csv");
        then.status(500)
            .header("Content-Type", "text/html")
            .body("<html><title>Erro [500]</title></html>");
    });

    let reader = CsvReader::new(test_client());
    let resource = CkanResource {
        id: "failed-remote-csv".to_string(),
        package_id: String::new(),
        url: format!("{}/failed.csv", server.url("")),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let error = match reader.read(&resource) {
        Ok(_) => anyhow::bail!("a failed remote CSV response should return an HTTP error"),
        Err(error) => error,
    };
    let message = error.to_string();
    assert!(message.contains("500"), "unexpected error: {message}");
    assert!(
        message.contains("Content-Type: text/html"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("Content-Encoding: <missing or invalid>"),
        "unexpected error: {message}"
    );
    Ok(())
}

#[test]
fn parses_dm_subitem_rec_utf8_csv_fixture() -> Result<()> {
    let reader = CsvReader::new(test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        package_id: String::new(),
        url: fixture_path("dm_subitem_rec.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = reader.read(&resource)?;

    assert_eq!(result.rows_processed, 7_134);
    assert_eq!(result.number_of_columns, 3);
    assert_eq!(result.encoding.as_deref(), Some("UTF-8"));
    Ok(())
}

#[test]
fn parses_csv_with_mixed_line_endings_in_quoted_header() -> Result<()> {
    let reader = CsvReader::new(test_client());

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
        package_id: String::new(),
        url: temporary_csv.0.to_str().unwrap().to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = reader.read(&resource)?;

    assert_eq!(result.rows_processed, 2);
    assert_eq!(result.number_of_columns, 10);
    assert_eq!(result.csv_strict_mode, Some(false));
    Ok(())
}

#[test]
fn csv_with_bom() -> Result<()> {
    let reader = CsvReader::new(test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        package_id: String::new(),
        url: fixture_path("csv_with_bom.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };
    let result = reader.read(&resource)?;
    assert_eq!(result.rows_processed, 804);
    assert_eq!(result.encoding.as_deref(), Some("UTF-8"));
    assert_eq!(result.csv_strict_mode, Some(true));
    assert_eq!(result.csv_delimiter.as_deref(), Some(","));
    assert_eq!(result.csv_samples.as_deref(), Some("25000"));
    Ok(())
}

#[test]
fn reads_csv_data_with_rows() -> Result<()> {
    let reader = CsvReader::new(test_client());

    let resource = CkanResource {
        id: "00000000-0000-0000-0000-ffff00000000".to_string(),
        package_id: String::new(),
        url: fixture_path("csv_with_bom.csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = reader.read(&resource)?;
    assert_eq!(result.rows_processed, 804);

    Ok(())
}

#[test]
fn parses_numeric_columns_with_whitespace_padded_dash_as_null() -> Result<()> {
    let reader = CsvReader::new(test_client());

    let resource = CkanResource {
        id: "cb05125e-e879-420f-9bf6-0fcbc75bbc8e".to_string(),
        package_id: String::new(),
        url: fixture_path("despesa_pessoal_mensal(1).csv")
            .to_str()
            .unwrap()
            .to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = reader.read(&resource)?;

    assert_eq!(result.rows_processed, 52);
    assert_eq!(result.number_of_columns, 19);
    let batches: Vec<_> =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(result.parquet.path())?)?
            .build()?
            .collect::<std::result::Result<_, _>>()?;
    assert_eq!(
        batches[0].schema().field(16).data_type(),
        &arrow::datatypes::DataType::Float64
    );
    let null_count: usize = batches
        .iter()
        .map(|batch| batch.column(16).null_count())
        .sum();
    assert_eq!(null_count, 36);
    Ok(())
}

#[test]
fn represents_an_entirely_empty_csv_column_as_nullable_text() -> Result<()> {
    let mut csv = tempfile::NamedTempFile::new()?;
    csv.write_all(b"id,always_empty\n1,\n2,\n")?;

    let reader = CsvReader::new(test_client());
    let resource = CkanResource {
        id: "all-null-column".to_string(),
        package_id: String::new(),
        url: csv.path().to_str().unwrap().to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = reader.read(&resource)?;
    let batches: Vec<_> =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(result.parquet.path())?)?
            .build()?
            .collect::<std::result::Result<_, _>>()?;

    assert_eq!(
        batches[0].schema().field(1).data_type(),
        &arrow::datatypes::DataType::Utf8
    );
    assert_eq!(batches[0].column(1).null_count(), 2);
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

    let reader = CsvReader::new(test_client());
    let resource = CkanResource {
        id: "cfba57bb-358b-4b43-96e6-477920e39f19".to_string(),
        package_id: String::new(),
        url: format!("{}/ft_diarias_2014.csv.gz", server.url("")),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
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

    for id in 1..50_001 {
        csv.push_str(&format!("{id};1;0;NOME\n"));
    }
    csv.push_str(
        "1254412;2;912488000123;\"COOPERATIVA DE CREDITO DE LIVRE ADMISSAO DO ALTO E MED. S; F\"\n",
    );

    let mock = server.mock(|when, then| {
        when.method(GET).path("/dm_favorecido.csv");
        then.status(200).body(csv);
    });

    let reader = CsvReader::with_delimiter(test_client(), Some(";".to_string()));
    let resource = CkanResource {
        id: "0331ad41-85e6-41da-bbf2-19c0505beef5".to_string(),
        package_id: String::new(),
        url: format!("{}/dm_favorecido.csv", server.url("")),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = reader.read(&resource);
    mock.assert();
    let result = result?;

    assert_eq!(result.rows_processed, 50_001);
    assert_eq!(result.encoding.as_deref(), Some("UTF-8"));
    Ok(())
}

#[test]
fn uses_csv_nose_metadata_types_beyond_arrows_inference_window() -> Result<()> {
    let tempdir = tempdir()?;
    let path = tempdir.path().join("metadata-types.csv");
    let mut csv = String::from("value\n");
    for value in 0..50_000 {
        csv.push_str(&format!("{value}\n"));
    }
    csv.push_str("not-a-number\n");
    fs::write(&path, csv)?;
    let resource = CkanResource {
        id: "metadata-types".to_string(),
        package_id: String::new(),
        url: path.to_string_lossy().to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = CsvReader::new(test_client()).read(&resource)?;

    assert_eq!(result.rows_processed, 50_001);
    assert_eq!(
        result.parquet.schema.field_with_name("value")?.data_type(),
        &arrow::datatypes::DataType::Utf8
    );
    Ok(())
}

#[test]
fn represents_unsigned_csv_values_as_signed_integers() -> Result<()> {
    let mut csv = tempfile::NamedTempFile::new()?;
    csv.write_all(b"count\n0\n42\n")?;

    let resource = CkanResource {
        id: "unsigned-integers".to_string(),
        package_id: String::new(),
        url: csv.path().to_string_lossy().to_string(),
        format: "CSV".to_string(),
        datastore_active: false,
        last_modified: String::new(),
    };

    let result = CsvReader::new(test_client()).read(&resource)?;

    assert_eq!(
        result.parquet.schema.field_with_name("count")?.data_type(),
        &arrow::datatypes::DataType::Int64
    );
    Ok(())
}
