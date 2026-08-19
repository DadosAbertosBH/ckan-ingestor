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
use ckan_ingestor_lib::config::S3Settings;
use ckan_ingestor_lib::s3_document_ingestor::S3DocumentIngestor;
use httpmock::{Method::GET, MockServer};

mod fixtures;
use crate::common::fixture_path;
use fixtures::s3::s3_settings;

#[test]
#[ignore = "requires a Docker daemon to run MinIO via testcontainers"]
fn ingest_pdf() -> Result<()> {
    let server = MockServer::start();
    let body = std::fs::read(fixture_path("a_pdf_file.pdf"))?;
    let _download = server.mock(|when, then| {
        when.method(GET)
            .path("/datastore/a_pdf_file")
            .query_param("format", "PDF");
        then.status(200)
            .header("Content-Type", "application/pdf")
            .body(body.clone());
    });

    let s3_settings: S3Settings = s3_settings();

    let ingestor = S3DocumentIngestor::new(s3_settings.clone())?;
    let url = format!("{}/datastore/a_pdf_file?format=PDF", server.base_url());
    let returned = ingestor.ingest("test.pdf", &url, "application/pdf")?;

    // Verify the object was persisted by downloading it using the same
    // authenticated bucket (rust-s3 doesn't manage public bucket policies).
    let bucket = s3_settings.bucket()?;
    let response_data = bucket.get_object("docs/test.pdf")?;
    let uploaded_content = response_data.bytes();
    assert_eq!(uploaded_content.as_ref(), body.as_slice());
    assert!(returned.ends_with("/test.pdf"));

    Ok(())
}
