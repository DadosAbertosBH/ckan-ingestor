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
use rstest::rstest;

mod fixtures;
use crate::common::fixture_path;
use fixtures::ckan::ckan_mock::{ckan_mock, CkanMock};
use fixtures::s3::s3_settings;

#[rstest]
#[tokio::test]
async fn ingest_pdf(
    #[future] s3_settings: S3Settings,
    #[future] ckan_mock: CkanMock,
) -> Result<()> {
    let ckan_mock = ckan_mock.await;
    let s3_settings = s3_settings.await;

    let ingestor = S3DocumentIngestor::new(s3_settings.clone())?;
    let url = format!(
        "http://{}/datastore/a_pdf_file?format=PDF",
        ckan_mock.server.address()
    );
    let returned = ingestor.ingest("test.pdf", &url, "application/pdf").await?;

    // Verify the object was persisted by downloading it using the same
    // authenticated bucket (rust-s3 doesn't manage public bucket policies).
    let bucket = s3_settings.bucket()?;
    let response_data = bucket.get_object("docs/test.pdf").await?;
    let uploaded_content = response_data.bytes();
    let file = fixture_path("a_pdf_file.pdf");
    let body = std::fs::read(file)?;
    assert_eq!(uploaded_content.as_ref(), body.as_slice());
    assert!(returned.ends_with("/test.pdf"));

    Ok(())
}
