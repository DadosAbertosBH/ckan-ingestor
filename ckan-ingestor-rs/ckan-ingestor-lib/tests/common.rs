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
use std::path::PathBuf;

pub fn fixture_path(file: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("data")
        .join(file)
}

/// Create a dummy S3DocumentIngestor for tests that don't need real S3.
/// Uses localhost with dummy credentials — the S3 client will fail if actually
/// called, but CSV/datastore/JSON tests never invoke S3 operations.
pub fn dummy_s3_ingestor() -> &'static ckan_ingestor_lib::s3_document_ingestor::S3DocumentIngestor {
    use aws_sdk_s3::config::{Credentials, Region};
    use ckan_ingestor_lib::config::S3Settings;
    use ckan_ingestor_lib::s3_document_ingestor::S3DocumentIngestor;
    use std::sync::OnceLock;

    static DUMMY: OnceLock<S3DocumentIngestor> = OnceLock::new();
    DUMMY.get_or_init(|| {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let config = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .endpoint_url("http://localhost:9000")
                .region(Region::new("us-east-1"))
                .credentials_provider(Credentials::new("dummy", "dummy", None, None, "static"))
                .load()
                .await;
            let s3_client = aws_sdk_s3::Client::new(&config);
            let settings = S3Settings {
                endpoint: "localhost:9000".into(),
                bucket: "warehouse".into(),
                use_ssl: false,
                access_key_id: "dummy".into(),
                secret_access_key: "dummy".into(),
                ..S3Settings::default()
            };
            S3DocumentIngestor::new(settings, s3_client).unwrap()
        })
    })
}
