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
use ckan_ingestor_lib::config::S3Settings;
use s3::creds::Credentials;
use s3::region::Region;
use s3::{Bucket, BucketConfiguration};
use std::sync::OnceLock;
use testcontainers::{core::IntoContainerPort, runners::SyncRunner, GenericImage, ImageExt};

static MINIO_ADDRESS: OnceLock<String> = OnceLock::new();

fn ensure_minio() -> &'static String {
    MINIO_ADDRESS.get_or_init(|| {
        let minio = GenericImage::new("minio/minio", "RELEASE.2024-09-22T00-33-43Z")
            .with_exposed_port(9000.tcp())
            .with_exposed_port(9001.tcp())
            .with_env_var("MINIO_ACCESS_KEY", "minioadmin")
            .with_env_var("MINIO_SECRET_KEY", "minioadmin")
            .with_cmd(vec![
                "server".to_string(),
                "/data".to_string(),
                "--address".to_string(),
                ":9000".to_string(),
                "--console-address".to_string(),
                ":9001".to_string(),
            ]);

        let container = minio.start().expect("Can't start minio.");
        let port = container
            .get_host_port_ipv4(9000)
            .expect("Failed to get host port for MinIO");

        std::mem::forget(container);

        format!("127.0.0.1:{}", port)
    })
}

pub fn s3_settings() -> S3Settings {
    let address = ensure_minio();
    let settings = S3Settings {
        endpoint: address.clone(),
        bucket: "warehouse".into(),
        use_ssl: false,
        access_key_id: "minioadmin".to_string(),
        secret_access_key: "minioadmin".to_string(),
        url_style: "path".into(),
        ..Default::default()
    };

    // Create the bucket (path-style, for MinIO).
    let region = Region::Custom {
        region: "us-east-1".into(),
        endpoint: format!("http://{}", address),
    };
    let credentials =
        Credentials::new(Some("minioadmin"), Some("minioadmin"), None, None, None).unwrap();
    if Bucket::new("warehouse", region, credentials)
        .unwrap()
        .exists()
        .unwrap_or(false)
    {
        return settings;
    }
    let creds = Credentials::new(Some("minioadmin"), Some("minioadmin"), None, None, None).unwrap();
    let _ = Bucket::create_with_path_style(
        "warehouse",
        Region::Custom {
            region: "us-east-1".into(),
            endpoint: format!("http://{}", address),
        },
        creds,
        BucketConfiguration::public(),
    )
    .expect("Failed to create MinIO bucket");

    settings
}
