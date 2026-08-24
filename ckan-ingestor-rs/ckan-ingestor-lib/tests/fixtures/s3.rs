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

static RUSTFS_ADDRESS: OnceLock<String> = OnceLock::new();

fn ensure_rustfs() -> &'static String {
    RUSTFS_ADDRESS.get_or_init(|| {
        let rustfs = GenericImage::new("rustfs/rustfs", "latest")
            .with_exposed_port(9000.tcp())
            .with_exposed_port(9001.tcp())
            .with_env_var("RUSTFS_ACCESS_KEY", "admin")
            .with_env_var("RUSTFS_SECRET_KEY", "password")
            .with_cmd(vec![
                "--access-key".to_string(),
                "admin".to_string(),
                "--secret-key".to_string(),
                "password".to_string(),
                "/data".to_string(),
            ]);

        let container = rustfs.start().expect("Can't start RustFS.");
        let port = container
            .get_host_port_ipv4(9000)
            .expect("Failed to get host port for RustFS");

        std::mem::forget(container);

        format!("127.0.0.1:{}", port)
    })
}

pub fn s3_settings() -> S3Settings {
    let address = ensure_rustfs();
    let settings = S3Settings {
        endpoint: address.clone(),
        bucket: "warehouse".into(),
        use_ssl: false,
        access_key_id: "admin".to_string(),
        secret_access_key: "password".to_string(),
        url_style: "path".into(),
        ..Default::default()
    };

    // Create the bucket using RustFS's S3-compatible API.
    let region = Region::Custom {
        region: "us-east-1".into(),
        endpoint: format!("http://{}", address),
    };
    let credentials = Credentials::new(Some("admin"), Some("password"), None, None, None).unwrap();
    if Bucket::new("warehouse", region, credentials)
        .unwrap()
        .exists()
        .unwrap_or(false)
    {
        return settings;
    }
    let creds = Credentials::new(Some("admin"), Some("password"), None, None, None).unwrap();
    let _ = Bucket::create_with_path_style(
        "warehouse",
        Region::Custom {
            region: "us-east-1".into(),
            endpoint: format!("http://{}", address),
        },
        creds,
        BucketConfiguration::public(),
    )
    .expect("Failed to create RustFS bucket");

    settings
}
