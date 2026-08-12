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
use aws_config::meta::region::RegionProviderChain;
use aws_config::BehaviorVersion;
use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::operation::RequestId;
use aws_sdk_s3::Client;
use ckan_ingestor_lib::config::S3Settings;
use rstest::fixture;
use testcontainers::{core::IntoContainerPort, runners::AsyncRunner, GenericImage, ImageExt};
use tokio::sync::OnceCell;

static MINIO_ADDRESS: OnceCell<String> = OnceCell::const_new();

async fn ensure_minio() -> &'static String {
    MINIO_ADDRESS
        .get_or_init(|| async {
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

            let container = minio.start().await.expect("Can't start minio.");
            let port = container
                .get_host_port_ipv4(9000)
                .await
                .expect("Failed to get host port for MinIO");

            std::mem::forget(container);

            format!("127.0.0.1:{}", port)
        })
        .await
}

#[fixture]
pub async fn s3_client() -> Client {
    let address = ensure_minio().await;
    //noinspection HttpUrlsUsage
    let endpoint_uri = format!("http://{}", address);
    let region_provider = RegionProviderChain::default_provider().or_else("us-east-1");
    let creds = Credentials::new("minioadmin", "minioadmin", None, None, "test");

    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(region_provider)
        .endpoint_url(endpoint_uri)
        .credentials_provider(creds)
        .load()
        .await;

    Client::new(&sdk_config)
}

pub async fn setup_minio_bucket_and_policy(client: &Client) {
    let bucket = "warehouse";
    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListMultipartUploadParts",
                    "s3:AbortMultipartUpload"
                ],
                "Resource": format!("arn:aws:s3:::{}{}", bucket, "/docs/*"),
            }
        ]
    });
    let _ = client.create_bucket().bucket(bucket).send().await.unwrap();
    let result = client
        .put_bucket_policy()
        .bucket(bucket)
        .policy(policy.to_string())
        .send()
        .await
        .unwrap();
    assert!(result.request_id().is_some())
}

#[fixture]
pub async fn s3_settings(#[future] s3_client: Client) -> S3Settings {
    let s3_client = s3_client.await;
    setup_minio_bucket_and_policy(&s3_client).await;
    let address = ensure_minio().await;
    S3Settings {
        endpoint: address.clone(),
        bucket: "warehouse".into(),
        use_ssl: false,
        access_key_id: "minioadmin".to_string(),
        secret_access_key: "minioadmin".to_string(),
        ..Default::default()
    }
}
