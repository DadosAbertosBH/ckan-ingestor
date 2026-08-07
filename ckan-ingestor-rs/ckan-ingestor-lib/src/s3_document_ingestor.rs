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
use crate::config::S3Settings;
use anyhow::Result;
use aws_sdk_s3::Client as S3Client;
use reqwest::Client as HttpClient;

pub struct S3DocumentIngestor {
    pub public_url: String,
    pub s3_client: S3Client,
    pub bucket: String,
}

impl S3DocumentIngestor {
    pub fn new(settings: S3Settings, s3_client: S3Client) -> Result<Self> {
        let protocol = if settings.use_ssl { "https" } else { "http" };
        let public_url = format!("{}://{}/{}", protocol, settings.endpoint, settings.bucket);
        Ok(Self {
            public_url,
            s3_client,
            bucket: settings.bucket,
        })
    }

    pub async fn ingest(
        &self,
        filename: &str,
        download_url: &str,
        content_type: &str,
    ) -> Result<String> {
        let client = HttpClient::new();
        let resp = client.get(download_url).send().await?;
        let bytes = resp.bytes().await?;
        self.put_object(filename, bytes, content_type).await
    }

    /// Synchronous version — for use inside spawn_blocking.
    pub fn ingest_blocking(
        &self,
        filename: &str,
        download_url: &str,
        content_type: &str,
    ) -> Result<String> {
        let client = reqwest::blocking::Client::new();
        let resp = client.get(download_url).send()?;
        let bytes = resp.bytes()?;
        let handle = tokio::runtime::Handle::current();
        handle.block_on(self.put_object(filename, bytes, content_type))
    }

    async fn put_object(
        &self,
        filename: &str,
        bytes: bytes::Bytes,
        content_type: &str,
    ) -> Result<String> {
        let object_url = format!("docs/{filename}");
        self.s3_client
            .put_object()
            .bucket(&self.bucket)
            .key(object_url.clone())
            .body(bytes.into())
            .content_type(content_type)
            .send()
            .await?;
        Ok(format!("{}/{}", self.public_url, object_url))
    }
}
