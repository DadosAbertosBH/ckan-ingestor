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
use reqwest::Client as HttpClient;
use s3::bucket::Bucket;

#[derive(Clone)]
pub struct S3DocumentIngestor {
    pub public_url: String,
    pub bucket: Box<Bucket>,
}

impl S3DocumentIngestor {
    pub fn new(settings: S3Settings) -> Result<Self> {
        let protocol = if settings.use_ssl { "https" } else { "http" };
        let public_url = format!("{}://{}/{}", protocol, settings.endpoint, settings.bucket);
        let bucket = settings.bucket()?;
        Ok(Self { public_url, bucket })
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
        self.put_object(filename, &bytes, content_type).await
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
        handle.block_on(self.put_object(filename, &bytes, content_type))
    }

    async fn put_object(&self, filename: &str, bytes: &[u8], content_type: &str) -> Result<String> {
        let object_url = format!("docs/{filename}");
        self.bucket
            .put_object_with_content_type(object_url.clone(), bytes, content_type)
            .await?;
        Ok(format!("{}/{}", self.public_url, object_url))
    }
}
