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
use anyhow::{Context, Result};
use object_store::{parse_url_opts, path::Path, Attribute, Attributes, ObjectStore, PutOptions};
use reqwest::blocking::Client as HttpClient;
use std::sync::Arc;
use url::Url;

#[derive(Clone)]
pub struct S3DocumentIngestor {
    pub public_url: String,
    store: Arc<dyn ObjectStore>,
}

impl S3DocumentIngestor {
    pub fn new(settings: S3Settings) -> Result<Self> {
        let protocol = if settings.use_ssl { "https" } else { "http" };
        let public_url = format!("{}://{}/{}", protocol, settings.endpoint, settings.bucket);
        let uri = Url::parse(&format!("s3://{}", settings.bucket))?;
        let (store, _) = parse_url_opts(&uri, settings.object_store_options())?;
        Ok(Self {
            public_url,
            store: store.into(),
        })
    }

    #[cfg(test)]
    pub(crate) fn new_with_store(public_url: String, store: Arc<dyn ObjectStore>) -> Self {
        Self { public_url, store }
    }

    pub fn ingest(&self, filename: &str, download_url: &str, content_type: &str) -> Result<String> {
        let client = HttpClient::new();
        let resp = client.get(download_url).send()?;
        let bytes = resp.bytes()?;
        let object_url = format!("docs/{filename}");
        let mut attributes = Attributes::new();
        attributes.insert(Attribute::ContentType, content_type.to_owned().into());
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        runtime
            .block_on(self.store.put_opts(
                &Path::from(object_url.clone()),
                bytes.into(),
                PutOptions {
                    attributes,
                    ..Default::default()
                },
            ))
            .with_context(|| {
                format!(
                    "error uploading document to S3 endpoint {}",
                    self.public_url
                )
            })?;
        Ok(format!("{}/{}", self.public_url, object_url))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::config::S3Settings;
    use object_store::{memory::InMemory, path::Path, Attribute, ObjectStoreExt};

    use super::S3DocumentIngestor;

    #[test]
    fn new_builds_the_public_document_base_url() {
        let settings = S3Settings {
            endpoint: "rustfs.local:9000".to_string(),
            bucket: "documents".to_string(),
            use_ssl: false,
            ..S3Settings::default()
        };

        let ingestor = S3DocumentIngestor::new(settings).expect("valid S3 settings");

        assert_eq!(ingestor.public_url, "http://rustfs.local:9000/documents");
    }

    #[test]
    fn uploads_documents_with_the_object_store_client() {
        let store = Arc::new(InMemory::new());
        let ingestor = S3DocumentIngestor::new_with_store(
            "https://files.example/documents".into(),
            store.clone(),
        );
        let server = httpmock::MockServer::start();
        let _download = server.mock(|when, then| {
            when.method(httpmock::Method::GET).path("/document.pdf");
            then.status(200).body("document");
        });

        let url = ingestor
            .ingest(
                "document.pdf",
                &format!("{}/document.pdf", server.base_url()),
                "application/pdf",
            )
            .unwrap();

        assert_eq!(url, "https://files.example/documents/docs/document.pdf");
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (stored, attributes) = runtime
            .block_on(async {
                let result = store.get(&Path::from("docs/document.pdf")).await?;
                let attributes = result.attributes.clone();
                Ok::<_, object_store::Error>((result.bytes().await?, attributes))
            })
            .unwrap();
        assert_eq!(stored, "document");
        assert_eq!(
            attributes.get(&Attribute::ContentType).map(AsRef::as_ref),
            Some("application/pdf")
        );
    }
}
