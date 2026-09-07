// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::sync::Arc;

use anyhow::{Context, Result};
use ducklake::{ConnectOptions, CreateOptions, Ducklake, DucklakeError};
use tokio::sync::OnceCell;
use url::Url;

use crate::config::S3Settings;

#[derive(Clone)]
pub struct DucklakeFactory {
    catalog_url: String,
    data_path: String,
    storage_options: Vec<(String, String)>,
    client: Arc<OnceCell<Arc<Ducklake>>>,
}

impl DucklakeFactory {
    pub fn from_env() -> Result<Self> {
        let catalog_url =
            std::env::var("DUCKLAKE_CATALOG_URI").context("DUCKLAKE_CATALOG_URI must be set")?;
        let s3 = S3Settings::from_env();
        Ok(Self::new(
            postgres_url(&catalog_url)?,
            format!("s3://{}", s3.bucket),
            s3_storage_options(&s3),
        ))
    }

    fn new(
        catalog_url: impl Into<String>,
        data_path: impl Into<String>,
        storage_options: Vec<(String, String)>,
    ) -> Self {
        Self {
            catalog_url: catalog_url.into(),
            data_path: data_path.into(),
            storage_options,
            client: Arc::new(OnceCell::new()),
        }
    }

    pub fn for_sqlite(catalog_path: &std::path::Path, data_path: &std::path::Path) -> Self {
        Self::new(
            format!("sqlite://{}", catalog_path.display()),
            data_path.to_string_lossy(),
            vec![],
        )
    }

    pub async fn client(&self) -> Result<Arc<Ducklake>> {
        self.client
            .get_or_try_init(|| self.open_client())
            .await
            .map(Arc::clone)
            .map_err(Into::into)
    }

    pub fn storage_options(&self) -> &[(String, String)] {
        &self.storage_options
    }

    pub async fn initialize(&self) -> Result<()> {
        self.client().await?;
        Ok(())
    }

    async fn open_client(&self) -> std::result::Result<Arc<Ducklake>, DucklakeError> {
        let connect = ConnectOptions::new(&self.catalog_url)
            .with_migrate(true)
            .with_storage_options(self.storage_options.clone());
        let client = match Ducklake::connect(connect).await {
            Ok(client) => client,
            Err(DucklakeError::CatalogNotInitialized) => {
                let create = CreateOptions::new(&self.catalog_url, &self.data_path)
                    .with_storage_options(self.storage_options.clone());
                Ducklake::create(create).await?
            }
            Err(error) => return Err(error),
        };
        Ok(Arc::new(client))
    }
}

fn s3_storage_options(settings: &S3Settings) -> Vec<(String, String)> {
    vec![
        ("aws_access_key_id".into(), settings.access_key_id.clone()),
        (
            "aws_secret_access_key".into(),
            settings.secret_access_key.clone(),
        ),
        ("aws_region".into(), settings.region.clone()),
        ("aws_endpoint".into(), settings.endpoint_url()),
        (
            "aws_virtual_hosted_style_request".into(),
            (settings.url_style != "path").to_string(),
        ),
        ("aws_allow_http".into(), (!settings.use_ssl).to_string()),
    ]
}

fn postgres_url(uri: &str) -> Result<String> {
    if uri.starts_with("postgresql://") || uri.starts_with("postgres://") {
        return Ok(uri.to_string());
    }
    let parameters = uri
        .strip_prefix("postgres:")
        .context("DUCKLAKE_CATALOG_URI must be a PostgreSQL URI")?
        .split_whitespace()
        .filter_map(|part| part.split_once('='))
        .collect::<std::collections::HashMap<_, _>>();
    let host = parameters.get("host").copied().unwrap_or("localhost");
    let port = parameters.get("port").copied().unwrap_or("5432");
    let database = parameters
        .get("dbname")
        .context("catalog dbname is required")?;
    let username = parameters.get("user").context("catalog user is required")?;
    let password = parameters
        .get("password")
        .context("catalog password is required")?;
    let mut url = Url::parse("postgresql://localhost")?;
    url.set_host(Some(host))?;
    url.set_port(Some(port.parse()?))
        .map_err(|_| anyhow::anyhow!("invalid catalog port"))?;
    url.set_path(database);
    url.set_username(username)
        .map_err(|_| anyhow::anyhow!("invalid catalog username"))?;
    url.set_password(Some(password))
        .map_err(|_| anyhow::anyhow!("invalid catalog password"))?;
    Ok(url.into())
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{postgres_url, DucklakeFactory};

    #[test]
    fn converts_the_existing_libpq_catalog_setting_to_a_postgresql_url() {
        assert_eq!(
            postgres_url(
                "postgres:host=database port=5433 dbname=lake user=worker password=secret"
            )
            .unwrap(),
            "postgresql://worker:secret@database:5433/lake"
        );
    }

    #[test]
    fn percent_encodes_credentials_in_the_catalog_url() {
        assert_eq!(
            postgres_url("postgres:dbname=lake user=worker@local password=p@ss").unwrap(),
            "postgresql://worker%40local:p%40ss@localhost:5432/lake"
        );
    }

    #[tokio::test]
    async fn initializes_the_ducklake_catalog() {
        let temp = tempdir().unwrap();
        let factory = DucklakeFactory::for_sqlite(
            &temp.path().join("catalog.sqlite"),
            &temp.path().join("data"),
        );

        factory.initialize().await.unwrap();

        factory.client().await.unwrap();
    }
}
