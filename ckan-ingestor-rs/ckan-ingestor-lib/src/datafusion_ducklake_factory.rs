// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::{Context, Result};
use datafusion::{execution::runtime_env::RuntimeEnv, prelude::SessionContext};
use datafusion_ducklake::{
    metadata_writer::MetadataWriter, DuckLakeCatalog, DuckLakeTableWriter, MetadataProvider,
    PostgresMetadataProvider, PostgresSingleCatalogMetadataWriter,
};
use object_store::{aws::AmazonS3Builder, local::LocalFileSystem, ObjectStore};
use std::sync::Arc;
use url::Url;

use crate::config::S3Settings;

#[derive(Clone, Debug)]
pub struct DatafusionDucklakeFactory {
    catalog_url: String,
    data_path: String,
    s3: Option<S3Settings>,
}

impl DatafusionDucklakeFactory {
    pub fn from_env() -> Result<Self> {
        let catalog_url =
            std::env::var("DUCKLAKE_CATALOG_URI").context("DUCKLAKE_CATALOG_URI must be set")?;
        let s3 = S3Settings::from_env();
        Ok(Self {
            catalog_url: postgres_url(&catalog_url)?,
            data_path: format!("s3://{}", s3.bucket),
            s3: Some(s3),
        })
    }

    #[cfg(test)]
    pub fn for_postgres(catalog_url: impl Into<String>, data_path: impl Into<String>) -> Self {
        Self {
            catalog_url: catalog_url.into(),
            data_path: data_path.into(),
            s3: None,
        }
    }

    pub fn table(&self, table: &str) -> String {
        let table = table.replace('"', "\"\"");
        format!("ducklake.main.\"{table}\"")
    }

    pub async fn session(&self) -> Result<SessionContext> {
        let runtime = Arc::new(RuntimeEnv::default());
        let store = self.object_store()?;
        if let Some(s3) = &self.s3 {
            runtime.register_object_store(&Url::parse(&format!("s3://{}/", s3.bucket))?, store);
        }
        let writer = self.metadata_writer().await?;
        let provider = Arc::new(PostgresMetadataProvider::new(&self.catalog_url).await?);
        let catalog = DuckLakeCatalog::with_writer(provider, Arc::new(writer))?;
        let ctx = if self.s3.is_some() {
            SessionContext::new_with_config_rt(Default::default(), runtime)
        } else {
            SessionContext::new()
        };
        ctx.register_catalog("ducklake", Arc::new(catalog));
        Ok(ctx)
    }

    pub async fn table_writer(&self) -> Result<DuckLakeTableWriter> {
        DuckLakeTableWriter::new(
            Arc::new(self.metadata_writer().await?),
            self.object_store()?,
        )
        .map_err(Into::into)
    }

    async fn metadata_writer(&self) -> Result<PostgresSingleCatalogMetadataWriter> {
        let writer = PostgresSingleCatalogMetadataWriter::new_with_init(&self.catalog_url).await?;
        writer.set_data_path(&self.data_path)?;
        let provider = PostgresMetadataProvider::new(&self.catalog_url).await?;
        let snapshot_id = provider.get_current_snapshot()?;
        if provider.get_schema_by_name("main", snapshot_id)?.is_none() {
            let snapshot_id = writer.create_snapshot()?;
            writer.get_or_create_schema("main", None, snapshot_id)?;
        }
        Ok(writer)
    }

    fn object_store(&self) -> Result<Arc<dyn ObjectStore>> {
        match &self.s3 {
            Some(s3) => Ok(Arc::new(
                AmazonS3Builder::new()
                    .with_endpoint(s3.endpoint_url())
                    .with_bucket_name(&s3.bucket)
                    .with_access_key_id(&s3.access_key_id)
                    .with_secret_access_key(&s3.secret_access_key)
                    .with_region(&s3.region)
                    .with_allow_http(!s3.use_ssl)
                    .with_virtual_hosted_style_request(s3.url_style != "path")
                    .build()?,
            )),
            None => Ok(Arc::new(LocalFileSystem::new())),
        }
    }
}

fn postgres_url(uri: &str) -> Result<String> {
    if uri.starts_with("postgresql://") {
        return Ok(uri.to_string());
    }
    let parameters = uri
        .strip_prefix("postgres:")
        .context("DUCKLAKE_CATALOG_URI must be a PostgreSQL URI")?
        .split_whitespace()
        .filter_map(|part| part.split_once('='))
        .collect::<std::collections::HashMap<_, _>>();
    let host = parameters.get("host").copied().unwrap_or("localhost");
    let port = parameters.get("port").copied().unwrap_or("5432").parse()?;
    let database = parameters
        .get("dbname")
        .context("catalog dbname is required")?;
    let username = parameters.get("user").context("catalog user is required")?;
    let password = parameters
        .get("password")
        .context("catalog password is required")?;
    let mut url = Url::parse("postgresql://localhost")?;
    url.set_host(Some(host))?;
    url.set_port(Some(port))
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
    use super::postgres_url;

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
}
