// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_metadata_ingestor::{DuckdbCkanMetadataIngestor, MetadataSyncCommand, MetadataSyncResult};
use duckdb::Connection;

use crate::duckdb_factory::DuckdbFactory;

#[derive(Debug, Clone)]
pub struct ResourceCandidate {
    pub resource_id: String,
    pub resource_name: Option<String>,
    pub resource_url: Option<String>,
    pub resource_format: Option<String>,
    pub dataset_name: String,
    pub datastore_active: bool,
}

pub struct RealMetadataProcessor {
    factory: DuckdbFactory,
    conn: Connection,
}

impl RealMetadataProcessor {
    pub fn new(factory: DuckdbFactory) -> anyhow::Result<Self> {
        let conn = factory.open()?;
        Ok(Self { factory, conn })
    }

    pub fn process(&self, command: MetadataSyncCommand) -> MetadataSyncResult {
        match DuckdbCkanMetadataIngestor::new(&self.conn).sync(&command) {
            Ok(result) => result,
            Err(error) => MetadataSyncResult {
                sync_id: command.sync_id,
                instance_id: command.instance_id,
                instance_name: command.instance_name,
                status: "failure".into(),
                total_packages: 0,
                new_datasets: 0,
                new_resources: 0,
                updated_datasets: 0,
                updated_resources: 0,
                dataset_count: 0,
                resource_count: 0,
                error_message: Some(error.to_string().chars().take(16_000).collect()),
            },
        }
    }

    pub fn outdated_resources(&self, ckan_url: &str) -> anyhow::Result<Vec<ResourceCandidate>> {
        query_outdated_resources(&self.conn, ckan_url)
    }
}

pub fn query_outdated_resources(
    conn: &Connection,
    ckan_url: &str,
) -> anyhow::Result<Vec<ResourceCandidate>> {
    let mut statement = conn.prepare("SELECT r.id, r.name, r.url, r.format, d.name, COALESCE(r.datastore_active, false) FROM ckan_resource r JOIN ckan_dataset d ON r.package_id = d.id ANTI JOIN ckan_resource_last_update u ON r.id = u.ckan_resource_id AND r.last_modified::TIMESTAMP < u.last_modified WHERE r.ckan_url = ?")?;
    let rows = statement.query_map([ckan_url], |row| {
        Ok(ResourceCandidate {
            resource_id: row.get(0)?,
            resource_name: row.get(1)?,
            resource_url: row.get(2)?,
            resource_format: row.get(3)?,
            dataset_name: row.get(4)?,
            datastore_active: row.get(5)?,
        })
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
}

#[cfg(test)]
#[allow(clippy::items_after_test_module)]
mod tests {
    use super::query_outdated_resources;
    use duckdb::Connection;

    #[test]
    fn outdated_resources_are_scoped_to_the_ckan_url() -> anyhow::Result<()> {
        let conn = Connection::open_in_memory()?;
        conn.execute_batch("CREATE TABLE ckan_dataset (id VARCHAR, name VARCHAR); CREATE TABLE ckan_resource (id VARCHAR, name VARCHAR, url VARCHAR, format VARCHAR, package_id VARCHAR, ckan_url VARCHAR, last_modified TIMESTAMP, datastore_active BOOLEAN); CREATE TABLE ckan_resource_last_update (ckan_resource_id VARCHAR, last_modified TIMESTAMP); INSERT INTO ckan_dataset VALUES ('d1', 'Dataset'); INSERT INTO ckan_resource VALUES ('a', 'A', 'https://a', 'CSV', 'd1', 'https://one', '2025-01-02', false), ('b', 'B', 'https://b', 'CSV', 'd1', 'https://two', '2025-01-02', true); INSERT INTO ckan_resource_last_update VALUES ('a', '2025-01-01'), ('b', '2025-01-01');")?;
        let resources = query_outdated_resources(&conn, "https://one")?;
        assert_eq!(resources.len(), 1);
        assert_eq!(resources[0].resource_id, "a");
        Ok(())
    }
}

impl Clone for RealMetadataProcessor {
    fn clone(&self) -> Self {
        let conn = self
            .conn
            .try_clone()
            .expect("failed to clone DuckDB connection");
        self.factory
            .configure(&conn)
            .expect("failed to configure DuckDB connection");
        Self {
            factory: self.factory.clone(),
            conn,
        }
    }
}
