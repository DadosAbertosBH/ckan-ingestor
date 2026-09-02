// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use ckan_ingestor_lib::duckdb_factory::DuckdbFactory;
use ckan_metadata_ingestor::{DuckdbCkanMetadataIngestor, MetadataSyncCommand, MetadataSyncResult};
use duckdb::Connection;

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
