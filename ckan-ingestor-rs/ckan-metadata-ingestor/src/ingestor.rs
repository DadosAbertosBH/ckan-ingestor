// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use crate::fetcher::CkanDatasetFetcher;
use crate::ipc::StructuredIpc;
use crate::models::{MetadataSyncCommand, MetadataSyncResult};
use anyhow::Result;
use duckdb::Connection;
use std::collections::HashSet;

pub struct DuckdbCkanMetadataIngestor<'a> {
    pub(crate) conn: &'a Connection,
}

impl<'a> DuckdbCkanMetadataIngestor<'a> {
    pub fn new(conn: &'a Connection) -> Self {
        Self { conn }
    }

    pub fn sync(&self, command: &MetadataSyncCommand) -> Result<MetadataSyncResult> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS ckan_resource_last_update \
             (ckan_resource_id VARCHAR, last_modified TIMESTAMP)",
        )?;
        let ipc = StructuredIpc::write(
            CkanDatasetFetcher::fetch(&command.instance_url)?,
            &command.instance_url,
        )?;
        self.ingest_ipc(command, &ipc)
    }

    fn ingest_ipc(
        &self,
        command: &MetadataSyncCommand,
        ipc: &StructuredIpc,
    ) -> Result<MetadataSyncResult> {
        self.conn.execute_batch("BEGIN")?;
        let result = (|| {
            let datasets = self.merge_ipc(ipc.packages(), "ckan_dataset", "metadata_modified")?;
            let resource_result = match ipc.resources() {
                Some(resources) => self.merge_ipc(resources, "ckan_resource", "last_modified")?,
                None => crate::duckdb::MergeResult {
                    new: 0,
                    updated_ids: vec![],
                },
            };
            let mut updated_datasets: HashSet<String> = datasets.updated_ids.into_iter().collect();
            for resource_id in &resource_result.updated_ids {
                if let Ok(package_id) = self.conn.query_row(
                    "SELECT package_id FROM ckan_resource WHERE id = ?",
                    [resource_id],
                    |row| row.get::<_, String>(0),
                ) {
                    updated_datasets.insert(package_id);
                }
            }
            Ok(MetadataSyncResult {
                sync_id: command.sync_id.clone(),
                instance_id: command.instance_id.clone(),
                instance_name: command.instance_name.clone(),
                status: "success".into(),
                total_packages: ipc.package_rows() as i64,
                new_datasets: datasets.new,
                new_resources: resource_result.new,
                updated_datasets: updated_datasets.len() as i64,
                updated_resources: resource_result.updated_ids.len() as i64,
                dataset_count: ipc.package_rows() as i64,
                resource_count: ipc.resource_rows() as i64,
                error_message: None,
            })
        })();
        match result {
            Ok(value) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(value)
            }
            Err(error) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }
}
