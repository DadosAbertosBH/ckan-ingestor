// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::{Context, Result, bail};
use duckdb::Connection;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::io::Write;

const PAGE_SIZE: usize = 100;
const CKAN_USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15; rv:132.0) Gecko/20100101 Firefox/132.0";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataSyncCommand {
    pub sync_id: String,
    pub instance_id: String,
    pub instance_name: String,
    pub instance_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MetadataSyncResult {
    pub sync_id: String,
    pub instance_id: String,
    pub instance_name: String,
    pub status: String,
    pub total_packages: i64,
    pub new_datasets: i64,
    pub new_resources: i64,
    pub updated_datasets: i64,
    pub updated_resources: i64,
    pub dataset_count: i64,
    pub resource_count: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
}

#[derive(Debug)]
struct MergeResult {
    new: i64,
    updated_ids: Vec<String>,
}

pub struct CkanDatasetFetcher;
impl CkanDatasetFetcher {
    pub fn fetch(url: &str) -> Result<Vec<Value>> {
        let client = reqwest::blocking::Client::builder()
            .user_agent(CKAN_USER_AGENT)
            .build()?;
        let base = url.trim_end_matches('/');
        let mut offset = 0;
        let mut packages = Vec::new();
        loop {
            let endpoint = format!(
                "{base}/api/action/current_package_list_with_resources?limit={PAGE_SIZE}&offset={offset}"
            );
            let response: Value = client
                .get(endpoint)
                .send()?
                .error_for_status()?
                .json()
                .context("invalid CKAN JSON response")?;
            let page = response
                .get("result")
                .and_then(Value::as_array)
                .context("CKAN response result must be an array")?;
            let count = page.len();
            packages.extend(page.iter().cloned());
            if count < PAGE_SIZE {
                break;
            }
            offset += PAGE_SIZE;
        }
        if packages.is_empty() {
            bail!("No packages returned from CKAN API");
        }
        Ok(packages)
    }
}

pub struct DuckdbCkanMetadataIngestor<'a> {
    conn: &'a Connection,
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
        let packages = CkanDatasetFetcher::fetch(&command.instance_url)?;
        self.ingest_packages(command, packages)
    }

    pub fn ingest_packages(
        &self,
        command: &MetadataSyncCommand,
        mut packages: Vec<Value>,
    ) -> Result<MetadataSyncResult> {
        for package in &mut packages {
            if let Some(object) = package.as_object_mut() {
                object.remove("extras");
            }
        }
        drop_empty_list_columns(&mut packages);
        validate_list_shapes(&packages)?;
        let resources = extract_resources(&packages, &command.instance_url);
        self.conn.execute_batch("BEGIN")?;
        let result = (|| {
            let datasets = self.merge_rows(&packages, "ckan_dataset", "metadata_modified", true)?;
            let resource_result =
                self.merge_rows(&resources, "ckan_resource", "last_modified", false)?;
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
                total_packages: packages.len() as i64,
                new_datasets: datasets.new,
                new_resources: resource_result.new,
                updated_datasets: updated_datasets.len() as i64,
                updated_resources: resource_result.updated_ids.len() as i64,
                dataset_count: packages.len() as i64,
                resource_count: resources.len() as i64,
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

    fn merge_rows(
        &self,
        rows: &[Value],
        table: &str,
        updated_at: &str,
        is_dataset: bool,
    ) -> Result<MergeResult> {
        if rows.is_empty() {
            return Ok(MergeResult {
                new: 0,
                updated_ids: vec![],
            });
        }
        let stage = "incoming_metadata";
        self.create_stage(rows, stage)?;
        if is_dataset {
            self.conn.execute_batch(&format!("CREATE OR REPLACE TEMP TABLE incoming_metadata_clean AS SELECT * EXCLUDE (resources) FROM {stage}"))?;
            self.conn.execute_batch(&format!("DROP TABLE {stage}"))?;
            self.conn
                .execute_batch("ALTER TABLE incoming_metadata_clean RENAME TO incoming_metadata")?;
        }
        if !self.table_exists(table)? {
            self.conn
                .execute_batch(&format!("CREATE TABLE {table} AS SELECT * FROM {stage}"))?;
            return Ok(MergeResult {
                new: rows.len() as i64,
                updated_ids: vec![],
            });
        }
        self.align_target_schema(table, stage)?;
        let updated_ids = self.updated_ids(table, stage, updated_at)?;
        self.conn.execute_batch(&format!("DELETE FROM {table} AS target USING {stage} AS source WHERE target.id = source.id AND CAST(source.\"{updated_at}\" AS VARCHAR) > CAST(target.\"{updated_at}\" AS VARCHAR)"))?;
        let names = self
            .columns(stage)?
            .iter()
            .map(|name| quote(name))
            .collect::<Vec<_>>()
            .join(", ");
        let inserted = self.conn.execute(
            &format!("INSERT INTO {table} ({names}) SELECT {names} FROM {stage} ANTI JOIN {table} USING (id)"),
            [],
        )? as i64;
        Ok(MergeResult {
            new: inserted - updated_ids.len() as i64,
            updated_ids,
        })
    }

    fn create_stage(&self, rows: &[Value], stage: &str) -> Result<()> {
        let mut file = tempfile::NamedTempFile::new()?;
        serde_json::to_writer(&mut file, rows)?;
        file.flush()?;
        let path = file.path().to_string_lossy().replace('\'', "''");
        self.conn.execute_batch(&format!(
            "CREATE OR REPLACE TEMP TABLE {stage} AS SELECT * FROM read_json_auto('{path}')"
        ))?;
        Ok(())
    }
    fn table_exists(&self, table: &str) -> Result<bool> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table],
            |row| row.get::<_, i64>(0),
        )? > 0)
    }
    fn columns(&self, table: &str) -> Result<Vec<String>> {
        let mut statement = self
            .conn
            .prepare(&format!("PRAGMA table_info('{table}')"))?;
        statement
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
    fn align_target_schema(&self, target: &str, stage: &str) -> Result<()> {
        let mut target_statement = self
            .conn
            .prepare(&format!("PRAGMA table_info('{target}')"))?;
        let target_fields = target_statement.query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })?;
        let existing =
            target_fields.collect::<std::result::Result<std::collections::HashMap<_, _>, _>>()?;
        let mut statement = self
            .conn
            .prepare(&format!("PRAGMA table_info('{stage}')"))?;
        let fields = statement.query_map([], |row| {
            Ok((row.get::<_, String>(1)?, row.get::<_, String>(2)?))
        })?;
        for field in fields {
            let (name, data_type) = field?;
            match existing.get(&name) {
                None => self.conn.execute_batch(&format!(
                    "ALTER TABLE {target} ADD COLUMN {} {data_type}",
                    quote(&name)
                ))?,
                Some(target_type) if target_type != &data_type && target_type != "VARCHAR" => {
                    self.conn.execute_batch(&format!(
                        "ALTER TABLE {target} ALTER {} TYPE VARCHAR",
                        quote(&name)
                    ))?;
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn updated_ids(&self, target: &str, stage: &str, updated_at: &str) -> Result<Vec<String>> {
        let mut statement = self.conn.prepare(&format!("SELECT DISTINCT source.id FROM {stage} AS source JOIN {target} AS target ON source.id = target.id AND CAST(source.\"{updated_at}\" AS VARCHAR) > CAST(target.\"{updated_at}\" AS VARCHAR)"))?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

fn extract_resources(packages: &[Value], instance_url: &str) -> Vec<Value> {
    packages
        .iter()
        .flat_map(|package| {
            package
                .get("resources")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .cloned()
                .map(|mut resource| {
                    if let Some(object) = resource.as_object_mut() {
                        object.insert("ckan_url".into(), Value::String(instance_url.into()));
                    }
                    resource
                })
        })
        .collect()
}

fn drop_empty_list_columns(rows: &mut [Value]) {
    let names = rows
        .iter()
        .filter_map(Value::as_object)
        .flat_map(|object| object.keys())
        .filter(|name| name.as_str() != "resources")
        .cloned()
        .collect::<HashSet<_>>();
    let empty_list_columns = names
        .into_iter()
        .filter(|name| {
            let mut saw_empty_list = false;
            let all_empty_or_null = rows.iter().all(|row| {
                let value = row.get(name).unwrap_or(&Value::Null);
                if let Some(values) = value.as_array()
                    && values.is_empty()
                {
                    saw_empty_list = true;
                    return true;
                }
                value.is_null()
            });
            saw_empty_list && all_empty_or_null
        })
        .collect::<Vec<_>>();
    for row in rows {
        if let Some(object) = row.as_object_mut() {
            for name in &empty_list_columns {
                object.remove(name);
            }
        }
    }
}

fn validate_list_shapes(rows: &[Value]) -> Result<()> {
    let mut shapes: HashMap<&str, &'static str> = HashMap::new();
    for row in rows {
        let Some(object) = row.as_object() else {
            continue;
        };
        for (name, value) in object {
            let Some(values) = value.as_array() else {
                continue;
            };
            let Some(first) = values.first() else {
                continue;
            };
            let shape = if first.is_object() {
                "object"
            } else if first.is_array() {
                "array"
            } else {
                "scalar"
            };
            if let Some(existing) = shapes.insert(name, shape)
                && existing != shape
            {
                bail!("incompatible list shape for column '{name}': {existing} versus {shape}");
            }
        }
    }
    Ok(())
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('\"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::{CkanDatasetFetcher, DuckdbCkanMetadataIngestor, MetadataSyncCommand};
    use duckdb::Connection;
    use httpmock::{Method::GET, MockServer};
    #[test]
    fn fetches_every_ckan_page() {
        let server = MockServer::start();
        let first = server.mock(|when, then| {
            when.method(GET)
                .path("/api/action/current_package_list_with_resources")
                .query_param("limit", "100")
                .query_param("offset", "0");
            then.status(200)
                .json_body(serde_json::json!({"result": [{"id": "one"}]}));
        });
        let datasets = CkanDatasetFetcher::fetch(&server.base_url()).unwrap();
        first.assert();
        assert_eq!(datasets, vec![serde_json::json!({"id": "one"})]);
    }

    #[test]
    fn sync_persists_datasets_resources_and_requested_counts() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET).path("/api/action/current_package_list_with_resources").query_param("limit", "100").query_param("offset", "0");
            then.status(200).json_body(serde_json::json!({"result": [{
                "id": "dataset-1", "name": "dataset-1", "metadata_modified": "2025-01-01",
                "resources": [
                    {"id": "resource-1", "package_id": "dataset-1", "last_modified": "2025-01-01", "url": "https://example.test/file.csv"},
                    {"id": "resource-2", "package_id": "dataset-1", "last_modified": "2025-01-01", "url": "https://example.test/file-2.csv"}
                ]
            }]}));
        });
        let conn = Connection::open_in_memory().unwrap();
        let command = MetadataSyncCommand {
            sync_id: "sync-1".into(),
            instance_id: "instance-1".into(),
            instance_name: "Test".into(),
            instance_url: server.base_url(),
        };

        let result = DuckdbCkanMetadataIngestor::new(&conn)
            .sync(&command)
            .unwrap();

        assert_eq!(result.status, "success");
        assert_eq!(result.total_packages, 1);
        assert_eq!(result.new_datasets, 1);
        assert_eq!(result.new_resources, 2);
        assert_eq!(result.dataset_count, 1);
        assert_eq!(result.resource_count, 2);
        assert_eq!(
            conn.query_row("SELECT ckan_url FROM ckan_resource", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            server.base_url()
        );
    }
}
