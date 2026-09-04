// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use crate::ingestor::DuckdbCkanMetadataIngestor;
use anyhow::Result;
use ckan_ingestor_lib::arrow_ipc_output::ArrowIpcOutput;

pub(crate) struct MergeResult {
    pub(crate) new: i64,
    pub(crate) updated_ids: Vec<String>,
}

impl DuckdbCkanMetadataIngestor<'_> {
    pub(crate) fn merge_ipc(
        &self,
        ipc: &ArrowIpcOutput,
        table: &str,
        updated_at: &str,
    ) -> Result<MergeResult> {
        let path = ipc.path().to_string_lossy().replace('\'', "''");
        let source = format!("read_arrow('{path}')");
        self.merge_source(ipc, table, updated_at, &source)
    }

    fn merge_source(
        &self,
        ipc: &ArrowIpcOutput,
        table: &str,
        updated_at: &str,
        source: &str,
    ) -> Result<MergeResult> {
        if !self.table_exists(table)? {
            self.conn
                .execute_batch(&format!("CREATE TABLE {table} AS SELECT * FROM {source}"))?;
            return Ok(MergeResult {
                new: ipc.rows as i64,
                updated_ids: vec![],
            });
        }
        self.align_target_schema(table, source)?;
        let updated_ids = self.updated_ids(table, source, updated_at)?;
        self.conn.execute_batch(&format!("DELETE FROM {table} AS target USING {source} AS source WHERE target.id = source.id AND CAST(source.\"{updated_at}\" AS VARCHAR) > CAST(target.\"{updated_at}\" AS VARCHAR)"))?;
        let names = ipc
            .schema
            .fields()
            .iter()
            .map(|field| quote(field.name()))
            .collect::<Vec<_>>()
            .join(", ");
        let inserted = self.conn.execute(&format!("INSERT INTO {table} ({names}) SELECT {names} FROM {source} ANTI JOIN {table} USING (id)"), [])? as i64;
        Ok(MergeResult {
            new: inserted - updated_ids.len() as i64,
            updated_ids,
        })
    }

    fn table_exists(&self, table: &str) -> Result<bool> {
        Ok(self.conn.query_row(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [table],
            |row| row.get::<_, i64>(0),
        )? > 0)
    }

    fn align_target_schema(&self, target: &str, source: &str) -> Result<()> {
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
            .prepare(&format!("DESCRIBE SELECT * FROM {source}"))?;
        let fields = statement.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut columns_to_retype = Vec::new();
        for field in fields {
            let (name, data_type) = field?;
            match existing.get(&name) {
                None => self.conn.execute_batch(&format!(
                    "ALTER TABLE {target} ADD COLUMN {} {data_type}",
                    quote(&name)
                ))?,
                Some(target_type) if target_type != &data_type && target_type != "VARCHAR" => {
                    columns_to_retype.push(name);
                }
                _ => {}
            }
        }
        if !columns_to_retype.is_empty() {
            self.retype_columns_as_varchar(target, &columns_to_retype)?;
        }
        Ok(())
    }

    fn retype_columns_as_varchar(&self, target: &str, columns: &[String]) -> Result<()> {
        let replacement = format!("{target}__retyped");
        let casts = columns
            .iter()
            .map(|column| {
                let column = quote(column);
                format!("CAST({column} AS VARCHAR) AS {column}")
            })
            .collect::<Vec<_>>()
            .join(", ");
        self.conn.execute_batch(&format!(
            "CREATE TABLE {replacement} AS SELECT * REPLACE ({casts}) FROM {target}; \
             DROP TABLE {target}; \
             ALTER TABLE {replacement} RENAME TO {target}"
        ))?;
        Ok(())
    }

    fn updated_ids(&self, target: &str, source: &str, updated_at: &str) -> Result<Vec<String>> {
        let mut statement = self.conn.prepare(&format!("SELECT DISTINCT source.id FROM {source} AS source JOIN {target} AS target ON source.id = target.id AND CAST(source.\"{updated_at}\" AS VARCHAR) > CAST(target.\"{updated_at}\" AS VARCHAR)"))?;
        statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
}

fn quote(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('\"', "\"\""))
}
