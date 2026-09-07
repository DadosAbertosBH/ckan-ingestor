// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;

/// Persists a metadata batch in its DuckLake table.
#[allow(async_fn_in_trait)]
pub trait DataWriter {
    async fn initialize_table(&self, table_name: &str, schema: &SchemaRef) -> Result<()>;
    async fn ingest(&self, table_name: &str, batch: &RecordBatch) -> Result<()>;
}
