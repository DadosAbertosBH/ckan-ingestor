// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::Result;

use crate::parquet_output::ParquetOutput;

#[allow(async_fn_in_trait)]
pub trait DataWriter {
    async fn ingest(&self, resource_id: &str, parquet: &ParquetOutput) -> Result<()>;
}
