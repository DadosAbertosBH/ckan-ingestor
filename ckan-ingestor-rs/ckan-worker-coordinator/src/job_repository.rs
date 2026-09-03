// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use std::collections::HashSet;

use anyhow::Result;
pub trait JobRepository: Send + Sync {
    fn in_flight_resource_ids(&self) -> Result<HashSet<String>>;
    fn failed_resource_ids(&self) -> Result<HashSet<String>>;
}
