// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

pub mod fetcher;
pub mod models;

mod ckan_schemas;
mod ipc;
mod package_processing;

pub use fetcher::CkanDatasetFetcher;
pub use ipc::StructuredIpc;
pub use models::{MetadataSyncCommand, MetadataSyncResult, MetadataSyncStatus};
