// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// ckan-ingestor-rs is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with ckan-ingestor-rs.  If not, see <https://www.gnu.org/licenses/>.
pub mod arrow_ipc_output;
pub mod ckan_dataset_fetcher;
pub mod ckan_resource;
pub mod config;
pub mod dataset_fetcher;
pub mod duckdb_ckan_data_ingestor;
pub mod duckdb_factory;
pub mod ingestor_outcome;
pub(crate) mod memory_profile;
pub mod readers;
pub mod s3_document_ingestor;

#[cfg(test)]
pub(crate) mod test_alloc;
