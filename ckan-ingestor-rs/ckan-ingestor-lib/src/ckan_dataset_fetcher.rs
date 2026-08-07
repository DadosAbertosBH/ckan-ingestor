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
use crate::dataset_fetcher::DatasetFetcher;
use anyhow::{anyhow, Result};
use reqwest::blocking::Client;
use serde_json::Value;

pub struct CkanDatasetFetcher {
    pub url: String,
}

impl CkanDatasetFetcher {
    pub fn new(url: impl Into<String>) -> Self {
        Self { url: url.into() }
    }
}

impl DatasetFetcher for CkanDatasetFetcher {
    fn fetch(&self) -> Result<Vec<Value>> {
        let client = Client::new();
        let url = format!(
            "{}/api/action/current_package_list_with_resources?limit=1000",
            self.url
        );
        let resp = client.get(&url).send()?;
        let json: Value = resp.json()?;
        let result = json
            .get("result")
            .ok_or_else(|| anyhow!("missing result"))?;
        match result {
            Value::Array(arr) => Ok(arr.clone()),
            _ => Err(anyhow!("result is not array")),
        }
    }
}
