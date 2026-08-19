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

#[cfg(test)]
mod tests {
    use httpmock::{Method::GET, MockServer};

    use super::{CkanDatasetFetcher, DatasetFetcher};

    #[test]
    fn fetches_the_dataset_list_from_the_ckan_action_endpoint() {
        let server = MockServer::start();
        let request = server.mock(|when, then| {
            when.method(GET)
                .path("/api/action/current_package_list_with_resources")
                .query_param("limit", "1000");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"{"result":[{"id":"dataset-1"}]}"#);
        });
        let fetcher = CkanDatasetFetcher::new(server.base_url());

        let datasets = fetcher.fetch().expect("CKAN response is valid");

        request.assert();
        assert_eq!(datasets, vec![serde_json::json!({"id": "dataset-1"})]);
    }

    #[test]
    fn rejects_a_response_without_an_array_result() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(GET)
                .path("/api/action/current_package_list_with_resources")
                .query_param("limit", "1000");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"{"result":{"id":"dataset-1"}}"#);
        });
        let fetcher = CkanDatasetFetcher::new(server.base_url());

        let error = fetcher.fetch().expect_err("result must be an array");

        assert!(error.to_string().contains("result is not array"));
    }
}
