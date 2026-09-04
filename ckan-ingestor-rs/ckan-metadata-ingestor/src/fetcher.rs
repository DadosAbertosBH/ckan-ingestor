// ckan-ingestor-rs
//
// This file is part of ckan-ingestor-rs.
//
// ckan-ingestor-rs is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published
// by the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

use anyhow::{Context, Result};
use serde_json::Value;

pub const PAGE_SIZE: usize = 25;
const CKAN_USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15; rv:132.0) Gecko/20100101 Firefox/132.0";

pub struct CkanDatasetFetcher;

pub struct CkanPackageStream {
    client: reqwest::blocking::Client,
    base: String,
    offset: usize,
    page: std::vec::IntoIter<Value>,
    fetched_packages: bool,
    finished: bool,
}

impl CkanDatasetFetcher {
    pub fn fetch(url: &str) -> Result<CkanPackageStream> {
        let client = reqwest::blocking::Client::builder()
            .user_agent(CKAN_USER_AGENT)
            .build()?;
        Ok(CkanPackageStream {
            client,
            base: url.trim_end_matches('/').to_owned(),
            offset: 0,
            page: Vec::new().into_iter(),
            fetched_packages: false,
            finished: false,
        })
    }
}

impl Iterator for CkanPackageStream {
    type Item = Result<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(package) = self.page.next() {
                return Some(Ok(package));
            }
            if self.finished {
                return None;
            }
            match self.next_page()? {
                Ok(page) => self.page = page.into_iter(),
                Err(error) => return Some(Err(error)),
            }
        }
    }
}

impl CkanPackageStream {
    pub(crate) fn next_page(&mut self) -> Option<Result<Vec<Value>>> {
        if self.finished {
            return None;
        }
        let endpoint = format!(
            "{}/api/action/current_package_list_with_resources?limit={PAGE_SIZE}&offset={}",
            self.base, self.offset
        );
        let page = match self.fetch_page(endpoint) {
            Ok(page) => page,
            Err(error) => {
                self.finished = true;
                return Some(Err(error));
            }
        };
        let count = page.len();
        if count == 0 {
            self.finished = true;
            return self
                .fetched_packages
                .then_some(Ok(page))
                .or_else(|| Some(Err(anyhow::anyhow!("No packages returned from CKAN API"))));
        }
        self.fetched_packages = true;
        if count < PAGE_SIZE {
            self.finished = true;
        }
        self.offset += PAGE_SIZE;
        Some(Ok(page))
    }

    fn fetch_page(&self, endpoint: String) -> Result<Vec<Value>> {
        let mut response: Value = self
            .client
            .get(endpoint)
            .send()?
            .error_for_status()?
            .json()
            .context("invalid CKAN JSON response")?;
        response
            .get_mut("result")
            .and_then(Value::as_array_mut)
            .map(std::mem::take)
            .context("CKAN response result must be an array")
    }
}
