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
use serde::Deserialize;

#[derive(Deserialize, Debug, Clone)]
#[serde(default)]
pub struct S3Settings {
    pub protocol: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub region: String,
    pub bucket: String,
    pub url_style: String,
    pub use_ssl: bool,
    pub account_id: Option<String>,
}

impl Default for S3Settings {
    fn default() -> Self {
        Self {
            protocol: "s3".into(),
            endpoint: "s3.amazonaws.com".into(),
            access_key_id: "admin".into(),
            secret_access_key: "password".into(),
            region: "us-west-1".into(),
            bucket: "warehouse".into(),
            url_style: "vhost".into(),
            use_ssl: true,
            account_id: None,
        }
    }
}

impl S3Settings {
    pub fn endpoint_url(&self) -> String {
        let scheme = if self.use_ssl { "https" } else { "http" };
        format!("{}://{}", scheme, self.endpoint)
    }
}
