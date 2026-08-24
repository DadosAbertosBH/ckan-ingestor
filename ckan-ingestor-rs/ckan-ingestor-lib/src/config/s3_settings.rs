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
use std::env;

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
    pub fn from_env() -> Self {
        Self::from_getter(|key| env::var(key).ok())
    }

    pub(crate) fn from_getter<F>(get: F) -> Self
    where
        F: Fn(&str) -> Option<String>,
    {
        Self {
            protocol: get("S3_PROTOCOL").unwrap_or_else(|| "s3".into()),
            endpoint: get("S3_ENDPOINT").unwrap_or_else(|| "rustfs:9000".into()),
            bucket: get("S3_BUCKET").unwrap_or_else(|| "warehouse".into()),
            access_key_id: get("S3_ACCESS_KEY_ID").unwrap_or_else(|| "admin".into()),
            secret_access_key: get("S3_SECRET_ACCESS_KEY").unwrap_or_else(|| "password".into()),
            region: get("S3_REGION").unwrap_or_else(|| "us-east-1".into()),
            url_style: get("S3_URL_STYLE").unwrap_or_else(|| "vhost".into()),
            use_ssl: get("S3_USE_SSL").map(|v| v == "true").unwrap_or(false),
            ..Self::default()
        }
    }

    pub fn endpoint_url(&self) -> String {
        let scheme = if self.use_ssl { "https" } else { "http" };
        format!("{}://{}", scheme, self.endpoint)
    }

    /// Build a `rust-s3` `Bucket` configured for this settings. Custom
    /// endpoint (RustFS or another S3-compatible service) is used when
    /// `endpoint` is not the default
    /// S3 host.
    pub fn bucket(&self) -> anyhow::Result<Box<s3::bucket::Bucket>> {
        let region = s3::region::Region::Custom {
            region: self.region.clone(),
            endpoint: self.endpoint_url(),
        };
        let credentials = s3::creds::Credentials::new(
            Some(&self.access_key_id),
            Some(&self.secret_access_key),
            None,
            None,
            None,
        )?;
        let bucket = s3::bucket::Bucket::new(&self.bucket, region, credentials)?;
        let bucket = if self.url_style == "path" {
            bucket.with_path_style()
        } else {
            bucket
        };
        Ok(bucket)
    }
}

#[cfg(test)]
mod tests {
    use super::S3Settings;
    use std::collections::HashMap;

    #[test]
    fn from_getter_reads_path_style_configuration() {
        let values = HashMap::from([(String::from("S3_URL_STYLE"), String::from("path"))]);
        let settings = S3Settings::from_getter(|key| values.get(key).cloned());

        assert_eq!(settings.url_style, "path");
    }
}
