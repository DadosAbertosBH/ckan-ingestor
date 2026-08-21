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
use anyhow::Result;
use duckdb::{Config, Connection};

const USER_AGENT: &str =
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0";

#[derive(Debug, Clone)]
pub struct DuckdbConfig {
    pub db_path: String,
    pub catalog_uri: String,
    pub data_path: String,
    pub s3_protocol: String,
    pub s3_endpoint: String,
    pub s3_bucket: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_url_style: String,
    pub s3_use_ssl: bool,
}

impl DuckdbConfig {
    pub fn from_env() -> Self {
        let s3_protocol = std::env::var("S3_PROTOCOL").unwrap_or_else(|_| "s3".to_string());
        let s3_bucket = std::env::var("S3_BUCKET").expect("S3_BUCKET must be set");
        Self {
            db_path: std::env::var("DUCKLAKE_DATABASE").expect("DUCKLAKE_DATABASE must be set"),
            catalog_uri: build_catalog_uri(
                std::env::var("DUCKLAKE_CATALOG_URI").ok(),
                std::env::var("DUCKLAKE_HOST").ok(),
                std::env::var("DUCKLAKE_PORT").ok(),
                std::env::var("DUCKLAKE_DBNAME").ok(),
                std::env::var("DUCKLAKE_USERNAME").ok(),
                std::env::var("DUCKLAKE_PASSWORD").ok(),
            )
            .expect("DuckLake catalog configuration is incomplete"),
            data_path: format!("{s3_protocol}://{s3_bucket}"),
            s3_protocol,
            s3_endpoint: std::env::var("S3_ENDPOINT").expect("S3_ENDPOINT must be set"),
            s3_bucket,
            s3_access_key: std::env::var("S3_ACCESS_KEY_ID").expect("S3_ACCESS_KEY_ID must be set"),
            s3_secret_key: std::env::var("S3_SECRET_ACCESS_KEY")
                .expect("S3_SECRET_ACCESS_KEY must be set"),
            s3_url_style: std::env::var("S3_URL_STYLE").unwrap_or_else(|_| "vhost".to_string()),
            s3_use_ssl: std::env::var("S3_USE_SSL")
                .map(|v| v == "true")
                .unwrap_or(false),
        }
    }

    pub fn for_local_ducklake(
        catalog_path: impl Into<String>,
        data_path: impl Into<String>,
    ) -> Self {
        Self {
            db_path: ":memory:".to_string(),
            catalog_uri: catalog_path.into(),
            data_path: data_path.into(),
            s3_protocol: "file".to_string(),
            s3_endpoint: String::new(),
            s3_bucket: String::new(),
            s3_access_key: String::new(),
            s3_secret_key: String::new(),
            s3_url_style: "path".to_string(),
            s3_use_ssl: false,
        }
    }
}

fn build_catalog_uri(
    catalog_uri: Option<String>,
    host: Option<String>,
    port: Option<String>,
    dbname: Option<String>,
    username: Option<String>,
    password: Option<String>,
) -> Option<String> {
    if let Some(uri) = catalog_uri.filter(|v| !v.is_empty()) {
        return Some(uri);
    }
    let host = host.filter(|v| !v.is_empty())?;
    let dbname = dbname.filter(|v| !v.is_empty())?;
    let username = username.filter(|v| !v.is_empty())?;
    let password = password.filter(|v| !v.is_empty())?;
    let port = port
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| "5432".to_string());
    Some(format!(
        "postgres:host={host} port={port} dbname={dbname} user={username} password={password}"
    ))
}

#[derive(Debug, Clone)]
pub struct DuckdbFactory {
    config: DuckdbConfig,
}

impl DuckdbFactory {
    pub fn from_env() -> Self {
        Self {
            config: DuckdbConfig::from_env(),
        }
    }
    pub fn new(config: DuckdbConfig) -> Self {
        Self { config }
    }
    pub fn open(&self) -> Result<Connection> {
        let config = Config::default()
            .threads(1)?
            .custom_user_agent(USER_AGENT)?
            .with("s3_endpoint", &self.config.s3_endpoint)?
            .with("s3_url_style", &self.config.s3_url_style)?
            .with(
                "s3_use_ssl",
                if self.config.s3_use_ssl {
                    "true"
                } else {
                    "false"
                },
            )?
            .with("s3_access_key_id", &self.config.s3_access_key)?
            .with("s3_secret_access_key", &self.config.s3_secret_key)?
            .with("enable_external_file_cache", "false")?
            .with("force_download", "true")?;
        let conn = Connection::open_with_flags(&self.config.db_path, config)?;
        self.configure(&conn)?;
        Ok(conn)
    }
    pub fn configure(&self, conn: &Connection) -> Result<()> {
        conn.execute_batch("INSTALL arrow FROM community; LOAD arrow;")?;
        conn.execute_batch("SET pg_debug_show_queries=false;")?;
        conn.execute_batch(&format!("ATTACH IF NOT EXISTS 'ducklake:{}' AS lake (DATA_PATH '{}', DATA_INLINING_ROW_LIMIT 0, AUTOMATIC_MIGRATION TRUE);", self.config.catalog_uri, self.config.data_path))?;
        conn.execute_batch("USE lake;")?;
        conn.execute_batch("SET ducklake_max_retry_count = 100;")?;
        Ok(())
    }
}
