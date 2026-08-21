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

/// DuckLake connection settings required to open and configure a connection.
///
/// Captured once (from the environment) so that a connection can be opened
/// lazily without re-reading environment variables on every message.
#[derive(Debug, Clone)]
pub struct DuckdbConfig {
    pub db_path: String,
    pub catalog_uri: String,
    pub s3_protocol: String,
    pub s3_endpoint: String,
    pub s3_bucket: String,
    pub s3_access_key: String,
    pub s3_secret_key: String,
    pub s3_url_style: String,
    pub s3_use_ssl: bool,
}

impl DuckdbConfig {
    /// Read the DuckLake configuration from the process environment.
    ///
    /// Each variable is required; a missing variable panics with a clear
    /// message consistent with the previous per-message behavior.
    pub fn from_env() -> Self {
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
            .expect("DUCKLAKE_CATALOG_URI or DUCKLAKE_HOST/DUCKLAKE_DBNAME/DUCKLAKE_USERNAME/DUCKLAKE_PASSWORD must be set"),
            s3_protocol: std::env::var("S3_PROTOCOL").unwrap_or_else(|_| "s3".to_string()),
            s3_endpoint: std::env::var("S3_ENDPOINT").expect("S3_ENDPOINT must be set"),
            s3_bucket: std::env::var("S3_BUCKET").expect("S3_BUCKET must be set"),
            s3_access_key: std::env::var("S3_ACCESS_KEY_ID").expect("S3_ACCESS_KEY_ID must be set"),
            s3_secret_key: std::env::var("S3_SECRET_ACCESS_KEY")
                .expect("S3_SECRET_ACCESS_KEY must be set"),
            s3_url_style: std::env::var("S3_URL_STYLE").unwrap_or_else(|_| "vhost".to_string()),
            s3_use_ssl: std::env::var("S3_USE_SSL")
                .map(|v| v == "true")
                .unwrap_or(false),
        }
    }
}

/// Build the DuckLake catalog URI.
///
/// Mirrors the Python `DucklakeSettings.get_catalog_uri`:
/// - prefers an explicit `DUCKLAKE_CATALOG_URI`,
/// - otherwise assembles one from the individual `DUCKLAKE_HOST`/`PORT`/`DBNAME`/
///   `USERNAME`/`PASSWORD` params (set by the CNPG secret),
/// - otherwise returns `None` (no usable catalog URI).
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

/// Opens and configures DuckLake connections.
///
/// The factory owns the configuration and produces ready-to-use connections.
/// Each produced connection is fully configured for DuckLake ingestion and
/// must not be shared across OS threads (DuckDB `Connection` is `!Sync`).
#[derive(Debug, Clone)]
pub struct DuckdbFactory {
    config: DuckdbConfig,
}

impl DuckdbFactory {
    /// Create a factory from environment configuration.
    pub fn from_env() -> Self {
        Self {
            config: DuckdbConfig::from_env(),
        }
    }

    pub fn new(config: DuckdbConfig) -> Self {
        Self { config }
    }

    /// Open and configure a fresh connection to the DuckLake catalog.
    ///
    /// Applies the DuckDB open-time `Config` (threads, user agent, S3
    /// credentials) plus the session settings required before ingestion.
    pub fn open(&self) -> Result<Connection> {
        let config = self.build_config()?;
        let conn = Connection::open_with_flags(&self.config.db_path, config)?;
        self.configure(&conn)?;
        Ok(conn)
    }

    /// Build the DuckDB open-time `Config`, mirroring the Python factory's
    /// `config=` dict (threads, S3 settings, user agent, force_download).
    fn build_config(&self) -> Result<Config> {
        let c = &self.config;
        let config = Config::default()
            .threads(1)?
            .custom_user_agent(USER_AGENT)?
            .with("s3_endpoint", &c.s3_endpoint)?
            .with("s3_url_style", &c.s3_url_style)?
            .with("s3_use_ssl", bool_str(c.s3_use_ssl))?
            .with("s3_access_key_id", &c.s3_access_key)?
            .with("s3_secret_access_key", &c.s3_secret_key)?
            .with("enable_external_file_cache", "false")?
            .with("force_download", "false")?;
        Ok(config)
    }

    /// Apply DuckLake session settings to an already-open connection.
    ///
    /// Session settings are per-connection and are not inherited by
    /// `Connection::try_clone`, so this must be re-applied on every clone.
    pub fn configure(&self, conn: &Connection) -> Result<()> {
        let c = &self.config;
        conn.execute_batch("INSTALL arrow FROM community; LOAD arrow;")?;
        conn.execute_batch("SET pg_debug_show_queries=false;")?;
        conn.execute_batch(&format!(
            "ATTACH IF NOT EXISTS 'ducklake:{}' AS lake (DATA_PATH '{}://{}', DATA_INLINING_ROW_LIMIT 10000, AUTOMATIC_MIGRATION TRUE);",
            c.catalog_uri, c.s3_protocol, c.s3_bucket
        ))?;
        conn.execute_batch("USE lake;")?;
        conn.execute_batch("SET ducklake_max_retry_count = 100;")?;
        Ok(())
    }
}

/// Render a `bool` as the lowercase string DuckDB configuration expects.
fn bool_str(b: bool) -> String {
    if b {
        "true".to_string()
    } else {
        "false".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::build_catalog_uri;

    fn some(s: &str) -> Option<String> {
        Some(s.to_string())
    }

    #[test]
    fn prefers_explicit_catalog_uri() {
        let uri = build_catalog_uri(
            some("postgres:host=x dbname=test"),
            some("ignored"),
            some("5432"),
            some("ignored"),
            some("ignored"),
            some("ignored"),
        );
        assert_eq!(uri.as_deref(), Some("postgres:host=x dbname=test"));
    }

    #[test]
    fn builds_uri_from_individual_params() {
        let uri = build_catalog_uri(
            None,
            some("myhost"),
            some("5432"),
            some("mydb"),
            some("myuser"),
            some("mypass"),
        );
        assert_eq!(
            uri.as_deref(),
            Some("postgres:host=myhost port=5432 dbname=mydb user=myuser password=mypass")
        );
    }

    #[test]
    fn defaults_port_to_5432_when_missing() {
        let uri = build_catalog_uri(
            None,
            some("myhost"),
            None,
            some("mydb"),
            some("myuser"),
            some("mypass"),
        );
        assert_eq!(
            uri.as_deref(),
            Some("postgres:host=myhost port=5432 dbname=mydb user=myuser password=mypass")
        );
    }

    #[test]
    fn returns_none_when_individual_params_incomplete() {
        let uri = build_catalog_uri(None, some("myhost"), None, None, None, None);
        assert_eq!(uri, None);
    }
}
