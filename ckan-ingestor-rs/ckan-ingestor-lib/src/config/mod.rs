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
pub use ducklake_settings::DucklakeSettings;
pub use rest_catalog_settings::RestCatalogSettings;
pub use s3_settings::S3Settings;

mod ducklake_settings;
mod rest_catalog_settings;
mod s3_settings;

#[cfg(test)]
mod tests {
    use super::{DucklakeSettings, RestCatalogSettings, S3Settings};

    #[test]
    fn defaults_describe_the_local_development_environment() {
        let s3 = S3Settings::default();
        let ducklake = DucklakeSettings::default();
        let catalog = RestCatalogSettings::default();

        assert_eq!(s3.endpoint_url(), "https://s3.amazonaws.com");
        assert_eq!(s3.bucket, "warehouse");
        assert_eq!(ducklake.database, "public");
        assert_eq!(ducklake.catalog_uri, ":memory:");
        assert_eq!(ducklake.data_path.bucket, "warehouse");
        assert_eq!(catalog.uri, "http://localhost:8080/catalog");
        assert_eq!(catalog.warehouse, "demo");
    }

    #[test]
    fn s3_endpoint_url_uses_http_when_ssl_is_disabled() {
        let settings = S3Settings {
            endpoint: "minio.local:9000".to_string(),
            use_ssl: false,
            ..S3Settings::default()
        };

        assert_eq!(settings.endpoint_url(), "http://minio.local:9000");
    }
}
