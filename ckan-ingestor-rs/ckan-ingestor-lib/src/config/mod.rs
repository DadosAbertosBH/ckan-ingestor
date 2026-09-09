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
pub use s3_settings::S3Settings;

mod s3_settings;

#[cfg(test)]
mod tests {
    use super::S3Settings;

    #[test]
    fn defaults_describe_the_local_development_environment() {
        let s3 = S3Settings::default();

        assert_eq!(s3.endpoint_url(), "https://s3.amazonaws.com");
        assert_eq!(s3.bucket, "warehouse");
    }

    #[test]
    fn s3_endpoint_url_uses_http_when_ssl_is_disabled() {
        let settings = S3Settings {
            endpoint: "rustfs.local:9000".to_string(),
            use_ssl: false,
            ..S3Settings::default()
        };

        assert_eq!(settings.endpoint_url(), "http://rustfs.local:9000");
    }

    #[test]
    fn environment_defaults_to_rustfs_for_local_s3() {
        let settings = S3Settings::from_getter(|_| None);

        assert_eq!(settings.endpoint, "rustfs:9000");
        assert_eq!(settings.url_style, "vhost");
    }
}
