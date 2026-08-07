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
pub struct DucklakeSettings {
    pub database: String,
    pub catalog_uri: String,
    pub datastore_url: String,
    pub data_path: S3Settings,
}

impl Default for DucklakeSettings {
    fn default() -> Self {
        Self {
            database: "public".into(),
            catalog_uri: ":memory:".into(),
            datastore_url: "https://dados.pbh.gov.br/datastore/dump".into(),
            data_path: S3Settings::default(),
        }
    }
}

use super::S3Settings;
