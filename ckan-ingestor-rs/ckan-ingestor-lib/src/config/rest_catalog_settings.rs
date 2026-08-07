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
pub struct RestCatalogSettings {
    pub uri: String,
    pub warehouse: String,
}

impl Default for RestCatalogSettings {
    fn default() -> Self {
        Self {
            uri: "http://localhost:8080/catalog".into(),
            warehouse: "demo".into(),
        }
    }
}
