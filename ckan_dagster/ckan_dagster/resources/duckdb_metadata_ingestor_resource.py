# Pedalin
# Copyright (C) 2025  Pedalin

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import dagster as dg
from dagster import InitResourceContext

from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor


class DuckdbMetadataIngestorResource(
    dg.ConfigurableResource[DuckdbCkanMetadataIngestor]
):
    ducklake_settings: dg.ResourceDependency[DucklakeSettings]

    def create_resource(
        self, context: InitResourceContext
    ) -> DuckdbCkanMetadataIngestor:
        metadata_ingestor = DuckdbCkanMetadataIngestor.from_settings(
            self.ducklake_settings
        )
        return metadata_ingestor
