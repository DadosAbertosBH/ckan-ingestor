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
from ckan_ingestor.config.s3_settings import S3Settings


class DucklakeSettingsResource(dg.ConfigurableResource[DucklakeSettings]):
    def create_resource(self, context: InitResourceContext) -> DucklakeSettings:
        settings = DucklakeSettings(
            database=":memory:",
            catalog_uri="postgres:dbname=postgres host=localhost user=postgres password=postgres",
            data_path=S3Settings(
                endpoint="localhost:9000",
                url_style="path",
                access_key_id="admin",
                secret_access_key="password",
                use_ssl=False,
            ),
        )
        return settings
