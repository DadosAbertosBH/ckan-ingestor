# Pedalin
# Copyright (C) 2025  Pedalin
import os

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
            database=os.getenv("DUCKLAKE_DATABASE", ":memory:"),
            catalog_uri=os.getenv("DUCKLAKE_CATALOG_URI",
                                  "postgres:dbname=postgres host=localhost user=postgres password=postgres"),
            data_path=S3Settings(
                endpoint=os.getenv("DUCKLAKE_DATA_PATH__ENDPOINT", "localhost:9000"),
                url_style=os.getenv("DUCKLAKE_DATA_PATH__URL_STYLE", "path"),
                access_key_id=os.getenv("DUCKLAKE_DATA_PATH__ACCESS_KEY_ID", "admin"),
                secret_access_key=os.getenv("DUCKLAKE_DATA_PATH__SECRET_ACCESS_KEY", "password"),
                use_ssl=os.getenv("DUCKLAKE_DATA_PATH__USE_SSL", "False") == "True",
            ),
        )
        return settings
