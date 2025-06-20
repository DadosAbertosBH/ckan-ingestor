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
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

from ckan_ingestor.config.s3_settings import S3Settings


class DucklakeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ducklake_", env_nested_delimiter="__")

    database: str = "public"
    catalog_uri: str = ":memory:"  # "postgres:dbname=ducklake_catalog host=localhost"
    data_path: S3Settings = S3Settings()

    datastore_url: str = "https://dados.pbh.gov.br/datastore/dump"
