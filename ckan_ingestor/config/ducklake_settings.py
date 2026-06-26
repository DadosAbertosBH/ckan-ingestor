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
from pydantic import Field
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

from ckan_ingestor.config.s3_settings import S3Settings


class DucklakeSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="ducklake_", env_nested_delimiter="__")

    database: str = ":memory:"
    catalog_uri: str = ""
    data_path: S3Settings = S3Settings()

    # Individual connection params (set by CNPG secret when postgres.deploy=true)
    host: str = ""
    port: str = ""
    dbname: str = ""
    username: str = ""
    password: str = ""

    ckan_url: str = Field(default="https://dados.pbh.gov.br", alias="CKAN_URL")

    def get_catalog_uri(self) -> str:
        """Build catalog URI from individual params or use explicit catalog_uri."""
        if self.catalog_uri:
            return self.catalog_uri
        if self.host and self.username and self.password and self.dbname:
            return (
                f"postgres:host={self.host} port={self.port}"
                f" dbname={self.dbname} user={self.username}"
                f" password={self.password}"
            )
        return ":memory:"

    @property
    def datastore_url(self) -> str:
        return f"{self.ckan_url.rstrip('/')}/datastore/dump"
