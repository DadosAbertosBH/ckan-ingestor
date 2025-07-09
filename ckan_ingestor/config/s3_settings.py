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
from typing import Annotated

from pydantic import HttpUrl, AfterValidator
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

HttpUrlString = Annotated[HttpUrl, AfterValidator(lambda v: str(v))]

S3_ENDPOINT_PROPERTY_NAME = "S3_ENDPOINT"


class S3Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="s3_")

    protocol: str = "s3"
    endpoint: str = "s3.amazonaws.com"
    access_key_id: str = "admin"
    secret_access_key: str = "password"
    region: str = "us-west-1"
    bucket: str = "warehouse"
    url_style: str = "vhost"
    use_ssl: bool = True
