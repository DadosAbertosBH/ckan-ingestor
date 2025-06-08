from typing import Annotated, Optional

from pydantic import HttpUrl, AfterValidator
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

HttpUrlString = Annotated[HttpUrl, AfterValidator(lambda v: str(v))]

S3_ENDPOINT_PROPERTY_NAME = 'S3_ENDPOINT'


class S3Settings(BaseSettings):

    model_config = SettingsConfigDict(env_prefix='s3_')

    protocol: str = "s3"
    endpoint: str = "s3.amazonaws.com"
    access_key_id: str = "admin"
    secret_access_key: str = "password"
    region: str = "us-west-1"
    bucket: str = "warehouse"
    url_style: str = "vhost"
    use_ssl: bool = True
    account_id: Optional[str] = None
