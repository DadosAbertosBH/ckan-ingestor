from typing import Annotated

from pydantic import HttpUrl, AfterValidator
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

HttpUrlString = Annotated[HttpUrl, AfterValidator(lambda v: str(v))]

S3_ENDPOINT_PROPERTY_NAME = 'S3_ENDPOINT'


class S3Settings(BaseSettings):

    model_config = SettingsConfigDict(env_prefix='s3_')

    endpoint: HttpUrlString = "http://localhost:9000"
    access_key_id: str = "admin"
    secret_access_key: str = "password"
    region: str = "us-west-1"
    bucket: str = "warehouse"
