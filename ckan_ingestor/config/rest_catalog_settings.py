from typing import Annotated

from pydantic import HttpUrl, AfterValidator
from pydantic_settings import (
    BaseSettings,
    SettingsConfigDict,
)

HttpUrlString = Annotated[HttpUrl, AfterValidator(lambda v: str(v))]


class RestCatalogSettings(BaseSettings):

    model_config = SettingsConfigDict(env_prefix='catalog_')

    uri: HttpUrlString = "http://localhost:8080/catalog"
    warehouse: str = "demo"
