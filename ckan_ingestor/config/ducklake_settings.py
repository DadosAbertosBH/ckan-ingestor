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
