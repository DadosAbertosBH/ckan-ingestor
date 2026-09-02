# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

import duckdb
from pydantic_settings import BaseSettings, SettingsConfigDict


class S3Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="s3_")

    protocol: str = "s3"
    endpoint: str = "s3.amazonaws.com"
    access_key_id: str = "admin"
    secret_access_key: str = "password"
    bucket: str = "warehouse"
    url_style: str = "vhost"
    use_ssl: bool = True


class DucklakeSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="ducklake_", env_nested_delimiter="__"
    )

    database: str = ":memory:"
    catalog_uri: str = ""
    data_path: S3Settings = S3Settings()
    host: str = ""
    port: str = ""
    dbname: str = ""
    username: str = ""
    password: str = ""

    def get_catalog_uri(self) -> str:
        if self.catalog_uri:
            return self.catalog_uri
        if self.host and self.username and self.password and self.dbname:
            return (
                f"postgres:host={self.host} port={self.port} "
                f"dbname={self.dbname} user={self.username} password={self.password}"
            )
        return ":memory:"


def from_settings(settings: DucklakeSettings | None = None):
    settings = settings or DucklakeSettings()
    conn = duckdb.connect(
        settings.database,
        config={
            "threads": 1,
            "s3_url_style": settings.data_path.url_style,
            "s3_use_ssl": settings.data_path.use_ssl,
            "s3_endpoint": settings.data_path.endpoint,
            "s3_access_key_id": settings.data_path.access_key_id,
            "s3_secret_access_key": settings.data_path.secret_access_key,
            "custom_user_agent": "ckan-ingestor-orchestrator",
            "force_download": True,
        },
    )
    for extension in ("mysql", "postgres", "httpfs"):
        conn.install_extension(extension)
    for extension in ("ducklake", "mysql", "postgres", "httpfs"):
        conn.load_extension(extension)
    conn.execute("SET pg_debug_show_queries=false")
    conn.execute(
        f"ATTACH IF NOT EXISTS 'ducklake:{settings.get_catalog_uri()}' AS lake "
        f"(DATA_PATH '{settings.data_path.protocol}://{settings.data_path.bucket}', "
        "DATA_INLINING_ROW_LIMIT 10000, AUTOMATIC_MIGRATION TRUE)"
    )
    conn.execute("USE lake")
    conn.execute("SET ducklake_max_retry_count = 100")
    return conn


def get_outdated_resource_ids(conn, ckan_url: str = "") -> list[str]:
    query = """
        SELECT id
        FROM ckan_resource
        ANTI JOIN ckan_resource_last_update
          ON id = ckan_resource_id
          AND ckan_resource.last_modified::TIMESTAMP
              < ckan_resource_last_update.last_modified
    """
    params = []
    if ckan_url:
        query += " WHERE ckan_resource.ckan_url = ?"
        params.append(ckan_url)
    return [row[0] for row in conn.execute(query, params).fetchall()]
