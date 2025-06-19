import os

import duckdb
import pytest
from testcontainers.minio import MinioContainer
from testcontainers.redis import RedisContainer

from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.duckdb_connection_factory import from_settings


@pytest.fixture(scope="session")
def minio_url(request) -> str:
    minio = MinioContainer(
        image="minio/minio:RELEASE.2024-09-22T00-33-43Z",
        access_key="admin",
        secret_key="password",
    )
    minio.with_env("MINIO_CONSOLE_ADDRESS", ":9001")
    minio.with_exposed_ports(9000, 9001)
    minio.start()
    host_ip = minio.get_container_host_ip()
    exposed_port = minio.get_exposed_port(minio.port)

    def remove_container():
        # minio.stop()
        pass

    request.addfinalizer(remove_container)
    minio.get_client().make_bucket("warehouse")

    return f"{host_ip}:{exposed_port}"


@pytest.fixture(scope="session")
def redis_client(request):
    container = RedisContainer()
    container.start()
    client = container.get_client()

    def remove_container():
        # minio.stop()
        pass

    request.addfinalizer(remove_container)
    return client


@pytest.fixture
def ducklake_settings(minio_url) -> DucklakeSettings:
    os.environ["DUCKLAKE_DATABASE"] = ":memory:"
    os.environ["DUCKLAKE_CATALOG_URI"] = ":memory:"
    os.environ["DUCKLAKE_DATA_PATH__ENDPOINT"] = minio_url
    os.environ["DUCKLAKE_DATA_PATH__URL_STYLE"] = "path"
    os.environ["DUCKLAKE_DATA_PATH__USE_SSL"] = "false"
    return DucklakeSettings()


@pytest.fixture
def in_memory_duckdb_conn(
    ducklake_settings: DucklakeSettings,
) -> duckdb.DuckDBPyConnection:
    return from_settings(ducklake_settings)
