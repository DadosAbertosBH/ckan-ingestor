import os

import pytest
from pyarrow import json
from testcontainers.minio import MinioContainer

from ckan_ingestor.config.s3_settings import S3Settings, S3_ENDPOINT_PROPERTY_NAME
from ckan_ingestor.delta_ckan_ingestor import DeltaCkanIngestor
from ckan_ingestor.iceberg_ingestor import IcebergCkanIngestor


@pytest.fixture(scope="session", autouse=True)
def setup(request):
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
    # os.environ[S3_ENDPOINT_PROPERTY_NAME] = f"http://{host_ip}:{exposed_port}"

    def remove_container():
        # minio.stop()
        pass
    request.addfinalizer(remove_container)
    minio.get_client().make_bucket("warehouse")


@pytest.fixture
def full_dataset():
    dataset = json.read_json("fixtures/full_dataset.jsonl")
    dataset = dataset.drop_columns(
        [
            "relationships_as_subject",
            "relationships_as_object",
            "tags",
            "extras",
            "groups",
            "license_url"
        ]
    )
    return dataset


def test_full_insert(full_dataset):
    subject = IcebergCkanIngestor(full_dataset)
    print(subject.ingest())
