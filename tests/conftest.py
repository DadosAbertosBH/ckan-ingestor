# Pedalin
# Copyright (C) 2025  Pedalin
import json
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
import logging
import os
import os.path
from threading import Thread

import duckdb
import pyarrow
import pytest
from flask import Flask, request, send_from_directory
from flask_cors import CORS
from testcontainers.minio import MinioContainer
from testcontainers.redis import RedisContainer

from ckan_ingestor.config.ducklake_settings import DucklakeSettings
from ckan_ingestor.duckdb_connection_factory import from_settings

INVALID_INPUT_JSON_ID = "e3bce367-2e62-41c2-840f-b1df6255e7e5"

@pytest.fixture(scope="session")
def ckman_mock_url():
    # ...existing code from ckan_mock.py...
    app = Flask(__name__)
    CORS(app)
    app.env = "development"
    app.testing = True
    app.logger.setLevel(logging.INFO)

    @app.route("/datastore/<string:resource_id>")
    def get(resource_id: str):
        format_param = request.args.get("format")
        offset = request.args.get("offset", default="0")

        if int(offset) > 0:
            return {"fields": [{"id": "_id", "type": "int"}], "records": []}

        module_directory = os.path.dirname(os.path.abspath(__file__))
        file_name =  f"{resource_id}.{format_param.lower()}"
        file_dir = os.path.join(module_directory, "fixtures", "data")
        return send_from_directory(file_dir, file_name)

    thread = Thread(
        target=app.run, daemon=True, kwargs=dict(host="localhost", port=5001)
    )
    thread.start()

    yield "http://localhost:5001"

@pytest.fixture(scope="session")
def minio_url(request) -> str:
    # ...existing code from infrastructure.py...
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
    bucket = "warehouse"
    minio.get_client().make_bucket(bucket)

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"AWS": "*"},
                "Action": [
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:DeleteObject",
                    "s3:ListMultipartUploadParts",
                    "s3:AbortMultipartUpload",
                ],
                "Resource": f"arn:aws:s3:::{bucket}/docs/*",
            },
        ],
    }
    minio.get_client().set_bucket_policy(bucket, json.dumps(policy))

    return f"{host_ip}:{exposed_port}"

@pytest.fixture(scope="session")
def redis_client(request):
    # ...existing code from infrastructure.py...
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
    # ...existing code from infrastructure.py...
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
    # ...existing code from infrastructure.py...
    return from_settings(ducklake_settings)

def read_json(filename: str) -> pyarrow.Table:
    """
    Returns the directory of the current module.
    """
    module_directory = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(module_directory, "fixtures" ,filename)) as f:
        with duckdb.connect(":memory:") as conn:
            return conn.execute(f"select * from read_json('{f.name}')").arrow().read_all()


@pytest.fixture
def raw_initial_dataset() -> pyarrow.Table:
    return read_json("initial_dataset.json")


@pytest.fixture
def initial_dataset() -> pyarrow.Table:
    """
    Dataset with two rows
    """
    return read_json("initial_dataset.json")


@pytest.fixture
def dataset_with_update() -> pyarrow.Table:
    """
    Same dataset as initial_dataset, but with an update in a row.
    """
    return read_json("dataset_with_update.json")


@pytest.fixture
def dataset_with_new_row() -> pyarrow.Table:
    """
    Same dataset as initial_dataset, but with an update in a row.
    """
    return read_json("dataset_with_new_row.json")


@pytest.fixture
def dataset_with_pdf() -> pyarrow.Table:
    return read_json("dataset_with_pdf_resource.json")


@pytest.fixture
def latin_encoded_csv_file() -> str:
    module_directory = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(module_directory, "fixtures/data/csv_with_latin_encode.csv")) as f:
        return f.name


@pytest.fixture
def non_latin1_and_non_utf8() -> str:
    module_directory = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(module_directory, "fixtures/data/non_latin1_and_non_utf8.csv")) as f:
        return f.name
