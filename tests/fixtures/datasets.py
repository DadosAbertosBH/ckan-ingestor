import os
import os.path

import duckdb
import pyarrow
import pytest


def read_json(filename: str) -> pyarrow.Table:
    """
    Returns the directory of the current module.
    """
    module_directory =  os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(module_directory, filename)) as f:
        with duckdb.connect(":memory:") as conn:
            return conn.execute(f"select * from read_json('{f.name}')").arrow()

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