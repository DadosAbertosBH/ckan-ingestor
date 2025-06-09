import json
import os
import os.path
from typing import Any

import pytest


def read_json(filename: str) -> list[dict[str, any]]:
    """
    Returns the directory of the current module.
    """
    module_directory =  os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(module_directory, filename)) as f:
        return json.load(f)

@pytest.fixture
def raw_initial_dataset() -> list[dict[str, any]]:
    return read_json("initial_dataset.json")


@pytest.fixture
def initial_dataset() -> list[dict[str, Any]]:
    """
    Dataset with two rows
    """
    return read_json("initial_dataset.json")

@pytest.fixture
def dataset_with_update() -> list[dict[str, Any]]:
    """
    Same dataset as initial_dataset, but with an update in a row.
    """
    return read_json("dataset_with_update.json")


@pytest.fixture
def dataset_with_new_row() -> list[dict[str, Any]]:
    """
    Same dataset as initial_dataset, but with an update in a row.
    """
    return read_json("dataset_with_new_row.json")