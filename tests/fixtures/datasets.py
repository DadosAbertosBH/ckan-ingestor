import pyarrow
import pytest
from pyarrow import json


@pytest.fixture
def full_dataset() -> pyarrow.Table:
    dataset = json.read_json("fixtures/full_dataset.jsonl")
    return dataset
