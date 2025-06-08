import pyarrow
import pytest
from pyarrow import json

from tests.fake_ckan_dataset_fetcher import FakeCkanDatasetFetcher


@pytest.fixture
def raw_full_dataset() -> pyarrow.Table:
    dataset = json.read_json("fixtures/full_dataset.jsonl")
    return dataset


@pytest.fixture
def full_dataset() -> pyarrow.Table:
    """
    Dataset with two rows
    """
    dataset = json.read_json("fixtures/full_dataset.jsonl")
    return FakeCkanDatasetFetcher(dataset).fetch()

@pytest.fixture
def dataset_with_update() -> pyarrow.Table:
    """
    Same dataset as full_dataset, but with an update in a row.
    """
    dataset = json.read_json("fixtures/full_dataset_update.jsonl")
    return FakeCkanDatasetFetcher(dataset).fetch()
