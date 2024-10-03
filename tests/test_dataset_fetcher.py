import pyarrow

from tests.fake_ckan_dataset_fetcher import FakeCkanDatasetFetcher
from tests.fixtures.datasets import full_dataset


def test_null_list_are_dropped(full_dataset: pyarrow.Table):
    original_schema = full_dataset.schema
    subject = FakeCkanDatasetFetcher(full_dataset)
    sanatized_schema = subject.fetch().schema
    assert "extras" in original_schema.names
    assert "extras" not in sanatized_schema.names


def test_null_structed_fields_are_dropped(full_dataset: pyarrow.Table):
    original_schema = full_dataset.schema
    subject = FakeCkanDatasetFetcher(full_dataset)
    sanatized_schema = subject.fetch().schema
    original_struct_type = original_schema.field("resources").type.value_type
    new_struct_type = sanatized_schema.field("resources").type.value_type
    assert new_struct_type.num_fields < original_struct_type.num_fields
