import pyarrow as pa

from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher
from tests.fake_ckan_dataset_fetcher import FakeCkanDatasetFetcher
from tests.fixtures.datasets import raw_full_dataset


def test_fetch():
    subject = CkanDatasetFetcher(url="https://dados.pbh.gov.br/")
    subject.fetch()


def test_null_list_are_dropped(raw_full_dataset: pa.Table):
    original_schema = raw_full_dataset.schema
    subject = FakeCkanDatasetFetcher(raw_full_dataset)
    sanatized_schema = subject.fetch().schema
    assert "extras" in original_schema.names
    assert "extras" not in sanatized_schema.names


def test_null_structed_fields_are_dropped(raw_full_dataset: pa.Table):
    original_schema = raw_full_dataset.schema
    subject = FakeCkanDatasetFetcher(raw_full_dataset)
    sanatized_schema = subject.fetch().schema
    original_struct_type = original_schema.field("resources").type.value_type
    new_struct_type = sanatized_schema.field("resources").type.value_type
    assert new_struct_type.num_fields < original_struct_type.num_fields
