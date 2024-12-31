from datetime import datetime

import dlt
import pyarrow
import requests

from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher

@dlt.source
def ckan(ckan_url=dlt.config.value):
    fetcher = CkanDatasetFetcher(ckan_url)
    packages = fetcher.fetch()

    @dlt.resource(
        name="ckan_package",
        write_disposition={"disposition": "merge", "strategy": "upsert"},
        primary_key="id",
        table_format="delta",
        columns={"metadata_modified": {"dedup_sort": "desc"}}
    )
    def ckan_package():
        yield packages

    yield ckan_package

    resources = packages["resources"].combine_chunks().flatten()

    @dlt.resource(
        name="ckan_resource",
        write_disposition="merge",
        primary_key="id",
        table_format="delta",
        columns={"last_modified": {"dedup_sort": "desc"}}
    )
    def ckan_resource():
        # noinspection PyArgumentList
        yield pyarrow.Table.from_struct_array(resources)

    yield ckan_resource

    @dlt.resource(
        parallelized=True,
        write_disposition="replace",
        standalone=True,
        table_format="delta",
        name=lambda r: _normalize_id(r["item"]["id"]),
    )
    def ckan_data(item):
        source_state = dlt.current.source_state()
        item_id = item['id']
        latest_sync_str = source_state.get(f"{item_id}_latest_sync", "1900-01-01T00:00:00.000000")
        latest_sync = datetime.fromisoformat(latest_sync_str)
        last_modified = datetime.fromisoformat(item["last_modified"])
        if latest_sync < last_modified:
            if item["datastore_active"]:
                # url = str(item["url"])
                url = f"https://dados.pbh.gov.br/datastore/dump/{item_id}?format=json"
                table = _fetch_and_parser_json(url)
                yield table
        source_state[f"{item_id}_latest_sync"] = item["last_modified"]

    for r in resources:
        yield ckan_data(r.as_py())

def _fetch_and_parser_json(url):
    json = requests.get(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0"
    }).json()
    fields = [_get_field_schema(x) for x in json["fields"]]
    schema = pyarrow.schema(fields)
    records = json["records"]
    # noinspection PyArgumentList
    table = pyarrow.Table.from_pylist(records, schema=schema)
    return table

def _normalize_id(id: str):
    return id.replace("-", "_")

def _get_field_schema(field):
    field_name = field["id"]
    match(field['type']):
        case "int":
            return field_name, pyarrow.int32()
        case "text":
            return field_name, pyarrow.string()
        case unkown:
            raise ValueError(f"Unkown field type: {unkown} for field: {field_name}")
