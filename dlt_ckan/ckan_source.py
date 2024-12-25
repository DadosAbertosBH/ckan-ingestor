from datetime import datetime

import dlt
import pyarrow
import requests

from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher


def _get_field_schema(field):
    field_name = field["id"]
    match(field['type']):
        case "int":
            return field_name, pyarrow.int32()
        case "text":
            return field_name, pyarrow.string()
        case unkown:
            raise ValueError(f"Unkown field type: {unkown} for field: {field_name}")

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
        columns={"last_modified": {"dedup_sort": "desc"}}
    )
    def ckan_resource():
        yield pyarrow.Table.from_struct_array(resources)

    yield ckan_resource

    source_state = dlt.current.source_state()

    @dlt.resource(
        parallelized=True,
        write_disposition="replace",
        standalone=True,
        name=lambda r: str(r["item"]["id"]),
    )
    def ckan_table(item):
        id_str = str(item['id'])
        latest_sync_str = source_state.get(f"${id_str}_latest_sync", "1900-01-01T00:00:00.000000")
        last_modified_str = str(item["last_modified"])
        latest_sync = datetime.fromisoformat(latest_sync_str)
        last_modified = datetime.fromisoformat(last_modified_str)
        if latest_sync <= last_modified:
            if str(item["format"]) == "CSV":
                # url = str(item["url"])
                url = f"https://dados.pbh.gov.br/datastore/dump/{str(item['id'])}?format=json"
                json = requests.get(url, headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0"
                }).json()
                fields = [_get_field_schema(x) for x in json["fields"]]
                schema = pyarrow.schema(fields)
                records = json["records"]
                table = pyarrow.Table.from_pylist(records, schema=schema)
                yield table
        source_state[f"${id_str}_latest_sync"] = last_modified_str


    for r in resources:
        yield ckan_table(r)
    # for tag in packages['tags'].flatten():
    #     yield dlt.resource(tag,
    #                        name="ckan_tag",
    #                        write_disposition="merge",
    #                        primary_key="id",
    #                        columns={"last_modified": {"dedup_sort": "desc"}}
    #                        )
