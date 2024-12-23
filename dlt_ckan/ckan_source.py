import dlt

from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher


@dlt.source
def ckan(ckan_url=dlt.config.value):
    fetcher = CkanDatasetFetcher(ckan_url)
    packages = fetcher.fetch()

    yield dlt.resource(packages,
                       name="ckan_package",
                       write_disposition={"disposition": "merge", "strategy": "upsert"},
                       primary_key="id",
                       table_format="delta",
                       columns={"metadata_modified": {"dedup_sort": "desc"}}
                       )

    resource = packages['resources'].flatten()
    yield dlt.resource(resource,
                       name="ckan_resource",
                       write_disposition="merge",
                       primary_key="id",
                       columns={"last_modified": {"dedup_sort": "desc"}}
                       )

    # for tag in packages['tags'].flatten():
    #     yield dlt.resource(tag,
    #                        name="ckan_tag",
    #                        write_disposition="merge",
    #                        primary_key="id",
    #                        columns={"last_modified": {"dedup_sort": "desc"}}
    #                        )
