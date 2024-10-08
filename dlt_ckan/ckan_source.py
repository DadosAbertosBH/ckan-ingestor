import dlt

from ckan_ingestor.ckan_dataset_fetcher import CkanDatasetFetcher


@dlt.source
def hubspot(ckan_url: dlt.config.value):
    fetcher = CkanDatasetFetcher(ckan_url)
    packages = fetcher.fetch()

    for package in packages:
        yield dlt.resource(package,
                           name="ckan_package",
                           write_disposition={"disposition": "merge", "strategy": "upsert"},
                           primary_key="id",
                           columns={"metadata_modified": {"dedup_sort": "desc"}}
                           )

    # for resource in packages['resources'].flatten():
    #     yield dlt.resource(resource,
    #                        name="ckan_resource",
    #                        write_disposition="merge",
    #                        primary_key="id",
    #                        columns={"last_modified": {"dedup_sort": "desc"}}
    #                        )
    #
    # for tag in packages['tags'].flatten():
    #     yield dlt.resource(tag,
    #                        name="ckan_tag",
    #                        write_disposition="merge",
    #                        primary_key="id",
    #                        columns={"last_modified": {"dedup_sort": "desc"}}
    #                        )
