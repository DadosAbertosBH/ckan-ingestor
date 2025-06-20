from warnings import deprecated

import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import In

from ckan_ingestor.config.rest_catalog_settings import RestCatalogSettings

@deprecated("This class is deprecated, until merge is implemented in pyiceberg.")
class IcebergCkanIngestor:
    packages: pa.Table
    bucket: str
    catalog: Catalog

    def __init__(self, dataset: pa.Table):
        catalog_settings = RestCatalogSettings()
        self.packages = dataset.drop_columns(["resources", "organization", "tags"])
        self.catalog = load_catalog(
            "default",  # must be the same
            **{"uri": catalog_settings.uri, "warehouse": catalog_settings.warehouse},
        )

    def ingest(self):
        namespaces = self.catalog.list_namespaces()
        if ("default",) not in namespaces:
            self.catalog.create_namespace("default")
        else:
            print("Catalog found")

        try:
            table = self.catalog.load_table("default.datasets")
            table.upsert(
                self.packages,
                join_cols=["id", "metadata_modified"],
                when_matched_update_all=False,
            )
            items = pc.unique(self.packages["id"]).to_pylist()
            table.overwrite(self.packages, overwrite_filter=In("id", items))
        except NoSuchTableError:
            table = self.catalog.create_table(
                "default.datasets", schema=self.packages.schema
            )
            table.append(self.packages)
