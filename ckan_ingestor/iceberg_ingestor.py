import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import In

from ckan_ingestor.config.rest_catalog_settings import RestCatalogSettings


class IcebergCkanIngestor:
    packages: pa.Table
    bucket: str
    catalog: Catalog

    def __init__(self, dataset: pa.Table):
        catalog_settings = RestCatalogSettings()
        self.packages = dataset
        self.catalog = load_catalog(
            "default",  # must be the same
            **{
                "uri": catalog_settings.uri,
                "warehouse": catalog_settings.warehouse
            }
        )

    def ingest(self):

        catalog_namespace = "default"
        namespaces = self.catalog.list_namespaces()
        if ("default",) not in namespaces:
            self.catalog.create_namespace("default")
        else:
            print("Catalog found")

        try:
            table = self.catalog.load_table("default.datasets")
            current_data = table.scan().to_arrow()
            inserted = self.packages.join(current_data, keys="id", join_type="anti") \
                .select(["id"])
            updated = self.packages.join(current_data, keys="id", right_suffix="r_", join_type="inner") \
                .filter(pc.field("metadata_modified") > pc.field("r__metadata_modified")) \
                .select(["id"])
            deleted = current_data.join(self.packages, keys="id", join_type="anti") \
                .select(["id"])
            items = inserted["id"].to_pylist() + updated["id"].to_pylist() + deleted["id"].to_pylist()
            table.overwrite(self.packages, overwrite_filter=In("id", items))
        except NoSuchTableError:
            table = self.catalog.create_table("default.datasets", schema=self.packages.schema)
            table.append(self.packages)
# if catalog.table_exists("warehouse.datasets"):
#     print("table found")
#     table = catalog.load_table("warehouse.datasets")    
#     
# else:
#     table = catalog.create_table_if_not_exists("warehouse.datasets", schema=datasets.schema)
#     table.append(datasets)
