import pyarrow as pa
import pyarrow.compute as pc
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import In
import time

from ckan_ingestor.config.rest_catalog_settings import RestCatalogSettings


class IcebergCkanIngestor:
    packages: pa.Table
    bucket: str
    catalog: Catalog

    def __init__(self, dataset: pa.Table):
        catalog_settings = RestCatalogSettings()
        self.packages = dataset.drop_columns(["resources", "organization", "tags"])
        self.catalog = load_catalog(
            "default",  # must be the same
            **{
                "uri": catalog_settings.uri,
                "warehouse": catalog_settings.warehouse
            }
        )

    def ingest(self):

        namespaces = self.catalog.list_namespaces()
        if ("default",) not in namespaces:
            self.catalog.create_namespace("default")
        else:
            print("Catalog found")

        try:
            table = self.catalog.load_table("default.datasets")
            start_time = time.time()
            current_data = table.scan(
                selected_fields=("id", "metadata_modified"),
            ).to_arrow()
            new_packages_ids = self.packages.select(["id", "metadata_modified"])
            print("--- %s seconds ---" % (time.time() - start_time))
            col_index = current_data.column_names.index('id')
            new_column = current_data['id'].cast(pa.string())
            current_data = current_data.set_column(col_index, 'id', new_column)

            inserted = new_packages_ids.join(current_data, keys="id", join_type="left anti").select(["id"])
            deleted = new_packages_ids.join(current_data, keys="id", join_type="right anti").select(["id"])
            updated = new_packages_ids.join(current_data, keys="id", right_suffix="_r", join_type="inner") \
                .filter(pc.field("metadata_modified") > pc.field("metadata_modified_r")) \
                .select(["id"])
            items = inserted["id"].to_pylist() + updated["id"].to_pylist() + deleted["id"].to_pylist()
            table.overwrite(self.packages, overwrite_filter=In("id", items))
        except NoSuchTableError:
            table = self.catalog.create_table("default.datasets", schema=self.packages.schema)
            table.append(self.packages)
