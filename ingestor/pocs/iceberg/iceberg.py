from pyiceberg.catalog import load_catalog
import pyarrow as pa
import dataset_fetcher

def get_clean_array(array):
    if pa.types.is_struct(array.type):
        if isinstance(array, pa.ChunkedArray):
            array = array.combine_chunks()
        arrays = [
            get_clean_array(array.field(index))
            for index, field in enumerate(array.type)
            if not pa.types.is_null(field.type)
        ]
        names = [field.name for field in array.type if not pa.types.is_null(field.type)]
        return pa.StructArray.from_arrays(arrays, names)
    else:
        return array


def get_clean_table(table):

    return pa.Table.from_pydict(
        {
            field.name: get_clean_array(table[field.name])
            for field in table.schema
            if not pa.types.is_null(field.type)
        }
    )


catalog = load_catalog(
    "default",
    **{
        "uri": "http://127.0.0.1:8181",
        "s3.endpoint": "http://127.0.0.1:9000",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
    }
)

catalog_namespace = "warehouse"

namespaces = catalog.list_namespaces()
if ("warehouse",) not in namespaces:
    catalog.create_namespace("warehouse")
else:
    print("Catalog found")


datasets = dataset_fetcher.fetch("https://dados.pbh.gov.br/")

print(datasets.schema)

table = catalog.create_table("warehouse.datasets", schema=datasets.schema)

table.append(datasets)
