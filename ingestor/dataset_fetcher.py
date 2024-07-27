from ckanapi import RemoteCKAN
import pyarrow as pa

def fetch(url):
    dadosBh = RemoteCKAN(url)
    packages = dadosBh.action.package_search(rows=10000)["results"]
    # Remove empty arrays to avoid erros on schema
    # packages = map(lambda dict: {k: v for k, v in dict.items() if v and v != []}, packages)

    df = pa.Table.from_pylist(packages)
    # resouces = df.column("resources")
    # tags = df.column("tags")
    datasets = df.drop_columns(
        [
            "resources",
            "relationships_as_subject",
            "relationships_as_object",
            "tags",
            "extras",
            "license_url"
        ]
    )
    return datasets
