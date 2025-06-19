import duckdb
import pyarrow
import requests

MAX_RECORDS_FETCH = 100_000


class DatastoreReader:
    def __init__(self, conn: duckdb.DuckDBPyConnection, dastore_url: str):
        self.datastore_url = dastore_url
        self.conn = conn

    def read(self, resource_id: str) -> pyarrow.Table:
        """
        Some resources seem to exceed max response lenght and return broken json.
        Here we are spliting the requests to avoid those cases
        """

        offset = 0
        tables: list[pyarrow.Table] = []
        url = f"{self.datastore_url}/{resource_id}?format=json&offset={offset}&limit={MAX_RECORDS_FETCH}"
        while data := DatastoreReader._read_json(url):
            offset = offset + MAX_RECORDS_FETCH
            url = f"{self.datastore_url}/{resource_id}?format=json&offset={offset}&limit={MAX_RECORDS_FETCH}"
            tables.append(data)
        if not tables:
            raise duckdb.IOException(f"Now rows from {url}")
        return pyarrow.concat_tables(tables, promote=True)

    @staticmethod
    def _read_json(url):
        response = requests.get(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0"
            },
        )
        response.raise_for_status()
        try:
            data = response.json()
        except requests.JSONDecodeError as e:
            print(f"Failed to parse json url = {url}")
            raise e
        row_data = data["records"]
        column_names = [field["id"] for field in data["fields"]]
        column_data = list(zip(*row_data))
        arrays = [pyarrow.array(col) for col in column_data]
        if not arrays:
            return None
        # noinspection PyArgumentList
        return pyarrow.Table.from_arrays(arrays, names=column_names)
