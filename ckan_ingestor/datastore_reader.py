# Pedalin
# Copyright (C) 2025  Pedalin

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import pyarrow
import requests

MAX_RECORDS_FETCH = 100_000


class DatastoreReader:
    def __init__(self, datastore_url: str):
        self.datastore_url = datastore_url

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
            return None
        return pyarrow.concat_tables(tables, promote_options="default")

    def get_total(self, resource_id: str) -> int | None:
        """Fetch the total record count from CKAN Datastore API."""
        url = f"{self.datastore_url}/{resource_id}?format=json&offset=0&limit=0"
        try:
            response = requests.get(
                url,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:132.0) Gecko/20100101 Firefox/132.0"
                },
                timeout=10,
            )
            response.raise_for_status()
            data = response.json()
            return data.get("total")
        except Exception:
            return None

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
