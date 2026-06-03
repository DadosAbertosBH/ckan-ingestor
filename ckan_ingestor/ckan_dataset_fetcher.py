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

from ckan_ingestor.dataset_fetcher import DatasetFetcher


class CkanDatasetFetcher(DatasetFetcher):
    url: str

    def __init__(self, url: str):
        super().__init__()
        self.url = url

    def fetch(self) -> pyarrow.Table:
        import duckdb

        with duckdb.connect(":memory:") as conn:
            return conn.execute(f"""
            select unnest(result, max_depth :=2) from 
            read_json('{self.url}/api/action/current_package_list_with_resources?limit=1000', maximum_object_size=1073741824)
            """).arrow().read_all()
