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
import logging

import pyarrow

from ckan_ingestor.dataset_fetcher import DatasetFetcher

logger = logging.getLogger(__name__)

PAGE_SIZE = 50

EXTRAS_TYPE = pyarrow.list_(
    pyarrow.struct(
        [
            pyarrow.field("key", pyarrow.string()),
            pyarrow.field("value", pyarrow.string()),
        ]
    )
)


class CkanDatasetFetcher(DatasetFetcher):
    url: str

    def __init__(self, url: str):
        super().__init__()
        self.url = url.rstrip("/")

    def fetch(self) -> pyarrow.Table:
        tables: list[pyarrow.Table] = []
        offset = 0

        while True:
            page = self._fetch_page(offset)
            if page.num_rows == 0:
                break
            tables.append(page)
            logger.info(f"Fetched {page.num_rows} packages (offset={offset})")
            if page.num_rows < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        if not tables:
            raise ValueError("No packages returned from CKAN API")

        return pyarrow.concat_tables(tables, promote_options="permissive")

    def _fetch_page(self, offset: int) -> pyarrow.Table:
        import duckdb

        with duckdb.connect(":memory:") as conn:
            table = (
                conn.execute(f"""
            select unnest(result, max_depth :=2) from
            read_json('{self.url}/api/action/current_package_list_with_resources?limit={PAGE_SIZE}&offset={offset}',
                maximum_object_size=1073741824)
            """)
                .arrow()
                .read_all()
            )

        if "extras" in table.column_names:
            table = self._normalize_extras(table)

        return table

    @staticmethod
    def _normalize_extras(table: pyarrow.Table) -> pyarrow.Table:
        col = table.column("extras")
        try:
            col = col.cast(EXTRAS_TYPE)
        except (pyarrow.ArrowInvalid, pyarrow.ArrowTypeError):
            col = pyarrow.nulls(table.num_rows, EXTRAS_TYPE)
        idx = table.schema.get_field_index("extras")
        return table.set_column(idx, "extras", col)
