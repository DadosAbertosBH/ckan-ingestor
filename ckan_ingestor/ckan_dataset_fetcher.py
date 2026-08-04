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

PAGE_SIZE = 25


class CkanDatasetFetcher(DatasetFetcher):
    url: str

    def __init__(self, url: str):
        super().__init__()
        self.url = url.rstrip("/")

    def fetch(self) -> pyarrow.Table:
        tables: list[pyarrow.Table] = []
        offset = 0
        base_schema = None

        while True:
            page = self._fetch_page(offset)
            if page.num_rows == 0:
                break
            if base_schema is None:
                base_schema = page.schema
            else:
                page = self._align_schema(page, base_schema)
            tables.append(page)
            logger.info(f"Fetched {page.num_rows} packages (offset={offset})")
            if page.num_rows < PAGE_SIZE:
                break
            offset += PAGE_SIZE

        if not tables:
            raise ValueError("No packages returned from CKAN API")

        return pyarrow.concat_tables(tables)

    def _fetch_page(self, offset: int) -> pyarrow.Table:
        import duckdb

        with duckdb.connect(":memory:") as conn:
            conn.execute("SET threads = 1")
            conn.execute("SET memory_limit = '1GB'")
            url = f"{self.url}/api/action/current_package_list_with_resources?limit={PAGE_SIZE}&offset={offset}"
            table = (
                conn.execute(
                    f"select unnest(result, max_depth :=2) from read_json('{url}',maximum_object_size=268435456)"
                )
                .arrow()
                .read_all()
            )

        if "extras" in table.column_names:
            table = table.drop_columns("extras")

        # Remove list columns that are all empty — DuckDB infers list<string>
        # for empty [] but later pages may have list<struct> with real data
        table = self._drop_empty_list_columns(table)

        return table

    @staticmethod
    def _drop_empty_list_columns(table: pyarrow.Table) -> pyarrow.Table:
        """Drop list columns where all values are null or empty."""
        drop = []
        for name in table.column_names:
            if not pyarrow.types.is_list(table.schema.field(name).type):
                continue
            col = table.column(name)
            if col.null_count == table.num_rows:
                drop.append(name)
                continue
            # Check all non-null values are empty lists
            all_empty = True
            for chunk in col.chunks:
                for val in chunk.tolist():
                    if val is not None and len(val) > 0:
                        all_empty = False
                        break
                if not all_empty:
                    break
            if all_empty:
                drop.append(name)
        return table.drop_columns(drop) if drop else table

    @staticmethod
    def _align_schema(
        table: pyarrow.Table, base_schema: pyarrow.Schema
    ) -> pyarrow.Table:
        """Align a table's schema to match the base schema.

        Missing columns: filled with nulls.
        Scalar type mismatch: cast to string.
        List/struct mismatch with data: raise.
        """
        for field in base_schema:
            if field.name not in table.column_names:
                table = table.append_column(
                    field, pyarrow.nulls(table.num_rows, field.type)
                )
                continue

            if table.schema.field(field.name).type == field.type:
                continue

            idx = table.schema.get_field_index(field.name)
            col = table.column(field.name)

            try:
                table = table.set_column(idx, field.name, col.cast(field.type))
                continue
            except (
                pyarrow.ArrowInvalid,
                pyarrow.ArrowTypeError,
                pyarrow.ArrowNotImplementedError,
            ):
                pass

            if not pyarrow.types.is_list(field.type):
                table = table.set_column(idx, field.name, col.cast(pyarrow.string()))
            else:
                raise TypeError(
                    f"Cannot align column '{field.name}': "
                    f"base={field.type}, page={table.schema.field(field.name).type}"
                )

        return table.select([f.name for f in base_schema])
