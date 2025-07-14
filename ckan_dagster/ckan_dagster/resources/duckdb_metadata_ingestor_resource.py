# Pedalin
# Copyright (C) 2025  Pedalin
from contextlib import contextmanager

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
import dagster as dg
from dagster_duckdb import DuckDBResource

from ckan_ingestor.duckdb_ckan_metadata_ingestor import DuckdbCkanMetadataIngestor


class DuckdbMetadataIngestorResource(
    dg.ConfigurableResource[DuckdbCkanMetadataIngestor]
):
    duckdb: DuckDBResource


    @contextmanager
    def yield_for_execution(self, context: dg.InitResourceContext):
        # keep connection open for the duration of the execution
        with self.duckdb.get_connection() as conn:
            # set up the connection attribute so it can be used in the execution
            metadata_ingestor = DuckdbCkanMetadataIngestor(
                conn
            )

            yield metadata_ingestor
