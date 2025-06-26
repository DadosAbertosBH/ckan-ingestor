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
import duckdb

from ckan_ingestor.config.ducklake_settings import DucklakeSettings


def from_settings(settings: DucklakeSettings = DucklakeSettings()):
    """Return a duckdb connection with required extensions."""
    conn = duckdb.connect(settings.database)
    conn.install_extension("ducklake FROM 'http://nightly-extensions.duckdb.org';")
    conn.load_extension("ducklake")
    conn.execute("INSTALL mysql; LOAD mysql;")
    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("SET pg_debug_show_queries=false;")

    account_id = (
        ""
        if settings.data_path.account_id is None
        else f",'ACCOUNT_ID '{settings.data_path.account_id}'"
    )

    stmt = f"""
            CREATE OR REPLACE SECRET secret (
                TYPE '{settings.data_path.protocol}',
                ENDPOINT '{settings.data_path.endpoint}',
                KEY_ID '{settings.data_path.access_key_id}',
                SECRET '{settings.data_path.secret_access_key}',
                USE_SSL '{settings.data_path.use_ssl}',
                URL_STYLE '{settings.data_path.url_style}'
                {account_id}
            );
        """

    conn.execute(stmt)

    try:
        stmt = (
            "ATTACH 'ducklake:{conn}' AS lake (DATA_PATH '{data_path_protocol}://{data_path_bucket}');"
        ).format(
            conn=settings.catalog_uri,
            data_path_protocol=settings.data_path.protocol,
            data_path_bucket=settings.data_path.bucket,
        )
        conn.execute(stmt)
    except duckdb.IOException as e:
        # Bug in mysql connection https://github.com/duckdb/ducklake/issues/214
        if "Table 'ducklake_metadata' already exist" not in str(e):
            raise e
        else:
            stmt = "ATTACH 'ducklake:{conn}' (CREATE_IF_NOT_EXISTS false); AS lake;".format(conn=settings.catalog_uri)
            conn.execute(stmt)
    conn.execute("USE lake;")
    return conn
