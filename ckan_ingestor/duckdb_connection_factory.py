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
    conn = duckdb.connect(settings.database, config={
        "threads": 1,
        "s3_url_style": settings.data_path.url_style,
        "s3_use_ssl": settings.data_path.use_ssl,
        "s3_endpoint": settings.data_path.endpoint,
        "s3_access_key_id": settings.data_path.access_key_id,
        "s3_secret_access_key": settings.data_path.secret_access_key
    })
    conn.install_extension("mysql")
    conn.install_extension("postgres")
    conn.install_extension("httpfs")
    conn.load_extension("ducklake")
    conn.load_extension("mysql")
    conn.load_extension("postgres")
    conn.load_extension("httpfs")
    conn.execute("SET pg_debug_show_queries=false;")

    conn.execute(f"ATTACH IF NOT EXISTS 'ducklake:{settings.catalog_uri}' AS lake "
                 f"(DATA_PATH '{settings.data_path.protocol}://{settings.data_path.bucket}', "
                 f"DATA_INLINING_ROW_LIMIT 10000, AUTOMATIC_MIGRATION TRUE);")
    conn.execute("USE lake;")
    return conn
