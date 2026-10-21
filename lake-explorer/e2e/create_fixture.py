# Noctcloud Desenvolvimento LTDA
# Copyright (C) 2026  Noctcloud Desenvolvimento LTDA

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
"""Create a real native catalog and remote Parquet fixture for browser tests."""

import importlib.util
import json
import shutil
import tempfile
from pathlib import Path

import duckdb

root = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "publisher", root / "analytics/publish_lake_snapshot.py"
)
publisher = importlib.util.module_from_spec(spec)
spec.loader.exec_module(publisher)
fixtures = Path(__file__).parent / "fixtures"
fixtures.mkdir(exist_ok=True)
with tempfile.TemporaryDirectory() as work:
    catalog = Path(work) / "source.duckdb"
    data = Path(work) / "data"
    with duckdb.connect() as conn:
        conn.execute("LOAD ducklake")
        conn.execute(f"ATTACH 'ducklake:{catalog}' AS lake (DATA_PATH '{data}')")
        conn.execute(
            "CREATE TABLE lake.despesas AS SELECT i AS id, CASE WHEN i % 2=0 THEN 'Recife' ELSE 'São Paulo' END AS cidade, i * 1.5 AS valor FROM range(5000) t(i)"
        )
        conn.execute("DELETE FROM lake.despesas WHERE id=0")
        conn.execute("CREATE TABLE lake.inline_table(id INTEGER)")
        conn.execute("INSERT INTO lake.inline_table VALUES (42)")
    published = Path(work) / "published.duckdb"
    manifest = publisher.export_catalog(str(catalog), published)
    shutil.copyfile(published, fixtures / "catalog.duckdb")
    manifest.update(
        bucket="fixture", catalogKey="catalog.duckdb", dataPath="s3://fixture/data"
    )
    files = [
        "s3://fixture/data/" + str(Path(p).relative_to(data))
        for p in manifest["protectedFiles"]
    ]
    manifest["fixtureFiles"] = files
    manifest.pop("protectedFiles")
    (fixtures / "manifest.json").write_text(json.dumps(manifest))
    shutil.copytree(data, fixtures / "data", dirs_exist_ok=True)
