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
"""Deployment configuration for the lake explorer."""

import json
from pathlib import Path


def test_local_explorer_uses_remote_r2_bucket():
    config_path = Path(__file__).parents[1] / "lake-explorer" / "wrangler.jsonc"
    config = json.loads(config_path.read_text())

    assert {
        "binding": "BUCKET",
        "bucket_name": "public-datasets",
        "remote": True,
    } in config["r2_buckets"]


def test_vite_proxy_rewrites_local_worker_origin():
    config_path = Path(__file__).parents[1] / "lake-explorer" / "vite.config.ts"
    config = config_path.read_text()

    assert "headers: { origin: 'http://127.0.0.1:8787' }" in config
