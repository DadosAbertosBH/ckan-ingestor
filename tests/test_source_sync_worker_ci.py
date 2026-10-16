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
"""Regression checks for the source sync worker image build."""

from pathlib import Path

import yaml


def test_source_sync_worker_build_uses_native_arm64_and_registry_cache():
    pipeline = yaml.safe_load(Path(".gitlab-ci.yml").read_text())
    job = pipeline["build-source-sync-worker"]
    script = "\n".join(job["script"])

    assert job["tags"] == ["saas-linux-small-arm64"]
    assert "--cache-from type=registry,ref=$CI_REGISTRY_IMAGE/source-sync-worker-cache:arm64" in script
    assert "--cache-to type=registry,ref=$CI_REGISTRY_IMAGE/source-sync-worker-cache:arm64,mode=max" in script
