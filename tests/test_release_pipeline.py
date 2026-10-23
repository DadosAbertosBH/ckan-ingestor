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
from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1]
PIPELINE_PATH = ROOT / ".gitlab-ci.yml"
RELEASE_BUILD_JOBS = (
    "build-lake-snapshot",
    "build-source-sync-worker",
    ".build-orchestrator-base",
    "build-orchestrator-arm64",
    "build-orchestrator",
    ".build-worker-base",
    "build-worker",
    "build-worker-coordinator",
)


def test_release_images_are_built_only_by_a_release_tag():
    pipeline = yaml.safe_load(PIPELINE_PATH.read_text())

    for job in RELEASE_BUILD_JOBS:
        assert pipeline[job]["rules"] == [{"if": "$CI_COMMIT_TAG"}]
