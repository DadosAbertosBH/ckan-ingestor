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
VALUES_PATH = ROOT / "charts/ingestor-orchestrator/values.yaml"
IMAGE_COMPONENTS = ("api", "worker", "workerCoordinator", "sourceSyncWorker", "lakeSnapshot")


def test_chart_uses_explicit_image_configuration_for_every_component():
    values = yaml.safe_load(VALUES_PATH.read_text())

    assert "image" not in values["global"]
    for component in IMAGE_COMPONENTS:
        assert values[component]["image"] == {
            "registry": "registry.gitlab.com/pedalin/ckan-ingestor",
            "repository": values[component]["image"]["repository"],
            "tag": "latest",
            "pullPolicy": "IfNotPresent",
        }
