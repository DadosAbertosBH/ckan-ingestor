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
import json
from pathlib import Path

import yaml


def test_lake_explorer_pins_pnpm_and_approves_required_builds():
    root = Path(__file__).resolve().parents[1]
    package = json.loads((root / "lake-explorer/package.json").read_text())
    workspace = yaml.safe_load((root / "lake-explorer/pnpm-workspace.yaml").read_text())
    assert package["packageManager"].startswith("pnpm@12.")
    assert workspace["allowBuilds"] == {"esbuild": True, "workerd": True}


def test_lake_explorer_uses_happy_dom_without_browser_test_files():
    root = Path(__file__).resolve().parents[1]
    package = json.loads((root / "lake-explorer/package.json").read_text())
    assert "happy-dom" in package["devDependencies"]
    assert "@playwright/test" not in package["devDependencies"]
    assert not (root / "lake-explorer/playwright.config.ts").exists()
