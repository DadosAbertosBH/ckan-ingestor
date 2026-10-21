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


def test_lake_storage_is_private_and_browser_access_is_read_only():
    root = Path(__file__).resolve().parents[1]
    config = json.loads((root / "lake-explorer/infra/main.tf.json").read_text())
    resources = config["resource"]
    assert resources["cloudflare_r2_managed_domain"]["lake"]["enabled"] is False
    assert resources["cloudflare_r2_custom_domain"]["existing"]["enabled"] is False
    rule = resources["cloudflare_r2_bucket_cors"]["explorer"]["rules"][0]
    assert rule["allowed"]["methods"] == ["GET", "HEAD"]
    assert rule["allowed"]["origins"] == ["${var.portal_origin}"]
    assert "x-amz-security-token" in rule["allowed"]["headers"]
    assert "Content-Range" in rule["expose_headers"]


def test_ci_builds_snapshot_and_tests_native_browser_flow():
    root = Path(__file__).resolve().parents[1]
    pipeline = yaml.safe_load((root / ".gitlab-ci.yml").read_text())
    assert (
        "uv run lake-explorer/e2e/create_fixture.py"
        in pipeline["test-lake-snapshot"]["script"]
    )
    assert "pnpm run test:e2e" in pipeline["test-lake-explorer"]["script"]
    assert "corepack enable" in pipeline["test-lake-explorer"]["before_script"]
    assert any(
        "analytics/Dockerfile.snapshot" in line
        for line in pipeline["build-lake-snapshot"]["script"]
    )


def test_lake_explorer_pins_pnpm_and_approves_required_builds():
    root = Path(__file__).resolve().parents[1]
    package = json.loads((root / "lake-explorer/package.json").read_text())
    workspace = yaml.safe_load((root / "lake-explorer/pnpm-workspace.yaml").read_text())
    assert package["packageManager"].startswith("pnpm@12.")
    assert workspace["allowBuilds"] == {"esbuild": True, "workerd": True}


def test_playwright_starts_vite_on_its_configured_port():
    root = Path(__file__).resolve().parents[1]
    config = (root / "lake-explorer/playwright.config.ts").read_text()
    assert "pnpm exec vite --host 127.0.0.1 --port 5179" in config
