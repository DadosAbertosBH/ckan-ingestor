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
import gzip
import importlib.util
import subprocess
from pathlib import Path

import yaml

spec = importlib.util.spec_from_file_location(
    "install_snapshot_extensions",
    Path(__file__).resolve().parents[1] / "analytics/install_snapshot_extensions.py",
)
install_snapshot_extensions = importlib.util.module_from_spec(spec)
spec.loader.exec_module(install_snapshot_extensions)


def test_snapshot_image_contains_catalog_ingestion():
    dockerfile = Path(__file__).resolve().parents[1] / "analytics/Dockerfile.snapshot"
    assert "analytics/catalog_ingestion.py" in dockerfile.read_text()


def test_snapshot_job_is_opt_in_and_uses_existing_credentials():
    chart = Path(__file__).resolve().parents[1] / "charts/ingestor-orchestrator"

    def render(*args):
        result = subprocess.run(
            ["helm", "template", "test", str(chart), *args],
            check=True,
            capture_output=True,
            text=True,
        )
        return [d for d in yaml.safe_load_all(result.stdout) if d]

    assert not any(
        d["kind"] == "CronJob" and "lake-snapshot" in d["metadata"]["name"]
        for d in render()
    )
    jobs = [
        d
        for d in render(
            "--set",
            "lakeSnapshot.enabled=true",
            "--set",
            "existingSecret=lake-credentials",
        )
        if d["kind"] == "CronJob"
    ]
    assert len(jobs) == 1
    job = jobs[0]["spec"]
    assert job["schedule"] == "0 4 * * *"
    assert job["timeZone"] == "America/Sao_Paulo"
    assert job["concurrencyPolicy"] == "Forbid"
    assert job["suspend"] is False
    assert job["jobTemplate"]["spec"]["activeDeadlineSeconds"] == 21600
    pod = job["jobTemplate"]["spec"]["template"]["spec"]
    assert pod["securityContext"]["runAsNonRoot"] is True
    container = pod["containers"][0]
    assert {"secretRef": {"name": "lake-credentials"}} in container["envFrom"]
    assert container["securityContext"]["readOnlyRootFilesystem"] is True
    cnpg_jobs = [
        d
        for d in render(
            "--set", "lakeSnapshot.enabled=true", "--set", "postgres.deploy=true"
        )
        if d["kind"] == "CronJob"
    ]
    env = cnpg_jobs[0]["spec"]["jobTemplate"]["spec"]["template"]["spec"]["containers"][
        0
    ]["env"]
    catalog = next(e for e in env if e["name"] == "DUCKLAKE_CATALOG_URI")
    assert catalog["valueFrom"]["secretKeyRef"]["key"] == "uri"


def test_snapshot_job_can_be_suspended_for_manual_debugging():
    chart = Path(__file__).resolve().parents[1] / "charts/ingestor-orchestrator"
    result = subprocess.run(
        [
            "helm",
            "template",
            "test",
            str(chart),
            "--set",
            "lakeSnapshot.enabled=true",
            "--set",
            "lakeSnapshot.suspend=true",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    cronjob = next(
        document
        for document in yaml.safe_load_all(result.stdout)
        if document and document["kind"] == "CronJob"
    )

    assert cronjob["spec"]["suspend"] is True


def test_extension_bootstrap_downloads_target_platform_without_duckdb_runtime(
    monkeypatch, tmp_path
):
    requested = []

    class Response:
        def read(self):
            return gzip.compress(b"signed-extension")

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

    def urlopen(request, timeout):
        requested.append((request.full_url, request.headers["User-agent"], timeout))
        return Response()

    monkeypatch.setattr(install_snapshot_extensions.urllib.request, "urlopen", urlopen)
    install_snapshot_extensions.download_extensions(
        tmp_path, version="1.5.6", platform="linux_amd64"
    )

    assert (tmp_path / "v1.5.6/linux_amd64/ducklake.duckdb_extension").read_bytes() == (
        b"signed-extension"
    )
    assert requested == [
        (
            f"https://extensions.duckdb.org/v1.5.6/linux_amd64/{name}.duckdb_extension.gz",
            "DuckDB/v1.5.6",
            60,
        )
        for name in ("httpfs", "postgres_scanner", "ducklake")
    ]
