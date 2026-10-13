# Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
# Licensed under the GNU Affero General Public License, version 3 or later.

from pathlib import Path

import pytest
import yaml


@pytest.mark.parametrize("service", ["source-sync-worker", "sicom-crawler"])
def test_images_use_official_lightpanda_nightly(service):
    root = Path(__file__).resolve().parents[1]
    dockerfile = (root / "ingestor_orchestrator" / service / "Dockerfile").read_text()
    assert "FROM lightpanda/browser:nightly AS lightpanda" in dockerfile
    assert "COPY --from=lightpanda /bin/lightpanda /usr/local/bin/lightpanda" in dockerfile
    assert "LIGHTPANDA_RUNTIME_IMAGE" not in dockerfile


def test_compose_worker_build_is_independent_of_a_lightpanda_service():
    root = Path(__file__).resolve().parents[1]
    compose = yaml.safe_load((root / "ingestor_orchestrator/docker-compose.yml").read_text())
    worker = compose["services"]["source-sync-worker"]
    assert "lightpanda-runtime" not in compose["services"]
    assert not worker.get("profiles")
    assert not worker["build"].get("additional_contexts")
    assert "LIGHTPANDA_RUNTIME_IMAGE" not in worker["build"].get("args", {})
    assert "lightpanda-runtime" not in worker["depends_on"]


def test_ci_builds_worker_directly_and_refreshes_nightly():
    root = Path(__file__).resolve().parents[1]
    pipeline = (root / ".gitlab-ci.yml").read_text()
    worker_build = pipeline.split("\nbuild-source-sync-worker:\n", 1)[1].split(
        "\n.rust-ci-base:", 1
    )[0]
    assert "--pull" in worker_build
    assert "lightpanda-runtime" not in pipeline
    assert "LIGHTPANDA_RUNTIME_IMAGE" not in pipeline
