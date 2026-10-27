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
RELEASE_BUILD_RULES = {
    "build-lake-snapshot": "$CI_COMMIT_TAG =~ /^lake-snapshot-\\d+\\.\\d+\\.\\d+$/",
    "build-source-sync-worker": "$CI_COMMIT_TAG =~ /^source-sync-worker-\\d+\\.\\d+\\.\\d+$/",
    "build-orchestrator-arm64": "$CI_COMMIT_TAG =~ /^orchestrator-\\d+\\.\\d+\\.\\d+$/",
    "build-orchestrator": "$CI_COMMIT_TAG =~ /^orchestrator-\\d+\\.\\d+\\.\\d+$/",
    "build-worker-arm64": "$CI_COMMIT_TAG =~ /^worker-\\d+\\.\\d+\\.\\d+$/",
    "build-worker": "$CI_COMMIT_TAG =~ /^worker-\\d+\\.\\d+\\.\\d+$/",
    "build-worker-coordinator-arm64": "$CI_COMMIT_TAG =~ /^worker-coordinator-\\d+\\.\\d+\\.\\d+$/",
    "build-worker-coordinator": "$CI_COMMIT_TAG =~ /^worker-coordinator-\\d+\\.\\d+\\.\\d+$/",
}


def test_release_images_are_built_only_by_their_component_tag():
    pipeline = yaml.safe_load(PIPELINE_PATH.read_text())

    for job, tag_rule in RELEASE_BUILD_RULES.items():
        assert pipeline[job]["rules"] == [{"if": tag_rule}]


def test_lake_snapshot_release_builds_natively_for_arm64():
    pipeline = yaml.safe_load(PIPELINE_PATH.read_text())
    job = pipeline["build-lake-snapshot"]
    script = "\n".join(job["script"])

    assert job["tags"] == ["noctcloud-k8s"]
    assert "docker buildx" not in script
    assert "docker build " in script
    assert 'IMAGE_TAG="${CI_COMMIT_TAG#lake-snapshot-}"' in script
    assert 'docker push "$CI_REGISTRY_IMAGE/lake-snapshot:$IMAGE_TAG"' in script


def test_component_release_tags_publish_the_semantic_version_without_prefix():
    pipeline = yaml.safe_load(PIPELINE_PATH.read_text())

    expected_versions = {
        "build-source-sync-worker": "source-sync-worker",
        "build-orchestrator": "orchestrator",
        "build-worker": "worker",
        "build-worker-coordinator": "worker-coordinator",
    }
    for job_name, component in expected_versions.items():
        script = "\n".join(pipeline[job_name]["script"])
        assert f'IMAGE_TAG="${{CI_COMMIT_TAG#{component}-}}"' in script
        assert f'--tag "$CI_REGISTRY_IMAGE/{component}:$IMAGE_TAG"' in script
