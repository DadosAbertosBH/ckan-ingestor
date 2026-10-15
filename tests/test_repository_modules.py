# Copyright (C) 2026 Noctcloud Desenvolvimento LTDA
# SPDX-License-Identifier: AGPL-3.0-or-later
# Licensed under the GNU Affero General Public License, version 3 or later.
"""Keep database implementations outside application and contract modules."""

from pathlib import Path

import yaml


ROOT = Path(__file__).resolve().parents[1] / "ingestor_orchestrator"


def test_repository_modules_are_independent():
    for name in ("repository-contracts", "mysql-repositories", "oracle-repositories"):
        module = ROOT / name
        assert (module / "go.mod").is_file(), f"missing module: {name}"
        for source in module.rglob("*.go"):
            content = source.read_text()
            assert "/backend/internal/" not in content, source
            if name == "repository-contracts":
                assert "go-sql-driver" not in content, source
                assert "sijms/go-ora" not in content, source


def test_application_selects_oracle_by_default():
    for source in (
        ROOT / "backend/internal/config/config.go",
        ROOT / "source-sync-worker/worker.go",
    ):
        assert 'env("INGEST_ORCH_DB_PROVIDER", "oracle")' in source.read_text()


def test_compose_selects_oracle_for_all_sql_consumers():
    services = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]
    for name in ("api", "db-init", "source-sync-worker"):
        assert services[name]["environment"]["INGEST_ORCH_DB_PROVIDER"] == "oracle"
        assert "oracle-bootstrap" in services[name]["depends_on"]
