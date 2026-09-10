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
"""Regression tests for the deleted terminal-status migration."""

import importlib.util
from pathlib import Path


def load_migration():
    migration_path = (
        Path(__file__).parents[1]
        / "migrations"
        / "versions"
        / "020_add_deleted_metadata_status.py"
    )
    spec = importlib.util.spec_from_file_location("deleted_metadata_status", migration_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_upgrade_reuses_an_existing_result_status_column(monkeypatch):
    module = load_migration()
    added_columns = []
    statements = []

    class Inspector:
        def get_columns(self, table_name):
            if table_name == "ckan_data_job_result":
                return [{"name": "status"}]
            return []

    monkeypatch.setattr(module.op, "get_bind", lambda: object())
    monkeypatch.setattr(module.sa, "inspect", lambda bind: Inspector())
    monkeypatch.setattr(
        module.op,
        "add_column",
        lambda table_name, column: added_columns.append((table_name, column.name)),
    )
    monkeypatch.setattr(module.op, "execute", statements.append)
    monkeypatch.setattr(module.op, "alter_column", lambda *args, **kwargs: None)

    module.upgrade()

    assert ("ckan_data_job_result", "status") not in added_columns
    assert any(
        "ALTER TABLE ckan_data_job_result MODIFY COLUMN status" in statement
        for statement in statements
    )
