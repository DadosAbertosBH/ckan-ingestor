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
"""Regression tests for the CSV hint seed migration."""

import importlib.util
from collections import Counter
from pathlib import Path


def load_migration(filename: str):
    migration_path = Path(__file__).parents[1] / "migrations" / "versions" / filename
    spec = importlib.util.spec_from_file_location("seed_csv_hints", migration_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_seed_migration_contains_every_exported_csv_hint():
    module = load_migration("015_seed_csv_hints.py")

    assert len(module.CSV_HINTS) == 849
    assert len({hint["resource_id"] for hint in module.CSV_HINTS}) == 849
    assert Counter(hint["delimiter"] for hint in module.CSV_HINTS) == {
        ",": 61,
        ";": 788,
    }


def test_incremental_seed_migration_contains_new_csv_hints():
    initial = load_migration("015_seed_csv_hints.py")
    incremental = load_migration("017_seed_additional_csv_hints.py")

    assert incremental.revision == "017"
    assert incremental.down_revision == "016"

    initial_ids = {hint["resource_id"] for hint in initial.CSV_HINTS}
    incremental_ids = {hint["resource_id"] for hint in incremental.CSV_HINTS}

    assert len(incremental.CSV_HINTS) == 3_215
    assert len(incremental_ids) == 3_215
    assert initial_ids.isdisjoint(incremental_ids)
    assert Counter(hint["delimiter"] for hint in incremental.CSV_HINTS) == {
        "\t": 1,
        ",": 257,
        ";": 2_957,
    }
