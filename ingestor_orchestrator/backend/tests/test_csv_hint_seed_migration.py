"""Regression tests for the CSV hint seed migration."""

import importlib.util
from collections import Counter
from pathlib import Path


def test_seed_migration_contains_every_exported_csv_hint():
    migration_path = (
        Path(__file__).parents[1]
        / "migrations"
        / "versions"
        / "015_seed_csv_hints.py"
    )
    spec = importlib.util.spec_from_file_location("seed_csv_hints", migration_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)

    assert len(module.CSV_HINTS) == 849
    assert len({hint["resource_id"] for hint in module.CSV_HINTS}) == 849
    assert Counter(hint["delimiter"] for hint in module.CSV_HINTS) == {
        ",": 61,
        ";": 788,
    }
