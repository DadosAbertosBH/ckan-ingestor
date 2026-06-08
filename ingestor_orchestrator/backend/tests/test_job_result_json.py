"""Test that dataset_preview with datetime values is JSON-serializable.

Regression test for: Object of type datetime is not JSON serializable.

DuckDB returns datetime.date and datetime.datetime objects in query results
(Arrow -> pylist conversion preserves these types). When the preview is stored
in CkanDataJobResult.dataset_preview (a JSON column), SQLAlchemy calls
json.dumps on it, which fails on non-serializable types.
"""

import datetime
import json

import pytest
from ingestor_orchestrator.json_utils import sanitize_json_preview
from ingestor_orchestrator.models import CkanDataJobResult


def _make_preview_with_datetimes():
    """Simulate the kind of dict list that DuckDB .arrow().to_pylist() returns."""
    return [
        {
            "id": "abc-123",
            "name": "Some dataset",
            "created": datetime.datetime(2025, 1, 15, 10, 30, 0),
            "date_col": datetime.date(2025, 3, 20),
            "count": 42,
        },
        {
            "id": "def-456",
            "name": "Another dataset",
            "created": datetime.datetime(2025, 2, 28, 14, 0, 0),
            "date_col": datetime.date(2025, 6, 1),
            "count": 100,
        },
    ]


def test_dataset_preview_with_datetime_raises_typeerror():
    """Before the fix, raw preview with datetime values is not JSON-serializable."""
    preview = _make_preview_with_datetimes()

    result = CkanDataJobResult(
        job_id="test-job-id",
        success=True,
        dataset_preview=preview,
        rows_processed=2,
    )

    # This is what SQLAlchemy does internally when flushing a JSON column
    with pytest.raises(TypeError, match="not JSON serializable"):
        json.dumps(result.dataset_preview)


def test_sanitize_preview_converts_datetimes_to_strings():
    """After the fix, sanitize_json_preview produces JSON-safe dicts."""
    preview = _make_preview_with_datetimes()
    sanitized = sanitize_json_preview(preview)

    # Must not raise
    serialized = json.dumps(sanitized)

    # datetime.datetime -> ISO 8601 string with time component
    assert '"2025-01-15T10:30:00"' in serialized
    # datetime.date -> ISO 8601 date string
    assert '"2025-03-20"' in serialized

    # Other values remain unchanged
    deserialized = json.loads(serialized)
    assert deserialized[0]["id"] == "abc-123"
    assert deserialized[0]["count"] == 42


def test_sanitize_preview_handles_none_and_empty():
    """Edge cases: None and empty list should pass through safely."""
    assert sanitize_json_preview(None) is None
    assert sanitize_json_preview([]) == []


def test_sanitize_preview_handles_nested_datetime():
    """Datetime values nested inside dicts within dicts."""
    preview = [
        {
            "metadata": {
                "last_updated": datetime.datetime(2025, 5, 1, 12, 0, 0),
                "published": datetime.date(2025, 5, 1),
            },
        }
    ]

    sanitized = sanitize_json_preview(preview)
    serialized = json.dumps(sanitized)

    assert '"2025-05-01T12:00:00"' in serialized
    assert '"2025-05-01"' in serialized
