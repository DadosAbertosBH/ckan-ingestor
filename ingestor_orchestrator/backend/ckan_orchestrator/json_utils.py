"""JSON serialization helpers for non-standard Python types."""

from datetime import date, datetime


def sanitize_json_preview(
    preview: list[dict] | dict | None,
) -> list[dict] | dict | None:
    """Convert non-JSON-serializable types (datetime/date) to ISO 8601 strings.

    DuckDB's Arrow -> pylist conversion returns datetime.date and
    datetime.datetime objects, which json.dumps cannot serialize.
    """
    if preview is None:
        return None

    def _convert(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, dict):
            return {k: _convert(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_convert(item) for item in obj]
        return obj

    return _convert(preview)
