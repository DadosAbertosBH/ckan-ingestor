# Pedalin
# Copyright (C) 2025  Pedalin

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
