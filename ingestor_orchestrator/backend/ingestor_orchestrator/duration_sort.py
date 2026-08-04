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
"""Sort key for duration-based ordering — cross-DB compatible."""

from datetime import datetime, timezone

from ingestor_orchestrator.models import CkanDataJob


def duration_sort_key(job: CkanDataJob, reverse: bool) -> tuple[int, float]:
    """Compute sort key for duration ordering — NULLs always last."""
    if job.started_at is None:
        return (1, 0.0)

    end = job.completed_at if job.completed_at else datetime.now(timezone.utc)
    start = (
        job.started_at.replace(tzinfo=timezone.utc)
        if job.started_at.tzinfo is None
        else job.started_at
    )
    end = end.replace(tzinfo=timezone.utc) if end.tzinfo is None else end
    ms = (end - start).total_seconds() * 1000
    return (0, -ms if reverse else ms)
