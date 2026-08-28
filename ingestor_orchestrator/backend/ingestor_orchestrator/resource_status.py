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
from ingestor_orchestrator.models.job_status import JobStatus
from ingestor_orchestrator.models.resource_status import ResourceStatus


def classify_resource_status(
    current_status: JobStatus,
    last_terminal_status: JobStatus | None,
) -> ResourceStatus:
    """Return the public operational status for a resource."""
    if current_status == JobStatus.PROCESSING:
        return ResourceStatus.PROCESSING
    if current_status == JobStatus.PENDING:
        if last_terminal_status == JobStatus.FAILED:
            return ResourceStatus.FAILED
        if last_terminal_status == JobStatus.COMPLETED:
            return ResourceStatus.OUTDATED
        return ResourceStatus.PENDING
    return ResourceStatus(current_status.value)
