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

import pytest

from ingestor_orchestrator.models import JobStatus, ResourceStatus
from ingestor_orchestrator.resource_status import classify_resource_status


@pytest.mark.parametrize(
    ("current", "terminal", "expected"),
    [
        (JobStatus.PENDING, None, ResourceStatus.PENDING),
        (JobStatus.PENDING, JobStatus.COMPLETED, ResourceStatus.OUTDATED),
        (JobStatus.PENDING, JobStatus.FAILED, ResourceStatus.FAILED),
        (JobStatus.PROCESSING, JobStatus.COMPLETED, ResourceStatus.PROCESSING),
        (JobStatus.PROCESSING, JobStatus.FAILED, ResourceStatus.PROCESSING),
        (JobStatus.COMPLETED, JobStatus.FAILED, ResourceStatus.COMPLETED),
        (JobStatus.FAILED, JobStatus.COMPLETED, ResourceStatus.FAILED),
    ],
)
def test_classify_resource_status(current, terminal, expected):
    assert classify_resource_status(current, terminal) == expected
