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
from datetime import datetime

from pydantic import BaseModel


class JobResultResponse(BaseModel):
    id: str
    job_id: str
    success: bool
    error_message: str | None
    error_trace: str | None
    dataset_preview: dict | list | None
    rows_processed: int | None
    expected_rows: int | None = None
    resource_size: int | None = None
    encoding: str | None = None
    created_at: datetime

    model_config = {"from_attributes": True}
