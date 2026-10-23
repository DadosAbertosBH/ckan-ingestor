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
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
COORDINATOR = ROOT / "ckan-ingestor-rs/ckan-worker-coordinator/src"


def test_coordinator_emits_lifecycle_logs_for_message_and_ducklake_work():
    processor = (COORDINATOR / "parquet_message_processor.rs").read_text()
    registrar = (COORDINATOR / "parquet_registrar.rs").read_text()

    for event in (
        "coordinator_result_received",
        "ducklake_registration_started",
        "ducklake_registration_completed",
        "ducklake_registration_skipped",
        "ducklake_registration_retry",
        "ducklake_registration_failed",
        "coordinator_child_merge_started",
        "coordinator_child_merge_completed",
    ):
        assert event in processor or event in registrar
