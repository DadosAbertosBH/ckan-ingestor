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
from urllib.parse import quote

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INGEST_ORCH_")

    # MySQL
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = ""
    mysql_database: str = "ingestor_orchestrator"

    # Apache Iggy
    iggy_address: str = "localhost:8090"
    iggy_username: str = "iggy"
    iggy_password: str = "iggy"
    iggy_stream: str = "ckan-ingestor"
    iggy_topic: str = "jobs"
    iggy_topic_retry: str = "jobs-retry"
    iggy_topic_results: str = "job-results"
    iggy_metadata_sync_topic: str = "ckan_metadata_sync"
    iggy_metadata_sync_result_topic: str = "ckan_metadata_sync_result"
    iggy_group_id: str = "ckan-worker"
    iggy_result_group_id: str = "ckan-result-consumer"
    iggy_metadata_sync_result_group_id: str = "ckan-metadata-sync-result-consumer"
    iggy_consumer_poll_interval_ms: int = 500
    iggy_partitions: int = 10

    # Scheduler
    scheduler_interval_minutes: int = 480  # 8 hours, same as Dagster sensor

    # App
    debug: bool = False

    @property
    def database_url(self) -> str:
        return (
            f"mysql+aiomysql://{self.mysql_user}:{quote(self.mysql_password)}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
        )

    @property
    def iggy_connection_string(self) -> str:
        return f"iggy+tcp://{self.iggy_username}:{self.iggy_password}@{self.iggy_address}"


settings = Settings()
