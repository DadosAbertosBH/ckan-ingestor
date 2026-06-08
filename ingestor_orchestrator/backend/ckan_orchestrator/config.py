from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="CKAN_ORCH_")

    # MySQL
    mysql_host: str = "localhost"
    mysql_port: int = 3306
    mysql_user: str = "root"
    mysql_password: str = ""
    mysql_database: str = "ckan_orchestrator"

    # NATS
    nats_url: str = "nats://localhost:4222"
    nats_stream: str = "CKAN_INGEST"
    nats_subject: str = "ckan.ingest.resource"

    # CKAN
    ckan_url: str = Field(default="https://dados.pbh.gov.br", alias="CKAN_URL")

    # Scheduler
    scheduler_interval_minutes: int = 480  # 8 hours, same as Dagster sensor

    # App
    debug: bool = False

    @property
    def database_url(self) -> str:
        return (
            f"mysql+aiomysql://{self.mysql_user}:{self.mysql_password}"
            f"@{self.mysql_host}:{self.mysql_port}/{self.mysql_database}"
        )


settings = Settings()
