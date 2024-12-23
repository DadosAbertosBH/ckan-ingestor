import dlt

import sqlalchemy as sa
from tests.dlt_trino.github import github_repo_events


def test_first_load() -> None:
    engine = sa.create_engine("trino://any@localhost:9999/iceberg")
    pipeline = dlt.pipeline(
        "github_events",
        destination=dlt.destinations.sqlalchemy(engine),
        dataset_name="test"
    )
    data = github_repo_events("dlt-hub", "dlt", access_token="")
    print(pipeline.run(data))