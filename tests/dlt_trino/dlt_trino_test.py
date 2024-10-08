import dlt

from dlt_trino.factory import trino
from tests.dlt_trino.github import github_repo_events


def test_first_load() -> None:
    pipeline = dlt.pipeline(
        "github_events", destination=trino(), dataset_name="test"
    )
    data = github_repo_events("apache", "airflow", access_token="")
    print(pipeline.run(data))