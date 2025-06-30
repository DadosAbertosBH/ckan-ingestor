from unittest.mock import MagicMock

import dagster as dg

from ckan_dagster.ckan_dagster.definitions import ckan_data_request_sensor, defs


def test_ckan_data_request_sensor_returns_expected_run_requests():
    # Mock do metadata_ingestor
    mock_metadata_ingestor = MagicMock()
    mock_metadata_ingestor.get_outdated_resources_id.return_value = ["res1", "res2"]

    context = dg.build_sensor_context(
        cursor="0",
        resources={
            "metadata_ingestor": mock_metadata_ingestor
        }
    )
    for run_request in ckan_data_request_sensor(context).run_requests:
        assert dg.validate_run_config(defs.get_job_def("ckan_data_job"), run_request.run_config)
        assert run_request.run_key in ["res1", "res2"]
