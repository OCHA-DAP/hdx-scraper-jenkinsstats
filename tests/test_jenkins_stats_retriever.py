from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from hdx.scraper.jenkinsstats.jenkins_stats_retriever import JenkinsStatsRetriever


def make_retriever(sample_configuration):
    config_mock = MagicMock()
    config_mock.__getitem__ = MagicMock(
        side_effect=lambda key: sample_configuration[key]
    )
    config_mock.get_hdx_site_url.return_value = "https://data.humdata.org"

    download_path = MagicMock()
    download_path.rename.return_value = Path("/tmp/jenkins_monthly_stats.csv")

    downloader_mock = MagicMock()
    downloader_mock.download_file.return_value = download_path

    return JenkinsStatsRetriever(config_mock, downloader_mock)


def make_df(rows):
    """rows: list of (projectName, result, buildDuration, days_ago)"""
    now = pd.Timestamp.now(tz="UTC")
    data = [
        {
            "projectName": project,
            "result": result,
            "buildDuration": duration,
            "buildTimestamp": (now - pd.Timedelta(days=days_ago)).isoformat(),
        }
        for project, result, duration, days_ago in rows
    ]
    return pd.DataFrame(data)


def run_process(retriever, df):
    builds_resource_mock = MagicMock()
    builds_resource_mock.__getitem__ = MagicMock(
        side_effect=lambda key: "builds-resource-id" if key == "id" else None
    )
    builds_dataset_mock = MagicMock()
    builds_dataset_mock.get_resource.return_value = builds_resource_mock

    stats_resource_mock = MagicMock()
    stats_resource_mock.__getitem__ = MagicMock(
        side_effect=lambda key: "stats-resource-id" if key == "id" else None
    )
    stats_dataset_mock = MagicMock()
    stats_dataset_mock.get_resource.return_value = stats_resource_mock

    def dataset_side_effect(name):
        return builds_dataset_mock if name == "jenkins-builds" else stats_dataset_mock

    with (
        patch(
            "hdx.scraper.jenkinsstats.jenkins_stats_retriever.Dataset.read_from_hdx",
            side_effect=dataset_side_effect,
        ),
        patch(
            "hdx.scraper.jenkinsstats.jenkins_stats_retriever.pd.read_csv",
            return_value=df,
        ),
        patch.object(retriever, "_upload_to_drive"),
    ):
        retriever.process()

    return stats_resource_mock


def test_process_empty_dataframe(sample_configuration):
    retriever = make_retriever(sample_configuration)
    resource_mock = run_process(retriever, make_df([]))
    resource_mock.update_datastore.assert_called_once_with([])


def test_process_single_project_success(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [
        ("proj-a", "SUCCESS", 60000, 1),
        ("proj-a", "SUCCESS", 60000, 2),
        ("proj-a", "SUCCESS", 60000, 3),
    ]
    resource_mock = run_process(retriever, make_df(rows))
    records = resource_mock.update_datastore.call_args[0][0]
    assert len(records) == 1
    r = records[0]
    assert r["projectName"] == "proj-a"
    assert r["num_runs"] == 3
    assert r["num_successful"] == 3
    assert r["num_failed"] == 0
    assert r["num_aborted"] == 0
    assert r["success_rate"] == 100.0


def test_process_mixed_results(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [
        ("proj-a", "SUCCESS", 60000, 1),
        ("proj-a", "SUCCESS", 60000, 2),
        ("proj-a", "FAILURE", 30000, 3),
        ("proj-a", "ABORTED", 10000, 4),
    ]
    resource_mock = run_process(retriever, make_df(rows))
    records = resource_mock.update_datastore.call_args[0][0]
    assert len(records) == 1
    r = records[0]
    assert r["num_runs"] == 4
    assert r["num_successful"] == 2
    assert r["num_failed"] == 1
    assert r["num_aborted"] == 1
    assert r["success_rate"] == 50.0


def test_process_multiple_projects(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [
        ("proj-a", "SUCCESS", 60000, 1),
        ("proj-b", "FAILURE", 30000, 2),
        ("proj-b", "SUCCESS", 30000, 3),
    ]
    resource_mock = run_process(retriever, make_df(rows))
    records = resource_mock.update_datastore.call_args[0][0]
    assert len(records) == 2
    names = {r["projectName"] for r in records}
    assert names == {"proj-a", "proj-b"}


def test_process_filters_old_rows(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [
        ("proj-a", "SUCCESS", 60000, 5),  # within last month
        ("proj-a", "SUCCESS", 60000, 40),  # older than a month
    ]
    resource_mock = run_process(retriever, make_df(rows))
    records = resource_mock.update_datastore.call_args[0][0]
    assert len(records) == 1
    assert records[0]["num_runs"] == 1


def test_process_avg_duration_seconds(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [
        ("proj-a", "SUCCESS", 60000, 1),
        ("proj-a", "SUCCESS", 120000, 2),
    ]
    resource_mock = run_process(retriever, make_df(rows))
    records = resource_mock.update_datastore.call_args[0][0]
    assert records[0]["avg_duration_seconds"] == 90.0


def test_process_uploads_dump(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [("proj-a", "SUCCESS", 60000, 1)]
    resource_mock = run_process(retriever, make_df(rows))
    assert retriever._downloader.download_file.call_count == 2
    resource_mock.set_file_to_upload.assert_called_once_with(
        Path("/tmp/jenkins_monthly_stats.csv")
    )
    resource_mock.update_in_hdx.assert_called_once()


def test_process_schema_and_pk(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [("proj-a", "SUCCESS", 60000, 1)]
    resource_mock = run_process(retriever, make_df(rows))
    create_args = resource_mock.create_datastore.call_args
    schema = create_args[0][0]
    pk = create_args[0][1]
    assert pk == ("projectName",)
    field_ids = [f["id"] for f in schema]
    assert field_ids == [
        "projectName",
        "num_runs",
        "num_successful",
        "num_failed",
        "num_aborted",
        "success_rate",
        "avg_duration_seconds",
    ]


def _make_drive_service(existing_files):
    service_mock = MagicMock()
    service_mock.files.return_value.list.return_value.execute.return_value = {
        "files": existing_files
    }
    return service_mock


def _run_upload_to_drive(retriever, service_mock):
    with (
        patch(
            "hdx.scraper.jenkinsstats.jenkins_stats_retriever.getenv",
            return_value='{"type": "service_account"}',
        ),
        patch(
            "hdx.scraper.jenkinsstats.jenkins_stats_retriever.Credentials.from_service_account_info"
        ),
        patch(
            "hdx.scraper.jenkinsstats.jenkins_stats_retriever.build",
            return_value=service_mock,
        ),
        patch("hdx.scraper.jenkinsstats.jenkins_stats_retriever.MediaFileUpload"),
    ):
        retriever._upload_to_drive(Path("/tmp/test.csv"))


def test_upload_to_drive_creates_new_file(sample_configuration):
    retriever = make_retriever(sample_configuration)
    service_mock = _make_drive_service([])
    _run_upload_to_drive(retriever, service_mock)
    service_mock.files.return_value.create.assert_called_once()
    service_mock.files.return_value.update.assert_not_called()


def test_upload_to_drive_overwrites_existing_file(sample_configuration):
    retriever = make_retriever(sample_configuration)
    service_mock = _make_drive_service([{"id": "existing-file-id"}])
    _run_upload_to_drive(retriever, service_mock)
    service_mock.files.return_value.update.assert_called_once()
    service_mock.files.return_value.create.assert_not_called()
