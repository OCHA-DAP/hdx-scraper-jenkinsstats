from datetime import UTC, datetime
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


def make_df(rows, base_date=None):
    """rows: list of (projectName, result, buildDuration, days_ago)"""
    base = base_date if base_date is not None else pd.Timestamp.now(tz="UTC")
    data = [
        {
            "projectName": project,
            "result": result,
            "buildDuration": duration,
            "buildTimestamp": (base - pd.Timedelta(days=days_ago)).isoformat(),
        }
        for project, result, duration, days_ago in rows
    ]
    return pd.DataFrame(data)


def run_process(
    retriever, df, start_from=None, today=datetime(2026, 5, 31, tzinfo=UTC)
):
    builds_resource_mock = MagicMock()
    builds_resource_mock.__getitem__ = MagicMock(
        side_effect=lambda key: "builds-resource-id" if key == "id" else None
    )
    builds_dataset_mock = MagicMock()
    builds_dataset_mock.get_resource.return_value = builds_resource_mock

    fivebuilds_resource_mock = MagicMock()
    fivebuilds_resource_mock.__getitem__ = MagicMock(
        side_effect=lambda key: "last5runs-resource-id" if key == "id" else None
    )
    stats_resource_mock = MagicMock()
    stats_resource_mock.__getitem__ = MagicMock(
        side_effect=lambda key: "stats-resource-id" if key == "id" else None
    )
    quarterly_resource_mock = MagicMock()
    quarterly_resource_mock.__getitem__ = MagicMock(
        side_effect=lambda key: "quarterly-resource-id" if key == "id" else None
    )
    stats_dataset_mock = MagicMock()
    stats_dataset_mock.get_resource.side_effect = (
        lambda *args: quarterly_resource_mock
        if args and args[0] == 2
        else (
            stats_resource_mock if args and args[0] == 1 else fivebuilds_resource_mock
        )
    )

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
        retriever.process(today, start_from)

    return fivebuilds_resource_mock, stats_resource_mock, quarterly_resource_mock


def test_process_empty_dataframe(sample_configuration):
    retriever = make_retriever(sample_configuration)
    _, resource_mock, _ = run_process(retriever, make_df([]))
    resource_mock.update_datastore.assert_called_once_with([])


def test_process_single_project_success(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [
        ("proj-a", "SUCCESS", 60, 1),
        ("proj-a", "SUCCESS", 60, 2),
        ("proj-a", "SUCCESS", 60, 3),
    ]
    _, resource_mock, _ = run_process(retriever, make_df(rows))
    records = resource_mock.update_datastore.call_args[0][0]
    assert len(records) == 2
    r = next(r for r in records if r["projectName"] == "proj-a")
    assert r["num_runs"] == 3
    assert r["num_successful"] == 3
    assert r["num_failed"] == 0
    assert r["num_aborted"] == 0
    assert r["success_rate"] == 100.0
    assert r["failure_rate"] == 0.0
    assert r["abort_rate"] == 0.0


def test_process_mixed_results(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [
        ("proj-a", "SUCCESS", 60, 1),
        ("proj-a", "SUCCESS", 60, 2),
        ("proj-a", "FAILURE", 30, 3),
        ("proj-a", "ABORTED", 10, 4),
    ]
    _, resource_mock, _ = run_process(retriever, make_df(rows))
    records = resource_mock.update_datastore.call_args[0][0]
    assert len(records) == 2
    r = next(r for r in records if r["projectName"] == "proj-a")
    assert r["num_runs"] == 4
    assert r["num_successful"] == 2
    assert r["num_failed"] == 1
    assert r["num_aborted"] == 1
    assert r["success_rate"] == 50.0
    assert r["failure_rate"] == 25.0
    assert r["abort_rate"] == 25.0


def test_process_multiple_projects(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [
        ("proj-a", "SUCCESS", 60, 1),
        ("proj-b", "FAILURE", 30, 2),
        ("proj-b", "SUCCESS", 30, 3),
    ]
    _, resource_mock, _ = run_process(retriever, make_df(rows))
    records = resource_mock.update_datastore.call_args[0][0]
    assert len(records) == 3
    names = {r["projectName"] for r in records}
    assert names == {"proj-a", "proj-b", "TOTAL"}


def test_process_total_row(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [
        ("proj-a", "SUCCESS", 60, 1),
        ("proj-a", "FAILURE", 30, 2),
        ("proj-b", "SUCCESS", 120, 3),
    ]
    _, resource_mock, _ = run_process(retriever, make_df(rows))
    records = resource_mock.update_datastore.call_args[0][0]
    total = next(r for r in records if r["projectName"] == "TOTAL")
    assert total["num_runs"] == 3
    assert total["num_successful"] == 2
    assert total["num_failed"] == 1
    assert total["num_aborted"] == 0
    assert total["success_rate"] == round(2 / 3 * 100, 2)
    assert total["failure_rate"] == round(1 / 3 * 100, 2)
    assert total["abort_rate"] == 0.0
    assert total["avg_duration"] == 90.0
    assert total["stddev_duration"] == 42.43


def test_process_build_date(sample_configuration):
    retriever = make_retriever(sample_configuration)
    today = datetime(2026, 5, 31, tzinfo=UTC)
    base = pd.Timestamp("2026-05-31", tz="UTC")
    rows = [
        ("proj-a", "SUCCESS", 60, 3),  # 2026-05-28
        ("proj-a", "SUCCESS", 60, 5),  # 2026-05-26
        ("proj-b", "SUCCESS", 60, 1),  # 2026-05-30
    ]
    _, resource_mock, _ = run_process(
        retriever, make_df(rows, base_date=base), today=today
    )
    records = resource_mock.update_datastore.call_args[0][0]
    proj_a = next(r for r in records if r["projectName"] == "proj-a")
    assert proj_a["stats_date"] == "2026-05-31"
    assert proj_a["build_date"] == "2026-05-28"  # most recent of proj-a's builds
    proj_b = next(r for r in records if r["projectName"] == "proj-b")
    assert proj_b["build_date"] == "2026-05-30"
    total = next(r for r in records if r["projectName"] == "TOTAL")
    assert total["build_date"] == "2026-05-30"  # most recent across all scrapers


def test_process_filters_old_rows(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [
        ("proj-a", "SUCCESS", 60, 5),  # within last month
        ("proj-a", "SUCCESS", 60, 40),  # older than a month
    ]
    _, resource_mock, _ = run_process(retriever, make_df(rows))
    records = resource_mock.update_datastore.call_args[0][0]
    assert len(records) == 2
    project_record = next(r for r in records if r["projectName"] == "proj-a")
    assert project_record["num_runs"] == 1


def test_process_backpopulate(sample_configuration):
    retriever = make_retriever(sample_configuration)
    # Build on 2026-04-30 falls in both monthly windows: Apr30 [03-30,05-01) and May31 [04-30,06-01)
    base = pd.Timestamp("2026-05-31", tz="UTC")
    rows = [("proj-a", "SUCCESS", 60, 31)]  # 2026-04-30
    _, resource_mock, _ = run_process(
        retriever,
        make_df(rows, base_date=base),
        start_from=datetime(2026, 4, 29, tzinfo=UTC),
        today=datetime(2026, 5, 31, tzinfo=UTC),
    )
    records = resource_mock.update_datastore.call_args[0][0]
    assert len(records) == 4  # proj-a + TOTAL for each of 2 month-ends
    assert {r["stats_date"] for r in records} == {"2026-04-30", "2026-05-31"}
    resource_mock.delete_datastore.assert_called_once()


def test_process_no_delete_datastore_without_start_from(sample_configuration):
    retriever = make_retriever(sample_configuration)
    _, resource_mock, _ = run_process(
        retriever, make_df([("proj-a", "SUCCESS", 60, 1)])
    )
    resource_mock.delete_datastore.assert_not_called()


def test_process_avg_duration(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [
        ("proj-a", "SUCCESS", 60, 1),
        ("proj-a", "SUCCESS", 120, 2),
    ]
    _, resource_mock, _ = run_process(retriever, make_df(rows))
    records = resource_mock.update_datastore.call_args[0][0]
    proj_a = next(r for r in records if r["projectName"] == "proj-a")
    assert proj_a["avg_duration"] == 90.0
    assert proj_a["stddev_duration"] == 42.43


def test_process_uploads_dump(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [("proj-a", "SUCCESS", 60, 1)]
    _, resource_mock, _ = run_process(retriever, make_df(rows))
    assert retriever._downloader.download_file.call_count == 3
    resource_mock.set_file_to_upload.assert_called_once_with(
        Path("/tmp/jenkins_monthly_stats.csv")
    )
    resource_mock.update_in_hdx.assert_called_once()


def test_process_schema_and_pk(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [("proj-a", "SUCCESS", 60, 1)]
    _, resource_mock, _ = run_process(retriever, make_df(rows))
    create_args = resource_mock.create_datastore.call_args
    schema = create_args[0][0]
    pk = create_args[0][1]
    assert pk == ("stats_date", "projectName")
    field_ids = [f["id"] for f in schema]
    assert field_ids == [
        "stats_date",
        "projectName",
        "build_date",
        "num_runs",
        "num_successful",
        "num_failed",
        "num_aborted",
        "success_rate",
        "failure_rate",
        "abort_rate",
        "avg_duration",
        "stddev_duration",
    ]


def test_process_monthly_on_month_end(sample_configuration):
    retriever = make_retriever(sample_configuration)
    _, resource_mock, _ = run_process(
        retriever,
        make_df([("proj-a", "SUCCESS", 60, 1)]),
        today=datetime(2026, 5, 31, tzinfo=UTC),
    )
    resource_mock.create_datastore.assert_called_once()


def test_process_no_monthly_on_non_month_end(sample_configuration):
    retriever = make_retriever(sample_configuration)
    _, resource_mock, _ = run_process(
        retriever,
        make_df([("proj-a", "SUCCESS", 60, 1)]),
        today=datetime(2026, 5, 20, tzinfo=UTC),
    )
    resource_mock.create_datastore.assert_not_called()
    resource_mock.update_datastore.assert_not_called()


def test_process_quarterly_on_quarter_end(sample_configuration):
    retriever = make_retriever(sample_configuration)
    q1_end = datetime(2026, 3, 31, tzinfo=UTC)
    base = pd.Timestamp("2026-03-31", tz="UTC")
    rows = [("proj-a", "SUCCESS", 60, 1)]  # March 30 — within Q1
    _, _, quarterly_mock = run_process(
        retriever, make_df(rows, base_date=base), today=q1_end
    )
    quarterly_mock.create_datastore.assert_called_once()
    records = quarterly_mock.update_datastore.call_args[0][0]
    assert len(records) == 2
    assert all(r["stats_date"] == "2026-03-31" for r in records)


def test_process_no_quarterly_on_non_quarter_end(sample_configuration):
    retriever = make_retriever(sample_configuration)
    _, _, quarterly_mock = run_process(
        retriever, make_df([("proj-a", "SUCCESS", 60, 1)])
    )
    quarterly_mock.create_datastore.assert_not_called()
    quarterly_mock.update_datastore.assert_not_called()


def test_process_quarterly_window(sample_configuration):
    # Q1 window is Jan 1 – Mar 31; builds before Jan 1 should be excluded
    retriever = make_retriever(sample_configuration)
    q1_end = datetime(2026, 3, 31, tzinfo=UTC)
    base = pd.Timestamp("2026-03-31", tz="UTC")
    rows = [
        ("proj-a", "SUCCESS", 60, 1),  # 2026-03-30 — in Q1
        ("proj-a", "SUCCESS", 60, 89),  # 2026-01-01 — in Q1 (boundary)
        ("proj-a", "FAILURE", 60, 91),  # 2025-12-30 — before Q1
    ]
    _, _, quarterly_mock = run_process(
        retriever, make_df(rows, base_date=base), today=q1_end
    )
    records = quarterly_mock.update_datastore.call_args[0][0]
    project_record = next(r for r in records if r["projectName"] == "proj-a")
    assert project_record["num_runs"] == 2
    assert project_record["num_successful"] == 2


def test_process_backpopulate_quarterly(sample_configuration):
    # Backpopulation spanning a quarter-end should populate the quarterly resource
    retriever = make_retriever(sample_configuration)
    base = pd.Timestamp("2026-03-31", tz="UTC")
    rows = [("proj-a", "SUCCESS", 60, 1)]  # 2026-03-30 — in Q1
    _, _, quarterly_mock = run_process(
        retriever,
        make_df(rows, base_date=base),
        start_from=datetime(2026, 3, 30, tzinfo=UTC),
        today=datetime(2026, 4, 2, tzinfo=UTC),
    )
    records = quarterly_mock.update_datastore.call_args[0][0]
    assert len(records) == 2
    assert all(r["stats_date"] == "2026-03-31" for r in records)
    quarterly_mock.delete_datastore.assert_called_once()


def test_process_backpopulate_no_quarterly_without_quarter_end(sample_configuration):
    # Backpopulation with no quarter-end in range should not touch quarterly resource
    retriever = make_retriever(sample_configuration)
    _, _, quarterly_mock = run_process(
        retriever,
        make_df([("proj-a", "SUCCESS", 60, 1)]),
        start_from=datetime(2026, 5, 18, tzinfo=UTC),
    )
    quarterly_mock.create_datastore.assert_not_called()
    quarterly_mock.delete_datastore.assert_not_called()


def test_process_fivebuilds_empty(sample_configuration):
    retriever = make_retriever(sample_configuration)
    fivebuilds_mock, _, _ = run_process(retriever, make_df([]))
    fivebuilds_mock.update_datastore.assert_called_once_with([])


def test_process_fivebuilds_takes_last_5(sample_configuration):
    retriever = make_retriever(sample_configuration)
    # 10 builds for proj-a; only the 5 most recent should count
    rows = [("proj-a", "SUCCESS", 60, i) for i in range(1, 11)]
    fivebuilds_mock, _, _ = run_process(retriever, make_df(rows))
    records = fivebuilds_mock.update_datastore.call_args[0][0]
    proj = next(r for r in records if r["projectName"] == "proj-a")
    assert proj["num_runs"] == 5


def test_process_fivebuilds_fewer_than_5(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [("proj-a", "SUCCESS", 60, i) for i in range(1, 4)]  # 3 builds
    fivebuilds_mock, _, _ = run_process(retriever, make_df(rows))
    records = fivebuilds_mock.update_datastore.call_args[0][0]
    proj = next(r for r in records if r["projectName"] == "proj-a")
    assert proj["num_runs"] == 3


def test_process_fivebuilds_total_row(sample_configuration):
    retriever = make_retriever(sample_configuration)
    rows = [
        ("proj-a", "SUCCESS", 60, 1),
        ("proj-a", "FAILURE", 30, 2),
        ("proj-b", "SUCCESS", 90, 1),
        ("proj-b", "SUCCESS", 90, 2),
        ("proj-b", "ABORTED", 90, 3),
    ]
    fivebuilds_mock, _, _ = run_process(retriever, make_df(rows))
    records = fivebuilds_mock.update_datastore.call_args[0][0]
    total = next(r for r in records if r["projectName"] == "TOTAL")
    assert total["num_runs"] == 5
    assert total["num_successful"] == 3
    assert total["num_failed"] == 1
    assert total["num_aborted"] == 1
    assert total["success_rate"] == 60.0
    assert total["failure_rate"] == 20.0
    assert total["abort_rate"] == 20.0


def test_process_fivebuilds_historic(sample_configuration):
    # On 2026-05-19, only the build from that day is visible (last 5 as of that date).
    # On 2026-05-20, both builds are visible.
    retriever = make_retriever(sample_configuration)
    base = pd.Timestamp("2026-05-20", tz="UTC")
    rows = [
        ("proj-a", "SUCCESS", 60, 1),  # 2026-05-19
        ("proj-a", "SUCCESS", 60, 0),  # 2026-05-20
    ]
    fivebuilds_mock, _, _ = run_process(
        retriever,
        make_df(rows, base_date=base),
        start_from=datetime(2026, 5, 19, tzinfo=UTC),
    )
    records = fivebuilds_mock.update_datastore.call_args[0][0]
    may19 = next(
        r
        for r in records
        if r["stats_date"] == "2026-05-19" and r["projectName"] == "proj-a"
    )
    may20 = next(
        r
        for r in records
        if r["stats_date"] == "2026-05-20" and r["projectName"] == "proj-a"
    )
    assert may19["num_runs"] == 1
    assert may20["num_runs"] == 2


def test_process_fivebuilds_deletes_on_start_from(sample_configuration):
    retriever = make_retriever(sample_configuration)
    fivebuilds_mock, _, _ = run_process(
        retriever,
        make_df([("proj-a", "SUCCESS", 60, 1)]),
        start_from=datetime(2026, 5, 19, tzinfo=UTC),
    )
    fivebuilds_mock.delete_datastore.assert_called_once()


def test_process_fivebuilds_no_delete_without_start_from(sample_configuration):
    retriever = make_retriever(sample_configuration)
    fivebuilds_mock, _, _ = run_process(
        retriever, make_df([("proj-a", "SUCCESS", 60, 1)])
    )
    fivebuilds_mock.delete_datastore.assert_not_called()


def test_process_fivebuilds_pk(sample_configuration):
    retriever = make_retriever(sample_configuration)
    fivebuilds_mock, _, _ = run_process(
        retriever, make_df([("proj-a", "SUCCESS", 60, 1)])
    )
    pk = fivebuilds_mock.create_datastore.call_args[0][1]
    assert pk == ("stats_date", "projectName")


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
