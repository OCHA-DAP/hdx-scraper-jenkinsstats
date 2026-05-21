import json
import logging
from datetime import timedelta
from os import getenv
from pathlib import Path

import pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from hdx.api.configuration import Configuration
from hdx.data.dataset import Dataset
from hdx.utilities.downloader import Download

logger = logging.getLogger(__name__)

_QUARTER_END_DAYS = {3: 31, 6: 30, 9: 30, 12: 31}


def _is_quarter_end(d) -> bool:
    return _QUARTER_END_DAYS.get(d.month) == d.day


def _is_month_end(d) -> bool:
    return (d + timedelta(days=1)).month != d.month


class JenkinsStatsRetriever:
    def __init__(self, configuration: Configuration, downloader: Download):
        self._configuration = configuration
        self._downloader = downloader
        self._drive_service = None

    def process(self, today, start_from=None) -> None:
        builds_dataset = Dataset.read_from_hdx(
            self._configuration["jenkins_builds_dataset"]
        )
        builds_resource = builds_dataset.get_resource()
        builds_dump_url = (
            f"{self._configuration.get_hdx_site_url()}"
            f"/datastore/dump/{builds_resource['id']}"
        )
        builds_file = self._downloader.download_file(builds_dump_url)

        df = pd.read_csv(builds_file)
        if not df.empty:
            df["buildTimestamp"] = pd.to_datetime(df["buildTimestamp"], utc=True)
            df = df.sort_values("buildTimestamp", ascending=False)

        dates = []
        if start_from is not None:
            current = start_from
            while current.date() <= today.date():
                dates.append(current)
                current += timedelta(days=1)
        else:
            dates = [today]

        fivebuilds_records = []
        for date_ts in dates:
            fivebuilds_records.extend(self._fivebuilds_stats(df, date_ts))

        schema = [
            {"id": "stats_date", "type": "date"},
            {"id": "projectName", "type": "text"},
            {"id": "build_date", "type": "date"},
            {"id": "num_runs", "type": "int4"},
            {"id": "num_successful", "type": "int4"},
            {"id": "num_failed", "type": "int4"},
            {"id": "num_aborted", "type": "int4"},
            {"id": "success_rate", "type": "float8"},
            {"id": "avg_duration", "type": "float8"},
        ]
        stats_dataset = Dataset.read_from_hdx(
            self._configuration["jenkins_stats_dataset"]
        )

        self._publish_resource(
            stats_dataset.get_resource(),
            schema,
            fivebuilds_records,
            "jenkins_fivebuilds_stats.csv",
            start_from,
        )

        monthly_dates = [d for d in dates if _is_month_end(d.date())]
        if monthly_dates:
            stats_records = []
            for date_ts in monthly_dates:
                stats_records.extend(self._stats_for_date(df, date_ts))
            self._publish_resource(
                stats_dataset.get_resource(1),
                schema,
                stats_records,
                "jenkins_monthly_stats.csv",
                start_from,
            )

        quarterly_dates = [d for d in dates if _is_quarter_end(d.date())]
        if quarterly_dates:
            quarterly_records = []
            for date_ts in quarterly_dates:
                quarterly_records.extend(self._quarterly_stats_for_date(df, date_ts))
            self._publish_resource(
                stats_dataset.get_resource(2),
                schema,
                quarterly_records,
                "jenkins_quarterly_stats.csv",
                start_from,
            )

    def _publish_resource(
        self, resource, schema, records, filename, start_from
    ) -> None:
        if start_from is not None:
            resource.delete_datastore()
        resource.create_datastore(schema, ("stats_date", "projectName"))
        resource.update_datastore(records)
        dump_url = (
            f"{self._configuration.get_hdx_site_url()}/datastore/dump/{resource['id']}"
        )
        file = self._downloader.download_file(dump_url)
        file = file.rename(file.parent / filename)
        resource.set_file_to_upload(file)
        resource.update_in_hdx()
        self._upload_to_drive(file)

    def _stats_for_date(self, df, date_ts) -> list:
        cutoff = pd.Timestamp(date_ts) - pd.DateOffset(months=1)
        upper = pd.Timestamp(date_ts) + pd.Timedelta(days=1)
        return self._compute_stats(df, date_ts, cutoff, upper)

    def _quarterly_stats_for_date(self, df, date_ts) -> list:
        quarter_start = date_ts.replace(
            month=date_ts.month - 2, day=1, hour=0, minute=0, second=0, microsecond=0
        )
        cutoff = pd.Timestamp(quarter_start)
        upper = pd.Timestamp(date_ts) + pd.Timedelta(days=1)
        return self._compute_stats(df, date_ts, cutoff, upper)

    def _compute_stats(self, df, date_ts, cutoff, upper) -> list:
        if df.empty:
            return []
        filtered = df[(df["buildTimestamp"] >= cutoff) & (df["buildTimestamp"] < upper)]
        date_str = date_ts.date().isoformat()
        records = [
            self._stats_record(date_str, project_name, group)
            for project_name, group in filtered.groupby("projectName")
        ]
        if records:
            self._append_total_row(records, date_str, filtered)
        return records

    def _fivebuilds_stats(self, df, date_ts) -> list:
        if df.empty:
            return []
        upper = pd.Timestamp(date_ts) + pd.Timedelta(days=1)
        eligible = df[df["buildTimestamp"] < upper]
        date_str = date_ts.date().isoformat()
        last5_df = eligible.groupby("projectName", group_keys=False).head(5)
        records = [
            self._stats_record(date_str, project_name, group)
            for project_name, group in last5_df.groupby("projectName")
        ]
        if records:
            self._append_total_row(records, date_str, last5_df)
        return records

    @staticmethod
    def _stats_record(date_str: str, project_name: str, group) -> dict:
        num_runs = len(group)
        num_successful = int((group["result"] == "SUCCESS").sum())
        num_failed = int((group["result"] == "FAILURE").sum())
        num_aborted = int((group["result"] == "ABORTED").sum())
        durations = group["buildDuration"].dropna().astype(float)
        return {
            "stats_date": date_str,
            "projectName": project_name,
            "build_date": group["buildTimestamp"].max().date().isoformat(),
            "num_runs": num_runs,
            "num_successful": num_successful,
            "num_failed": num_failed,
            "num_aborted": num_aborted,
            "success_rate": round(num_successful / num_runs * 100, 2),
            "avg_duration": round(durations.mean(), 2) if not durations.empty else 0.0,
        }

    @staticmethod
    def _append_total_row(records: list, date_str: str, all_builds_df) -> None:
        total_runs = sum(r["num_runs"] for r in records)
        total_successful = sum(r["num_successful"] for r in records)
        total_failed = sum(r["num_failed"] for r in records)
        total_aborted = sum(r["num_aborted"] for r in records)
        all_durations = all_builds_df["buildDuration"].dropna().astype(float)
        records.append(
            {
                "stats_date": date_str,
                "projectName": "TOTAL",
                "build_date": max(r["build_date"] for r in records),
                "num_runs": total_runs,
                "num_successful": total_successful,
                "num_failed": total_failed,
                "num_aborted": total_aborted,
                "success_rate": round(total_successful / total_runs * 100, 2),
                "avg_duration": round(all_durations.mean(), 2)
                if not all_durations.empty
                else 0.0,
            }
        )

    def _upload_to_drive(self, file: Path) -> None:
        if self._drive_service is None:
            credentials = Credentials.from_service_account_info(
                json.loads(getenv("GOOGLE_SERVICE_ACCOUNT")),
                scopes=["https://www.googleapis.com/auth/drive"],
            )
            self._drive_service = build("drive", "v3", credentials=credentials)
        service = self._drive_service
        folder_id = self._configuration["google_drive_folder_id"]
        media = MediaFileUpload(str(file), mimetype="text/csv")
        existing = (
            service.files()
            .list(
                q=f"name='{file.name}' and '{folder_id}' in parents and trashed=false",
                fields="files(id)",
                supportsAllDrives=True,
                includeItemsFromAllDrives=True,
            )
            .execute()
            .get("files", [])
        )
        if existing:
            service.files().update(
                fileId=existing[0]["id"],
                media_body=media,
                supportsAllDrives=True,
            ).execute()
        else:
            service.files().create(
                body={"name": file.name, "parents": [folder_id]},
                media_body=media,
                fields="id",
                supportsAllDrives=True,
            ).execute()
