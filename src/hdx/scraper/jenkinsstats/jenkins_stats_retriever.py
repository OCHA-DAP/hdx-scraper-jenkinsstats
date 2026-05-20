import json
import logging
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


class JenkinsStatsRetriever:
    def __init__(self, configuration: Configuration, downloader: Download):
        self._configuration = configuration
        self._downloader = downloader

    def process(self, today) -> None:
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
            cutoff = pd.Timestamp.now(tz="UTC") - pd.DateOffset(months=1)
            df = df[df["buildTimestamp"] >= cutoff]

        stats_records = []
        for project_name, group in df.groupby("projectName") if not df.empty else []:
            num_runs = len(group)
            num_successful = int((group["result"] == "SUCCESS").sum())
            num_failed = int((group["result"] == "FAILURE").sum())
            num_aborted = int((group["result"] == "ABORTED").sum())
            success_rate = round(num_successful / num_runs * 100, 2)
            durations = group["buildDuration"].dropna().astype(float)
            avg_duration_seconds = (
                round(durations.mean() / 1000, 2) if len(durations) > 0 else 0.0
            )
            stats_records.append(
                {
                    "date": today.date().isoformat(),
                    "projectName": project_name,
                    "num_runs": num_runs,
                    "num_successful": num_successful,
                    "num_failed": num_failed,
                    "num_aborted": num_aborted,
                    "success_rate": success_rate,
                    "avg_duration_seconds": avg_duration_seconds,
                }
            )

        schema = [
            {"id": "date", "type": "date"},
            {"id": "projectName", "type": "text"},
            {"id": "num_runs", "type": "int4"},
            {"id": "num_successful", "type": "int4"},
            {"id": "num_failed", "type": "int4"},
            {"id": "num_aborted", "type": "int4"},
            {"id": "success_rate", "type": "float8"},
            {"id": "avg_duration_seconds", "type": "float8"},
        ]
        stats_dataset = Dataset.read_from_hdx(
            self._configuration["jenkins_stats_dataset"]
        )
        stats_resource = stats_dataset.get_resource()
        stats_resource.delete_datastore()
        stats_resource.create_datastore(schema, ("date", "projectName"))
        stats_resource.update_datastore(stats_records)

        stats_dump_url = (
            f"{self._configuration.get_hdx_site_url()}"
            f"/datastore/dump/{stats_resource['id']}"
        )
        stats_file = self._downloader.download_file(stats_dump_url)
        stats_file = stats_file.rename(stats_file.parent / "jenkins_monthly_stats.csv")
        stats_resource.set_file_to_upload(stats_file)
        stats_resource.update_in_hdx()
        self._upload_to_drive(stats_file)

    def _upload_to_drive(self, file: Path) -> None:
        credentials = Credentials.from_service_account_info(
            json.loads(getenv("GOOGLE_SERVICE_ACCOUNT")),
            scopes=["https://www.googleapis.com/auth/drive"],
        )
        service = build("drive", "v3", credentials=credentials)
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
