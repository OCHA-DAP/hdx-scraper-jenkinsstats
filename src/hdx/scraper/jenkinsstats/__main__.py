#!/usr/bin/python
"""
Reads the jenkins-builds HDX datastore, computes monthly per-pipeline
statistics, writes them to the jenkins-stats HDX dataset, and uploads
the CSV to Google Drive.
"""

import logging
from os.path import expanduser, join
from pathlib import Path

from hdx.api.configuration import Configuration
from hdx.api.utilities.url_utils import get_ckan_ready_session
from hdx.data.user import User
from hdx.facades.simple import facade
from hdx.utilities.dateparse import now_utc
from hdx.utilities.downloader import Download
from hdx.utilities.path import script_dir_plus_file

from hdx.scraper.jenkinsstats import __version__
from hdx.scraper.jenkinsstats.jenkins_stats_retriever import JenkinsStatsRetriever

logger = logging.getLogger(__name__)

_LOOKUP = "hdx-scraper-jenkinsstats"


def main() -> None:
    logger.info(f"##### {_LOOKUP} version {__version__} ####")
    configuration = Configuration.read()
    today = now_utc()

    User.check_current_user_write_access("hdx")

    session = get_ckan_ready_session(configuration)
    with Download(session=session) as downloader:
        JenkinsStatsRetriever(configuration, downloader).process(today)


if __name__ == "__main__":
    facade(
        main,
        user_agent_config_yaml=Path(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup=_LOOKUP,
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
