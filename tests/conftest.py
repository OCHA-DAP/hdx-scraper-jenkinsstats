import pytest


@pytest.fixture
def sample_configuration():
    return {
        "jenkins_builds_dataset": "jenkins-builds",
        "jenkins_stats_dataset": "jenkins-stats",
        "google_drive_folder_id": "1x8-HeuhrwEWYeoCZdbCc7DKD-ONPSVHU",
    }
