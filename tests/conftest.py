from pathlib import Path
import shutil

import pytest


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

SAMPLE_DIR = (
    PROJECT_ROOT
    / "data"
    / "sample"
)

SFTP_DIR = (
    PROJECT_ROOT
    / "data"
    / "sftp"
)


def reset_sftp_test_data() -> None:
    SFTP_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    for item in SFTP_DIR.iterdir():
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            shutil.rmtree(item)

    for source_file in SAMPLE_DIR.glob(
        "report_*.txt"
    ):
        shutil.copy2(
            source_file,
            SFTP_DIR / source_file.name,
        )


@pytest.fixture(
    scope="session",
    autouse=True,
)
def restore_sftp_test_data():
    reset_sftp_test_data()

    yield

    reset_sftp_test_data()