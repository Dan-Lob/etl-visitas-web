from pathlib import Path

import pytest

from etl_visitas.config.settings import StorageSettings
from etl_visitas.ingestion.staging_service import (
    StagingError,
    StagingService,
)
from etl_visitas.models.file_models import (
    RemoteFileMetadata,
)


class FakeSFTPClient:

    def __init__(self, content: bytes):
        self._content = content

    def download(
        self,
        remote_path: str,
        local_path: str,
    ) -> None:
        Path(local_path).write_bytes(
            self._content
        )


def test_stage_file_downloads_and_validates_size(
    tmp_path,
):
    content = b"email,jyv\nuser@example.com,\n"

    remote_file = RemoteFileMetadata(
        file_name="report_100.txt",
        remote_path="archivosVisitas/report_100.txt",
        size_bytes=len(content),
    )

    settings = StorageSettings(
        staging_path=str(tmp_path)
    )

    service = StagingService(settings)

    staged = service.stage_file(
        FakeSFTPClient(content),
        remote_file,
    )

    assert staged.file_name == "report_100.txt"
    assert staged.remote_size_bytes == len(content)
    assert staged.local_size_bytes == len(content)
    assert len(staged.checksum_sha256) == 64
    assert Path(staged.local_path).exists()


def test_stage_file_rejects_size_mismatch(
    tmp_path,
):
    expected_content = b"1234567890"
    downloaded_content = b"123"

    remote_file = RemoteFileMetadata(
        file_name="report_100.txt",
        remote_path="archivosVisitas/report_100.txt",
        size_bytes=len(expected_content),
    )

    settings = StorageSettings(
        staging_path=str(tmp_path)
    )

    service = StagingService(settings)

    with pytest.raises(
        StagingError,
        match="File size mismatch",
    ):
        service.stage_file(
            FakeSFTPClient(downloaded_content),
            remote_file,
        )