from pathlib import Path

from etl_visitas.config.settings import StorageSettings
from etl_visitas.ingestion.checksum import calculate_sha256
from etl_visitas.ingestion.sftp_client import SFTPClient
from etl_visitas.models.file_models import (
    RemoteFileMetadata,
    StagedFile,
)


class StagingError(Exception):
    pass


class StagingService:

    def __init__(
        self,
        storage_settings: StorageSettings,
    ):
        self._storage_settings = storage_settings

    def stage_file(
        self,
        sftp_client: SFTPClient,
        remote_file: RemoteFileMetadata,
    ) -> StagedFile:

        staging_dir = Path(
            self._storage_settings.staging_path
        )

        staging_dir.mkdir(
            parents=True,
            exist_ok=True,
        )

        local_path = (
            staging_dir
            / remote_file.file_name
        )

        # Evitamos procesar restos parciales
        # de una descarga anterior.
        if local_path.exists():
            local_path.unlink()

        sftp_client.download(
            remote_path=remote_file.remote_path,
            local_path=str(local_path),
        )

        if not local_path.exists():
            raise StagingError(
                f"Downloaded file was not created: "
                f"{local_path}"
            )

        local_size = local_path.stat().st_size

        if local_size != remote_file.size_bytes:
            local_path.unlink(missing_ok=True)

            raise StagingError(
                "File size mismatch after download. "
                f"File={remote_file.file_name}, "
                f"remote={remote_file.size_bytes}, "
                f"local={local_size}"
            )

        checksum = calculate_sha256(
            str(local_path)
        )

        return StagedFile(
            file_name=remote_file.file_name,
            remote_path=remote_file.remote_path,
            local_path=str(local_path),
            remote_size_bytes=remote_file.size_bytes,
            local_size_bytes=local_size,
            checksum_sha256=checksum,
        )