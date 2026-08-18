import hashlib
from pathlib import Path
from zipfile import (
    ZIP_DEFLATED,
    BadZipFile,
    ZipFile,
)

from etl_visitas.config.settings import (
    StorageSettings,
)
from etl_visitas.ingestion.checksum import (
    calculate_sha256,
)
from etl_visitas.models.file_models import (
    BackupFile,
    StagedFile,
)


class BackupError(Exception):
    pass


class BackupService:

    def __init__(
        self,
        storage_settings: StorageSettings,
    ) -> None:
        self._storage_settings = (
            storage_settings
        )

    def create_backup(
        self,
        staged_file: StagedFile,
        run_id: str,
    ) -> BackupFile:

        backup_root = Path(
            self._storage_settings.backup_path
        )

        backup_root.mkdir(
            parents=True,
            exist_ok=True,
        )

        source_path = Path(
            staged_file.local_path
        )

        if not source_path.exists():
            raise BackupError(
                "Source file does not exist "
                "for backup. "
                f"Path={source_path}"
            )

        zip_name = (
            f"{source_path.stem}_"
            f"{run_id}.zip"
        )

        backup_path = (
            backup_root
            / zip_name
        )

        if backup_path.exists():
            backup_path.unlink()

        try:
            with ZipFile(
                backup_path,
                mode="w",
                compression=ZIP_DEFLATED,
            ) as zip_file:

                zip_file.write(
                    source_path,
                    arcname=(
                        staged_file.file_name
                    ),
                )

        except Exception as error:
            backup_path.unlink(
                missing_ok=True
            )

            raise BackupError(
                "Backup creation failed. "
                f"File="
                f"{staged_file.file_name}"
            ) from error

        self._validate_backup(
            backup_path=backup_path,
            expected_file_name=(
                staged_file.file_name
            ),
            expected_source_checksum=(
                staged_file.checksum_sha256
            ),
        )

        backup_size = (
            backup_path.stat().st_size
        )

        backup_checksum = (
            calculate_sha256(
                str(backup_path)
            )
        )

        return BackupFile(
            source_file_name=(
                staged_file.file_name
            ),
            backup_path=str(
                backup_path
            ),
            size_bytes=backup_size,
            checksum_sha256=(
                backup_checksum
            ),
        )

    def _validate_backup(
        self,
        backup_path: Path,
        expected_file_name: str,
        expected_source_checksum: str,
    ) -> None:

        if not backup_path.exists():
            raise BackupError(
                "Backup file was not created"
            )

        if backup_path.stat().st_size == 0:
            raise BackupError(
                "Backup file is empty"
            )

        try:
            with ZipFile(
                backup_path,
                mode="r",
            ) as zip_file:

                bad_file = (
                    zip_file.testzip()
                )

                if bad_file is not None:
                    raise BackupError(
                        "Corrupted file found "
                        f"inside ZIP: {bad_file}"
                    )

                names = (
                    zip_file.namelist()
                )

                if names != [
                    expected_file_name
                ]:
                    raise BackupError(
                        "Unexpected ZIP content. "
                        f"Expected="
                        f"{expected_file_name}, "
                        f"found={names}"
                    )

                extracted_content = (
                    zip_file.read(
                        expected_file_name
                    )
                )

        except BadZipFile as error:
            raise BackupError(
                "Generated backup is not "
                "a valid ZIP file"
            ) from error

        extracted_checksum = (
            hashlib.sha256(
                extracted_content
            ).hexdigest()
        )

        if (
            extracted_checksum
            != expected_source_checksum
        ):
            raise BackupError(
                "Backup content checksum "
                "does not match source file. "
                f"Expected="
                f"{expected_source_checksum}, "
                f"actual="
                f"{extracted_checksum}"
            )