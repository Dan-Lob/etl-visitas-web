from etl_visitas.config.settings import AppSettings
from etl_visitas.ingestion.sftp_client import SFTPClient
from etl_visitas.ingestion.staging_service import (
    StagingService,
)
from etl_visitas.models.enums import FileStatus
from etl_visitas.models.file_models import RemoteFileMetadata
from etl_visitas.models.processing_models import (
    FileProcessingResult,
)
from etl_visitas.validation.file_validator import (
    FileValidator,
)
from etl_visitas.validation.record_processor import (
    process_records,
)


class FileProcessingService:

    def __init__(
        self,
        settings: AppSettings,
    ):
        self._settings = settings
        self._staging_service = StagingService(
            settings.storage
        )
        self._file_validator = FileValidator()

    def process(
        self,
        sftp_client: SFTPClient,
        remote_file: RemoteFileMetadata,
    ) -> FileProcessingResult:

        staged_file = self._staging_service.stage_file(
            sftp_client=sftp_client,
            remote_file=remote_file,
        )

        file_validation = self._file_validator.validate(
            staged_file.local_path
        )

        if not file_validation.is_valid:
            return FileProcessingResult(
                staged_file=staged_file,
                status=FileStatus.REJECTED_LAYOUT,
                file_validation=file_validation,
                records=None,
            )

        records = process_records(
            file_path=staged_file.local_path,
            source_file=staged_file.file_name,
        )

        return FileProcessingResult(
            staged_file=staged_file,
            status=FileStatus.PREPARED,
            file_validation=file_validation,
            records=records,
        )