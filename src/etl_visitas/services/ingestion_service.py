from etl_visitas.config.settings import AppSettings
from etl_visitas.ingestion.file_discovery import (
    filter_candidate_files,
)
from etl_visitas.ingestion.sftp_client import SFTPClient
from etl_visitas.models.processing_models import (
    FileProcessingResult,
)
from etl_visitas.services.file_processing_service import (
    FileProcessingService,
)


class IngestionService:

    def __init__(
        self,
        settings: AppSettings,
    ):
        self._settings = settings
        self._file_processor = FileProcessingService(
            settings
        )

    def run(self) -> list[FileProcessingResult]:

        results: list[FileProcessingResult] = []

        with SFTPClient(
            self._settings.sftp
        ) as sftp_client:

            remote_files = sftp_client.list_files()

            candidates = filter_candidate_files(
                remote_files
            )

            for remote_file in candidates:
                result = self._file_processor.process(
                    sftp_client=sftp_client,
                    remote_file=remote_file,
                )

                results.append(result)

        return results