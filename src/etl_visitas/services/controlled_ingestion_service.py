from datetime import date, datetime, timezone
from uuid import uuid4

from etl_visitas.config.settings import AppSettings
from etl_visitas.ingestion.file_discovery import (
    filter_candidate_files,
)
from etl_visitas.ingestion.sftp_client import SFTPClient
from etl_visitas.models.audit_models import RunContext
from etl_visitas.models.enums import (
    ErrorCode,
    FileStatus,
    RunStatus,
)
from etl_visitas.repositories.audit_repository import (
    AuditRepository,
)
from etl_visitas.repositories.business_load_repository import (
    BusinessLoadRepository,
)
from etl_visitas.repositories.mysql_connection import (
    MySQLConnectionFactory,
)
from etl_visitas.services.backup_service import (
    BackupService,
)
from etl_visitas.services.file_processing_service import (
    FileProcessingService,
)
from etl_visitas.services.load_service import (
    LoadService,
)


class ControlledIngestionService:

    def __init__(
        self,
        settings: AppSettings,
    ) -> None:
        self._settings = settings

        self._connection_factory = (
            MySQLConnectionFactory(
                settings.mysql
            )
        )

        self._audit_repository = (
            AuditRepository(
                self._connection_factory
            )
        )

        self._file_processor = (
            FileProcessingService(
                settings
            )
        )

        self._load_service = LoadService(
            connection_factory=(
                self._connection_factory
            ),
            repository=(
                BusinessLoadRepository()
            ),
        )

        self._backup_service = BackupService(
            settings.storage
        )

    def run(
        self,
        run_id: str | None = None,
        dag_id: str = "etl_visitas_daily",
        reference_date: date | None = None,
    ) -> str:

        actual_run_id = (
            run_id
            if run_id is not None
            else f"manual_{uuid4()}"
        )

        now = datetime.now(
            timezone.utc
        ).replace(tzinfo=None)

        effective_reference_date = (
            reference_date
            if reference_date is not None
            else date.today()
        )

        run_context = RunContext(
            run_id=actual_run_id,
            dag_id=dag_id,
            execution_date=now,
            start_time=now,
        )

        self._audit_repository.start_run(
            run_context
        )

        run_status = RunStatus.SUCCESS

        try:
            with SFTPClient(
                self._settings.sftp
            ) as sftp_client:

                remote_files = (
                    sftp_client.list_files()
                )

                candidates = (
                    filter_candidate_files(
                        remote_files
                    )
                )

                for remote_file in candidates:
                    file_id = (
                        self._audit_repository
                        .register_file(
                            run_id=actual_run_id,
                            remote_file=remote_file,
                        )
                    )

                    try:
                        result = (
                            self._file_processor
                            .process(
                                sftp_client=sftp_client,
                                remote_file=remote_file,
                            )
                        )

                        self._audit_repository.update_file_staged(
                            file_id=file_id,
                            staged_file=result.staged_file,
                        )

                        existing_file = (
                            self._audit_repository
                            .get_successful_file_by_checksum(
                                result
                                .staged_file
                                .checksum_sha256
                            )
                        )

                        if (
                            existing_file is not None
                            and existing_file.file_id
                            != file_id
                        ):
                            self._audit_repository.update_file_status(
                                file_id=file_id,
                                status=(
                                    FileStatus
                                    .SKIPPED_ALREADY_PROCESSED
                                ),
                                error_code=(
                                    ErrorCode
                                    .ALREADY_PROCESSED
                                    .value
                                ),
                                error_message=(
                                    "File content was already "
                                    "processed successfully. "
                                    f"Original file_id="
                                    f"{existing_file.file_id}, "
                                    f"original run_id="
                                    f"{existing_file.run_id}"
                                ),
                            )

                            continue

                        if (
                            result.status
                            == FileStatus.REJECTED_LAYOUT
                        ):
                            error_code = (
                                result
                                .file_validation
                                .error_code
                                .value
                                if (
                                    result
                                    .file_validation
                                    .error_code
                                    is not None
                                )
                                else None
                            )

                            self._audit_repository.update_file_status(
                                file_id=file_id,
                                status=(
                                    FileStatus.REJECTED_LAYOUT
                                ),
                                error_code=error_code,
                                error_message=(
                                    result
                                    .file_validation
                                    .error_message
                                ),
                            )

                            if (
                                run_status
                                != RunStatus.FAILED
                            ):
                                run_status = (
                                    RunStatus
                                    .PARTIAL_SUCCESS
                                )

                            continue

                        if result.records is None:
                            raise RuntimeError(
                                "Prepared file has no "
                                "record-processing result. "
                                f"File="
                                f"{remote_file.file_name}"
                            )

                        records_read = (
                            result.records.records_read
                        )

                        records_valid = (
                            result.records.records_valid
                        )

                        records_invalid = (
                            result.records.records_invalid
                        )

                        if (
                            records_read
                            != records_valid
                            + records_invalid
                        ):
                            raise RuntimeError(
                                "Record reconciliation failed. "
                                f"File="
                                f"{remote_file.file_name}, "
                                f"read={records_read}, "
                                f"valid={records_valid}, "
                                f"invalid={records_invalid}"
                            )

                        self._audit_repository.update_file_metrics(
                            file_id=file_id,
                            records_read=records_read,
                            records_valid=records_valid,
                            records_invalid=records_invalid,
                        )

                        self._audit_repository.update_file_status(
                            file_id=file_id,
                            status=FileStatus.PREPARED,
                        )

                        self._audit_repository.update_file_status(
                            file_id=file_id,
                            status=FileStatus.LOADING,
                        )

                        self._load_service.load_file(
                            file_id=file_id,
                            run_id=actual_run_id,
                            file_name=(
                                remote_file.file_name
                            ),
                            records=result.records,
                            reference_date=(
                                effective_reference_date
                            ),
                        )

                        try:
                            backup_file = (
                                self._backup_service
                                .create_backup(
                                    staged_file=(
                                        result.staged_file
                                    ),
                                    run_id=(
                                        actual_run_id
                                    ),
                                )
                            )

                        except Exception as error:
                            self._audit_repository.update_file_status(
                                file_id=file_id,
                                status=(
                                    FileStatus
                                    .PENDING_BACKUP
                                ),
                                error_code=(
                                    ErrorCode
                                    .BACKUP_ERROR
                                    .value
                                ),
                                error_message=str(error),
                            )

                            run_status = RunStatus.FAILED

                            continue

                        self._audit_repository.update_file_backed_up(
                            file_id=file_id,
                            backup_path=(
                                backup_file.backup_path
                            ),
                        )

                        try:
                            sftp_client.delete(
                                remote_file.remote_path
                            )

                        except Exception as error:
                            self._audit_repository.update_file_status(
                                file_id=file_id,
                                status=(
                                    FileStatus
                                    .PENDING_SOURCE_DELETE
                                ),
                                error_code=(
                                    ErrorCode
                                    .SFTP_DELETE_ERROR
                                    .value
                                ),
                                error_message=str(error),
                            )

                            run_status = RunStatus.FAILED

                            continue

                        self._audit_repository.mark_source_deleted(
                            file_id=file_id
                        )

                        if (
                            records_invalid > 0
                            and run_status
                            != RunStatus.FAILED
                        ):
                            run_status = (
                                RunStatus.PARTIAL_SUCCESS
                            )

                    except Exception as error:
                        self._audit_repository.update_file_status(
                            file_id=file_id,
                            status=FileStatus.FAILED,
                            error_code=(
                                error
                                .__class__
                                .__name__
                            ),
                            error_message=str(error),
                        )

                        run_status = RunStatus.FAILED

            self._audit_repository.update_run_metrics(
                actual_run_id
            )

            self._audit_repository.finish_run(
                run_id=actual_run_id,
                status=run_status,
            )

            return actual_run_id

        except Exception:
            try:
                self._audit_repository.update_run_metrics(
                    actual_run_id
                )
            finally:
                self._audit_repository.finish_run(
                    run_id=actual_run_id,
                    status=RunStatus.FAILED,
                )

            raise