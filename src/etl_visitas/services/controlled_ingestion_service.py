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
from etl_visitas.models.file_models import (
    RemoteFileMetadata,
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

        self._backup_service = (
            BackupService(
                settings.storage
            )
        )

    # ==================================================
    # RUN CONTROL
    # ==================================================

    def start_run(
        self,
        run_id: str | None = None,
        dag_id: str = "etl_visitas_daily",
    ) -> str:

        actual_run_id = (
            run_id
            if run_id is not None
            else f"manual_{uuid4()}"
        )

        now = datetime.now(
            timezone.utc
        ).replace(tzinfo=None)

        context = RunContext(
            run_id=actual_run_id,
            dag_id=dag_id,
            execution_date=now,
            start_time=now,
        )

        self._audit_repository.start_run(
            context
        )

        return actual_run_id

    # ==================================================
    # DISCOVERY
    # ==================================================

    def discover_files(
        self,
    ) -> list[dict]:

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

        # Plain dictionaries intentionally returned.
        # They can safely travel through Airflow XCom.
        return [
            {
                "file_name": (
                    file.file_name
                ),
                "remote_path": (
                    file.remote_path
                ),
                "size_bytes": (
                    file.size_bytes
                ),
            }
            for file in candidates
        ]

    # ==================================================
    # SINGLE FILE PROCESSING
    # ==================================================

    def process_single_file(
        self,
        run_id: str,
        file_data: dict,
        reference_date: date | None = None,
    ) -> str:

        effective_reference_date = (
            reference_date
            if reference_date is not None
            else date.today()
        )

        remote_file = RemoteFileMetadata(
            file_name=(
                file_data["file_name"]
            ),
            remote_path=(
                file_data["remote_path"]
            ),
            size_bytes=int(
                file_data["size_bytes"]
            ),
        )

        file_id = (
            self._audit_repository
            .register_file(
                run_id=run_id,
                remote_file=remote_file,
            )
        )

        try:
            with SFTPClient(
                self._settings.sftp
            ) as sftp_client:

                result = (
                    self._file_processor
                    .process(
                        sftp_client=sftp_client,
                        remote_file=remote_file,
                    )
                )

                # --------------------------------------
                # STAGED
                # --------------------------------------
                self._audit_repository.update_file_staged(
                    file_id=file_id,
                    staged_file=result.staged_file,
                )

                # --------------------------------------
                # IDEMPOTENCY
                # --------------------------------------
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

                    return (
                        FileStatus
                        .SKIPPED_ALREADY_PROCESSED
                        .value
                    )

                # --------------------------------------
                # STRUCTURAL VALIDATION
                # --------------------------------------
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
                            FileStatus
                            .REJECTED_LAYOUT
                        ),
                        error_code=error_code,
                        error_message=(
                            result
                            .file_validation
                            .error_message
                        ),
                    )

                    return (
                        FileStatus
                        .REJECTED_LAYOUT
                        .value
                    )

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

                # --------------------------------------
                # RECONCILIATION
                # --------------------------------------
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

                # --------------------------------------
                # PREPARED
                # --------------------------------------
                self._audit_repository.update_file_status(
                    file_id=file_id,
                    status=(
                        FileStatus.PREPARED
                    ),
                )

                # --------------------------------------
                # LOADING
                # --------------------------------------
                self._audit_repository.update_file_status(
                    file_id=file_id,
                    status=(
                        FileStatus.LOADING
                    ),
                )

                # --------------------------------------
                # TRANSACTIONAL BUSINESS LOAD
                # --------------------------------------
                self._load_service.load_file(
                    file_id=file_id,
                    run_id=run_id,
                    file_name=(
                        remote_file.file_name
                    ),
                    records=result.records,
                    reference_date=(
                        effective_reference_date
                    ),
                )

                # --------------------------------------
                # BACKUP
                # --------------------------------------
                try:
                    backup_file = (
                        self._backup_service
                        .create_backup(
                            staged_file=(
                                result.staged_file
                            ),
                            run_id=run_id,
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

                    return (
                        FileStatus
                        .PENDING_BACKUP
                        .value
                    )

                self._audit_repository.update_file_backed_up(
                    file_id=file_id,
                    backup_path=(
                        backup_file.backup_path
                    ),
                )

                # --------------------------------------
                # SOURCE DELETE
                # --------------------------------------
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

                    return (
                        FileStatus
                        .PENDING_SOURCE_DELETE
                        .value
                    )

                # --------------------------------------
                # FINAL FILE SUCCESS
                # --------------------------------------
                self._audit_repository.mark_source_deleted(
                    file_id=file_id
                )

                return (
                    FileStatus.SUCCESS.value
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

            raise

    # ==================================================
    # FINALIZE RUN
    # ==================================================

    def finalize_run(
        self,
        run_id: str,
    ) -> RunStatus:

        self._audit_repository.update_run_metrics(
            run_id
        )

        statuses = (
            self._audit_repository
            .get_file_statuses(
                run_id
            )
        )

        if not statuses:
            run_status = RunStatus.SUCCESS

        elif any(
            status in {
                FileStatus.FAILED.value,
                FileStatus.PENDING_BACKUP.value,
                FileStatus.PENDING_SOURCE_DELETE.value,
            }
            for status in statuses
        ):
            run_status = RunStatus.FAILED

        elif any(
            status
            == FileStatus.REJECTED_LAYOUT.value
            for status in statuses
        ):
            run_status = (
                RunStatus.PARTIAL_SUCCESS
            )

        else:
            run_status = RunStatus.SUCCESS

        self._audit_repository.finish_run(
            run_id=run_id,
            status=run_status,
        )

        return run_status

    # ==================================================
    # SEQUENTIAL EXECUTION
    #
    # Kept for local execution and existing tests.
    # ==================================================

    def run(
        self,
        run_id: str | None = None,
        dag_id: str = "etl_visitas_daily",
        reference_date: date | None = None,
    ) -> str:

        actual_run_id = self.start_run(
            run_id=run_id,
            dag_id=dag_id,
        )

        try:
            files = self.discover_files()

            for file_data in files:
                try:
                    self.process_single_file(
                        run_id=actual_run_id,
                        file_data=file_data,
                        reference_date=(
                            reference_date
                        ),
                    )

                except Exception:
                    # File state has already been
                    # persisted as FAILED.
                    #
                    # Sequential mode continues with
                    # remaining files.
                    continue

            self.finalize_run(
                actual_run_id
            )

            return actual_run_id

        except Exception:
            self._audit_repository.update_run_metrics(
                actual_run_id
            )

            self._audit_repository.finish_run(
                run_id=actual_run_id,
                status=RunStatus.FAILED,
            )

            raise