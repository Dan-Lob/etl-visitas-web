from datetime import datetime, timezone
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
from etl_visitas.repositories.mysql_connection import (
    MySQLConnectionFactory,
)
from etl_visitas.services.file_processing_service import (
    FileProcessingService,
)


class ControlledIngestionService:

    def __init__(
        self,
        settings: AppSettings,
    ) -> None:
        self._settings = settings

        connection_factory = MySQLConnectionFactory(
            settings.mysql
        )

        self._audit_repository = AuditRepository(
            connection_factory
        )

        self._file_processor = FileProcessingService(
            settings
        )

    def run(
        self,
        run_id: str | None = None,
        dag_id: str = "etl_visitas_daily",
    ) -> str:
        """
        Executes the controlled ingestion flow.

        Current scope:
        - Starts ETL run audit.
        - Connects to SFTP.
        - Discovers report_<n>.txt files.
        - Registers each file in etl_file_control.
        - Downloads/stages each file.
        - Calculates and persists checksum.
        - Detects already successfully processed content.
        - Validates file layout.
        - Validates and normalizes records.
        - Persists per-file metrics/status.
        - Consolidates run metrics.
        - Finishes the run.

        Business-table loading is intentionally not included yet.
        """

        actual_run_id = (
            run_id
            if run_id is not None
            else f"manual_{uuid4()}"
        )

        now = datetime.now(
            timezone.utc
        ).replace(tzinfo=None)

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

                        # -------------------------------------
                        # File was downloaded/staged correctly.
                        # Persist size, SHA-256 and STAGED state.
                        # -------------------------------------
                        self._audit_repository.update_file_staged(
                            file_id=file_id,
                            staged_file=result.staged_file,
                        )

                        # -------------------------------------
                        # Idempotency control.
                        #
                        # A previously LOADED/BACKED_UP/SUCCESS
                        # file with the same content must not
                        # be processed again.
                        # -------------------------------------
                        existing_file = (
                            self._audit_repository
                            .get_successful_file_by_checksum(
                                result
                                .staged_file
                                .checksum_sha256
                            )
                        )

                        if existing_file is not None:
                            # Important:
                            # get_successful_file_by_checksum()
                            # can potentially find the current
                            # file in future stages, so protect
                            # against self-matching.
                            if (
                                existing_file.file_id
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
                                        "File content was "
                                        "already processed "
                                        "successfully. "
                                        f"Original file_id="
                                        f"{existing_file.file_id}, "
                                        f"original run_id="
                                        f"{existing_file.run_id}"
                                    ),
                                )

                                continue

                        # -------------------------------------
                        # Structural file rejection.
                        #
                        # This is a data-quality condition, not
                        # a technical exception. No retry is
                        # required.
                        # -------------------------------------
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

                            # Do not overwrite FAILED if a
                            # previous file already had a
                            # technical failure.
                            if (
                                run_status
                                != RunStatus.FAILED
                            ):
                                run_status = (
                                    RunStatus
                                    .PARTIAL_SUCCESS
                                )

                            continue

                        # -------------------------------------
                        # A structurally valid file must have
                        # its record-processing result.
                        # -------------------------------------
                        if result.records is None:
                            raise RuntimeError(
                                "Prepared file has no "
                                "record-processing result. "
                                f"File="
                                f"{remote_file.file_name}"
                            )

                        # -------------------------------------
                        # Reconciliation control.
                        #
                        # Every row read must end in exactly
                        # one of:
                        # - valid
                        # - invalid
                        # -------------------------------------
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
                                "Record reconciliation "
                                "failed. "
                                f"File="
                                f"{remote_file.file_name}, "
                                f"read={records_read}, "
                                f"valid={records_valid}, "
                                f"invalid="
                                f"{records_invalid}"
                            )

                        # -------------------------------------
                        # Persist validation metrics.
                        # -------------------------------------
                        self._audit_repository.update_file_metrics(
                            file_id=file_id,
                            records_read=records_read,
                            records_valid=records_valid,
                            records_invalid=records_invalid,
                        )

                        # -------------------------------------
                        # The file is ready for the next phase:
                        # transactional MySQL business loading.
                        # -------------------------------------
                        self._audit_repository.update_file_status(
                            file_id=file_id,
                            status=FileStatus.PREPARED,
                        )

                        # Record-level invalid data does not
                        # technically fail the file, but it
                        # should be visible in the overall run.
                        if (
                            records_invalid > 0
                            and run_status
                            != RunStatus.FAILED
                        ):
                            run_status = (
                                RunStatus.PARTIAL_SUCCESS
                            )

                    except Exception as error:
                        # -------------------------------------
                        # Technical failure isolated to one
                        # file.
                        #
                        # Other candidate files are allowed to
                        # continue processing.
                        # -------------------------------------
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

            # ---------------------------------------------
            # Consolidate per-file counters into run-level
            # metrics before closing the execution.
            # ---------------------------------------------
            self._audit_repository.update_run_metrics(
                actual_run_id
            )

            self._audit_repository.finish_run(
                run_id=actual_run_id,
                status=run_status,
            )

            return actual_run_id

        except Exception:
            # ---------------------------------------------
            # Global failure:
            # SFTP connection failure, discovery failure,
            # unexpected orchestration-level exception, etc.
            # ---------------------------------------------
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