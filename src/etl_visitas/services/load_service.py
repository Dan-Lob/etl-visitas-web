from datetime import date

from etl_visitas.models.load_models import (
    FileLoadResult,
)
from etl_visitas.repositories.business_load_repository import (
    BusinessLoadRepository,
)
from etl_visitas.repositories.mysql_connection import (
    MySQLConnectionFactory,
)
from etl_visitas.validation.record_processor import (
    RecordProcessingResult,
)


class LoadService:

    def __init__(
        self,
        connection_factory: MySQLConnectionFactory,
        repository: BusinessLoadRepository,
    ) -> None:
        self._connection_factory = (
            connection_factory
        )

        self._repository = repository

    def load_file(
        self,
        file_id: int,
        run_id: str,
        file_name: str,
        records: RecordProcessingResult,
        reference_date: date | None = None,
    ) -> FileLoadResult:
        """
        Loads one complete file as a single MySQL
        transaction.

        Transaction scope:
            errores
            estadistica
            visitante
            etl_file_control
        """

        effective_reference_date = (
            reference_date
            if reference_date is not None
            else date.today()
        )

        connection = (
            self._connection_factory
            .create_connection()
        )

        try:
            # -------------------------------------
            # 1. Load valid detail
            # -------------------------------------
            statistics_inserted = (
                self._repository
                .insert_statistics(
                    connection=connection,
                    file_id=file_id,
                    run_id=run_id,
                    records=(
                        records.valid_records
                    ),
                )
            )

            # -------------------------------------
            # 2. Load rejected detail
            # -------------------------------------
            errors_inserted = (
                self._repository.insert_errors(
                    connection=connection,
                    file_id=file_id,
                    run_id=run_id,
                    file_name=file_name,
                    errors=(
                        records.invalid_records
                    ),
                )
            )

            # -------------------------------------
            # 3. Initial reconciliation
            # -------------------------------------
            self._validate_input_reconciliation(
                records=records,
                statistics_inserted=(
                    statistics_inserted
                ),
                errors_inserted=errors_inserted,
            )

            # -------------------------------------
            # 4. Determine affected visitors
            # -------------------------------------
            affected_visitors = (
                self._repository
                .prepare_affected_emails(
                    connection=connection,
                    records=(
                        records.valid_records
                    ),
                )
            )

            existing_visitors = 0

            if affected_visitors > 0:
                existing_visitors = (
                    self._repository
                    .count_existing_visitors(
                        connection
                    )
                )

                # ---------------------------------
                # 5. Recalculate + UPSERT visitante
                # ---------------------------------
                self._repository.upsert_affected_visitors(
                    connection=connection,
                    reference_date=(
                        effective_reference_date
                    ),
                )

            visitors_inserted = (
                affected_visitors
                - existing_visitors
            )

            visitors_updated = (
                existing_visitors
            )

            # -------------------------------------
            # 6. Database reconciliation
            # -------------------------------------
            statistics_in_database = (
                self._repository
                .count_statistics_by_file(
                    connection=connection,
                    file_id=file_id,
                )
            )

            errors_in_database = (
                self._repository
                .count_errors_by_file(
                    connection=connection,
                    file_id=file_id,
                )
            )

            self._validate_database_reconciliation(
                records=records,
                statistics_in_database=(
                    statistics_in_database
                ),
                errors_in_database=(
                    errors_in_database
                ),
            )

            # -------------------------------------
            # 7. Update file control using the
            # SAME transaction.
            # -------------------------------------
            self._repository.mark_file_loaded(
                connection=connection,
                file_id=file_id,
                records_loaded=(
                    statistics_inserted
                ),
                visitors_inserted=(
                    visitors_inserted
                ),
                visitors_updated=(
                    visitors_updated
                ),
            )

            # -------------------------------------
            # 8. Only here do we COMMIT everything.
            # -------------------------------------
            connection.commit()

            return FileLoadResult(
                statistics_inserted=(
                    statistics_inserted
                ),
                errors_inserted=errors_inserted,
                visitors_inserted=(
                    visitors_inserted
                ),
                visitors_updated=(
                    visitors_updated
                ),
                records_loaded=(
                    statistics_inserted
                ),
            )

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    @staticmethod
    def _validate_input_reconciliation(
        records: RecordProcessingResult,
        statistics_inserted: int,
        errors_inserted: int,
    ) -> None:

        if (
            statistics_inserted
            != records.records_valid
        ):
            raise RuntimeError(
                "Statistics reconciliation failed. "
                f"expected="
                f"{records.records_valid}, "
                f"inserted="
                f"{statistics_inserted}"
            )

        if (
            errors_inserted
            != records.records_invalid
        ):
            raise RuntimeError(
                "Errors reconciliation failed. "
                f"expected="
                f"{records.records_invalid}, "
                f"inserted="
                f"{errors_inserted}"
            )

        if (
            records.records_read
            != statistics_inserted
            + errors_inserted
        ):
            raise RuntimeError(
                "Input reconciliation failed. "
                f"read={records.records_read}, "
                f"statistics="
                f"{statistics_inserted}, "
                f"errors={errors_inserted}"
            )

    @staticmethod
    def _validate_database_reconciliation(
        records: RecordProcessingResult,
        statistics_in_database: int,
        errors_in_database: int,
    ) -> None:

        if (
            statistics_in_database
            != records.records_valid
        ):
            raise RuntimeError(
                "Database statistics "
                "reconciliation failed. "
                f"expected="
                f"{records.records_valid}, "
                f"database="
                f"{statistics_in_database}"
            )

        if (
            errors_in_database
            != records.records_invalid
        ):
            raise RuntimeError(
                "Database errors "
                "reconciliation failed. "
                f"expected="
                f"{records.records_invalid}, "
                f"database="
                f"{errors_in_database}"
            )