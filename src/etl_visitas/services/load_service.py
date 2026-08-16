from etl_visitas.models.load_models import (
    DetailLoadResult,
)
from etl_visitas.validation.record_processor import (
    RecordProcessingResult,
)
from etl_visitas.repositories.business_load_repository import (
    BusinessLoadRepository,
)
from etl_visitas.repositories.mysql_connection import (
    MySQLConnectionFactory,
)


class LoadService:

    def __init__(
        self,
        connection_factory: MySQLConnectionFactory,
        repository: BusinessLoadRepository,
    ) -> None:
        self._connection_factory = connection_factory
        self._repository = repository

    def load_details(
        self,
        file_id: int,
        run_id: str,
        file_name: str,
        records: RecordProcessingResult,
    ) -> DetailLoadResult:
        connection = (
            self._connection_factory
            .create_connection()
        )

        try:
            statistics_inserted = (
                self._repository.insert_statistics(
                    connection=connection,
                    file_id=file_id,
                    run_id=run_id,
                    records=records.valid_records,
                )
            )

            errors_inserted = (
                self._repository.insert_errors(
                    connection=connection,
                    file_id=file_id,
                    run_id=run_id,
                    file_name=file_name,
                    errors=records.invalid_records,
                )
            )

            self._validate_reconciliation(
                records=records,
                statistics_inserted=(
                    statistics_inserted
                ),
                errors_inserted=(
                    errors_inserted
                ),
            )

            connection.commit()

            return DetailLoadResult(
                statistics_inserted=(
                    statistics_inserted
                ),
                errors_inserted=errors_inserted,
            )

        except Exception:
            connection.rollback()
            raise

        finally:
            connection.close()

    @staticmethod
    def _validate_reconciliation(
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
                f"expected={records.records_valid}, "
                f"inserted={statistics_inserted}"
            )

        if (
            errors_inserted
            != records.records_invalid
        ):
            raise RuntimeError(
                "Errors reconciliation failed. "
                f"expected={records.records_invalid}, "
                f"inserted={errors_inserted}"
            )

        if (
            records.records_read
            != statistics_inserted
            + errors_inserted
        ):
            raise RuntimeError(
                "Detail load reconciliation failed. "
                f"read={records.records_read}, "
                f"statistics={statistics_inserted}, "
                f"errors={errors_inserted}"
            )