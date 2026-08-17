from etl_visitas.repositories.mysql_connection import (
    MySQLConnectionFactory,
)


class RunSummaryService:

    def __init__(
        self,
        connection_factory: MySQLConnectionFactory,
    ) -> None:
        self._connection_factory = (
            connection_factory
        )

    def get_run_summary(
        self,
        run_id: str,
    ) -> dict:

        sql = """
        SELECT
            run_id,
            dag_id,
            status,
            files_detected,
            files_processed,
            files_success,
            files_rejected,
            files_failed,
            records_read,
            records_valid,
            records_invalid,
            records_loaded,
            start_time,
            end_time
        FROM etl_run_control
        WHERE run_id = %s
        """

        with (
            self._connection_factory
            .connection()
            as connection
        ):
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (run_id,),
                )

                row = cursor.fetchone()

        if row is None:
            raise RuntimeError(
                "Run not found. "
                f"run_id={run_id}"
            )

        return dict(row)