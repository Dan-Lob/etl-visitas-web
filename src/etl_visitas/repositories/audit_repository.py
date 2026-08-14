from datetime import datetime, timezone

from etl_visitas.models.audit_models import (
    FileControlRecord,
    RunContext,
)
from etl_visitas.models.enums import (
    FileStatus,
    RunStatus,
)
from etl_visitas.models.file_models import (
    RemoteFileMetadata,
    StagedFile,
)
from etl_visitas.repositories.mysql_connection import (
    MySQLConnectionFactory,
)


class AuditRepository:

    def __init__(
        self,
        connection_factory: MySQLConnectionFactory,
    ):
        self._connection_factory = connection_factory

    def start_run(
        self,
        context: RunContext,
    ) -> None:
        sql = """
        INSERT INTO etl_run_control (
            run_id,
            dag_id,
            execution_date,
            start_time,
            status
        )
        VALUES (%s, %s, %s, %s, %s)
        """

        with self._connection_factory.connection() as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        sql,
                        (
                            context.run_id,
                            context.dag_id,
                            context.execution_date,
                            context.start_time,
                            RunStatus.RUNNING.value,
                        ),
                    )

                connection.commit()

            except Exception:
                connection.rollback()
                raise

    def finish_run(
        self,
        run_id: str,
        status: RunStatus,
    ) -> None:
        sql = """
        UPDATE etl_run_control
        SET
            status = %s,
            end_time = CURRENT_TIMESTAMP
        WHERE run_id = %s
        """

        with self._connection_factory.connection() as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        sql,
                        (
                            status.value,
                            run_id,
                        ),
                    )

                connection.commit()

            except Exception:
                connection.rollback()
                raise

    def get_run_status(
        self,
        run_id: str,
    ) -> str | None:
        sql = """
        SELECT status
        FROM etl_run_control
        WHERE run_id = %s
        """

        with self._connection_factory.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (run_id,),
                )

                row = cursor.fetchone()

        if row is None:
            return None

        return row["status"]

    def register_file(
        self,
        run_id: str,
        remote_file: RemoteFileMetadata,
    ) -> int:
        sql = """
        INSERT INTO etl_file_control (
            run_id,
            file_name,
            source_path,
            file_size,
            detected_at,
            status
        )
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        detected_at = datetime.now(
            timezone.utc
        ).replace(tzinfo=None)

        with self._connection_factory.connection() as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        sql,
                        (
                            run_id,
                            remote_file.file_name,
                            remote_file.remote_path,
                            remote_file.size_bytes,
                            detected_at,
                            FileStatus.DETECTED.value,
                        ),
                    )

                    file_id = cursor.lastrowid

                connection.commit()

            except Exception:
                connection.rollback()
                raise

        return file_id

    def update_file_staged(
        self,
        file_id: int,
        staged_file: StagedFile,
    ) -> None:
        sql = """
        UPDATE etl_file_control
        SET
            file_size = %s,
            checksum_sha256 = %s,
            staged_at = CURRENT_TIMESTAMP,
            staging_uri = %s,
            status = %s
        WHERE file_id = %s
        """

        with self._connection_factory.connection() as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        sql,
                        (
                            staged_file.local_size_bytes,
                            staged_file.checksum_sha256,
                            staged_file.local_path,
                            FileStatus.STAGED.value,
                            file_id,
                        ),
                    )

                connection.commit()

            except Exception:
                connection.rollback()
                raise

    def update_file_status(
        self,
        file_id: int,
        status: FileStatus,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        sql = """
        UPDATE etl_file_control
        SET
            status = %s,
            error_code = %s,
            error_message = %s
        WHERE file_id = %s
        """

        with self._connection_factory.connection() as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        sql,
                        (
                            status.value,
                            error_code,
                            error_message,
                            file_id,
                        ),
                    )

                connection.commit()

            except Exception:
                connection.rollback()
                raise

    def update_file_metrics(
        self,
        file_id: int,
        records_read: int,
        records_valid: int,
        records_invalid: int,
    ) -> None:
        sql = """
        UPDATE etl_file_control
        SET
            records_read = %s,
            records_valid = %s,
            records_invalid = %s
        WHERE file_id = %s
        """

        with self._connection_factory.connection() as connection:
            try:
                with connection.cursor() as cursor:
                    cursor.execute(
                        sql,
                        (
                            records_read,
                            records_valid,
                            records_invalid,
                            file_id,
                        ),
                    )

                connection.commit()

            except Exception:
                connection.rollback()
                raise

    def get_successful_file_by_checksum(
        self,
        checksum_sha256: str,
    ) -> FileControlRecord | None:
        sql = """
        SELECT
            file_id,
            run_id,
            file_name,
            checksum_sha256,
            status
        FROM etl_file_control
        WHERE checksum_sha256 = %s
          AND status IN (
              'LOADED',
              'BACKED_UP',
              'PENDING_SOURCE_DELETE',
              'SUCCESS'
          )
        ORDER BY file_id DESC
        LIMIT 1
        """

        with self._connection_factory.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (checksum_sha256,),
                )

                row = cursor.fetchone()

        if row is None:
            return None

        return FileControlRecord(
            file_id=row["file_id"],
            run_id=row["run_id"],
            file_name=row["file_name"],
            checksum_sha256=row["checksum_sha256"],
            status=row["status"],
        )

    def get_run_status(
        self,
        run_id: str,
    ) -> str | None:

        sql = """
        SELECT status
        FROM etl_run_control
        WHERE run_id = %s
        """

        with self._connection_factory.connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (run_id,),
                )

                row = cursor.fetchone()

        if row is None:
            return None

        return row["status"]