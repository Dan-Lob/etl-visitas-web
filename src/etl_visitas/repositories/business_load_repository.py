from datetime import date

from etl_visitas.models.record_models import (
    RecordError,
    VisitRecord,
)


class BusinessLoadRepository:

    def insert_statistics(
        self,
        connection,
        file_id: int,
        run_id: str,
        records: list[VisitRecord],
    ) -> int:
        if not records:
            return 0

        sql = """
        INSERT INTO estadistica (
            file_id,
            run_id,
            email,
            jyv,
            badmail,
            baja,
            fecha_envio,
            fecha_open,
            opens,
            opens_virales,
            fecha_click,
            clicks,
            clicks_virales,
            links,
            ips,
            navegadores,
            plataformas,
            source_file,
            source_line
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        """

        values = [
            (
                file_id,
                run_id,
                record.email,
                record.jyv,
                record.badmail,
                record.baja,
                record.fecha_envio,
                record.fecha_open,
                record.opens,
                record.opens_virales,
                record.fecha_click,
                record.clicks,
                record.clicks_virales,
                record.links,
                record.ips,
                record.navegadores,
                record.plataformas,
                record.source_file,
                record.source_line,
            )
            for record in records
        ]

        with connection.cursor() as cursor:
            cursor.executemany(
                sql,
                values,
            )

        return len(records)

    def insert_errors(
        self,
        connection,
        file_id: int,
        run_id: str,
        file_name: str,
        errors: list[RecordError],
    ) -> int:
        if not errors:
            return 0

        sql = """
        INSERT INTO errores (
            file_id,
            run_id,
            file_name,
            line_number,
            email,
            error_code,
            error_description,
            raw_record
        )
        VALUES (
            %s, %s, %s, %s,
            %s, %s, %s, %s
        )
        """

        values = [
            (
                file_id,
                run_id,
                file_name,
                error.line_number,
                error.email,
                self._serialize_error_codes(
                    error.error_codes
                ),
                error.error_description,
                error.raw_record,
            )
            for error in errors
        ]

        with connection.cursor() as cursor:
            cursor.executemany(
                sql,
                values,
            )

        return len(errors)

    def prepare_affected_emails(
        self,
        connection,
        records: list[VisitRecord],
    ) -> int:
        """
        Creates a temporary table containing the unique
        emails affected by the current file.

        The temporary table uses the same charset/collation
        as the business tables to avoid comparison conflicts.
        """

        with connection.cursor() as cursor:
            cursor.execute(
                """
                DROP TEMPORARY TABLE
                IF EXISTS tmp_affected_emails
                """
            )

            cursor.execute(
                """
                CREATE TEMPORARY TABLE
                tmp_affected_emails (
                    email VARCHAR(320)
                    COLLATE utf8mb4_unicode_ci
                    NOT NULL PRIMARY KEY
                )
                ENGINE=MEMORY
                DEFAULT CHARSET=utf8mb4
                COLLATE=utf8mb4_unicode_ci
                """
            )

            unique_emails = sorted(
                {
                    record.email
                    for record in records
                }
            )

            if not unique_emails:
                return 0

            cursor.executemany(
                """
                INSERT INTO tmp_affected_emails (
                    email
                )
                VALUES (%s)
                """,
                [
                    (email,)
                    for email in unique_emails
                ],
            )

        return len(unique_emails)

    def count_existing_visitors(
        self,
        connection,
    ) -> int:
        """
        Counts how many affected emails already exist
        in visitante before performing the UPSERT.
        """

        sql = """
        SELECT COUNT(*) AS total
        FROM visitante v
        INNER JOIN tmp_affected_emails a
            ON a.email = v.email
        """

        with connection.cursor() as cursor:
            cursor.execute(sql)
            row = cursor.fetchone()

        return int(row["total"])

    def upsert_affected_visitors(
        self,
        connection,
        reference_date: date,
    ) -> None:
        """
        Recalculates visitor metrics from estadistica only
        for the emails affected by the current file.

        Current provisional business rule:
            fecha_visita = fecha_envio
        """

        sql = """
        INSERT INTO visitante (
            email,
            fecha_primera_visita,
            fecha_ultima_visita,
            visitas_totales,
            visitas_anio_actual,
            visitas_mes_actual
        )
        SELECT
            e.email,

            MIN(
                DATE(e.fecha_envio)
            ) AS fecha_primera_visita,

            MAX(
                DATE(e.fecha_envio)
            ) AS fecha_ultima_visita,

            COUNT(*) AS visitas_totales,

            SUM(
                CASE
                    WHEN YEAR(e.fecha_envio)
                         = YEAR(%s)
                    THEN 1
                    ELSE 0
                END
            ) AS visitas_anio_actual,

            SUM(
                CASE
                    WHEN YEAR(e.fecha_envio)
                         = YEAR(%s)
                     AND MONTH(e.fecha_envio)
                         = MONTH(%s)
                    THEN 1
                    ELSE 0
                END
            ) AS visitas_mes_actual

        FROM estadistica e

        INNER JOIN tmp_affected_emails a
            ON a.email = e.email

        GROUP BY e.email

        ON DUPLICATE KEY UPDATE
            fecha_primera_visita =
                VALUES(fecha_primera_visita),

            fecha_ultima_visita =
                VALUES(fecha_ultima_visita),

            visitas_totales =
                VALUES(visitas_totales),

            visitas_anio_actual =
                VALUES(visitas_anio_actual),

            visitas_mes_actual =
                VALUES(visitas_mes_actual)
        """

        with connection.cursor() as cursor:
            cursor.execute(
                sql,
                (
                    reference_date,
                    reference_date,
                    reference_date,
                ),
            )

    def count_statistics_by_file(
        self,
        connection,
        file_id: int,
    ) -> int:
        sql = """
        SELECT COUNT(*) AS total
        FROM estadistica
        WHERE file_id = %s
        """

        with connection.cursor() as cursor:
            cursor.execute(
                sql,
                (file_id,),
            )

            row = cursor.fetchone()

        return int(row["total"])

    def count_errors_by_file(
        self,
        connection,
        file_id: int,
    ) -> int:
        sql = """
        SELECT COUNT(*) AS total
        FROM errores
        WHERE file_id = %s
        """

        with connection.cursor() as cursor:
            cursor.execute(
                sql,
                (file_id,),
            )

            row = cursor.fetchone()

        return int(row["total"])

    def mark_file_loaded(
        self,
        connection,
        file_id: int,
        records_loaded: int,
        visitors_inserted: int,
        visitors_updated: int,
    ) -> None:
        """
        Important:
        this UPDATE occurs using the same connection and
        transaction as estadistica/errores/visitante.
        """

        sql = """
        UPDATE etl_file_control
        SET
            status = 'LOADED',
            load_finished_at =
                CURRENT_TIMESTAMP,
            records_loaded = %s,
            visitors_inserted = %s,
            visitors_updated = %s
        WHERE file_id = %s
        """

        with connection.cursor() as cursor:
            cursor.execute(
                sql,
                (
                    records_loaded,
                    visitors_inserted,
                    visitors_updated,
                    file_id,
                ),
            )

    @staticmethod
    def _serialize_error_codes(
        error_codes,
    ) -> str:
        return ",".join(
            error_code.value
            for error_code in error_codes
        )