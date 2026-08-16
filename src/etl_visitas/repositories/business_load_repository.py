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

    @staticmethod
    def _serialize_error_codes(
        error_codes,
    ) -> str:
        return ",".join(
            error_code.value
            for error_code in error_codes
        )