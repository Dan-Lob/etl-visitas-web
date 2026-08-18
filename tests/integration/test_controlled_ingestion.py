from datetime import date
from pathlib import Path
from uuid import uuid4

from etl_visitas.config.settings import (
    load_settings,
)
from etl_visitas.repositories.audit_repository import (
    AuditRepository,
)
from etl_visitas.repositories.mysql_connection import (
    MySQLConnectionFactory,
)
from etl_visitas.services.controlled_ingestion_service import (
    ControlledIngestionService,
)


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[2]
)

SFTP_DIR = (
    PROJECT_ROOT
    / "data"
    / "sftp"
)


VALID_HEADER = (
    "email,jyv,Badmail,Baja,"
    "Fecha envio,Fecha open,"
    "Opens,Opens virales,"
    "Fecha click,Clicks,"
    "Clicks virales,Links,"
    "IPs,Navegadores,Plataformas"
)


def clear_sftp_test_data() -> None:
    SFTP_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    for item in SFTP_DIR.iterdir():
        if item.is_file():
            item.unlink()


def test_controlled_ingestion_executes_run():
    clear_sftp_test_data()

    unique_id = uuid4().hex

    file_number = int(
        unique_id[:8],
        16,
    )

    file_name = (
        f"report_{file_number}.txt"
    )

    unique_email = (
        f"controlled_{unique_id}"
        "@example.com"
    )

    source_file = (
        SFTP_DIR
        / file_name
    )

    row = (
        f"{unique_email},"
        ","
        ","
        ","
        "08/02/2013 18:30,"
        "-,"
        "0,"
        "0,"
        "-,"
        "0,"
        "0,"
        "-,"
        "-,"
        "-,"
        "-"
    )

    source_file.write_text(
        f"{VALID_HEADER}\n"
        f"{row}\n",
        encoding="utf-8",
    )

    settings = load_settings()

    run_id = (
        f"controlled_test_{uuid4()}"
    )

    service = (
        ControlledIngestionService(
            settings
        )
    )

    try:
        result_run_id = service.run(
            run_id=run_id,
            reference_date=date(
                2013,
                2,
                15,
            ),
        )

        assert (
            result_run_id
            == run_id
        )

        audit_repository = (
            AuditRepository(
                MySQLConnectionFactory(
                    settings.mysql
                )
            )
        )

        status = (
            audit_repository
            .get_run_status(
                run_id
            )
        )

        assert status == "SUCCESS"

        # Successful lifecycle:
        # source must be deleted.
        assert not source_file.exists()

        connection_factory = (
            MySQLConnectionFactory(
                settings.mysql
            )
        )

        with (
            connection_factory.connection()
            as connection
        ):
            with (
                connection.cursor()
                as cursor
            ):
                cursor.execute(
                    """
                    SELECT
                        status,
                        records_loaded,
                        backup_at,
                        backup_uri,
                        source_deleted_at
                    FROM etl_file_control
                    WHERE run_id = %s
                      AND file_name = %s
                    """,
                    (
                        run_id,
                        file_name,
                    ),
                )

                file_row = (
                    cursor.fetchone()
                )

                assert (
                    file_row
                    is not None
                )

                assert (
                    file_row["status"]
                    == "SUCCESS"
                )

                assert (
                    file_row[
                        "records_loaded"
                    ]
                    == 1
                )

                assert (
                    file_row[
                        "backup_at"
                    ]
                    is not None
                )

                assert (
                    file_row[
                        "backup_uri"
                    ]
                    is not None
                )

                assert (
                    file_row[
                        "source_deleted_at"
                    ]
                    is not None
                )

                cursor.execute(
                    """
                    SELECT
                        visitas_totales,
                        visitas_anio_actual,
                        visitas_mes_actual
                    FROM visitante
                    WHERE email = %s
                    """,
                    (
                        unique_email,
                    ),
                )

                visitor = (
                    cursor.fetchone()
                )

                assert (
                    visitor
                    is not None
                )

                assert (
                    visitor[
                        "visitas_totales"
                    ]
                    == 1
                )

                assert (
                    visitor[
                        "visitas_anio_actual"
                    ]
                    == 1
                )

                assert (
                    visitor[
                        "visitas_mes_actual"
                    ]
                    == 1
                )

    finally:
        clear_sftp_test_data()