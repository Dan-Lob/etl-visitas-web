from datetime import date, datetime, timezone
from uuid import uuid4

from etl_visitas.config.settings import (
    load_settings,
)
from etl_visitas.models.audit_models import (
    RunContext,
)
from etl_visitas.models.enums import (
    ErrorCode,
)
from etl_visitas.models.file_models import (
    RemoteFileMetadata,
)
from etl_visitas.models.record_models import (
    RecordError,
    VisitRecord,
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
from etl_visitas.services.load_service import (
    LoadService,
)
from etl_visitas.validation.record_processor import (
    RecordProcessingResult,
)


def test_load_valid_and_invalid_records():
    settings = load_settings()

    connection_factory = (
        MySQLConnectionFactory(
            settings.mysql
        )
    )

    audit_repository = AuditRepository(
        connection_factory
    )

    run_id = f"load_test_{uuid4()}"
    unique_email = f"valid_{uuid4().hex}@example.com"

    now = datetime.now(
        timezone.utc
    ).replace(tzinfo=None)

    # --------------------------------------------------
    # Create parent run
    # --------------------------------------------------
    audit_repository.start_run(
        RunContext(
            run_id=run_id,
            dag_id="integration_test",
            execution_date=now,
            start_time=now,
        )
    )

    # --------------------------------------------------
    # Register test file
    # --------------------------------------------------
    remote_file = RemoteFileMetadata(
        file_name="report_999.txt",
        remote_path=(
            "archivosVisitas/report_999.txt"
        ),
        size_bytes=100,
    )

    file_id = (
        audit_repository.register_file(
            run_id=run_id,
            remote_file=remote_file,
        )
    )

    # --------------------------------------------------
    # One valid record
    # --------------------------------------------------
    valid_record = VisitRecord(
        email=unique_email,
        jyv=None,
        badmail=None,
        baja=None,
        fecha_envio=datetime(
            2013,
            2,
            8,
            18,
            30,
        ),
        fecha_open=None,
        opens=0,
        opens_virales=0,
        fecha_click=None,
        clicks=0,
        clicks_virales=0,
        links=None,
        ips=None,
        navegadores=None,
        plataformas=None,
        source_file="report_999.txt",
        source_line=2,
    )

    # --------------------------------------------------
    # One invalid record
    # --------------------------------------------------
    invalid_record = RecordError(
        line_number=3,
        email="invalid@@example.com",
        error_codes=(
            ErrorCode.INVALID_EMAIL,
        ),
        error_description=(
            "Invalid email"
        ),
        raw_record=(
            "invalid@@example.com,..."
        ),
    )

    processing_result = (
        RecordProcessingResult(
            valid_records=[
                valid_record
            ],
            invalid_records=[
                invalid_record
            ],
        )
    )

    # --------------------------------------------------
    # Execute transactional business load
    # --------------------------------------------------
    loader = LoadService(
        connection_factory=(
            connection_factory
        ),
        repository=(
            BusinessLoadRepository()
        ),
    )

    load_result = loader.load_file(
        file_id=file_id,
        run_id=run_id,
        file_name="report_999.txt",
        records=processing_result,
        reference_date=date(
            2013,
            2,
            15,
        ),
    )

    # --------------------------------------------------
    # Validate returned metrics
    # --------------------------------------------------
    assert (
        load_result.statistics_inserted
        == 1
    )

    assert (
        load_result.errors_inserted
        == 1
    )

    assert (
        load_result.visitors_inserted
        == 1
    )

    assert (
        load_result.visitors_updated
        == 0
    )

    assert (
        load_result.records_loaded
        == 1
    )

    # --------------------------------------------------
    # Validate persisted database state
    # --------------------------------------------------
    with (
        connection_factory.connection()
        as connection
    ):
        with connection.cursor() as cursor:

            cursor.execute(
                """
                SELECT COUNT(*) AS total
                FROM estadistica
                WHERE file_id = %s
                """,
                (file_id,),
            )

            statistics_row = (
                cursor.fetchone()
            )

            assert (
                statistics_row["total"]
                == 1
            )

            cursor.execute(
                """
                SELECT COUNT(*) AS total
                FROM errores
                WHERE file_id = %s
                """,
                (file_id,),
            )

            errors_row = (
                cursor.fetchone()
            )

            assert (
                errors_row["total"]
                == 1
            )

            cursor.execute(
                """
                SELECT
                    email,
                    fecha_primera_visita,
                    fecha_ultima_visita,
                    visitas_totales,
                    visitas_anio_actual,
                    visitas_mes_actual
                FROM visitante
                WHERE email = %s
                """,
                (unique_email,),
            )

            visitor_row = (
                cursor.fetchone()
            )

            assert visitor_row is not None

            assert (
                visitor_row["email"]
                == unique_email
            )

            assert (
                visitor_row[
                    "fecha_primera_visita"
                ].isoformat()
                == "2013-02-08"
            )

            assert (
                visitor_row[
                    "fecha_ultima_visita"
                ].isoformat()
                == "2013-02-08"
            )

            assert (
                visitor_row[
                    "visitas_totales"
                ]
                == 1
            )

            assert (
                visitor_row[
                    "visitas_anio_actual"
                ]
                == 1
            )

            assert (
                visitor_row[
                    "visitas_mes_actual"
                ]
                == 1
            )

            cursor.execute(
                """
                SELECT
                    status,
                    records_loaded,
                    visitors_inserted,
                    visitors_updated
                FROM etl_file_control
                WHERE file_id = %s
                """,
                (file_id,),
            )

            file_control_row = (
                cursor.fetchone()
            )

            assert (
                file_control_row["status"]
                == "LOADED"
            )

            assert (
                file_control_row[
                    "records_loaded"
                ]
                == 1
            )

            assert (
                file_control_row[
                    "visitors_inserted"
                ]
                == 1
            )

            assert (
                file_control_row[
                    "visitors_updated"
                ]
                == 0
            )