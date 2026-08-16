from datetime import datetime, timezone
from uuid import uuid4

from etl_visitas.config.settings import (
    load_settings,
)
from etl_visitas.models.audit_models import (
    RunContext,
)
from etl_visitas.models.record_models import (
    RecordError,
    VisitRecord,
)
from etl_visitas.models.enums import (
    ErrorCode,
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
from etl_visitas.models.file_models import (
    RemoteFileMetadata,
)


def test_load_valid_and_invalid_records():
    settings = load_settings()

    factory = MySQLConnectionFactory(
        settings.mysql
    )

    audit = AuditRepository(factory)

    run_id = f"load_test_{uuid4()}"

    now = datetime.now(
        timezone.utc
    ).replace(tzinfo=None)

    audit.start_run(
        RunContext(
            run_id=run_id,
            dag_id="integration_test",
            execution_date=now,
            start_time=now,
        )
    )

    remote_file = RemoteFileMetadata(
        file_name="report_999.txt",
        remote_path=(
            "archivosVisitas/report_999.txt"
        ),
        size_bytes=100,
    )

    file_id = audit.register_file(
        run_id=run_id,
        remote_file=remote_file,
    )

    valid_record = VisitRecord(
        email="valid@example.com",
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

    loader = LoadService(
        connection_factory=factory,
        repository=(
            BusinessLoadRepository()
        ),
    )

    result = loader.load_details(
        file_id=file_id,
        run_id=run_id,
        file_name="report_999.txt",
        records=processing_result,
    )

    assert result.statistics_inserted == 1
    assert result.errors_inserted == 1