from datetime import date
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


def test_controlled_ingestion_executes_run():

    settings = load_settings()

    run_id = (
        f"controlled_test_{uuid4()}"
    )

    service = ControlledIngestionService(
        settings
    )

    result_run_id = service.run(
        run_id=run_id,
        reference_date=date(
            2013,
            2,
            15,
        ),
    )

    assert result_run_id == run_id

    audit = AuditRepository(
        MySQLConnectionFactory(
            settings.mysql
        )
    )

    status = audit.get_run_status(
        run_id
    )

    assert status in {
        "SUCCESS",
        "PARTIAL_SUCCESS",
    }