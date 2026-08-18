from datetime import datetime, timezone
from uuid import uuid4

from etl_visitas.config.settings import load_settings
from etl_visitas.models.audit_models import RunContext
from etl_visitas.models.enums import RunStatus
from etl_visitas.repositories.audit_repository import (
    AuditRepository,
)
from etl_visitas.repositories.mysql_connection import (
    MySQLConnectionFactory,
)


def test_start_and_finish_run():

    settings = load_settings()

    repository = AuditRepository(
        MySQLConnectionFactory(
            settings.mysql
        )
    )

    run_id = (
        f"integration_test_{uuid4()}"
    )

    now = datetime.now(
        timezone.utc
    ).replace(tzinfo=None)

    context = RunContext(
        run_id=run_id,
        dag_id="etl_visitas_daily",
        execution_date=now,
        start_time=now,
    )

    repository.start_run(context)

    repository.finish_run(
        run_id=run_id,
        status=RunStatus.SUCCESS,
    )


    status = repository.get_run_status(
        run_id
    )

    assert status == RunStatus.SUCCESS.value