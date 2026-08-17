from __future__ import annotations

from datetime import date, timedelta

import pendulum

from airflow.sdk import dag, task

from etl_visitas.config.settings import load_settings
from etl_visitas.repositories.audit_repository import (
    AuditRepository,
)
from etl_visitas.repositories.mysql_connection import (
    MySQLConnectionFactory,
)
from etl_visitas.services.controlled_ingestion_service import (
    ControlledIngestionService,
)


DAG_ID = "etl_visitas_daily"


@dag(
    dag_id=DAG_ID,

    # Por ahora ejecución manual.
    # Activaremos el schedule diario después de validar
    # correctamente el DAG local.
    schedule=None,

    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz="America/Mexico_City",
    ),

    catchup=False,

    max_active_runs=1,

    default_args={
        "retries": 2,
        "retry_delay": timedelta(
            minutes=2
        ),
    },

    tags=[
        "etl",
        "visitas",
        "sftp",
        "mysql",
    ],
)
def etl_visitas_daily():

    @task(
        task_id="preflight",
        retries=1,
        retry_delay=timedelta(
            seconds=30
        ),
    )
    def preflight() -> dict:
        """
        Validates that the ETL runtime configuration
        required by the pipeline is available.
        """

        settings = load_settings()

        required_values = {
            "environment": (
                settings.environment
            ),
            "sftp_host": (
                settings.sftp.host
            ),
            "sftp_remote_path": (
                settings.sftp.remote_path
            ),
            "mysql_host": (
                settings.mysql.host
            ),
            "mysql_database": (
                settings.mysql.database
            ),
            "staging_path": (
                settings.storage.staging_path
            ),
            "backup_path": (
                settings.storage.backup_path
            ),
        }

        missing = [
            key
            for key, value
            in required_values.items()
            if not value
        ]

        if missing:
            raise RuntimeError(
                "Missing required ETL "
                "configuration: "
                f"{missing}"
            )

        return {
            "environment": (
                settings.environment
            ),
            "status": "OK",
        }

    @task(
        task_id="run_etl",
        retries=1,
        retry_delay=timedelta(
            minutes=1
        ),
        execution_timeout=timedelta(
            minutes=30
        ),
    )
    def run_etl(
        preflight_result: dict,
    ) -> str:
        """
        Executes the controlled ETL flow.

        Business logic remains outside Airflow
        inside ControlledIngestionService.
        """

        if (
            preflight_result["status"]
            != "OK"
        ):
            raise RuntimeError(
                "Preflight validation "
                "did not succeed"
            )

        settings = load_settings()

        service = (
            ControlledIngestionService(
                settings
            )
        )

        run_id = service.run(
            dag_id=DAG_ID,

            # Temporary reference date for
            # the supplied historical test data.
            #
            # This will be revisited when the
            # business rule is confirmed.
            reference_date=date(
                2013,
                2,
                15,
            ),
        )

        return run_id

    @task(
        task_id="validate_run",
        retries=0,
    )
    def validate_run(
        etl_run_id: str,
    ) -> None:
        """
        Verifies that the ETL execution was
        correctly persisted in the audit tables.
        """

        settings = load_settings()

        repository = AuditRepository(
            MySQLConnectionFactory(
                settings.mysql
            )
        )

        status = (
            repository.get_run_status(
                etl_run_id
            )
        )

        if status is None:
            raise RuntimeError(
                "ETL run was not found "
                "in etl_run_control. "
                f"run_id={etl_run_id}"
            )

        allowed_statuses = {
            "SUCCESS",
            "PARTIAL_SUCCESS",
        }

        if status not in allowed_statuses:
            raise RuntimeError(
                "ETL run finished with "
                "an invalid status. "
                f"run_id={etl_run_id}, "
                f"status={status}"
            )

        print(
            "ETL run validation: OK"
        )

        print(
            f"run_id={etl_run_id}"
        )

        print(
            f"status={status}"
        )

    preflight_result = preflight()

    run_id = run_etl(
        preflight_result
    )

    validate_run(
        run_id
    )


etl_visitas_daily()