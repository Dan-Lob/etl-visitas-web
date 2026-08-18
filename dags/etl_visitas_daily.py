from __future__ import annotations

from datetime import date, timedelta

import pendulum

from airflow.sdk import (
    dag,
    get_current_context,
    task,
)
from airflow.utils.trigger_rule import TriggerRule

from etl_visitas.config.settings import (
    load_airflow_settings,
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
from etl_visitas.operations.run_summary_service import (
    RunSummaryService,
)
from etl_visitas.operations.alert_service import (
    AlertService,
)


DAG_ID = "etl_visitas_daily"

DAG_SETTINGS = (
    load_airflow_settings()
)

DAG_TIMEZONE = (
    DAG_SETTINGS.timezone
)

DAG_SCHEDULE = (
    DAG_SETTINGS.schedule
)


@dag(
    dag_id=DAG_ID,
    schedule=DAG_SCHEDULE,

    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz=DAG_TIMEZONE,
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

    # ==================================================
    # PREFLIGHT
    # ==================================================

    @task(
        task_id="preflight",
        retries=1,
        retry_delay=timedelta(
            seconds=30
        ),
    )
    def preflight() -> dict:
        """
        Validates the runtime configuration required
        by the ETL before starting the execution.
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

    # ==================================================
    # START RUN
    # ==================================================

    @task(
        task_id="start_run",
        retries=1,
    )
    def start_run(
        preflight_result: dict,
    ) -> str:
        """
        Creates the ETL execution in etl_run_control.
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

        etl_run_id = service.start_run(
            dag_id=DAG_ID
        )

        print(
            f"ETL run started: "
            f"{etl_run_id}"
        )

        return etl_run_id

    # ==================================================
    # DISCOVER FILES
    # ==================================================

    @task(
        task_id="discover_files",
        retries=1,
        retry_delay=timedelta(
            seconds=30
        ),
    )
    def discover_files(
        etl_run_id: str,
    ) -> list[dict]:
        """
        Discovers candidate report_<n>.txt files
        available in the configured SFTP directory.
        """

        if not etl_run_id:
            raise RuntimeError(
                "ETL run_id was not provided"
            )

        settings = load_settings()

        service = (
            ControlledIngestionService(
                settings
            )
        )

        files = service.discover_files()

        print(
            "Candidate files discovered: "
            f"{len(files)}"
        )

        for file_data in files:
            print(
                "Discovered file: "
                f"{file_data['file_name']}"
            )

        return files

    # ==================================================
    # PROCESS SINGLE FILE
    # ==================================================

    @task(
        task_id="process_file",
        retries=1,
        retry_delay=timedelta(
            minutes=1
        ),
        execution_timeout=timedelta(
            minutes=30
        ),
    )
    def process_file(
        file_data: dict,
        etl_run_id: str,
    ) -> str:
        """
        Processes exactly one file.

        Airflow dynamically creates one mapped
        TaskInstance for every file discovered
        by discover_files.
        """

        settings = load_settings()

        service = (
            ControlledIngestionService(
                settings
            )
        )

        file_name = (
            file_data["file_name"]
        )

        print(
            f"Processing file: "
            f"{file_name}"
        )

        context = get_current_context()

        logical_date = context.get(
            "logical_date"
        )

        if logical_date is not None:
            reference_date = (
                logical_date.date()
            )
        else:
            reference_date = date.today()

        status = (
            service.process_single_file(
                run_id=etl_run_id,
                file_data=file_data,
                reference_date=reference_date,
            )
        )

        print(
            f"File={file_name}, "
            f"status={status}"
        )

        return status

    # ==================================================
    # FINALIZE RUN
    # ==================================================

    @task(
        task_id="finalize_run",
        retries=0,

        # Debe ejecutarse aunque alguna instancia
        # process_file[n] termine en FAILED.
        trigger_rule=(
            TriggerRule.ALL_DONE
        ),
    )
    def finalize_run(
        etl_run_id: str,
    ) -> str:
        """
        Consolidates file-level metrics and determines
        the final ETL run status.
        """

        settings = load_settings()

        service = (
            ControlledIngestionService(
                settings
            )
        )

        status = service.finalize_run(
            etl_run_id
        )

        print(
            "ETL run finalized. "
            f"run_id={etl_run_id}, "
            f"status={status.value}"
        )

        return etl_run_id

    # ==================================================
    # EMIT OPERATIONAL ALERTS
    # ==================================================

    @task(
        task_id="emit_alerts",
        retries=0,
    )
    def emit_alerts(
        etl_run_id: str,
    ) -> str:
        """
        Evaluates the final file states and emits
        operational alerts when attention is required.
        """

        settings = load_settings()

        connection_factory = (
            MySQLConnectionFactory(
                settings.mysql
            )
        )

        alert_service = AlertService(
            connection_factory
        )

        alerts = (
            alert_service.emit_run_alerts(
                etl_run_id
            )
        )

        print(
            "Operational alert evaluation "
            f"completed. alerts={len(alerts)}"
        )

        return etl_run_id

    # ==================================================
    # VALIDATE RUN
    # ==================================================

    @task(
        task_id="validate_run",
        retries=0,
    )
    def validate_run(
        etl_run_id: str,
    ) -> None:
        """
        Verifies that the final execution status
        was correctly persisted in etl_run_control.
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

        summary_service = RunSummaryService(
            MySQLConnectionFactory(
                settings.mysql
            )
        )

        summary = summary_service.get_run_summary(
            etl_run_id
        )

        print(
            "ETL operational summary:"
        )

        print(
            f"files_detected="
            f"{summary['files_detected']}"
        )

        print(
            f"files_success="
            f"{summary['files_success']}"
        )

        print(
            f"files_rejected="
            f"{summary['files_rejected']}"
        )

        print(
            f"files_failed="
            f"{summary['files_failed']}"
        )

        print(
            f"records_read="
            f"{summary['records_read']}"
        )

        print(
            f"records_loaded="
            f"{summary['records_loaded']}"
        )

    # ==================================================
    # DAG GRAPH
    # ==================================================

    preflight_result = (
        preflight()
    )

    etl_run_id = (
        start_run(
            preflight_result
        )
    )

    discovered_files = (
        discover_files(
            etl_run_id
        )
    )

    mapped_results = (
        process_file
        .partial(
            etl_run_id=etl_run_id
        )
        .expand(
            file_data=discovered_files
        )
    )

    finalized_run_id = (
        finalize_run(
            etl_run_id=etl_run_id
        )
    )

    mapped_results >> finalized_run_id

    alerted_run_id = (
        emit_alerts(
            finalized_run_id
        )
    )

    validate_run(
        alerted_run_id
    )

etl_visitas_daily()