from __future__ import annotations

import logging
from dataclasses import dataclass

from etl_visitas.operations.incident_classifier import (
    IncidentCategory,
    IncidentSeverity,
    classify_incident,
)
from etl_visitas.repositories.mysql_connection import (
    MySQLConnectionFactory,
)


logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OperationalAlert:
    run_id: str
    file_name: str | None
    status: str
    severity: IncidentSeverity
    category: IncidentCategory
    error_code: str | None
    error_message: str | None


class AlertService:

    def __init__(
        self,
        connection_factory: MySQLConnectionFactory,
    ) -> None:
        self._connection_factory = (
            connection_factory
        )

    def get_run_alerts(
        self,
        run_id: str,
    ) -> list[OperationalAlert]:
        """
        Returns only conditions that require
        operational visibility.

        INFO events are intentionally excluded.
        """

        sql = """
        SELECT
            file_name,
            status,
            error_code,
            error_message
        FROM etl_file_control
        WHERE run_id = %s
        ORDER BY file_id
        """

        with (
            self._connection_factory
            .connection()
            as connection
        ):
            with connection.cursor() as cursor:
                cursor.execute(
                    sql,
                    (run_id,),
                )

                rows = cursor.fetchall()

        alerts: list[OperationalAlert] = []

        for row in rows:

            severity, category = (
                classify_incident(
                    status=row["status"],
                    error_code=row["error_code"],
                )
            )

            if severity == IncidentSeverity.INFO:
                continue

            alerts.append(
                OperationalAlert(
                    run_id=run_id,
                    file_name=row["file_name"],
                    status=row["status"],
                    severity=severity,
                    category=category,
                    error_code=row["error_code"],
                    error_message=row["error_message"],
                )
            )

        return alerts

    def emit_run_alerts(
        self,
        run_id: str,
    ) -> list[OperationalAlert]:
        """
        Emits structured alerts to the application log.

        The notification transport is intentionally
        decoupled so it can later be replaced by
        email, Teams, Slack or Cloud Monitoring.
        """

        alerts = self.get_run_alerts(
            run_id
        )

        if not alerts:
            logger.info(
                "ETL_ALERT "
                "run_id=%s "
                "severity=INFO "
                "message='No operational "
                "alerts detected'",
                run_id,
            )

            return alerts

        for alert in alerts:

            message = (
                "ETL_ALERT "
                f"run_id={alert.run_id} "
                f"file_name={alert.file_name} "
                f"status={alert.status} "
                f"severity={alert.severity.value} "
                f"category={alert.category.value} "
                f"error_code={alert.error_code} "
                f"error_message={alert.error_message}"
            )

            if (
                alert.severity
                == IncidentSeverity.CRITICAL
            ):
                logger.critical(message)

            elif (
                alert.severity
                == IncidentSeverity.ERROR
            ):
                logger.error(message)

            elif (
                alert.severity
                == IncidentSeverity.WARNING
            ):
                logger.warning(message)

            else:
                logger.info(message)

        return alerts