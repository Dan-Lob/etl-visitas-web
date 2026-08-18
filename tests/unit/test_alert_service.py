from unittest.mock import MagicMock

from etl_visitas.operations.alert_service import (
    AlertService,
)
from etl_visitas.operations.incident_classifier import (
    IncidentCategory,
    IncidentSeverity,
)


def _create_service_with_rows(
    rows: list[dict],
) -> AlertService:

    cursor = MagicMock()
    cursor.fetchall.return_value = rows

    cursor_context = MagicMock()
    cursor_context.__enter__.return_value = (
        cursor
    )

    connection = MagicMock()
    connection.cursor.return_value = (
        cursor_context
    )

    connection_context = MagicMock()
    connection_context.__enter__.return_value = (
        connection
    )

    connection_factory = MagicMock()
    connection_factory.connection.return_value = (
        connection_context
    )

    return AlertService(
        connection_factory
    )


def test_success_does_not_generate_alert():

    service = _create_service_with_rows(
        [
            {
                "file_name": "report_8.txt",
                "status": "SUCCESS",
                "error_code": None,
                "error_message": None,
            }
        ]
    )

    alerts = service.get_run_alerts(
        "run-test"
    )

    assert alerts == []


def test_duplicate_does_not_generate_alert():

    service = _create_service_with_rows(
        [
            {
                "file_name": "report_8.txt",
                "status": (
                    "SKIPPED_ALREADY_PROCESSED"
                ),
                "error_code": (
                    "ALREADY_PROCESSED"
                ),
                "error_message": (
                    "File already processed"
                ),
            }
        ]
    )

    alerts = service.get_run_alerts(
        "run-test"
    )

    assert alerts == []


def test_rejected_layout_generates_warning():

    service = _create_service_with_rows(
        [
            {
                "file_name": "report_7.txt",
                "status": "REJECTED_LAYOUT",
                "error_code": "INVALID_HEADER",
                "error_message": (
                    "Unexpected header"
                ),
            }
        ]
    )

    alerts = service.get_run_alerts(
        "run-test"
    )

    assert len(alerts) == 1

    alert = alerts[0]

    assert (
        alert.severity
        == IncidentSeverity.WARNING
    )

    assert (
        alert.category
        == IncidentCategory.DATA_QUALITY
    )


def test_failed_generates_error_alert():

    service = _create_service_with_rows(
        [
            {
                "file_name": "report_8.txt",
                "status": "FAILED",
                "error_code": (
                    "DATABASE_LOAD_ERROR"
                ),
                "error_message": (
                    "Database unavailable"
                ),
            }
        ]
    )

    alerts = service.get_run_alerts(
        "run-test"
    )

    assert len(alerts) == 1

    assert (
        alerts[0].severity
        == IncidentSeverity.ERROR
    )

    assert (
        alerts[0].category
        == IncidentCategory.TECHNICAL
    )


def test_reconciliation_generates_critical_alert():

    service = _create_service_with_rows(
        [
            {
                "file_name": "report_8.txt",
                "status": "FAILED",
                "error_code": (
                    "RECONCILIATION_ERROR"
                ),
                "error_message": (
                    "Record counts do not match"
                ),
            }
        ]
    )

    alerts = service.get_run_alerts(
        "run-test"
    )

    assert len(alerts) == 1

    assert (
        alerts[0].severity
        == IncidentSeverity.CRITICAL
    )

    assert (
        alerts[0].category
        == IncidentCategory.INTEGRITY
    )