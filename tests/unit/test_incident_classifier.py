from etl_visitas.operations.incident_classifier import (
    IncidentCategory,
    IncidentSeverity,
    classify_incident,
)


def test_success_is_info():
    severity, category = classify_incident(
        status="SUCCESS",
        error_code=None,
    )

    assert severity == IncidentSeverity.INFO
    assert category == IncidentCategory.NONE


def test_rejected_layout_is_data_quality_warning():
    severity, category = classify_incident(
        status="REJECTED_LAYOUT",
        error_code="INVALID_HEADER",
    )

    assert severity == IncidentSeverity.WARNING

    assert (
        category
        == IncidentCategory.DATA_QUALITY
    )


def test_reconciliation_error_is_critical():
    severity, category = classify_incident(
        status="FAILED",
        error_code="RECONCILIATION_ERROR",
    )

    assert (
        severity
        == IncidentSeverity.CRITICAL
    )

    assert (
        category
        == IncidentCategory.INTEGRITY
    )


def test_pending_backup_requires_operational_action():
    severity, category = classify_incident(
        status="PENDING_BACKUP",
        error_code="BACKUP_ERROR",
    )

    assert severity == IncidentSeverity.ERROR

    assert (
        category
        == IncidentCategory.OPERATIONAL
    )


def test_duplicate_is_informational():
    severity, category = classify_incident(
        status="SKIPPED_ALREADY_PROCESSED",
        error_code="ALREADY_PROCESSED",
    )

    assert severity == IncidentSeverity.INFO

    assert (
        category
        == IncidentCategory.IDEMPOTENCY
    )