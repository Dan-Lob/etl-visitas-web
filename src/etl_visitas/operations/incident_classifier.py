from enum import Enum


class IncidentSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class IncidentCategory(str, Enum):
    NONE = "NONE"
    DATA_QUALITY = "DATA_QUALITY"
    TECHNICAL = "TECHNICAL"
    INTEGRITY = "INTEGRITY"
    OPERATIONAL = "OPERATIONAL"
    IDEMPOTENCY = "IDEMPOTENCY"


def classify_incident(
    status: str,
    error_code: str | None,
) -> tuple[
    IncidentSeverity,
    IncidentCategory,
]:

    if status == "SUCCESS":
        return (
            IncidentSeverity.INFO,
            IncidentCategory.NONE,
        )

    if status == "SKIPPED_ALREADY_PROCESSED":
        return (
            IncidentSeverity.INFO,
            IncidentCategory.IDEMPOTENCY,
        )

    if status == "REJECTED_LAYOUT":
        return (
            IncidentSeverity.WARNING,
            IncidentCategory.DATA_QUALITY,
        )

    if status in {
        "PENDING_BACKUP",
        "PENDING_SOURCE_DELETE",
    }:
        return (
            IncidentSeverity.ERROR,
            IncidentCategory.OPERATIONAL,
        )

    if error_code == "RECONCILIATION_ERROR":
        return (
            IncidentSeverity.CRITICAL,
            IncidentCategory.INTEGRITY,
        )

    if status == "FAILED":
        return (
            IncidentSeverity.ERROR,
            IncidentCategory.TECHNICAL,
        )

    return (
        IncidentSeverity.WARNING,
        IncidentCategory.TECHNICAL,
    )