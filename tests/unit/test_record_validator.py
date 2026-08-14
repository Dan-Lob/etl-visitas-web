from datetime import datetime

from etl_visitas.models.enums import ErrorCode
from etl_visitas.models.record_models import (
    RecordError,
    VisitRecord,
)
from etl_visitas.validation.record_validator import (
    RecordValidator,
)


def make_valid_row() -> list[str]:
    return [
        "USER@example.com",
        "",
        "",
        "",
        "08/02/2013 18:30",
        "-",
        "0",
        "0",
        "-",
        "0",
        "0",
        "-",
        "-",
        "-",
        "-",
    ]


def test_valid_record_is_normalized():
    validator = RecordValidator()

    row = make_valid_row()

    result = validator.validate(
        row=row,
        raw_record=",".join(row),
        source_file="report_100.txt",
        line_number=2,
    )

    assert isinstance(result, VisitRecord)

    assert result.email == "user@example.com"

    assert result.fecha_envio == datetime(
        2013,
        2,
        8,
        18,
        30,
    )

    assert result.fecha_open is None
    assert result.fecha_click is None

    assert result.opens == 0
    assert result.clicks == 0

    assert result.links is None
    assert result.ips is None


def test_invalid_email_is_rejected():
    validator = RecordValidator()

    row = make_valid_row()
    row[0] = "usuario@@example.com"

    result = validator.validate(
        row=row,
        raw_record=",".join(row),
        source_file="report_100.txt",
        line_number=2,
    )

    assert isinstance(result, RecordError)

    assert ErrorCode.INVALID_EMAIL in (
        result.error_codes
    )


def test_invalid_date_is_rejected():
    validator = RecordValidator()

    row = make_valid_row()
    row[4] = "31/02/2013 18:30"

    result = validator.validate(
        row=row,
        raw_record=",".join(row),
        source_file="report_100.txt",
        line_number=2,
    )

    assert isinstance(result, RecordError)

    assert ErrorCode.INVALID_DATE_VALUE in (
        result.error_codes
    )


def test_invalid_integer_is_rejected():
    validator = RecordValidator()

    row = make_valid_row()
    row[6] = "ABC"

    result = validator.validate(
        row=row,
        raw_record=",".join(row),
        source_file="report_100.txt",
        line_number=2,
    )

    assert isinstance(result, RecordError)

    assert ErrorCode.INVALID_INTEGER in (
        result.error_codes
    )


def test_negative_integer_is_rejected():
    validator = RecordValidator()

    row = make_valid_row()
    row[9] = "-3"

    result = validator.validate(
        row=row,
        raw_record=",".join(row),
        source_file="report_100.txt",
        line_number=2,
    )

    assert isinstance(result, RecordError)

    assert ErrorCode.INVALID_INTEGER in (
        result.error_codes
    )


def test_multiple_errors_are_collected():
    validator = RecordValidator()

    row = make_valid_row()

    row[0] = "invalid-email"
    row[4] = "31/02/2013 18:30"
    row[6] = "ABC"

    result = validator.validate(
        row=row,
        raw_record=",".join(row),
        source_file="report_100.txt",
        line_number=2,
    )

    assert isinstance(result, RecordError)

    assert ErrorCode.INVALID_EMAIL in (
        result.error_codes
    )

    assert ErrorCode.INVALID_DATE_VALUE in (
        result.error_codes
    )

    assert ErrorCode.INVALID_INTEGER in (
        result.error_codes
    )

##Test de cantidad incorrecta de columnas

def test_invalid_column_count_is_rejected():
    validator = RecordValidator()

    row = [
        "user@example.com",
        "only",
        "three",
    ]

    result = validator.validate(
        row=row,
        raw_record=",".join(row),
        source_file="report_100.txt",
        line_number=10,
    )

    assert isinstance(result, RecordError)

    assert result.error_codes == (
        ErrorCode.INVALID_COLUMN_COUNT,
    )