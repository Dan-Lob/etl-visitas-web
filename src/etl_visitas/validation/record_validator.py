import csv
from io import StringIO

from etl_visitas.config.schema import EXPECTED_COLUMN_COUNT
from etl_visitas.models.enums import ErrorCode
from etl_visitas.models.record_models import (
    RecordError,
    VisitRecord,
)
from etl_visitas.validation.normalizer import (
    normalize_datetime,
    normalize_email,
    normalize_non_negative_integer,
    normalize_optional_text,
)
from etl_visitas.validation.rules import is_valid_email


class RecordValidator:

    def validate(
        self,
        row: list[str],
        raw_record: str,
        source_file: str,
        line_number: int,
    ) -> VisitRecord | RecordError:

        errors: list[ErrorCode] = []
        descriptions: list[str] = []

        if len(row) != EXPECTED_COLUMN_COUNT:
            return RecordError(
                line_number=line_number,
                email=row[0] if row else None,
                error_codes=(
                    ErrorCode.INVALID_COLUMN_COUNT,
                ),
                error_description=(
                    f"Expected {EXPECTED_COLUMN_COUNT} columns, "
                    f"found {len(row)}"
                ),
                raw_record=raw_record,
            )

        email_raw = row[0]

        if not is_valid_email(email_raw):
            errors.append(ErrorCode.INVALID_EMAIL)
            descriptions.append(
                f"Invalid email: {email_raw!r}"
            )

        fecha_envio = None
        fecha_open = None
        fecha_click = None

        try:
            fecha_envio = normalize_datetime(
                row[4],
                required=True,
            )
        except ValueError:
            errors.append(
                ErrorCode.INVALID_DATE_VALUE
            )
            descriptions.append(
                f"Invalid Fecha envio: {row[4]!r}"
            )

        try:
            fecha_open = normalize_datetime(
                row[5],
                required=False,
            )
        except ValueError:
            errors.append(
                ErrorCode.INVALID_DATE_VALUE
            )
            descriptions.append(
                f"Invalid Fecha open: {row[5]!r}"
            )

        try:
            fecha_click = normalize_datetime(
                row[8],
                required=False,
            )
        except ValueError:
            errors.append(
                ErrorCode.INVALID_DATE_VALUE
            )
            descriptions.append(
                f"Invalid Fecha click: {row[8]!r}"
            )

        numeric_values: dict[str, int] = {}

        numeric_fields = {
            "opens": row[6],
            "opens_virales": row[7],
            "clicks": row[9],
            "clicks_virales": row[10],
        }

        for field_name, value in numeric_fields.items():
            try:
                numeric_values[field_name] = (
                    normalize_non_negative_integer(value)
                )
            except (ValueError, TypeError):
                errors.append(
                    ErrorCode.INVALID_INTEGER
                )
                descriptions.append(
                    f"Invalid {field_name}: {value!r}"
                )

        if errors:
            return RecordError(
                line_number=line_number,
                email=(
                    normalize_email(email_raw)
                    if email_raw
                    else None
                ),
                error_codes=tuple(errors),
                error_description="; ".join(
                    descriptions
                ),
                raw_record=raw_record,
            )

        return VisitRecord(
            email=normalize_email(email_raw),
            jyv=normalize_optional_text(row[1]),
            badmail=normalize_optional_text(row[2]),
            baja=normalize_optional_text(row[3]),
            fecha_envio=fecha_envio,
            fecha_open=fecha_open,
            opens=numeric_values["opens"],
            opens_virales=numeric_values[
                "opens_virales"
            ],
            fecha_click=fecha_click,
            clicks=numeric_values["clicks"],
            clicks_virales=numeric_values[
                "clicks_virales"
            ],
            links=normalize_optional_text(row[11]),
            ips=normalize_optional_text(row[12]),
            navegadores=normalize_optional_text(
                row[13]
            ),
            plataformas=normalize_optional_text(
                row[14]
            ),
            source_file=source_file,
            source_line=line_number,
        )