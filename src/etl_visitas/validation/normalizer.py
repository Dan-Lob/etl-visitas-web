from datetime import datetime

from etl_visitas.config.schema import SOURCE_DATE_FORMAT


def normalize_optional_text(
    value: str | None,
) -> str | None:
    """
    Normaliza campos de texto opcionales.

    Reglas:
    - None -> None
    - "" -> None
    - "-" -> None
    - Elimina espacios al inicio y final.
    """

    if value is None:
        return None

    cleaned = value.strip()

    if cleaned in {"", "-"}:
        return None

    return cleaned


def normalize_email(value: str) -> str:
    """
    Normaliza el email eliminando espacios
    y convirtiéndolo a minúsculas.
    """

    return value.strip().lower()


def normalize_datetime(
    value: str | None,
    required: bool = False,
) -> datetime | None:
    """
    Convierte una fecha del formato fuente:

        dd/MM/yyyy HH:mm

    a un objeto datetime de Python.

    Para campos opcionales:
    - None -> None
    - "" -> None
    - "-" -> None

    Para campos obligatorios:
    esos mismos valores generan ValueError.
    """

    if value is None:
        if required:
            raise ValueError(
                "Required datetime is missing"
            )

        return None

    cleaned = value.strip()

    if cleaned in {"", "-"}:
        if required:
            raise ValueError(
                "Required datetime is missing"
            )

        return None

    return datetime.strptime(
        cleaned,
        SOURCE_DATE_FORMAT,
    )


def normalize_non_negative_integer(
    value: str | None,
) -> int:
    """
    Normaliza métricas numéricas como:

    - Opens
    - Opens virales
    - Clicks
    - Clicks virales

    Reglas:
    - None -> 0
    - "" -> 0
    - "-" -> 0
    - Debe ser un entero >= 0
    """

    if value is None:
        return 0

    cleaned = value.strip()

    if cleaned in {"", "-"}:
        return 0

    parsed = int(cleaned)

    if parsed < 0:
        raise ValueError(
            f"Expected non-negative integer, found {parsed}"
        )

    return parsed