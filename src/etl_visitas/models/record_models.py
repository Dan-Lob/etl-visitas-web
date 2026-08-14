from dataclasses import dataclass
from datetime import datetime

from etl_visitas.models.enums import ErrorCode


@dataclass(frozen=True)
class VisitRecord:
    email: str
    jyv: str | None
    badmail: str | None
    baja: str | None

    fecha_envio: datetime
    fecha_open: datetime | None

    opens: int
    opens_virales: int

    fecha_click: datetime | None

    clicks: int
    clicks_virales: int

    links: str | None
    ips: str | None
    navegadores: str | None
    plataformas: str | None

    source_file: str
    source_line: int


@dataclass(frozen=True)
class RecordError:
    line_number: int
    email: str | None
    error_codes: tuple[ErrorCode, ...]
    error_description: str
    raw_record: str