from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class RunContext:
    run_id: str
    dag_id: str
    execution_date: datetime
    start_time: datetime


@dataclass(frozen=True)
class FileControlRecord:
    file_id: int
    run_id: str
    file_name: str
    checksum_sha256: str | None
    status: str