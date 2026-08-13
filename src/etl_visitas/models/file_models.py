from dataclasses import dataclass
from datetime import datetime

from etl_visitas.models.enums import ErrorCode


@dataclass(frozen=True)
class RemoteFileMetadata:
    file_name: str
    remote_path: str
    size_bytes: int
    modified_at: datetime | None = None


@dataclass(frozen=True)
class StagedFile:
    file_name: str
    remote_path: str
    local_path: str
    size_bytes: int
    checksum_sha256: str


@dataclass(frozen=True)
class FileValidationResult:
    is_valid: bool
    error_code: ErrorCode | None = None
    error_message: str | None = None