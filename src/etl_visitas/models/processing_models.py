from dataclasses import dataclass

from etl_visitas.models.enums import FileStatus
from etl_visitas.models.file_models import (
    FileValidationResult,
    StagedFile,
)
from etl_visitas.validation.record_processor import (
    RecordProcessingResult,
)


@dataclass(frozen=True)
class FileProcessingResult:
    staged_file: StagedFile
    status: FileStatus
    file_validation: FileValidationResult
    records: RecordProcessingResult | None