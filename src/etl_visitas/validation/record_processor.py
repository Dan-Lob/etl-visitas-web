from dataclasses import dataclass

from etl_visitas.ingestion.record_reader import (
    read_records,
)
from etl_visitas.models.record_models import (
    RecordError,
    VisitRecord,
)
from etl_visitas.validation.record_validator import (
    RecordValidator,
)


@dataclass(frozen=True)
class RecordProcessingResult:
    valid_records: list[VisitRecord]
    invalid_records: list[RecordError]

    @property
    def records_read(self) -> int:
        return (
            len(self.valid_records)
            + len(self.invalid_records)
        )

    @property
    def records_valid(self) -> int:
        return len(self.valid_records)

    @property
    def records_invalid(self) -> int:
        return len(self.invalid_records)


def process_records(
    file_path: str,
    source_file: str,
) -> RecordProcessingResult:

    validator = RecordValidator()

    valid_records: list[VisitRecord] = []
    invalid_records: list[RecordError] = []

    for line_number, row, raw_record in read_records(
        file_path
    ):
        result = validator.validate(
            row=row,
            raw_record=raw_record,
            source_file=source_file,
            line_number=line_number,
        )

        if isinstance(result, VisitRecord):
            valid_records.append(result)
        else:
            invalid_records.append(result)

    return RecordProcessingResult(
        valid_records=valid_records,
        invalid_records=invalid_records,
    )