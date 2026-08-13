import csv
import re
from pathlib import Path

from etl_visitas.config.schema import (
    EXPECTED_COLUMNS,
    EXPECTED_COLUMN_COUNT,
    FILE_NAME_PATTERN,
)
from etl_visitas.models.enums import ErrorCode
from etl_visitas.models.file_models import FileValidationResult


class FileValidator:

    def validate(self, file_path: str) -> FileValidationResult:
        path = Path(file_path)

        filename_result = self._validate_filename(path.name)
        if not filename_result.is_valid:
            return filename_result

        empty_result = self._validate_not_empty(path)
        if not empty_result.is_valid:
            return empty_result

        header_result = self._validate_header(path)
        if not header_result.is_valid:
            return header_result

        return FileValidationResult(is_valid=True)

    def _validate_filename(
        self,
        file_name: str,
    ) -> FileValidationResult:

        if not re.fullmatch(FILE_NAME_PATTERN, file_name):
            return FileValidationResult(
                is_valid=False,
                error_code=ErrorCode.INVALID_FILENAME,
                error_message=(
                    f"Invalid file name '{file_name}'. "
                    "Expected pattern report_<number>.txt"
                ),
            )

        return FileValidationResult(is_valid=True)

    def _validate_not_empty(
        self,
        path: Path,
    ) -> FileValidationResult:

        if not path.exists():
            return FileValidationResult(
                is_valid=False,
                error_code=ErrorCode.EMPTY_FILE,
                error_message=f"File does not exist: {path}",
            )

        if path.stat().st_size == 0:
            return FileValidationResult(
                is_valid=False,
                error_code=ErrorCode.EMPTY_FILE,
                error_message=f"File is empty: {path.name}",
            )

        return FileValidationResult(is_valid=True)

    def _validate_header(
        self,
        path: Path,
    ) -> FileValidationResult:

        with path.open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as file:
            reader = csv.reader(file)

            try:
                header = next(reader)
            except StopIteration:
                return FileValidationResult(
                    is_valid=False,
                    error_code=ErrorCode.EMPTY_FILE,
                    error_message=f"File has no header: {path.name}",
                )

        if len(header) != EXPECTED_COLUMN_COUNT:
            return FileValidationResult(
                is_valid=False,
                error_code=ErrorCode.INVALID_COLUMN_COUNT,
                error_message=(
                    f"Expected {EXPECTED_COLUMN_COUNT} columns, "
                    f"found {len(header)} in file {path.name}"
                ),
            )

        if header != EXPECTED_COLUMNS:
            differences = self._find_header_differences(header)

            return FileValidationResult(
                is_valid=False,
                error_code=ErrorCode.INVALID_HEADER,
                error_message=(
                    f"Invalid header in file {path.name}. "
                    f"Differences: {differences}"
                ),
            )

        return FileValidationResult(is_valid=True)

    def _find_header_differences(
        self,
        actual_header: list[str],
    ) -> list[str]:

        differences: list[str] = []

        for index, (expected, actual) in enumerate(
            zip(EXPECTED_COLUMNS, actual_header),
            start=1,
        ):
            if expected != actual:
                differences.append(
                    f"column {index}: expected '{expected}', "
                    f"found '{actual}'"
                )

        return differences