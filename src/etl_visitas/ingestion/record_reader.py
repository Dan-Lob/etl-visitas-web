import csv
from pathlib import Path
from typing import Iterator


def read_records(
    file_path: str,
) -> Iterator[tuple[int, list[str], str]]:

    path = Path(file_path)

    with path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as file:

        # Saltamos header
        header = file.readline()

        line_number = 1

        for raw_line in file:
            line_number += 1

            raw_record = raw_line.rstrip(
                "\r\n"
            )

            row = next(
                csv.reader([raw_record])
            )

            yield (
                line_number,
                row,
                raw_record,
            )