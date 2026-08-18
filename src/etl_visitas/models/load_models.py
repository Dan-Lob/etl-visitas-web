from dataclasses import dataclass


@dataclass(frozen=True)
class FileLoadResult:
    statistics_inserted: int
    errors_inserted: int

    visitors_inserted: int
    visitors_updated: int

    records_loaded: int