from dataclasses import dataclass


@dataclass(frozen=True)
class DetailLoadResult:
    statistics_inserted: int
    errors_inserted: int