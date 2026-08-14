import re

from etl_visitas.config.schema import FILE_NAME_PATTERN
from etl_visitas.models.file_models import RemoteFileMetadata


def filter_candidate_files(
    files: list[RemoteFileMetadata],
) -> list[RemoteFileMetadata]:

    pattern = re.compile(FILE_NAME_PATTERN)

    candidates = [
        file
        for file in files
        if pattern.fullmatch(file.file_name)
    ]

    return sorted(
        candidates,
        key=lambda file: file.file_name,
    )