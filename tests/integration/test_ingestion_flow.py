from etl_visitas.config.settings import load_settings
from etl_visitas.models.enums import FileStatus
from etl_visitas.services.ingestion_service import (
    IngestionService,
)


def test_real_local_ingestion_flow():
    settings = load_settings()

    service = IngestionService(settings)

    results = service.run()

    by_name = {
        result.staged_file.file_name: result
        for result in results
    }

    assert "report_7.txt" in by_name
    assert "report_8.txt" in by_name
    assert "report_9.txt" in by_name

    assert (
        by_name["report_7.txt"].status
        == FileStatus.REJECTED_LAYOUT
    )

    assert (
        by_name["report_8.txt"].status
        == FileStatus.PREPARED
    )

    assert (
        by_name["report_9.txt"].status
        == FileStatus.REJECTED_LAYOUT
    )

    report_8 = by_name["report_8.txt"]

    assert report_8.records is not None

    assert (
        report_8.records.records_read
        ==
        report_8.records.records_valid
        +
        report_8.records.records_invalid
    )