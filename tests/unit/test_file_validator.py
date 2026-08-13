from pathlib import Path

from etl_visitas.models.enums import ErrorCode
from etl_visitas.validation.file_validator import FileValidator


SAMPLE_DATA_DIR = (
    Path(__file__)
    .resolve()
    .parents[2]
    / "data"
    / "sample"
)


def test_report_7_has_invalid_header():
    validator = FileValidator()

    result = validator.validate(
        str(SAMPLE_DATA_DIR / "report_7.txt")
    )

    assert result.is_valid is False
    assert result.error_code == ErrorCode.INVALID_HEADER
    assert "expected 'jyv'" in result.error_message
    assert "found 'jk'" in result.error_message


def test_report_8_has_valid_layout():
    validator = FileValidator()

    result = validator.validate(
        str(SAMPLE_DATA_DIR / "report_8.txt")
    )

    assert result.is_valid is True
    assert result.error_code is None
    assert result.error_message is None


def test_report_9_has_invalid_header():
    validator = FileValidator()

    result = validator.validate(
        str(SAMPLE_DATA_DIR / "report_9.txt")
    )

    assert result.is_valid is False
    assert result.error_code == ErrorCode.INVALID_HEADER
    assert "expected 'jyv'" in result.error_message
    assert "found 'fgh'" in result.error_message

def test_empty_file_is_rejected(tmp_path):
    file_path = tmp_path / "report_100.txt"
    file_path.write_text("")

    validator = FileValidator()
    result = validator.validate(str(file_path))

    assert result.is_valid is False
    assert result.error_code == ErrorCode.EMPTY_FILE


def test_invalid_filename_is_rejected(tmp_path):
    file_path = tmp_path / "visitas_100.txt"

    file_path.write_text(
        ",".join(
            [
                "email",
                "jyv",
                "Badmail",
                "Baja",
                "Fecha envio",
                "Fecha open",
                "Opens",
                "Opens virales",
                "Fecha click",
                "Clicks",
                "Clicks virales",
                "Links",
                "IPs",
                "Navegadores",
                "Plataformas",
            ]
        )
    )

    validator = FileValidator()
    result = validator.validate(str(file_path))

    assert result.is_valid is False
    assert result.error_code == ErrorCode.INVALID_FILENAME


def test_invalid_column_count_is_rejected(tmp_path):
    file_path = tmp_path / "report_100.txt"

    file_path.write_text(
        "email,jyv,Badmail\n",
        encoding="utf-8",
    )

    validator = FileValidator()
    result = validator.validate(str(file_path))

    assert result.is_valid is False
    assert result.error_code == ErrorCode.INVALID_COLUMN_COUNT