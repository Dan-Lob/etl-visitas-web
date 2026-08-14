import hashlib

from etl_visitas.ingestion.checksum import (
    calculate_sha256,
)


def test_calculate_sha256(tmp_path):
    file_path = tmp_path / "test.txt"

    content = b"hello-etl"
    file_path.write_bytes(content)

    expected = hashlib.sha256(
        content
    ).hexdigest()

    result = calculate_sha256(
        str(file_path)
    )

    assert result == expected
    assert len(result) == 64