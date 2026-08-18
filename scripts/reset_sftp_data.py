from pathlib import Path
import shutil


PROJECT_ROOT = (
    Path(__file__)
    .resolve()
    .parents[1]
)

SAMPLE_DIR = (
    PROJECT_ROOT
    / "data"
    / "sample"
)

SFTP_DIR = (
    PROJECT_ROOT
    / "data"
    / "sftp"
)


def reset_sftp_data() -> None:
    SFTP_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    for item in SFTP_DIR.iterdir():
        if item.is_file():
            item.unlink()
        elif item.is_dir():
            shutil.rmtree(item)

    sample_files = list(
        SAMPLE_DIR.glob("report_*.txt")
    )

    if not sample_files:
        raise RuntimeError(
            "No sample report files found "
            f"in {SAMPLE_DIR}"
        )

    for source_file in sample_files:
        destination = (
            SFTP_DIR
            / source_file.name
        )

        shutil.copy2(
            source_file,
            destination,
        )

    print(
        f"SFTP test data restored: "
        f"{len(sample_files)} files"
    )


if __name__ == "__main__":
    reset_sftp_data()