from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from bigdata.pyspark.transform_visitas import (
    add_validation_columns,
    create_statistics,
    create_visitors,
    normalize_source,
    read_source,
)


@pytest.fixture(scope="session")
def spark() -> SparkSession:

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName(
            "etl-visitas-pyspark-tests"
        )
        .config(
            "spark.sql.session.timeZone",
            "America/Mexico_City",
        )
        .getOrCreate()
    )

    yield session

    session.stop()


def test_valid_report_has_503_valid_records(
    spark: SparkSession,
):

    source_df = read_source(
        spark=spark,
        input_path=(
            "/app/input/report_8.txt"
        ),
    )

    normalized_df = normalize_source(
        source_df=source_df,
        run_id="spark-test-valid",
    )

    validated_df = (
        add_validation_columns(
            normalized_df
        )
    )

    valid_count = (
        validated_df
        .filter("is_valid")
        .count()
    )

    invalid_count = (
        validated_df
        .filter("NOT is_valid")
        .count()
    )

    assert source_df.count() == 503
    assert valid_count == 503
    assert invalid_count == 0


def test_invalid_report_is_split_correctly(
    spark: SparkSession,
):

    source_df = read_source(
        spark=spark,
        input_path=(
            "/app/input/"
            "report_pyspark_invalid_test.txt"
        ),
    )

    normalized_df = normalize_source(
        source_df=source_df,
        run_id="spark-test-invalid",
    )

    validated_df = (
        add_validation_columns(
            normalized_df
        )
    )

    valid_df = (
        validated_df
        .filter("is_valid")
    )

    invalid_df = (
        validated_df
        .filter("NOT is_valid")
    )

    assert source_df.count() == 4
    assert valid_df.count() == 1
    assert invalid_df.count() == 3


def test_invalid_report_has_expected_error_codes(
    spark: SparkSession,
):

    source_df = read_source(
        spark=spark,
        input_path=(
            "/app/input/"
            "report_pyspark_invalid_test.txt"
        ),
    )

    validated_df = (
        add_validation_columns(
            normalize_source(
                source_df=source_df,
                run_id=(
                    "spark-test-errors"
                ),
            )
        )
    )

    rows = (
        validated_df
        .filter("NOT is_valid")
        .select(
            "email_raw",
            "error_codes",
        )
        .collect()
    )

    errors_by_email = {
        row["email_raw"]:
            row["error_codes"]
        for row in rows
    }

    assert (
        errors_by_email[
            "email_invalido"
        ]
        == ["INVALID_EMAIL"]
    )

    assert (
        errors_by_email[
            "fecha.invalida@example.com"
        ]
        == [
            "INVALID_FECHA_ENVIO"
        ]
    )

    assert (
        errors_by_email[
            "numero.invalido@example.com"
        ]
        == ["INVALID_INTEGER"]
    )


def test_statistics_contains_only_valid_records(
    spark: SparkSession,
):

    source_df = read_source(
        spark=spark,
        input_path=(
            "/app/input/"
            "report_pyspark_invalid_test.txt"
        ),
    )

    validated_df = (
        add_validation_columns(
            normalize_source(
                source_df=source_df,
                run_id=(
                    "spark-test-statistics"
                ),
            )
        )
    )

    statistics_df = (
        create_statistics(
            validated_df
        )
    )

    assert statistics_df.count() == 1

    row = statistics_df.first()

    assert (
        row["email"]
        == "usuario.valido@example.com"
    )


def test_visitors_are_aggregated_from_valid_records(
    spark: SparkSession,
):

    source_df = read_source(
        spark=spark,
        input_path=(
            "/app/input/"
            "report_pyspark_invalid_test.txt"
        ),
    )

    validated_df = (
        add_validation_columns(
            normalize_source(
                source_df=source_df,
                run_id=(
                    "spark-test-visitors"
                ),
            )
        )
    )

    statistics_df = (
        create_statistics(
            validated_df
        )
    )

    visitors_df = (
        create_visitors(
            statistics_df=(
                statistics_df
            ),
            reference_date=(
                "2013-02-15"
            ),
        )
    )

    assert visitors_df.count() == 1

    visitor = visitors_df.first()

    assert (
        visitor["email"]
        == "usuario.valido@example.com"
    )

    assert (
        visitor["visitas_totales"]
        == 1
    )

    assert (
        visitor[
            "visitas_anio_actual"
        ]
        == 1
    )

    assert (
        visitor[
            "visitas_mes_actual"
        ]
        == 1
    )