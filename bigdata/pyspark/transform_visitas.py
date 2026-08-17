from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path
from uuid import uuid4

from pyspark.sql import (
    DataFrame,
    SparkSession,
)
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
)
import os

DATE_FORMAT = "dd/MM/yyyy HH:mm"


INPUT_SCHEMA = StructType(
    [
        StructField(
            "email",
            StringType(),
            True,
        ),
        StructField(
            "jyv",
            StringType(),
            True,
        ),
        StructField(
            "Badmail",
            StringType(),
            True,
        ),
        StructField(
            "Baja",
            StringType(),
            True,
        ),
        StructField(
            "Fecha envio",
            StringType(),
            True,
        ),
        StructField(
            "Fecha open",
            StringType(),
            True,
        ),
        StructField(
            "Opens",
            StringType(),
            True,
        ),
        StructField(
            "Opens virales",
            StringType(),
            True,
        ),
        StructField(
            "Fecha click",
            StringType(),
            True,
        ),
        StructField(
            "Clicks",
            StringType(),
            True,
        ),
        StructField(
            "Clicks virales",
            StringType(),
            True,
        ),
        StructField(
            "Links",
            StringType(),
            True,
        ),
        StructField(
            "IPs",
            StringType(),
            True,
        ),
        StructField(
            "Navegadores",
            StringType(),
            True,
        ),
        StructField(
            "Plataformas",
            StringType(),
            True,
        ),
    ]
)


EMAIL_PATTERN = (
    r"^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+"
    r"@[A-Za-z0-9-]+"
    r"(?:\.[A-Za-z0-9-]+)+$"
)


INTEGER_PATTERN = r"^\d+$"


NUMERIC_SOURCE_COLUMNS = [
    "opens_raw",
    "opens_virales_raw",
    "clicks_raw",
    "clicks_virales_raw",
]


def parse_arguments():
    parser = argparse.ArgumentParser(
        description=(
            "PySpark Big Data variant "
            "for ETL Visitas"
        )
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Input TXT/CSV path",
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Base output path",
    )

    parser.add_argument(
        "--reference-date",
        default=date.today().isoformat(),
        help=(
            "Reference date used for "
            "current year/month metrics. "
            "Format YYYY-MM-DD."
        ),
    )

    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional processing run ID",
    )

    return parser.parse_args()


def create_spark_session() -> SparkSession:

    builder = (
        SparkSession.builder
        .appName(
            "etl-visitas-pyspark"
        )
        .config(
            "spark.sql.session.timeZone",
            "America/Mexico_City",
        )
    )

    spark_master = os.getenv(
        "SPARK_MASTER"
    )

    if spark_master:
        builder = builder.master(
            spark_master
        )

    return builder.getOrCreate()


def read_source(
    spark: SparkSession,
    input_path: str,
) -> DataFrame:

    return (
        spark.read
        .schema(INPUT_SCHEMA)
        .option(
            "header",
            True,
        )
        .option(
            "delimiter",
            ",",
        )
        .option(
            "quote",
            '"',
        )
        .option(
            "escape",
            '"',
        )
        .option(
            "mode",
            "PERMISSIVE",
        )
        .csv(input_path)
    )


def normalize_nullable_string(
    column_name: str,
):
    value = F.trim(
        F.col(column_name)
    )

    return (
        F.when(
            value.isNull()
            | (value == "")
            | (value == "-"),
            F.lit(None),
        )
        .otherwise(value)
    )


def normalize_numeric_string(
    column_name: str,
):
    value = F.trim(
        F.col(column_name)
    )

    return (
        F.when(
            value.isNull()
            | (value == "")
            | (value == "-"),
            F.lit("0"),
        )
        .otherwise(value)
    )


def normalize_source(
    source_df: DataFrame,
    run_id: str,
) -> DataFrame:

    return (
        source_df

        # ------------------------------------------
        # Technical metadata
        # ------------------------------------------
        .withColumn(
            "run_id",
            F.lit(run_id),
        )
        .withColumn(
            "source_file",
            F.regexp_extract(
                F.input_file_name(),
                r"([^/\\]+)$",
                1,
            ),
        )

        # ------------------------------------------
        # Preserve raw values used for validation
        # ------------------------------------------
        .withColumn(
            "email_raw",
            F.trim(
                F.col("email")
            ),
        )
        .withColumn(
            "fecha_envio_raw",
            F.trim(
                F.col("Fecha envio")
            ),
        )
        .withColumn(
            "fecha_open_raw",
            F.trim(
                F.col("Fecha open")
            ),
        )
        .withColumn(
            "fecha_click_raw",
            F.trim(
                F.col("Fecha click")
            ),
        )
        .withColumn(
            "opens_raw",
            normalize_numeric_string(
                "Opens"
            ),
        )
        .withColumn(
            "opens_virales_raw",
            normalize_numeric_string(
                "Opens virales"
            ),
        )
        .withColumn(
            "clicks_raw",
            normalize_numeric_string(
                "Clicks"
            ),
        )
        .withColumn(
            "clicks_virales_raw",
            normalize_numeric_string(
                "Clicks virales"
            ),
        )

        # ------------------------------------------
        # Business normalization
        # ------------------------------------------
        .withColumn(
            "email",
            F.lower(
                F.trim(
                    F.col("email")
                )
            ),
        )
        .withColumn(
            "jyv",
            normalize_nullable_string(
                "jyv"
            ),
        )
        .withColumn(
            "badmail",
            normalize_nullable_string(
                "Badmail"
            ),
        )
        .withColumn(
            "baja",
            normalize_nullable_string(
                "Baja"
            ),
        )
        .withColumn(
            "links",
            normalize_nullable_string(
                "Links"
            ),
        )
        .withColumn(
            "ips",
            normalize_nullable_string(
                "IPs"
            ),
        )
        .withColumn(
            "navegadores",
            normalize_nullable_string(
                "Navegadores"
            ),
        )
        .withColumn(
            "plataformas",
            normalize_nullable_string(
                "Plataformas"
            ),
        )

        # ------------------------------------------
        # Dates
        # ------------------------------------------
        .withColumn(
            "fecha_envio",
            F.try_to_timestamp(
                F.col(
                    "fecha_envio_raw"
                ),
                F.lit(DATE_FORMAT),
            ),
        )
        .withColumn(
            "fecha_open",
            F.when(
                F.col(
                    "fecha_open_raw"
                ).isin(
                    "",
                    "-",
                )
                | F.col(
                    "fecha_open_raw"
                ).isNull(),
                F.lit(None).cast(
                    "timestamp"
                ),
            )
            .otherwise(
                F.try_to_timestamp(
                    F.col(
                        "fecha_open_raw"
                    ),
                    F.lit(DATE_FORMAT),
                )
            ),
        )
        .withColumn(
            "fecha_click",
            F.when(
                F.col(
                    "fecha_click_raw"
                ).isin(
                    "",
                    "-",
                )
                | F.col(
                    "fecha_click_raw"
                ).isNull(),
                F.lit(None).cast(
                    "timestamp"
                ),
            )
            .otherwise(
                F.try_to_timestamp(
                    F.col(
                        "fecha_click_raw"
                    ),
                    F.lit(DATE_FORMAT),
                )
            ),
        )
        # ------------------------------------------
        # Numeric normalization
        # ------------------------------------------
        .withColumn(
            "opens",
            F.expr(
                "try_cast(opens_raw AS BIGINT)"
            ),
        )
        .withColumn(
            "opens_virales",
            F.expr(
                "try_cast(opens_virales_raw AS BIGINT)"
            ),
        )
        .withColumn(
            "clicks",
            F.expr(
                "try_cast(clicks_raw AS BIGINT)"
            ),
        )
        .withColumn(
            "clicks_virales",
            F.expr(
                "try_cast(clicks_virales_raw AS BIGINT)"
            ),
        )
        # ------------------------------------------
        # Stable record identifier
        # ------------------------------------------
        .withColumn(
            "record_id",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(
                        F.col(
                            "source_file"
                        ),
                        F.lit(""),
                    ),
                    F.coalesce(
                        F.col(
                            "email_raw"
                        ),
                        F.lit(""),
                    ),
                    F.coalesce(
                        F.col(
                            "fecha_envio_raw"
                        ),
                        F.lit(""),
                    ),
                    F.coalesce(
                        F.col(
                            "fecha_open_raw"
                        ),
                        F.lit(""),
                    ),
                    F.coalesce(
                        F.col(
                            "fecha_click_raw"
                        ),
                        F.lit(""),
                    ),
                ),
                256,
            ),
        )
    )


def add_validation_columns(
    df: DataFrame,
) -> DataFrame:

    email_valid = (
        F.col("email").isNotNull()
        & F.col("email").rlike(
            EMAIL_PATTERN
        )
    )

    fecha_envio_valid = (
        F.col(
            "fecha_envio_raw"
        ).isNotNull()
        & (
            F.trim(
                F.col(
                    "fecha_envio_raw"
                )
            )
            != ""
        )
        & F.col(
            "fecha_envio"
        ).isNotNull()
    )

    fecha_open_valid = (
        F.col(
            "fecha_open_raw"
        ).isNull()
        | F.col(
            "fecha_open_raw"
        ).isin(
            "",
            "-",
        )
        | F.col(
            "fecha_open"
        ).isNotNull()
    )

    fecha_click_valid = (
        F.col(
            "fecha_click_raw"
        ).isNull()
        | F.col(
            "fecha_click_raw"
        ).isin(
            "",
            "-",
        )
        | F.col(
            "fecha_click"
        ).isNotNull()
    )

    numeric_valid = F.lit(True)

    for column_name in (
        NUMERIC_SOURCE_COLUMNS
    ):
        numeric_valid = (
            numeric_valid
            & F.col(
                column_name
            ).rlike(
                INTEGER_PATTERN
            )
        )

    error_codes = F.array_compact(
        F.array(
            F.when(
                ~email_valid,
                F.lit(
                    "INVALID_EMAIL"
                ),
            ),
            F.when(
                ~fecha_envio_valid,
                F.lit(
                    "INVALID_FECHA_ENVIO"
                ),
            ),
            F.when(
                ~fecha_open_valid,
                F.lit(
                    "INVALID_FECHA_OPEN"
                ),
            ),
            F.when(
                ~fecha_click_valid,
                F.lit(
                    "INVALID_FECHA_CLICK"
                ),
            ),
            F.when(
                ~numeric_valid,
                F.lit(
                    "INVALID_INTEGER"
                ),
            ),
        )
    )

    return (
        df
        .withColumn(
            "error_codes",
            error_codes,
        )
        .withColumn(
            "is_valid",
            F.size(
                F.col(
                    "error_codes"
                )
            )
            == 0,
        )
    )


def create_statistics(
    validated_df: DataFrame,
) -> DataFrame:

    return (
        validated_df
        .filter(
            F.col("is_valid")
        )
        .select(
            "run_id",
            "record_id",
            "source_file",
            "email",
            "jyv",
            "badmail",
            "baja",
            "fecha_envio",
            "fecha_open",
            "opens",
            "opens_virales",
            "fecha_click",
            "clicks",
            "clicks_virales",
            "links",
            "ips",
            "navegadores",
            "plataformas",
        )
        .withColumn(
            "event_year",
            F.year(
                "fecha_envio"
            ),
        )
        .withColumn(
            "event_month",
            F.month(
                "fecha_envio"
            ),
        )
    )


def create_errors(
    validated_df: DataFrame,
) -> DataFrame:

    return (
        validated_df
        .filter(
            ~F.col("is_valid")
        )
        .select(
            "run_id",
            "record_id",
            "source_file",
            F.col(
                "email_raw"
            ).alias(
                "email"
            ),
            "error_codes",
            "fecha_envio_raw",
            "fecha_open_raw",
            "fecha_click_raw",
            "opens_raw",
            "opens_virales_raw",
            "clicks_raw",
            "clicks_virales_raw",
        )
        .withColumn(
            "processing_date",
            F.current_date(),
        )
    )


def create_visitors(
    statistics_df: DataFrame,
    reference_date: str,
) -> DataFrame:

    reference = F.to_date(
        F.lit(
            reference_date
        )
    )

    return (
        statistics_df
        .groupBy(
            "email"
        )
        .agg(
            F.min(
                F.to_date(
                    "fecha_envio"
                )
            ).alias(
                "fecha_primera_visita"
            ),
            F.max(
                F.to_date(
                    "fecha_envio"
                )
            ).alias(
                "fecha_ultima_visita"
            ),
            F.count(
                F.lit(1)
            ).alias(
                "visitas_totales"
            ),
            F.sum(
                F.when(
                    F.year(
                        "fecha_envio"
                    )
                    == F.year(
                        reference
                    ),
                    1,
                ).otherwise(0)
            ).alias(
                "visitas_anio_actual"
            ),
            F.sum(
                F.when(
                    (
                        F.year(
                            "fecha_envio"
                        )
                        == F.year(
                            reference
                        )
                    )
                    & (
                        F.month(
                            "fecha_envio"
                        )
                        == F.month(
                            reference
                        )
                    ),
                    1,
                ).otherwise(0)
            ).alias(
                "visitas_mes_actual"
            ),
        )
    )


def write_outputs(
    statistics_df: DataFrame,
    errors_df: DataFrame,
    visitors_df: DataFrame,
    output_path: str,
) -> None:

    (
        statistics_df.write
        .mode("overwrite")
        .partitionBy(
            "event_year",
            "event_month",
        )
        .parquet(
            f"{output_path}/estadistica"
        )
    )

    (
        errors_df.write
        .mode("overwrite")
        .parquet(
            f"{output_path}/errores"
        )
    )

    (
        visitors_df.write
        .mode("overwrite")
        .parquet(
            f"{output_path}/visitante"
        )
    )


def print_summary(
    source_df: DataFrame,
    statistics_df: DataFrame,
    errors_df: DataFrame,
    visitors_df: DataFrame,
) -> None:

    records_read = (
        source_df.count()
    )

    valid_records = (
        statistics_df.count()
    )

    invalid_records = (
        errors_df.count()
    )

    visitors = (
        visitors_df.count()
    )

    print(
        "================================"
    )
    print(
        "PySpark ETL Visitas - Summary"
    )
    print(
        "================================"
    )

    print(
        f"records_read={records_read}"
    )

    print(
        f"records_valid={valid_records}"
    )

    print(
        f"records_invalid={invalid_records}"
    )

    print(
        f"visitors={visitors}"
    )

    print(
        "reconciliation="
        f"{records_read == valid_records + invalid_records}"
    )

    print(
        "================================"
    )


def main() -> None:

    args = parse_arguments()

    run_id = (
        args.run_id
        if args.run_id
        else (
            "spark_"
            f"{uuid4()}"
        )
    )

    spark = (
        create_spark_session()
    )

    try:
        source_df = read_source(
            spark=spark,
            input_path=args.input,
        )

        normalized_df = (
            normalize_source(
                source_df=source_df,
                run_id=run_id,
            )
        )

        validated_df = (
            add_validation_columns(
                normalized_df
            )
        )

        statistics_df = (
            create_statistics(
                validated_df
            )
        )

        errors_df = (
            create_errors(
                validated_df
            )
        )

        visitors_df = (
            create_visitors(
                statistics_df=statistics_df,
                reference_date=(
                    args.reference_date
                ),
            )
        )

        write_outputs(
            statistics_df=statistics_df,
            errors_df=errors_df,
            visitors_df=visitors_df,
            output_path=(
                args.output
            ),
        )

        print_summary(
            source_df=source_df,
            statistics_df=statistics_df,
            errors_df=errors_df,
            visitors_df=visitors_df,
        )

        print(
            f"run_id={run_id}"
        )

        print(
            f"output={args.output}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()