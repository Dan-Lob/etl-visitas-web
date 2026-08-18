from __future__ import annotations

import os
from datetime import timedelta

import pendulum

from airflow.sdk import (
    dag,
    task,
    get_current_context,
)

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateBatchOperator,
)


DAG_ID = "etl_visitas_gcp_poc"

PROJECT_ID = os.environ["GCP_PROJECT_ID"]

REGION = os.getenv(
    "GCP_REGION",
    "us-central1",
)

BUCKET = os.environ["GCS_BUCKET"]

BQ_DATASET = os.getenv(
    "BQ_DATASET",
    "visitas_poc",
)

INPUT_PREFIX = "landing/visitas/"

INPUT_PATTERN = (
    f"gs://{BUCKET}/"
    "landing/visitas/"
    "report_*.txt"
)

SPARK_SCRIPT_URI = (
    f"gs://{BUCKET}/"
    "jobs/pyspark/"
    "transform_visitas.py"
)

OUTPUT_URI = (
    f"gs://{BUCKET}/"
    "processed/pyspark/"
    "demo_latest"
)

GCP_CONN_ID = "google_cloud_default"


@dag(
    dag_id=DAG_ID,

    schedule=None,

    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz="America/Mexico_City",
    ),

    catchup=False,

    max_active_runs=1,

    default_args={
        "retries": 1,
        "retry_delay": timedelta(
            minutes=1
        ),
    },

    tags=[
        "etl",
        "gcp",
        "pyspark",
        "dataproc",
        "bigquery",
        "poc",
    ],
)
def etl_visitas_gcp_poc():

    # ==============================================
    # 1. VALIDATE INPUT
    # ==============================================

    @task(
        task_id="validate_input"
    )
    def validate_input() -> int:
        """
        Confirms that report_*.txt input files
        exist in the GCS landing area.
        """

        from google.cloud import storage

        client = storage.Client(
            project=PROJECT_ID
        )

        blobs = list(
            client.list_blobs(
                BUCKET,
                prefix=INPUT_PREFIX,
            )
        )

        files = [
            blob.name
            for blob in blobs
            if (
                blob.name.startswith(
                    f"{INPUT_PREFIX}report_"
                )
                and blob.name.endswith(
                    ".txt"
                )
            )
        ]

        if not files:
            raise RuntimeError(
                "No report_*.txt files "
                "were found in GCS landing."
            )

        print(
            f"Input files detected="
            f"{len(files)}"
        )

        for file_name in sorted(files):
            print(
                f"input_file={file_name}"
            )

        return len(files)

    input_count = validate_input()

    # ==============================================
    # 2. BUILD EXECUTION CONFIG
    # ==============================================

    @task(
        task_id="build_execution_config"
    )
    def build_execution_config(
        detected_files: int,
    ) -> dict:
        """
        Builds the Dataproc batch identifier
        for the current Airflow execution.
        """

        context = (
            get_current_context()
        )

        airflow_run_id = (
            context["run_id"]
        )

        safe_run_id = (
            airflow_run_id
            .lower()
            .replace("_", "-")
            .replace(":", "-")
            .replace("+", "-")
            .replace(".", "-")
        )

        safe_run_id = (
            safe_run_id[-35:]
        )

        batch_id = (
            f"etl-visitas-{safe_run_id}"
        )

        print(
            f"detected_files="
            f"{detected_files}"
        )

        print(
            f"batch_id={batch_id}"
        )

        print(
            f"output_uri={OUTPUT_URI}"
        )

        return {
            "batch_id": batch_id,
            "output_uri": OUTPUT_URI,
            "detected_files": (
                detected_files
            ),
        }

    execution_config = (
        build_execution_config(
            input_count
        )
    )

    # ==============================================
    # 3. SERVERLESS PYSPARK
    # ==============================================

    spark_batch = (
        DataprocCreateBatchOperator(
            task_id="submit_spark_batch",

            project_id=PROJECT_ID,

            region=REGION,

            batch_id=(
                "{{ ti.xcom_pull("
                "task_ids="
                "'build_execution_config'"
                ")['batch_id'] }}"
            ),

            batch={
                "pyspark_batch": {

                    "main_python_file_uri": (
                        SPARK_SCRIPT_URI
                    ),

                    "args": [
                        "--input",
                        INPUT_PATTERN,

                        "--output",
                        OUTPUT_URI,

                        "--reference-date",
                        "2013-02-15",

                        "--run-id",
                        (
                            "{{ ti.xcom_pull("
                            "task_ids="
                            "'build_execution_config'"
                            ")['batch_id'] }}"
                        ),
                    ],
                },
            },

            gcp_conn_id=(
                GCP_CONN_ID
            ),
        )
    )

    # ==============================================
    # 4. VALIDATE SPARK OUTPUT
    # ==============================================

    @task(
        task_id="validate_spark_output"
    )
    def validate_spark_output(
        output_uri: str,
    ) -> str:
        """
        Confirms that the expected Spark
        output datasets exist in GCS.
        """

        from google.cloud import storage

        client = storage.Client(
            project=PROJECT_ID
        )

        prefix = (
            output_uri
            .replace(
                f"gs://{BUCKET}/",
                "",
            )
            .rstrip("/")
        )

        required_prefixes = [
            f"{prefix}/estadistica/",
            f"{prefix}/errores/",
            f"{prefix}/visitante/",
        ]

        for required_prefix in (
            required_prefixes
        ):

            blobs = list(
                client.list_blobs(
                    BUCKET,
                    prefix=(
                        required_prefix
                    ),
                    max_results=1,
                )
            )

            if not blobs:
                raise RuntimeError(
                    "Expected Spark output "
                    "was not found. "
                    f"prefix={required_prefix}"
                )

            print(
                "Output found: "
                f"{required_prefix}"
            )

        print(
            "Spark output validation: OK"
        )

        return output_uri

    validated_output = (
        validate_spark_output(
            execution_config[
                "output_uri"
            ]
        )
    )

    # ==============================================
    # DEPENDENCIES
    # ==============================================

    input_count >> execution_config

    execution_config >> spark_batch

    spark_batch >> validated_output


etl_visitas_gcp_poc()