USE visitas_db;


-- =====================================================
-- 1. RESUMEN OPERATIVO POR EJECUCIÓN
-- =====================================================

CREATE OR REPLACE VIEW vw_etl_run_operational_summary AS
SELECT
    run_id,
    dag_id,
    execution_date,
    start_time,
    end_time,
    status,

    files_detected,
    files_processed,
    files_success,
    files_rejected,
    files_failed,

    records_read,
    records_valid,
    records_invalid,
    records_loaded,

    CASE
        WHEN end_time IS NOT NULL
        THEN TIMESTAMPDIFF(
            SECOND,
            start_time,
            end_time
        )
        ELSE NULL
    END AS duration_seconds,

    CASE
        WHEN files_detected > 0
        THEN ROUND(
            (
                files_success
                / files_detected
            ) * 100,
            2
        )
        ELSE 0
    END AS file_success_percentage,

    CASE
        WHEN records_read > 0
        THEN ROUND(
            (
                records_invalid
                / records_read
            ) * 100,
            2
        )
        ELSE 0
    END AS invalid_record_percentage,

    error_code,
    error_message,

    created_at,
    updated_at

FROM etl_run_control;

-- =====================================================
-- 2. DETALLE OPERATIVO POR ARCHIVO
-- =====================================================

CREATE OR REPLACE VIEW vw_etl_file_operational_detail AS
SELECT
    f.file_id,
    f.run_id,
    r.dag_id,

    f.file_name,
    f.source_path,
    f.file_size,
    f.checksum_sha256,

    f.status,

    f.detected_at,
    f.staged_at,
    f.validation_started_at,
    f.validation_finished_at,
    f.load_started_at,
    f.load_finished_at,
    f.backup_at,
    f.source_deleted_at,

    f.records_read,
    f.records_valid,
    f.records_invalid,
    f.records_loaded,

    f.visitors_inserted,
    f.visitors_updated,

    f.staging_uri,
    f.backup_uri,

    f.retry_count,

    f.error_code,
    f.error_message,

    CASE
        WHEN f.status = 'SUCCESS'
        THEN 'OK'

        WHEN f.status = 'REJECTED_LAYOUT'
        THEN 'DATA_QUALITY'

        WHEN f.status = 'SKIPPED_ALREADY_PROCESSED'
        THEN 'IDEMPOTENCY'

        WHEN f.status IN (
            'PENDING_BACKUP',
            'PENDING_SOURCE_DELETE'
        )
        THEN 'OPERATIONAL_ACTION_REQUIRED'

        WHEN f.status = 'FAILED'
        THEN 'TECHNICAL_FAILURE'

        ELSE 'IN_PROGRESS'
    END AS operational_category,

    f.created_at,
    f.updated_at

FROM etl_file_control f

INNER JOIN etl_run_control r
    ON r.run_id = f.run_id;

-- =====================================================
-- 3. RESUMEN MENSUAL
-- =====================================================

CREATE OR REPLACE VIEW vw_etl_monthly_summary AS
SELECT
    DATE_FORMAT(
        execution_date,
        '%Y-%m'
    ) AS processing_month,

    COUNT(*) AS total_runs,

    SUM(files_detected)
        AS files_detected,

    SUM(files_processed)
        AS files_processed,

    SUM(files_success)
        AS files_success,

    SUM(files_rejected)
        AS files_rejected,

    SUM(files_failed)
        AS files_failed,

    SUM(records_read)
        AS records_read,

    SUM(records_valid)
        AS records_valid,

    SUM(records_invalid)
        AS records_invalid,

    SUM(records_loaded)
        AS records_loaded,

    SUM(
        CASE
            WHEN status = 'SUCCESS'
            THEN 1
            ELSE 0
        END
    ) AS successful_runs,

    SUM(
        CASE
            WHEN status = 'PARTIAL_SUCCESS'
            THEN 1
            ELSE 0
        END
    ) AS partial_success_runs,

    SUM(
        CASE
            WHEN status = 'FAILED'
            THEN 1
            ELSE 0
        END
    ) AS failed_runs

FROM etl_run_control

GROUP BY
    DATE_FORMAT(
        execution_date,
        '%Y-%m'
    );