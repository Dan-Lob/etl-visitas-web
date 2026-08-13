CREATE TABLE IF NOT EXISTS etl_run_control (
    run_id VARCHAR(255) NOT NULL,
    dag_id VARCHAR(255) NOT NULL,

    execution_date DATETIME NOT NULL,
    start_time DATETIME NOT NULL,
    end_time DATETIME NULL,

    status VARCHAR(50) NOT NULL,

    files_detected INT NOT NULL DEFAULT 0,
    files_processed INT NOT NULL DEFAULT 0,
    files_success INT NOT NULL DEFAULT 0,
    files_rejected INT NOT NULL DEFAULT 0,
    files_failed INT NOT NULL DEFAULT 0,

    records_read BIGINT NOT NULL DEFAULT 0,
    records_valid BIGINT NOT NULL DEFAULT 0,
    records_invalid BIGINT NOT NULL DEFAULT 0,
    records_loaded BIGINT NOT NULL DEFAULT 0,

    error_code VARCHAR(100) NULL,
    error_message TEXT NULL,

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (run_id),

    INDEX idx_etl_run_execution_date (execution_date),
    INDEX idx_etl_run_status (status)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;