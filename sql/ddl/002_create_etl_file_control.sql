CREATE TABLE IF NOT EXISTS etl_file_control (
    file_id BIGINT NOT NULL AUTO_INCREMENT,
    run_id VARCHAR(255) NOT NULL,

    file_name VARCHAR(255) NOT NULL,
    source_path VARCHAR(1000) NOT NULL,

    file_size BIGINT NULL,
    checksum_sha256 CHAR(64) NULL,

    detected_at DATETIME NOT NULL,
    staged_at DATETIME NULL,

    validation_started_at DATETIME NULL,
    validation_finished_at DATETIME NULL,

    load_started_at DATETIME NULL,
    load_finished_at DATETIME NULL,

    backup_at DATETIME NULL,
    source_deleted_at DATETIME NULL,

    status VARCHAR(100) NOT NULL,

    records_read BIGINT NOT NULL DEFAULT 0,
    records_valid BIGINT NOT NULL DEFAULT 0,
    records_invalid BIGINT NOT NULL DEFAULT 0,
    records_loaded BIGINT NOT NULL DEFAULT 0,

    visitors_inserted BIGINT NOT NULL DEFAULT 0,
    visitors_updated BIGINT NOT NULL DEFAULT 0,

    staging_uri VARCHAR(1000) NULL,
    backup_uri VARCHAR(1000) NULL,

    retry_count INT NOT NULL DEFAULT 0,

    error_code VARCHAR(100) NULL,
    error_message TEXT NULL,

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (file_id),

    CONSTRAINT fk_etl_file_run
        FOREIGN KEY (run_id)
        REFERENCES etl_run_control(run_id),

    INDEX idx_etl_file_run_id (run_id),
    INDEX idx_etl_file_name (file_name),
    INDEX idx_etl_file_checksum (checksum_sha256),
    INDEX idx_etl_file_status (status)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;