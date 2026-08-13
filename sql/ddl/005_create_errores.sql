CREATE TABLE IF NOT EXISTS errores (
    id_error BIGINT NOT NULL AUTO_INCREMENT,

    file_id BIGINT NOT NULL,
    run_id VARCHAR(255) NOT NULL,

    file_name VARCHAR(255) NOT NULL,
    line_number INT NOT NULL,

    email VARCHAR(320) NULL,

    error_code VARCHAR(100) NOT NULL,
    error_description VARCHAR(1000) NOT NULL,

    raw_record TEXT NOT NULL,

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id_error),

    CONSTRAINT uk_error_source_record
        UNIQUE (file_id, line_number),

    CONSTRAINT fk_error_file
        FOREIGN KEY (file_id)
        REFERENCES etl_file_control(file_id),

    INDEX idx_error_run_id (run_id),
    INDEX idx_error_file_name (file_name),
    INDEX idx_error_code (error_code)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;