CREATE TABLE IF NOT EXISTS estadistica (
    id_estadistica BIGINT NOT NULL AUTO_INCREMENT,

    file_id BIGINT NOT NULL,
    run_id VARCHAR(255) NOT NULL,

    email VARCHAR(320) NOT NULL,

    jyv VARCHAR(255) NULL,
    badmail VARCHAR(50) NULL,
    baja VARCHAR(20) NULL,

    fecha_envio DATETIME NOT NULL,
    fecha_open DATETIME NULL,

    opens INT NOT NULL DEFAULT 0,
    opens_virales INT NOT NULL DEFAULT 0,

    fecha_click DATETIME NULL,

    clicks INT NOT NULL DEFAULT 0,
    clicks_virales INT NOT NULL DEFAULT 0,

    links TEXT NULL,
    ips TEXT NULL,
    navegadores TEXT NULL,
    plataformas TEXT NULL,

    source_file VARCHAR(255) NOT NULL,
    source_line INT NOT NULL,

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,

    PRIMARY KEY (id_estadistica),

    CONSTRAINT uk_estadistica_source_record
        UNIQUE (file_id, source_line),

    CONSTRAINT fk_estadistica_file
        FOREIGN KEY (file_id)
        REFERENCES etl_file_control(file_id),

    INDEX idx_estadistica_email (email),
    INDEX idx_estadistica_fecha_envio (fecha_envio),
    INDEX idx_estadistica_run_id (run_id),
    INDEX idx_estadistica_source_file (source_file)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;