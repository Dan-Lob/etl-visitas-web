CREATE TABLE IF NOT EXISTS visitante (
    id_visitante BIGINT NOT NULL AUTO_INCREMENT,

    email VARCHAR(320) NOT NULL,

    fecha_primera_visita DATE NOT NULL,
    fecha_ultima_visita DATE NOT NULL,

    visitas_totales BIGINT NOT NULL DEFAULT 0,
    visitas_anio_actual BIGINT NOT NULL DEFAULT 0,
    visitas_mes_actual BIGINT NOT NULL DEFAULT 0,

    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        ON UPDATE CURRENT_TIMESTAMP,

    PRIMARY KEY (id_visitante),

    CONSTRAINT uk_visitante_email
        UNIQUE (email)
) ENGINE=InnoDB
  DEFAULT CHARSET=utf8mb4
  COLLATE=utf8mb4_unicode_ci;