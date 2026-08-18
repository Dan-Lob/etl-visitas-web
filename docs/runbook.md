# Runbook Operativo - ETL Visitas Web

## 1. Objetivo

Este documento proporciona al equipo de Operaciones la información necesaria para monitorear y atender incidentes del proceso ETL de visitas web.

El proceso es orquestado mediante Apache Airflow y procesa diariamente archivos:

```text
report_<consecutivo>.txt
```

provenientes del servidor SFTP.

---

# 2. Monitoreo principal

La primera herramienta de monitoreo es Apache Airflow.

El equipo operativo debe revisar:

- estado del DAG
- estado de las tareas
- fecha y hora de ejecución
- duración
- número de reintentos
- logs
- errores

DAG principal:

```text
etl_visitas_daily
```

Estados generales esperados:

```text
SUCCESS
FAILED
RUNNING
```

---

# 3. Métricas operativas

Además de Airflow, las tablas de control permiten conocer el resultado funcional del ETL.

## etl_run_control

Principales métricas:

```text
files_detected
files_processed
files_success
files_rejected
files_failed

records_read
records_valid
records_invalid
records_loaded
```

También contiene:

```text
status
start_time
end_time
error_code
error_message
```

## etl_file_control

Permite investigar cada archivo individualmente.

Información relevante:

```text
file_name
checksum_sha256
status

records_read
records_valid
records_invalid
records_loaded

visitors_inserted
visitors_updated

staging_uri
backup_uri

retry_count
error_code
error_message
```

---

# 4. Validaciones operativas después de una ejecución

Una ejecución correcta debe cumplir las reconciliaciones esperadas.

## Reconciliación de registros

```text
records_read
=
records_valid + records_invalid
```

Ejemplo:

```text
2005 registros leídos
2002 registros válidos
3 registros inválidos

2005 = 2002 + 3
```

## Reconciliación de carga

Para los archivos cargados correctamente:

```text
records_valid = records_loaded
```

cuando `records_loaded` representa los registros válidos cargados en la tabla de detalle.

## Archivos

Los archivos procesados deben quedar clasificados según su resultado.

Por ejemplo:

```text
SUCCESS
REJECTED
FAILED
```

Un archivo detectado no necesariamente significa que haya sido cargado, ya que puede ser omitido por controles de idempotencia.

---

# 5. Estados de archivo

Los estados permiten conocer hasta qué punto llegó un archivo.

Ejemplos:

```text
DISCOVERED
STAGED
VALIDATED
LOADED
BACKED_UP
SUCCESS
REJECTED
FAILED
```

La interpretación debe realizarse junto con los timestamps y mensajes registrados en `etl_file_control`.

---

# 6. Archivo SUCCESS

Un archivo exitoso debe haber completado:

```text
detección
   ↓
staging
   ↓
validación
   ↓
carga
   ↓
backup
   ↓
eliminación del origen
```

Se debe comprobar:

```text
status = SUCCESS

backup_at IS NOT NULL
backup_uri IS NOT NULL

source_deleted_at IS NOT NULL
```

Además:

```text
source_deleted_at >= backup_at
```

El archivo nunca debe eliminarse del origen antes de generar correctamente su respaldo.

---

# 7. Archivo REJECTED

Un archivo puede ser rechazado cuando no cumple condiciones necesarias para continuar de forma segura.

Ejemplo:

```text
layout incorrecto
```

Un archivo rechazado es diferente de un registro inválido.

## Archivo rechazado

```text
report_x.txt
    ↓
layout incorrecto
    ↓
REJECTED
```

## Registro inválido

```text
archivo válido
    ↓
fila con email/fecha inválida
    ↓
errores
```

Un archivo válido puede contener simultáneamente registros válidos e inválidos.

---

# 8. Registros inválidos

Los registros inválidos se almacenan en:

```text
errores
```

El equipo operativo puede consultar:

```sql
SELECT
    file_name,
    error_code,
    COUNT(*) AS total
FROM errores
GROUP BY
    file_name,
    error_code
ORDER BY
    file_name,
    total DESC;
```

Esto permite identificar rápidamente:

- archivo
- tipo de error
- cantidad de registros afectados

Para investigar el detalle:

```sql
SELECT
    file_id,
    run_id,
    file_name,
    line_number,
    email,
    error_code,
    error_description,
    raw_record
FROM errores
ORDER BY id_error DESC;
```

---

# 9. Incidente: SFTP no disponible

## Síntomas

- fallo en tarea de descubrimiento
- timeout de conexión
- error de autenticación
- host inaccesible

## Acciones

1. Revisar logs de Airflow.
2. Confirmar conectividad al servidor.
3. Validar disponibilidad del SFTP.
4. Verificar credenciales/conexión configurada.
5. Confirmar que el directorio remoto exista.
6. No modificar las tablas de negocio.
7. Una vez restaurado el servicio, reintentar la tarea o ejecución según el procedimiento autorizado.

## Importante

No deben generarse cargas parciales artificiales para compensar la indisponibilidad.

---

# 10. Incidente: MySQL no disponible

## Síntomas

- error de conexión
- timeout
- fallo durante INSERT/UPDATE
- rollback transaccional

## Acciones

1. Revisar logs.
2. Confirmar disponibilidad de MySQL.
3. Revisar conectividad.
4. Validar credenciales.
5. Identificar el `run_id`.
6. Identificar los archivos involucrados.
7. Verificar el estado de las tablas de control.
8. Restaurar la conectividad.
9. Reprocesar únicamente utilizando el mecanismo normal del ETL.

## Importante

No insertar manualmente registros en:

```text
estadistica
visitante
errores
```

salvo procedimiento extraordinario autorizado.

---

# 11. Incidente: fallo durante backup

Este escenario debe tratarse con especial cuidado.

Si los datos fueron cargados pero el backup falla:

```text
datos cargados
     ↓
backup FAILED
     ↓
NO eliminar origen
```

## Acciones

1. Revisar `error_code` y `error_message`.
2. Confirmar disponibilidad del storage local.
3. Validar espacio disponible.
4. Verificar permisos.
5. Confirmar que el archivo todavía existe en origen.
6. Corregir el problema.
7. Reintentar de forma controlada.

## Regla

```text
Sin backup confirmado
=
no borrar origen
```

---

# 12. Incidente: fallo al eliminar archivo del SFTP

Puede ocurrir:

```text
procesamiento OK
backup OK
delete FAILED
```

En este escenario el principal riesgo es que el archivo vuelva a aparecer en la siguiente ejecución.

Sin embargo, el control de idempotencia mediante checksum debe impedir que vuelva a afectar las tablas de negocio.

## Acciones

1. Revisar el estado del archivo.
2. Confirmar que el backup existe.
3. Confirmar que la carga ya fue realizada.
4. Verificar checksum.
5. Resolver el problema de permisos/conectividad SFTP.
6. Eliminar el archivo mediante el procedimiento autorizado.

No volver a cargar manualmente el archivo.

---

# 13. Incidente: archivo ya procesado

Si un archivo aparece nuevamente en el origen, el proceso debe comprobar su identidad mediante sus controles de idempotencia.

El operador debe revisar:

```text
file_name
checksum_sha256
status previo
```

Si el archivo ya fue procesado correctamente, no debe volver a cargarse.

Esto protege:

```text
estadistica
visitante
errores
```

contra duplicados.

---

# 14. Reprocesos

Un reproceso nunca debe realizarse simplemente eliminando registros de las tablas de control.

Antes de reprocesar se debe conocer:

```text
run_id
file_id
file_name
checksum_sha256
status
```

El mecanismo normal del ETL debe utilizarse para preservar la idempotencia.

## Reproceso por fallo técnico

Puede ser válido cuando el procesamiento no terminó correctamente.

Ejemplo:

```text
SFTP OK
validación OK
MySQL FAILED
```

Una vez corregido MySQL, puede ejecutarse nuevamente el proceso.

## Archivo previamente SUCCESS

No debe volver a cargarse salvo que exista un procedimiento extraordinario autorizado.

---

# 15. Consultas operativas

## Últimas ejecuciones

```sql
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
    error_code,
    error_message
FROM etl_run_control
ORDER BY start_time DESC
LIMIT 20;
```

## Archivos de una ejecución

```sql
SELECT
    file_id,
    file_name,
    status,
    records_read,
    records_valid,
    records_invalid,
    records_loaded,
    backup_uri,
    backup_at,
    source_deleted_at,
    error_code,
    error_message
FROM etl_file_control
WHERE run_id = 'RUN_ID'
ORDER BY file_id;
```

## Archivos fallidos

```sql
SELECT
    file_id,
    run_id,
    file_name,
    status,
    retry_count,
    error_code,
    error_message
FROM etl_file_control
WHERE status = 'FAILED'
ORDER BY updated_at DESC;
```

## Archivos rechazados

```sql
SELECT
    file_id,
    run_id,
    file_name,
    status,
    error_code,
    error_message
FROM etl_file_control
WHERE status = 'REJECTED'
ORDER BY updated_at DESC;
```

---

# 16. Reconciliación por archivo

Buscar inconsistencias:

```sql
SELECT
    file_id,
    file_name,
    records_read,
    records_valid,
    records_invalid
FROM etl_file_control
WHERE records_read <> records_valid + records_invalid;
```

Resultado esperado:

```text
0 filas
```

---

# 17. Validación de backup

Buscar archivos exitosos sin backup:

```sql
SELECT
    file_id,
    file_name,
    backup_uri,
    backup_at
FROM etl_file_control
WHERE status = 'SUCCESS'
  AND (
      backup_uri IS NULL
      OR backup_at IS NULL
  );
```

Resultado esperado:

```text
0 filas
```

---

# 18. Validación de eliminación segura

```sql
SELECT
    file_id,
    file_name,
    backup_at,
    source_deleted_at
FROM etl_file_control
WHERE backup_at IS NOT NULL
  AND source_deleted_at IS NOT NULL
  AND source_deleted_at < backup_at;
```

Resultado esperado:

```text
0 filas
```

---

# 19. Reporte mensual

El equipo operativo puede generar:

```sql
SELECT
    DATE_FORMAT(execution_date, '%Y-%m') AS periodo,

    COUNT(*) AS ejecuciones,

    SUM(files_detected) AS archivos_detectados,
    SUM(files_processed) AS archivos_procesados,
    SUM(files_success) AS archivos_exitosos,
    SUM(files_rejected) AS archivos_rechazados,
    SUM(files_failed) AS archivos_fallidos,

    SUM(records_read) AS registros_leidos,
    SUM(records_valid) AS registros_validos,
    SUM(records_invalid) AS registros_invalidos,
    SUM(records_loaded) AS registros_cargados

FROM etl_run_control

GROUP BY
    DATE_FORMAT(execution_date, '%Y-%m')

ORDER BY periodo;
```

Las métricas históricas deben interpretarse considerando que ambientes de desarrollo pueden contener ejecuciones de pruebas. En producción, los runs de testing deben mantenerse separados del historial operativo.

---

# 20. Alertamiento recomendado para producción

Se recomienda generar alertas ante:

```text
DAG FAILED
archivo FAILED
SFTP no disponible
MySQL no disponible
backup fallido
delete fallido
reintentos agotados
duración superior al SLA
cantidad anormal de archivos
cantidad anormal de errores
reconciliación incorrecta
```

Las alertas pueden integrarse con las herramientas corporativas disponibles:

```text
email
Microsoft Teams
Slack
PagerDuty
Cloud Monitoring
```

---

# 21. Información necesaria para escalar un incidente

Antes de escalar al equipo de desarrollo, Operaciones debe proporcionar:

```text
run_id
file_id
file_name
status
fecha/hora
task de Airflow
error_code
error_message
fragmento relevante del log
número de reintentos
```

Esto reduce significativamente el tiempo de diagnóstico.

---

# 22. Acciones que Operaciones NO debe realizar

Sin autorización, no se debe:

- modificar checksums
- eliminar registros de `etl_file_control`
- eliminar registros de `etl_run_control`
- modificar manualmente contadores de `visitante`
- insertar manualmente visitas
- marcar manualmente un archivo como SUCCESS
- borrar el archivo del origen sin comprobar el backup
- reprocesar ignorando los controles de idempotencia

Estas acciones pueden comprometer la trazabilidad y consistencia del ETL.

---

# 23. Criterio general de operación

Ante cualquier incidente:

```text
1. Identificar run_id
        ↓
2. Identificar file_id
        ↓
3. Revisar Airflow
        ↓
4. Revisar etl_run_control
        ↓
5. Revisar etl_file_control
        ↓
6. Determinar etapa del fallo
        ↓
7. Corregir causa
        ↓
8. Reintentar de forma controlada
        ↓
9. Reconciliar resultados
```

El objetivo es recuperar el proceso sin perder trazabilidad ni duplicar información.