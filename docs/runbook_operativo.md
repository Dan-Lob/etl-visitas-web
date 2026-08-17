# Runbook Operativo - ETL Visitas

## 1. Objetivo

Este documento define los procedimientos operativos para monitorear,
diagnosticar y atender incidentes del proceso ETL de visitas.

El objetivo es permitir que el equipo de Operaciones pueda identificar
rápidamente:

- Si una ejecución terminó correctamente.
- Qué archivos fueron procesados.
- Qué archivos fueron rechazados.
- Qué archivos presentaron una falla técnica.
- Cuántos registros fueron leídos, validados y cargados.
- Si un incidente requiere retry.
- Si un archivo puede reprocesarse de forma segura.
- Cuándo debe escalarse el incidente a Data Engineering.

El proceso implementa mecanismos de auditoría, idempotencia, checksum,
backup y control de estados para permitir una operación segura y evitar
duplicidad de información durante reprocesos.


---

# 2. Flujo general del proceso

El DAG principal de Airflow es:

`etl_visitas_daily`

El flujo de ejecución es:

```text
preflight
    |
    v
start_run
    |
    v
discover_files
    |
    v
process_file[]
    |
    v
finalize_run
    |
    v
validate_run
```

La tarea `process_file` utiliza Dynamic Task Mapping.

Airflow genera dinámicamente una instancia por cada archivo descubierto:

```text
process_file[0]
process_file[1]
process_file[2]
...
```

Esto permite monitorear y diagnosticar cada archivo de forma independiente.


---

# 3. Fuentes de monitoreo

La operación del ETL puede monitorearse desde tres fuentes principales.

## 3.1 Airflow

DAG:

`etl_visitas_daily`

Permite consultar:

- Estado general del DAG.
- Estado individual de cada tarea.
- Estado de cada instancia `process_file[n]`.
- Número de reintentos.
- Duración de las tareas.
- Logs técnicos.
- Excepciones.

## 3.2 MySQL

Las principales tablas de auditoría son:

```text
etl_run_control
etl_file_control
```

`etl_run_control` contiene información a nivel de ejecución.

`etl_file_control` contiene información a nivel de archivo.

Las vistas operativas disponibles son:

```text
vw_etl_run_operational_summary
vw_etl_file_operational_detail
vw_etl_monthly_summary
```

## 3.3 Logs

Los logs de Airflow permiten consultar el detalle técnico de cada tarea.

Para problemas asociados a un archivo específico se debe revisar la
instancia correspondiente:

```text
process_file[n]
```


---

# 4. Estados generales de una ejecución

## SUCCESS

### Significado

La ejecución terminó correctamente y todos los archivos procesables
alcanzaron un estado final exitoso.

### Acción

No requiere intervención operativa.


---

## PARTIAL_SUCCESS

### Significado

La ejecución terminó técnicamente, pero uno o más archivos fueron
rechazados por reglas funcionales o de calidad.

Por ejemplo:

```text
report_7.txt -> REJECTED_LAYOUT
report_8.txt -> SUCCESS
report_9.txt -> REJECTED_LAYOUT
```

El DAG puede terminar correctamente aunque el run tenga estado:

```text
PARTIAL_SUCCESS
```

porque un rechazo de calidad no necesariamente representa una falla
técnica del pipeline.

### Acción

Identificar los archivos rechazados y revisar:

```text
status
error_code
error_message
```

No ejecutar un retry técnico automático cuando el problema corresponda
a calidad de datos.


---

## FAILED

### Significado

La ejecución presentó una falla técnica o de integridad que impidió
completar correctamente el procesamiento.

### Acción

Revisar:

1. Airflow.
2. Logs de la tarea afectada.
3. `etl_run_control`.
4. `etl_file_control`.
5. `error_code`.
6. `error_message`.
7. `retry_count`.

La causa debe identificarse antes de realizar un reproceso manual.


---

# 5. Estados operativos por archivo

## SUCCESS

El archivo fue:

```text
descubierto
    ->
descargado
    ->
validado
    ->
procesado
    ->
cargado
    ->
respaldado
    ->
eliminado del SFTP
```

No requiere intervención.


---

## REJECTED_LAYOUT

El archivo no cumple con la estructura esperada.

Ejemplos:

- Header incorrecto.
- Número incorrecto de columnas.
- Archivo vacío.
- Nombre de archivo inválido.

### Acción

No ejecutar retry técnico.

El archivo debe revisarse o corregirse en origen antes de intentar
procesarlo nuevamente.


---

## SKIPPED_ALREADY_PROCESSED

El archivo ya fue procesado anteriormente.

El control de idempotencia detectó que no debe volver a cargarse.

La identificación utiliza información de control como:

```text
file_name
checksum_sha256
```

### Acción

No requiere intervención.

Este estado representa un comportamiento esperado del ETL.


---

## FAILED

El procesamiento del archivo presentó una falla técnica.

### Acción

Revisar:

```text
error_code
error_message
retry_count
```

y posteriormente los logs de:

```text
process_file[n]
```

El archivo puede reintentarse después de resolver la causa técnica,
siempre respetando los controles de idempotencia.


---

## PENDING_BACKUP

La carga de información en MySQL ya terminó, pero el respaldo del
archivo no pudo completarse.

### Importante

Los datos ya fueron cargados.

NO debe ejecutarse nuevamente la carga completa.

### Acción

La recuperación debe continuar únicamente desde:

```text
backup
    ->
source delete
```

Esto evita duplicar información.


---

## PENDING_SOURCE_DELETE

La carga y el backup terminaron correctamente, pero el archivo no pudo
eliminarse del SFTP.

### Importante

Los datos ya fueron cargados y respaldados.

NO debe repetirse:

```text
validación
carga
backup
```

### Acción

Reintentar únicamente:

```text
source delete
```


---

# 6. Matriz de incidentes

| Estado / Error | Tipo | Severidad | Retry automático | Acción operativa |
|---|---|---|---|---|
| SUCCESS | Correcto | INFO | No | Sin acción |
| SKIPPED_ALREADY_PROCESSED | Idempotencia | INFO | No | Comportamiento esperado |
| REJECTED_LAYOUT | Calidad de datos | WARNING | No | Revisar layout y archivo recibido |
| INVALID_EMAIL | Calidad de datos | WARNING | No | Registro enviado a `errores`; revisar si excede el comportamiento esperado |
| INVALID_DATE_FORMAT | Calidad de datos | WARNING | No | Revisar formato recibido |
| INVALID_INTEGER | Calidad de datos | WARNING | No | Revisar campo numérico recibido |
| SFTP_CONNECTION_ERROR | Técnico | ERROR | Sí | Validar SFTP, red y credenciales |
| SFTP_DOWNLOAD_ERROR | Técnico | ERROR | Sí | Validar conectividad y existencia del archivo |
| CHECKSUM_MISMATCH | Integridad | ERROR | Sí | Reintentar descarga; escalar si persiste |
| DATABASE_CONNECTION_ERROR | Técnico | ERROR | Sí | Validar MySQL, red y credenciales |
| DATABASE_LOAD_ERROR | Técnico | ERROR | Sí | Revisar error SQL y estado de la transacción |
| RECONCILIATION_ERROR | Integridad | CRITICAL | No automático | Detener y escalar a Data Engineering |
| PENDING_BACKUP | Operativo | ERROR | Solo backup | No volver a cargar; recuperar desde backup |
| PENDING_SOURCE_DELETE | Operativo | ERROR | Solo delete | No volver a cargar ni respaldar |
| FAILED | Técnico | ERROR | Depende de causa | Revisar error antes del reproceso |


---

# 7. Procedimiento de diagnóstico

## Paso 1 - Revisar el último run

Ejecutar:

```sql
SELECT
    run_id,
    start_time,
    end_time,
    status,
    files_detected,
    files_processed,
    files_success,
    files_rejected,
    files_failed,
    records_read,
    records_loaded,
    duration_seconds
FROM vw_etl_run_operational_summary
ORDER BY start_time DESC
LIMIT 1;
```

Validar principalmente:

```text
status
files_detected
files_processed
files_success
files_rejected
files_failed
records_read
records_loaded
```


---

## Paso 2 - Identificar archivos afectados

Utilizar el `run_id` obtenido anteriormente:

```sql
SELECT
    file_id,
    file_name,
    status,
    operational_category,
    records_read,
    records_valid,
    records_invalid,
    records_loaded,
    error_code,
    error_message,
    retry_count
FROM vw_etl_file_operational_detail
WHERE run_id = '<RUN_ID>'
ORDER BY file_id;
```

Esto permite identificar exactamente qué archivo presentó el problema.


---

## Paso 3 - Identificar archivos que requieren atención

```sql
SELECT
    file_id,
    run_id,
    file_name,
    status,
    operational_category,
    error_code,
    error_message,
    retry_count
FROM vw_etl_file_operational_detail
WHERE operational_category IN (
    'TECHNICAL_FAILURE',
    'OPERATIONAL_ACTION_REQUIRED',
    'DATA_QUALITY'
)
ORDER BY created_at DESC;
```


---

## Paso 4 - Revisar Airflow

Abrir el DAG:

```text
etl_visitas_daily
```

Validar las tareas:

```text
preflight
start_run
discover_files
process_file[n]
finalize_run
validate_run
```

Si el problema corresponde a un archivo específico, identificar la
instancia:

```text
process_file[n]
```

y revisar sus logs.


---

# 8. Diagnóstico por tarea de Airflow

## preflight

Si falla:

Revisar configuración del runtime.

Ejemplos:

```text
SFTP_HOST
SFTP_REMOTE_PATH
MYSQL_HOST
MYSQL_DATABASE
STAGING_PATH
BACKUP_PATH
```

No continuar hasta corregir la configuración.


---

## start_run

Si falla:

Revisar principalmente conectividad con MySQL y creación del registro
en:

```text
etl_run_control
```


---

## discover_files

Si falla:

Revisar:

- Conectividad SFTP.
- Credenciales.
- Ruta remota.
- Permisos.
- Disponibilidad del servidor.


---

## process_file[n]

Si falla:

Identificar primero el archivo correspondiente.

Revisar:

```text
file_name
file_id
status
error_code
error_message
retry_count
```

Después revisar el log específico de esa instancia.


---

## finalize_run

Si falla:

Revisar las métricas registradas en:

```text
etl_run_control
etl_file_control
```

y validar que los archivos hayan alcanzado estados consistentes.


---

## validate_run

Si falla:

Revisar el estado final almacenado en:

```text
etl_run_control
```

Los estados esperados para una ejecución procesada son:

```text
SUCCESS
PARTIAL_SUCCESS
```

Un estado diferente debe investigarse.


---

# 9. Reglas de reproceso

El reproceso debe respetar siempre los mecanismos de idempotencia.

## REJECTED_LAYOUT

No reprocesar automáticamente.

El archivo debe corregirse antes de intentar nuevamente.


---

## FAILED antes de carga

Puede reintentarse después de corregir la falla técnica.

Los controles de auditoría e idempotencia deben mantenerse activos.


---

## PENDING_BACKUP

NO volver a ejecutar la carga MySQL.

Continuar únicamente:

```text
backup
    ->
source delete
```


---

## PENDING_SOURCE_DELETE

NO volver a ejecutar:

```text
validación
carga
backup
```

Continuar únicamente:

```text
source delete
```


---

## SUCCESS

No volver a cargar.

El checksum SHA-256 y las tablas de control deben impedir un segundo
procesamiento del mismo archivo.


---

## SKIPPED_ALREADY_PROCESSED

No realizar ninguna acción.

El ETL detectó correctamente un archivo previamente procesado.


---

# 10. Validación de integridad

Ante una posible inconsistencia revisar:

```text
estadistica
errores
visitante
etl_file_control
etl_run_control
```

Comparar principalmente:

```text
records_read
records_valid
records_invalid
records_loaded
```

Conceptualmente debe cumplirse:

```text
records_read
=
records_valid
+
records_invalid
```

y los registros cargados deben corresponder con los registros válidos
según las reglas funcionales implementadas.

Una inconsistencia debe tratarse como un problema de integridad y
escalarse a Data Engineering.


---

# 11. Backup

Los archivos procesados correctamente deben generar un backup antes de
ser eliminados del SFTP.

La ubicación del backup se registra en:

```text
etl_file_control.backup_uri
```

También se registra:

```text
backup_at
```

Antes de eliminar el archivo del origen, el proceso debe confirmar que
el backup fue creado correctamente.


---

# 12. Eliminación del archivo origen

Un archivo solo debe eliminarse del SFTP después de completar
correctamente:

```text
validación
    ->
carga
    ->
backup
    ->
validación del backup
```

La eliminación queda registrada mediante:

```text
source_deleted_at
```

Si la eliminación falla después del backup, el archivo debe quedar en:

```text
PENDING_SOURCE_DELETE
```

para evitar volver a ejecutar la carga.


---

# 13. Idempotencia

El ETL está diseñado para soportar reprocesos sin duplicar información.

Se utilizan mecanismos como:

```text
checksum SHA-256
file_name
etl_file_control
restricciones únicas
UPSERT
transacciones
```

Antes de reprocesar manualmente un archivo siempre debe revisarse su
historial en:

```text
etl_file_control
```

Nunca debe eliminarse manualmente la información de control únicamente
para forzar un reproceso sin analizar previamente el estado de las
tablas de negocio.


---

# 14. Reporte mensual

La vista:

```text
vw_etl_monthly_summary
```

permite generar la bitácora mensual del proceso.

Consulta:

```sql
SELECT *
FROM vw_etl_monthly_summary
ORDER BY processing_month DESC;
```

El reporte permite consultar:

```text
total_runs
files_detected
files_processed
files_success
files_rejected
files_failed
records_read
records_valid
records_invalid
records_loaded
successful_runs
partial_success_runs
failed_runs
```


---

# 15. Escalamiento

El incidente debe escalarse a Data Engineering cuando ocurra cualquiera
de las siguientes condiciones:

1. Existe un `RECONCILIATION_ERROR`.

2. Una falla técnica continúa después de los retries configurados.

3. Existe inconsistencia entre:

   ```text
   estadistica
   errores
   visitante
   etl_file_control
   etl_run_control
   ```

4. Un archivo permanece en:

   ```text
   PENDING_BACKUP
   ```

   y el backup no puede recuperarse.

5. Un archivo permanece en:

   ```text
   PENDING_SOURCE_DELETE
   ```

   y no puede eliminarse de forma segura.

6. Se detecta posible duplicidad de información.

7. Las métricas de registros no cuadran.

8. El volumen procesado presenta una variación significativa respecto
   al comportamiento habitual.

9. La duración del proceso aumenta significativamente respecto al
   comportamiento habitual.

10. Existe una falla de integridad que pueda comprometer información
    previamente cargada.


---

# 16. Información requerida para escalar

Al escalar un incidente se debe proporcionar como mínimo:

```text
DAG:
etl_visitas_daily

run_id:
<run_id>

file_name:
<archivo afectado>

file_id:
<file_id>

status:
<status>

error_code:
<error_code>

error_message:
<error_message>

retry_count:
<número de retries>

fecha/hora:
<timestamp>
```

También debe adjuntarse o referenciarse el log correspondiente de
Airflow.

Esto permite que Data Engineering pueda investigar el incidente sin
tener que reconstruir inicialmente todo el contexto.


---

# 17. Principio operativo principal

Antes de realizar cualquier reproceso manual se debe determinar hasta
qué etapa llegó el archivo.

Nunca asumir que una falla implica que todo el proceso debe ejecutarse
nuevamente.

El estado registrado en:

```text
etl_file_control
```

determina desde qué punto puede continuar la recuperación.

El objetivo es evitar:

```text
duplicidad de datos
pérdida de información
doble actualización de visitante
backups inconsistentes
eliminación prematura del archivo origen
```

y garantizar un reproceso controlado, auditable e idempotente.