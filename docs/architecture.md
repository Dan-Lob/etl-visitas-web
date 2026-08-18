# Arquitectura y flujo ETL - Visitas Web

## 1. Objetivo

El proceso ETL integra diariamente archivos de visitas provenientes de un servidor SFTP.

Origen:

```text
/home/vinkOS/archivosVisitas
```

Patrón esperado:

```text
report_<consecutivo>.txt
```

No existe un número fijo de archivos por día, por lo que el proceso debe descubrir dinámicamente todos los archivos disponibles.

Cada registro representa una visita y `fecha_envio` se utiliza como fecha de la visita.

---

# 2. Arquitectura funcional

```text
                    ┌───────────────┐
                    │     SFTP      │
                    │ report_*.txt  │
                    └───────┬───────┘
                            │
                            ▼
                 ┌─────────────────────┐
                 │       Airflow       │
                 │   Orquestación ETL  │
                 └─────────┬───────────┘
                           │
                           ▼
                  Descubrimiento archivos
                           │
                           ▼
                    Registro ejecución
                           │
                           ▼
                   Control idempotencia
                           │
                           ▼
                        Staging
                           │
                           ▼
                     Validaciones
                           │
                 ┌─────────┴─────────┐
                 │                   │
                 ▼                   ▼
          Registros válidos    Registros inválidos
                 │                   │
                 ▼                   ▼
           estadistica             errores
                 │
                 ▼
             visitante
                 │
                 ▼
              Backup
                 │
                 ▼
          Eliminación SFTP
                 │
                 ▼
          Cierre de ejecución
```

---

# 3. Flujo detallado

## Paso 1 - Inicio de ejecución

Airflow inicia una nueva ejecución del DAG.

Se genera un `run_id` único que permite relacionar todos los elementos procesados durante esa ejecución.

Se registra la ejecución en:

```text
etl_run_control
```

Estado inicial:

```text
RUNNING
```

### Punto de control

Debe existir una ejecución identificable antes de comenzar el procesamiento.

Esto permite auditar posteriormente qué ocurrió incluso si el proceso falla.

---

## Paso 2 - Descubrimiento de archivos

El proceso consulta el directorio remoto:

```text
/home/vinkOS/archivosVisitas
```

y busca archivos que cumplan:

```text
report_*.txt
```

El número de archivos es dinámico.

Por cada archivo encontrado se genera su correspondiente control en:

```text
etl_file_control
```

### Punto de control

Se actualiza:

```text
files_detected
```

en `etl_run_control`.

Un archivo detectado todavía no implica que haya sido procesado correctamente.

---

## Paso 3 - Control de idempotencia

Antes de procesar un archivo se verifica si ya fue tratado anteriormente.

Para ello se utilizan principalmente:

```text
file_name
checksum_sha256
etl_file_control
```

El checksum SHA-256 permite identificar el contenido del archivo.

### Regla

Un archivo previamente procesado correctamente no debe volver a afectar las tablas de negocio.

### Objetivo

Evitar duplicación durante:

- reprocesos
- reintentos
- ejecuciones manuales
- reenvío accidental de archivos

---

## Paso 4 - Staging

El archivo es transferido desde el origen hacia el área de trabajo del ETL.

En `etl_file_control` se registra:

```text
staged_at
staging_uri
```

### Punto de control

El procesamiento solamente continúa cuando el archivo se encuentra disponible correctamente en staging.

---

## Paso 5 - Validación de layout

Antes de procesar los registros se verifica que el archivo corresponda con la estructura esperada.

El layout esperado contiene:

```text
email
jyv
Badmail
Baja
Fecha envio
Fecha open
Opens
Opens virales
Fecha click
Clicks
Clicks virales
Links
IPs
Navegadores
Plataformas
```

Si el archivo no cumple con el layout esperado, debe tratarse como un error estructural y evitarse una carga incorrecta.

En la bitácora se conservan los datos necesarios para diagnosticar el problema.

---

## Paso 6 - Validación de registros

Cada registro es validado individualmente.

### Email

Debe cumplir con un formato de email válido.

Ejemplo válido:

```text
usuario@example.com
```

### Fecha

Las fechas informadas deben respetar:

```text
dd/MM/yyyy HH:mm
```

`fecha_envio` es especialmente importante porque representa la fecha de la visita.

### Resultado

Cada registro se clasifica como:

```text
VALID
```

o:

```text
INVALID
```

Los registros inválidos no deben contaminar las tablas de negocio.

---

## Paso 7 - Carga de errores

Los registros inválidos se almacenan en:

```text
errores
```

Se conserva:

```text
file_id
run_id
file_name
line_number
email
error_code
error_description
raw_record
created_at
```

### Trazabilidad

La combinación:

```text
file_id + line_number
```

permite identificar el registro original dentro del archivo.

La restricción:

```text
uk_error_source_record
```

evita insertar dos veces el mismo error de origen.

---

## Paso 8 - Carga de estadística

Los registros válidos se insertan en:

```text
estadistica
```

Cada fila válida representa una visita.

Además de los datos originales se almacena trazabilidad:

```text
file_id
run_id
source_file
source_line
```

La restricción:

```text
uk_estadistica_source_record
(file_id, source_line)
```

proporciona protección adicional ante duplicados durante reprocesos.

---

## Paso 9 - Actualización de visitante

La tabla:

```text
visitante
```

mantiene solamente un registro por email.

Esto se garantiza mediante:

```text
uk_visitante_email
```

Por cada email se mantienen:

```text
fecha_primera_visita
fecha_ultima_visita
visitas_totales
visitas_anio_actual
visitas_mes_actual
```

### Email nuevo

Si el email no existe:

```text
INSERT
```

### Email existente

Si ya existe:

```text
UPDATE
```

Se recalculan las métricas correspondientes utilizando `fecha_envio` como fecha de visita.

### Ejemplo

Si un mismo email aparece tres veces:

```text
usuario@example.com
usuario@example.com
usuario@example.com
```

se cargan tres visitas en `estadistica`, pero solamente existe un registro en `visitante`.

Por lo tanto:

```text
visitas_totales = 3
```

considerando las visitas acumuladas correspondientes.

---

# 4. Orden de carga

El orden es importante:

```text
1. etl_run_control
2. etl_file_control
3. errores / estadistica
4. visitante
5. actualización de controles
6. backup
7. eliminación del origen
8. cierre de ejecución
```

Las tablas de control se crean primero para garantizar trazabilidad.

La información detallada válida se carga antes de consolidar las métricas del visitante.

---

# 5. Administración del archivo

Una vez procesado correctamente el archivo se genera un backup.

Destino solicitado:

```text
/home/etl/visitas/bckp
```

El archivo debe almacenarse comprimido en formato ZIP.

En `etl_file_control` se dispone de:

```text
backup_at
backup_uri
```

para registrar el respaldo.

---

# 6. Eliminación del archivo origen

El archivo solamente puede eliminarse del SFTP después de confirmar:

```text
procesamiento correcto
        +
backup correcto
```

Orden:

```text
procesar
   ↓
cargar
   ↓
backup
   ↓
validar backup
   ↓
eliminar origen
```

Nunca:

```text
eliminar origen
   ↓
intentar procesar
```

Esta regla reduce el riesgo de pérdida de información.

Al eliminarlo se registra:

```text
source_deleted_at
```

---

# 7. Estados y trazabilidad

`etl_file_control` permite identificar en qué etapa se encuentra cada archivo.

Ejemplos de estados utilizados durante el flujo:

```text
DISCOVERED
STAGED
VALIDATED
LOADED
BACKED_UP
COMPLETED
REJECTED
FAILED
```

Los estados permiten determinar si un archivo:

- fue solamente detectado
- llegó a staging
- fue validado
- fue cargado
- fue respaldado
- terminó correctamente
- fue rechazado
- falló técnicamente

---

# 8. Errores de datos vs errores técnicos

Es importante distinguir ambos conceptos.

## Error de datos

Ejemplos:

```text
email inválido
fecha inválida
registro inválido
```

El ETL puede continuar procesando los demás registros.

El registro incorrecto se almacena en:

```text
errores
```

## Error técnico

Ejemplos:

```text
SFTP no disponible
MySQL no disponible
error al generar backup
error de escritura
problema de permisos
```

Estos errores pueden impedir continuar de manera segura.

En ese escenario se registra:

```text
status = FAILED
error_code
error_message
```

---

# 9. Bitácora de ejecución

Existen dos niveles de control.

## Nivel ejecución

Tabla:

```text
etl_run_control
```

Responde preguntas como:

```text
¿Cuándo se ejecutó el ETL?
¿Cuántos archivos encontró?
¿Cuántos procesó?
¿Cuántos registros leyó?
¿Cuántos fueron válidos?
¿Cuántos fueron inválidos?
¿Terminó correctamente?
```

## Nivel archivo

Tabla:

```text
etl_file_control
```

Responde:

```text
¿Qué ocurrió con report_7.txt?
¿Cuántos registros tenía?
¿Cuántos fueron válidos?
¿Cuál era su checksum?
¿Fue respaldado?
¿Fue eliminado del origen?
¿En qué ejecución se procesó?
```

---

# 10. Reporte mensual

Las tablas de control permiten generar el reporte mensual solicitado por negocio.

Métricas de ejecución:

```text
ejecuciones
archivos_detectados
archivos_procesados
archivos_exitosos
archivos_rechazados
archivos_fallidos
registros_leidos
registros_validos
registros_invalidos
registros_cargados
```

También es posible consultar información a nivel archivo y distribución de errores.

---

# 11. Reprocesos

El reproceso debe ser seguro e idempotente.

Antes de volver a procesar se revisa:

```text
file_name
checksum_sha256
estado anterior
```

Las restricciones únicas en `estadistica` y `errores` agregan una segunda capa de protección.

El objetivo es que ejecutar nuevamente el ETL no duplique información previamente cargada.

---

# 12. POC Big Data GCP

Además de la implementación funcional se construyó una POC utilizando:

```text
GCS
 ↓
Airflow
 ↓
Dataproc Serverless
 ↓
PySpark
 ↓
Parquet
 ↓
BigQuery
```

La POC permite procesar múltiples archivos:

```text
report_*.txt
```

en una misma ejecución.

PySpark genera salidas separadas para:

```text
estadistica
visitante
errores
```

y realiza reconciliación de cantidades procesadas.

---

# 13. Evolución hacia Hadoop

Si el destino fueran tablas Hive o Impala, el flujo conceptual se mantendría.

```text
SFTP
 ↓
Landing / HDFS
 ↓
Spark
 ↓
Validaciones
 ↓
Parquet
 ↓
Hive Metastore
 ↓
Hive / Impala
```

Se recomendaría almacenar los datos en formato columnar:

```text
Parquet
```

y aplicar particionamiento de acuerdo con la fecha de negocio.

Hive podría utilizarse para procesamiento SQL batch y administración de tablas.

Impala permitiría consultas analíticas de baja latencia sobre los mismos datos almacenados en HDFS.

Los principios de:

```text
idempotencia
auditoría
validación
trazabilidad
reproceso
particionamiento
```

permanecerían independientemente de la tecnología destino.