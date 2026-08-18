# ETL Visitas Web

Prueba técnica para el diseño e implementación de un proceso ETL encargado de integrar información de visitas de un sitio web proveniente de archivos planos.

## Objetivo

Procesar diariamente archivos con formato:

```text
report_<consecutivo>.txt
```

Los archivos son obtenidos desde un servidor SFTP y contienen información de visitas realizadas por usuarios.

Cada registro válido representa una visita y `fecha_envio` se considera la fecha de la visita.

El proceso valida, transforma y carga la información manteniendo controles de auditoría, trazabilidad e idempotencia.

---

## Arquitectura de la solución

La solución fue desarrollada en dos componentes complementarios.

### 1. Implementación funcional local

Tecnologías:

- Apache Airflow
- Python
- MySQL
- SFTP
- Docker / Docker Compose

Flujo general:

```text
SFTP
  |
  v
Descubrimiento de archivos
  |
  v
Control de archivos / checksum
  |
  v
Staging
  |
  v
Validación
  |
  +------------------+
  |                  |
  v                  v
Registros válidos   Registros inválidos
  |                  |
  v                  v
estadistica         errores
  |
  v
visitante
  |
  v
Backup
  |
  v
Eliminación del origen
```

### 2. POC Big Data en GCP

Se implementó adicionalmente una POC para demostrar cómo escalar el procesamiento utilizando tecnologías Big Data.

Tecnologías:

- Google Cloud Storage
- Apache Spark / PySpark
- Dataproc Serverless
- BigQuery
- Apache Airflow

Flujo:

```text
GCS Landing
    |
    v
Airflow
    |
    v
Dataproc Serverless
    |
    v
PySpark
    |
    +----------------+
    |                |
    v                v
Valid records      Invalid records
    |                |
    v                v
Parquet            Parquet
    |
    v
BigQuery
```

La POC procesa múltiples archivos `report_*.txt` y genera datasets de estadísticas, visitantes y errores.

---

## Reglas de negocio

### Registro de visita

Cada registro válido del archivo representa una visita.

La columna:

```text
fecha_envio
```

se utiliza como fecha de la visita.

### Tabla visitante

Existe un único registro por email.

Para cada visitante se mantienen:

- fecha de primera visita
- fecha de última visita
- visitas totales
- visitas del año actual
- visitas del mes actual

Si el email no existe, se inserta.

Si ya existe, sus métricas son actualizadas.

### Tabla estadistica

Contiene el detalle de cada registro válido procesado.

Cada registro conserva información de trazabilidad mediante:

- `file_id`
- `run_id`
- `source_file`
- `source_line`

### Tabla errores

Los registros que no cumplen las validaciones son enviados a la tabla `errores`.

Se conserva:

- archivo
- línea
- email
- código de error
- descripción
- registro original
- ejecución

---

## Validaciones

El proceso valida, entre otros controles:

### Layout

Se verifica que el archivo corresponda con la estructura esperada.

### Email

Se valida que el email tenga un formato válido.

### Fechas

Las fechas deben cumplir el formato:

```text
dd/MM/yyyy HH:mm
```

Los registros inválidos no son cargados en las tablas de negocio y quedan disponibles para análisis en la tabla de errores.

---

## Idempotencia

Uno de los principales controles del proceso es evitar que un archivo sea cargado más de una vez.

Para ello se utiliza:

- nombre del archivo
- checksum SHA-256
- tabla de control por archivo
- restricciones únicas a nivel de base de datos

La combinación permite detectar archivos previamente procesados y proteger las cargas ante reprocesos.

---

## Auditoría y control

Se utilizan dos niveles de bitácora.

### etl_run_control

Mantiene información de cada ejecución del ETL:

- archivos detectados
- archivos procesados
- archivos exitosos
- archivos rechazados
- archivos fallidos
- registros leídos
- registros válidos
- registros inválidos
- registros cargados
- estado de ejecución

### etl_file_control

Mantiene trazabilidad individual por archivo:

- nombre
- checksum
- estado
- registros procesados
- timestamps de cada etapa
- visitantes insertados
- visitantes actualizados
- ubicación de staging
- ubicación del backup
- errores

Estas tablas permiten generar reportes operativos y mensuales.

---

## Administración de archivos

Después de una carga correcta, el proceso contempla:

1. creación del backup
2. validación del backup
3. actualización de la bitácora
4. eliminación del archivo del origen

El archivo de origen solamente debe eliminarse después de confirmar que su procesamiento y respaldo fueron exitosos.

---

## Manejo de errores

Los errores se manejan en dos niveles.

### Errores de datos

Registros con:

- email inválido
- fecha inválida
- estructura incorrecta

son enviados a la tabla `errores`.

### Errores técnicos

Problemas como:

- indisponibilidad del SFTP
- error de base de datos
- error de almacenamiento
- fallo durante la ejecución

provocan el fallo controlado de la ejecución y quedan registrados en las bitácoras y logs de Airflow.

---

## Orquestación

Apache Airflow se utiliza como orquestador del proceso.

La implementación permite visualizar:

- ejecución del DAG
- estado de cada tarea
- reintentos
- dependencias
- duración
- logs
- errores

Esto proporciona al equipo operativo visibilidad sobre la ejecución diaria del ETL.

---

## Big Data

Para escenarios con mayores volúmenes, la transformación puede realizarse mediante Apache Spark.

La POC desarrollada utiliza:

```text
GCS → Airflow → Dataproc Serverless → PySpark → Parquet → BigQuery
```

El mismo diseño puede evolucionar hacia un ecosistema Hadoop utilizando:

```text
HDFS
  |
  v
Spark
  |
  +------> Hive
  |
  +------> Impala
```

manteniendo los mismos principios de:

- validación
- trazabilidad
- idempotencia
- particionamiento
- auditoría
- reproceso

---

## Consideraciones para producción

Para liberar la solución a producción se deben considerar:

- administración segura de credenciales
- conexiones administradas de Airflow
- secretos fuera del código fuente
- configuración por ambiente
- monitoreo y alertamiento
- políticas de reintentos
- SLAs
- estrategia de reproceso
- respaldo y retención
- pruebas unitarias e integración
- observabilidad
- permisos mediante mínimo privilegio
- documentación operativa
- procedimiento de rollback

---

## Operación

El equipo operativo debe contar con visibilidad de:

- última ejecución
- estado del DAG
- archivos detectados
- archivos procesados
- archivos rechazados
- registros válidos
- registros inválidos
- duración
- errores
- reintentos

Ante un incidente, las tablas de control permiten identificar el archivo, ejecución y etapa exacta donde ocurrió el problema.

---

## Conclusión

La solución fue diseñada separando:

- orquestación
- procesamiento
- reglas de negocio
- persistencia
- auditoría
- administración de archivos

Esto permite que el proceso sea mantenible, auditable, idempotente y escalable.

La implementación local demuestra el flujo funcional end-to-end, mientras que la POC en GCP demuestra la evolución del procesamiento hacia una arquitectura Big Data basada en PySpark.