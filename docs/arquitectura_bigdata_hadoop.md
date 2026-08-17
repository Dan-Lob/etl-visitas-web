# Arquitectura Big Data - Hadoop / Hive / Impala

## 1. Objetivo

Describir cómo cambiaría la arquitectura y el flujo del ETL de visitas si el destino dejara de ser MySQL y pasara a ser un entorno Big Data basado en Hadoop, Hive e Impala.

El objetivo no es únicamente reemplazar el motor de almacenamiento, sino adaptar la estrategia de procesamiento para soportar mayores volúmenes de información de manera distribuida.

---

## 2. Arquitectura actual

La solución actual utiliza:

```text
SFTP
  ↓
Airflow
  ↓
Python
  ↓
Validación / Normalización
  ↓
MySQL
  ├── visitante
  ├── estadistica
  └── errores
  ↓
Backup ZIP
  ↓
Eliminación segura del origen
```

La estrategia actual es adecuada para volúmenes pequeños o medianos, donde el procesamiento puede realizarse dentro de un único runtime Python sin necesidad de distribuir memoria y CPU entre múltiples nodos.

---

## 3. Arquitectura propuesta Big Data

Si el volumen creciera significativamente, el flujo propuesto sería:

```text
                    SFTP
                      │
                      ▼
              Landing / Raw HDFS
                      │
                      ▼
                   Airflow
                      │
                      ▼
                 Spark / PySpark
                      │
          ┌───────────┴───────────┐
          │                       │
          ▼                       ▼
    Registros válidos       Registros inválidos
          │                       │
          ▼                       ▼
   Silver / Parquet         Rejects / Parquet
          │                       │
          └───────────┬───────────┘
                      ▼
                Hive Metastore
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
        Hive                    Impala
          │                       │
          └───────────┬───────────┘
                      ▼
                 Curated / Gold
```

El procesamiento pesado pasaría de Python tradicional a Spark.

Airflow continuaría siendo el orquestador.

---

## 4. Cambio principal de paradigma

### 4.1 Solución actual con MySQL

En la solución actual se trabaja principalmente con operaciones:

```text
INSERT
UPDATE
UPSERT
TRANSACTION
PRIMARY KEY
FOREIGN KEY
INDEX
```

La lógica está orientada principalmente a registros y transacciones.

Por ejemplo:

```text
email
  ↓
buscar visitante
  ↓
INSERT / UPDATE
```

### 4.2 Solución Big Data

En Hadoop el procesamiento se realizaría sobre datasets completos o particiones de datos.

La estrategia cambia a:

```text
lectura distribuida
transformaciones Spark
agregaciones distribuidas
datasets Parquet
particionamiento
escritura batch
Hive Metastore
consulta mediante Hive / Impala
```

El objetivo es evitar operaciones fila por fila y aprovechar el procesamiento distribuido.

---

## 5. Ingesta desde SFTP

La detección de archivos puede mantenerse bajo Airflow.

```text
Airflow
  ↓
discover_files
  ↓
Dynamic Task Mapping
```

Por cada archivo detectado se realizaría:

```text
SFTP
  ↓
HDFS Landing
```

En lugar de utilizar únicamente almacenamiento temporal local.

Una estructura posible sería:

```text
/data/visitas/landing/
    year=2026/
      month=08/
        day=17/
          report_100.txt
          report_101.txt
```

La capa Landing conservaría el archivo recibido sin modificaciones.

---

## 6. Capa Landing / Raw

La capa Landing representa la evidencia original recibida desde el SFTP.

Sus principales características serían:

- No transformar la información.
- Mantener el contenido original.
- Registrar checksum.
- Registrar archivo origen.
- Registrar fecha de llegada.
- Permitir reprocesos.
- Mantener trazabilidad.

El archivo podría almacenarse en:

```text
HDFS
```

En una arquitectura Cloud moderna, el mismo concepto podría implementarse mediante almacenamiento de objetos como:

```text
GCS
S3
ADLS
```

La lógica conceptual sería equivalente.

---

## 7. Validación estructural

La validación del layout seguiría siendo necesaria antes de procesar el contenido.

Entre las validaciones actuales se encuentran:

```text
nombre report_<consecutivo>.txt
header esperado
número de columnas
archivo no vacío
campo jyv obligatorio
```

Para archivos pequeños estas validaciones podrían continuar realizándose en Python.

Para volúmenes grandes, la lectura y validación del dataset podría realizarse mediante Spark.

Ejemplo conceptual con PySpark:

```python
df = (
    spark.read
    .option("header", True)
    .option("delimiter", ",")
    .csv(input_path)
)
```

Posteriormente se validarían aspectos como:

```text
columnas esperadas
tipos de datos
campos obligatorios
estructura del dataset
```

---

## 8. Validación de registros

Las reglas funcionales actuales se mantendrían.

Por ejemplo:

```text
email válido
fecha válida
entero válido
valores negativos inválidos
```

La diferencia sería la forma de ejecución.

En lugar de iterar registro por registro desde Python, las reglas se aplicarían mediante transformaciones y expresiones distribuidas de Spark.

Ejemplo conceptual:

```python
valid_df = (
    df
    .filter(email_is_valid)
    .filter(date_is_valid)
)
```

Los registros inválidos se separarían en otro DataFrame:

```python
invalid_df = (
    df
    .filter(validation_error)
)
```

De esta manera se mantienen las mismas reglas de calidad, pero aplicadas de forma distribuida.

---

## 9. Capa Silver - datos válidos

Los registros válidos se escribirían preferentemente en formato Parquet.

Por ejemplo:

```text
/data/visitas/silver/estadistica/
```

El dataset podría particionarse por fecha:

```text
year
month
day
```

Generando una estructura como:

```text
/data/visitas/silver/estadistica/
  year=2026/
    month=08/
      day=17/
```

Parquet proporciona ventajas importantes para escenarios analíticos:

- almacenamiento columnar;
- compresión;
- predicate pushdown;
- lectura selectiva de columnas;
- menor volumen de datos leído;
- mejor rendimiento analítico.

---

## 10. Manejo de errores

La tabla `errores` de la solución actual podría convertirse en un dataset de registros rechazados.

Por ejemplo:

```text
/data/visitas/rejects/
```

El dataset podría contener información como:

```text
run_id
file_id
file_name
source_line
raw_record
error_code
error_message
processing_date
```

También podría registrarse como una tabla Hive:

```text
visitas_db.errores
```

De esta manera los errores continuarían siendo auditables y consultables.

La separación conceptual seguiría siendo:

```text
Dataset origen
     │
     ├── registros válidos   → Silver
     │
     └── registros inválidos → Rejects
```

---

## 11. Tabla `estadistica`

La tabla MySQL:

```text
estadistica
```

pasaría a ser una tabla Hive sobre archivos Parquet.

Conceptualmente:

```sql
CREATE EXTERNAL TABLE estadistica (
    email STRING,
    jyv STRING,
    badmail STRING,
    baja STRING,
    fecha_envio TIMESTAMP,
    fecha_open TIMESTAMP,
    opens INT,
    opens_virales INT,
    fecha_click TIMESTAMP,
    clicks INT,
    clicks_virales INT,
    links STRING,
    ips STRING,
    navegadores STRING,
    plataformas STRING,
    source_file STRING,
    source_line BIGINT
)
PARTITIONED BY (
    year INT,
    month INT,
    day INT
)
STORED AS PARQUET;
```

Impala podría consultar la información registrada en el catálogo/metastore compartido por el entorno Hadoop.

Esto permitiría utilizar la misma información tanto para procesamiento batch como para consultas analíticas.

---

## 12. Tabla `visitante`

La tabla `visitante` requiere una estrategia diferente porque representa un agregado acumulado por email.

Actualmente MySQL utiliza una estrategia de UPSERT.

Conceptualmente, en Spark se realizarían agregaciones distribuidas.

Por ejemplo:

```text
estadistica
    ↓
Spark
    ↓
groupBy(email)
    ↓
MIN(fecha)
MAX(fecha)
COUNT(*)
visitas año actual
visitas mes actual
    ↓
visitante
```

Ejemplo conceptual:

```python
visitor_df = (
    statistics_df
    .groupBy("email")
    .agg(
        min("fecha_envio"),
        max("fecha_envio"),
        count("*"),
    )
)
```

Sin embargo, para volúmenes grandes no sería conveniente recalcular todo el histórico diariamente.

Se utilizaría una estrategia incremental.

---

## 13. Estrategia incremental para `visitante`

En lugar de procesar nuevamente todo el histórico:

```text
visitante actual
      +
agregado de visitas nuevas
      ↓
Spark
      ↓
merge de agregados
      ↓
visitante actualizado
```

El nuevo lote se agregaría primero por email.

Posteriormente se combinaría con el estado acumulado de `visitante`.

Conceptualmente:

```text
visitor_current
      JOIN
visitor_daily
      ON email
```

Se actualizarían campos como:

```text
fechaPrimeraVisita
fechaUltimaVisita
visitasTotales
visitasAnioActual
visitasMesActual
```

Esto evita recalcular todo el histórico en cada ejecución.

La implementación exacta dependería de las capacidades transaccionales y formato de tabla disponibles en el cluster.

---

## 14. Hive e Impala

Hive e Impala pueden formar parte del mismo ecosistema de datos, pero cumplen funciones diferentes.

### Hive

Es apropiado principalmente para:

- procesamiento batch;
- ETL;
- SQL sobre grandes datasets;
- integración con el ecosistema Hadoop.

### Impala

Está orientado principalmente a:

- consultas analíticas interactivas;
- baja latencia;
- exploración de datos;
- consumo desde herramientas analíticas.

Conceptualmente:

```text
Spark
  ↓
Parquet
  ↓
Metastore / Catalog
  ↓
┌──────────────┐
│              │
Hive          Impala
Batch         Consulta interactiva
```

Spark realizaría principalmente las transformaciones pesadas y Hive/Impala proporcionarían acceso SQL a los datasets resultantes.

---

## 15. Particionamiento

El particionamiento se vuelve especialmente importante al trabajar con grandes volúmenes.

Para `estadistica`, una estrategia posible sería:

```text
year
month
day
```

Por ejemplo, una consulta:

```sql
SELECT *
FROM estadistica
WHERE year = 2026
  AND month = 8;
```

podría limitar la lectura únicamente a las particiones necesarias.

No sería recomendable particionar por columnas de cardinalidad extremadamente alta, como:

```text
email
```

porque podría generar una cantidad excesiva de particiones y archivos pequeños.

---

## 16. Small Files Problem

El requerimiento indica que no se conoce exactamente cuántos archivos pueden llegar diariamente.

En Hadoop esto introduce una consideración adicional:

```text
Small Files Problem
```

Una gran cantidad de archivos pequeños genera sobrecarga de metadata y puede reducir la eficiencia de procesamiento.

Por lo tanto, después de la ingesta podría realizarse un proceso de compactación.

Por ejemplo:

```text
500 TXT pequeños
      ↓
Spark
      ↓
archivos Parquet de mayor tamaño
```

La cantidad y tamaño final de archivos dependería del volumen real y de la configuración del cluster.

Esto mejora:

- administración de metadata;
- lectura de datos;
- paralelismo;
- rendimiento de consultas.

---

## 17. Idempotencia

La regla principal se mantiene:

```text
un archivo no debe procesarse exitosamente más de una vez
```

La estrategia actual basada en:

```text
file_name
checksum SHA-256
etl_file_control
```

puede conservarse.

Antes del procesamiento:

```text
calcular checksum
       ↓
buscar checksum previamente procesado
       ↓
       ├── existe → SKIPPED_ALREADY_PROCESSED
       │
       └── no existe → procesar
```

Además, los datasets resultantes pueden conservar información de trazabilidad:

```text
run_id
file_id
source_file
source_line
```

Esto permite conocer el origen de cada registro.

---

## 18. Reprocesos

Los reprocesos deben continuar siendo idempotentes.

Supongamos que existe una partición:

```text
year=2026/month=08/day=17
```

Si fuera necesario reprocesar ese día, la estrategia sería identificar el alcance afectado y reconstruir únicamente esa partición o conjunto de datos.

Conceptualmente:

```text
identificar partición afectada
          ↓
reprocesar origen correspondiente
          ↓
validar nuevo resultado
          ↓
reemplazar partición
```

Esto evita simplemente agregar nuevamente los mismos registros y producir duplicados.

La estrategia concreta de reemplazo o MERGE dependerá del formato y capacidades transaccionales disponibles en la plataforma.

---

## 19. Control operacional

Las tablas:

```text
etl_run_control
etl_file_control
```

no necesariamente tendrían que migrarse a Hadoop.

Pueden permanecer en una base de datos relacional utilizada específicamente para metadata operacional.

Conceptualmente:

```text
                 Airflow
                    │
        ┌───────────┴───────────┐
        │                       │
        ▼                       ▼
  Control Database           Hadoop
        │                       │
etl_run_control             datasets
etl_file_control            negocio
```

Esto resulta conveniente porque las tablas de control manejan volúmenes pequeños y requieren consultas operativas rápidas.

La separación permite:

- seguimiento de estados;
- auditoría;
- trazabilidad;
- locking;
- monitoreo;
- dashboards operativos.

---

## 20. Backup y retención

En la arquitectura actual se utiliza:

```text
archivo procesado
      ↓
Backup ZIP
```

En Hadoop, la capa Landing / Raw podría convertirse en el respaldo histórico del archivo original.

Por ejemplo:

```text
SFTP
  ↓
HDFS Landing / Raw
  ↓
validación de persistencia
  ↓
procesamiento
```

Si la política de retención permite conservar el archivo original en Raw, no sería obligatorio generar un ZIP individual para cada archivo.

Sin embargo, si existe un requerimiento contractual o de negocio que exige explícitamente archivos ZIP, la estrategia actual podría mantenerse.

---

## 21. Borrado seguro del SFTP

La regla de seguridad no cambia:

```text
NUNCA eliminar el archivo del SFTP
antes de confirmar que existe una copia persistente.
```

En la arquitectura Big Data:

```text
SFTP
  ↓
HDFS Landing / Raw
  ↓
validar copia
  ↓
validar checksum
  ↓
procesamiento
  ↓
confirmar persistencia
  ↓
eliminar del SFTP
```

El borrado debería continuar siendo auditable.

Por ejemplo:

```text
source_deleted_at
```

De esta manera puede demostrarse cuándo se eliminó el archivo del origen.

---

## 22. Orquestación con Airflow

Airflow continuaría siendo el componente de orquestación.

Una posible estructura del DAG sería:

```text
preflight
   ↓
start_run
   ↓
discover_files
   ↓
stage_to_hdfs[]
   ↓
spark_processing
   ↓
validate_outputs
   ↓
finalize_run
   ↓
emit_alerts
   ↓
validate_run
```

Dynamic Task Mapping puede seguir utilizándose cuando los archivos sean independientes.

Sin embargo, para cantidades muy grandes de archivos podría ser más eficiente agruparlos y ejecutar un job Spark por lote en lugar de lanzar un job Spark independiente por cada archivo.

---

## 23. Ejecución de Spark

Airflow debe funcionar como orquestador y no como motor de procesamiento distribuido.

Conceptualmente:

```text
Airflow
   ↓
Spark Submit
   ↓
YARN
   ↓
Spark Cluster
```

El worker de Airflow enviaría el trabajo al cluster y supervisaría su ejecución.

En una arquitectura Cloud administrada, el mismo patrón podría ser:

```text
Cloud Composer / Airflow
        ↓
Dataproc Job
        ↓
Spark
```

La separación de responsabilidades sería:

```text
Airflow → orquestación
Spark   → procesamiento
HDFS    → almacenamiento
Hive    → metadata / SQL
Impala  → consultas interactivas
```

---

## 24. Escalabilidad

La solución Python actual es suficiente mientras se cumplan condiciones como:

```text
volumen limitado
archivos relativamente pequeños
memoria de un nodo suficiente
tiempo de ejecución dentro del SLA
```

Una migración hacia Spark tendría sentido cuando aparezcan condiciones como:

```text
el volumen supera la capacidad de una máquina
el SLA deja de cumplirse
se requiere mayor paralelismo
existen grandes joins o agregaciones
el volumen crece hacia cientos de GB o TB
```

La decisión de utilizar Spark debe basarse en necesidades reales de volumen, procesamiento y SLA.

---

## 25. Por qué no utilizar Spark directamente en la solución actual

Los archivos proporcionados para el ejercicio son pequeños.

Utilizar Spark desde el inicio introduciría:

```text
mayor infraestructura
mayor tiempo de inicialización
mayor consumo de recursos
mayor complejidad operacional
```

sin aportar una ventaja proporcional para el volumen actual.

Por esa razón, la decisión para la solución actual es:

```text
volumen actual
      ↓
Python
```

manteniendo una evolución posible hacia:

```text
volumen Big Data
      ↓
Spark / PySpark
```

Esta decisión evita sobrearquitectura y mantiene la solución proporcional al problema actual.

---

## 26. Flujo completo propuesto

La arquitectura completa quedaría conceptualmente:

```text
                      SFTP
                        │
                        ▼
                  Airflow Discovery
                        │
                        ▼
                  Checksum Control
                        │
                        ▼
                 HDFS Landing / Raw
                        │
                        ▼
                 Spark Processing
                        │
               ┌────────┴────────┐
               │                 │
               ▼                 ▼
           Valid Data         Rejects
               │                 │
               ▼                 ▼
            Parquet           Parquet
               │                 │
               └────────┬────────┘
                        ▼
                 Metastore / Catalog
                        │
              ┌─────────┴─────────┐
              ▼                   ▼
            Hive                Impala
              │                   │
              └─────────┬─────────┘
                        ▼
              Visitor Aggregation
                        │
                        ▼
                  Curated / Gold
                        │
                        ▼
                 Analytics / BI
```

---

## 27. Equivalencias entre ambas arquitecturas

| Solución actual | Hadoop / Big Data |
|---|---|
| Python | PySpark / Spark |
| Storage local temporal | HDFS |
| TXT staging | HDFS Landing / Raw |
| MySQL `estadistica` | Parquet + Hive/Impala |
| MySQL `errores` | Reject dataset + Hive |
| MySQL `visitante` | Curated aggregate dataset |
| INSERT | Escritura distribuida |
| UPSERT | Merge incremental / reconstrucción controlada |
| Índices | Particionamiento y organización física |
| Transacciones MySQL | Estrategia idempotente por datasets/particiones |
| Backup ZIP | Retención Landing / Raw |
| Airflow | Airflow |
| Base de control | Base relacional de metadata operacional |

---

## 28. Ventajas de la arquitectura Big Data

La arquitectura permitiría:

- procesamiento distribuido;
- escalabilidad horizontal;
- soporte para mayores volúmenes;
- almacenamiento columnar;
- paralelismo;
- procesamiento incremental;
- consultas interactivas mediante Impala;
- integración con el ecosistema Hadoop;
- separación entre almacenamiento y procesamiento;
- optimización de lecturas mediante particionamiento.

---

## 29. Consideraciones adicionales

Al aumentar significativamente el volumen sería necesario considerar aspectos como:

```text
tamaño de archivos
número de particiones Spark
shuffle
data skew
small files
compaction
memory tuning
executor sizing
retención
lineage
schema evolution
```

Estas consideraciones no representan un problema relevante para el volumen actual del ejercicio, pero pasan a ser importantes al escalar la solución.

---

## 30. Conclusión

La solución actual utiliza Python y MySQL porque es proporcional al volumen recibido y permite mantener el proceso simple, confiable y económico.

Sin embargo, la lógica funcional no está ligada exclusivamente a esta arquitectura.

Si la cantidad de información creciera hasta requerir procesamiento distribuido, Airflow podría continuar como orquestador mientras la capa de procesamiento migraría hacia Spark y el almacenamiento analítico hacia HDFS y Parquet, exponiendo los datasets mediante Hive e Impala.

Los principales controles de la solución actual:

```text
checksum
idempotencia
auditoría
validación
rechazos
reprocesos
monitoreo
alertamiento
```

se mantendrían.

El cambio principal sería:

```text
procesamiento en un único runtime Python
                ↓
procesamiento distribuido con Spark


MySQL
                ↓
HDFS + Parquet + Hive / Impala


UPSERT transaccional
                ↓
procesamiento incremental por datasets / particiones
```

De esta manera, la misma solución lógica puede evolucionar desde un proceso ETL de volumen moderado hasta un escenario Big Data sin perder trazabilidad, calidad, idempotencia ni capacidad operativa.