# Production Readiness - ETL Visitas Web

## 1. Objetivo

Este documento define las consideraciones necesarias para liberar el proceso ETL de visitas web a un ambiente productivo.

La liberación debe garantizar:

- seguridad
- estabilidad
- trazabilidad
- idempotencia
- observabilidad
- capacidad de recuperación
- operación controlada
- separación de ambientes

---

# 2. Ambientes

La solución debe manejar ambientes independientes:

```text
Development
Testing / QA
Production
```

Cada ambiente debe disponer de configuración y recursos independientes.

No deben compartirse entre ambientes:

- bases de datos
- credenciales
- directorios de procesamiento
- backups
- conexiones Airflow
- secretos

La configuración debe externalizarse mediante variables de ambiente, conexiones administradas o mecanismos equivalentes.

---

# 3. Credenciales y secretos

Las credenciales no deben almacenarse dentro del código fuente.

Esto incluye:

```text
SFTP username/password
MySQL username/password
claves privadas
tokens
credenciales cloud
```

En Airflow se recomienda utilizar:

```text
Connections
Variables
Secret Backend
```

En ambientes cloud se recomienda utilizar un administrador de secretos, por ejemplo:

```text
Google Secret Manager
```

El repositorio Git no debe contener archivos `.env` con credenciales reales.

---

# 4. Principio de mínimo privilegio

Las identidades utilizadas por el ETL deben disponer únicamente de los permisos necesarios.

Ejemplos:

## SFTP

Permisos solamente sobre:

```text
/home/vinkOS/archivosVisitas
```

según las operaciones requeridas.

## MySQL

Permisos sobre las tablas utilizadas por el proceso:

```text
visitante
estadistica
errores
etl_run_control
etl_file_control
```

No se recomienda utilizar usuarios administradores.

## Cloud

Las service accounts deben utilizar roles específicos y evitar permisos administrativos globales.

---

# 5. Configuración

Todos los parámetros dependientes del ambiente deben externalizarse.

Ejemplos:

```text
SFTP_HOST
SFTP_PORT
SFTP_USER
SFTP_REMOTE_PATH

MYSQL_HOST
MYSQL_PORT
MYSQL_DATABASE

BACKUP_PATH

GCP_PROJECT_ID
GCS_BUCKET
GCP_REGION
BQ_DATASET
```

No deben existir valores productivos hardcodeados en el código.

---

# 6. Fecha de referencia

Para los datos históricos utilizados durante la POC se puede utilizar una fecha de referencia controlada.

En producción:

```text
reference_date = fecha de ejecución / fecha de negocio
```

Esta fecha se utiliza para calcular correctamente:

```text
visitas_anio_actual
visitas_mes_actual
```

La fecha de referencia no debe quedar hardcodeada con una fecha utilizada únicamente para pruebas.

---

# 7. Programación del DAG

El DAG productivo debe ejecutarse diariamente.

Antes de definir el horario debe acordarse con negocio:

- hora límite de generación de archivos
- zona horaria
- SLA
- ventana de procesamiento
- dependencias con otros procesos

La zona horaria debe configurarse explícitamente.

---

# 8. Concurrencia

Debe evitarse que dos ejecuciones del mismo ETL procesen simultáneamente los mismos archivos.

Se recomienda controlar:

```text
max_active_runs
```

y los mecanismos de idempotencia del propio ETL.

La concurrencia debe evaluarse considerando:

- volumen
- capacidad de MySQL
- capacidad del servidor ETL
- cantidad de archivos diarios

---

# 9. Idempotencia

Antes de producción debe probarse que:

```text
mismo archivo
+
mismo contenido
+
nuevo intento
=
sin duplicación
```

Los principales mecanismos son:

```text
checksum SHA-256
etl_file_control
file_id
restricciones UNIQUE
```

Debe realizarse una prueba formal de reproceso antes de liberar.

---

# 10. Validaciones de datos

Las reglas de validación deben estar documentadas y probadas.

Actualmente:

```text
layout
email
fechas dd/MM/yyyy HH:mm
```

Debe existir una decisión de negocio documentada sobre cualquier regla adicional.

Los registros inválidos deben conservar trazabilidad suficiente para diagnóstico.

---

# 11. Integridad referencial

Antes de producción deben validarse las relaciones:

```text
etl_run_control
       |
       v
etl_file_control
       |
       +------> estadistica
       |
       +------> errores
```

Las foreign keys permiten garantizar que cada registro cargado pueda relacionarse con su archivo y ejecución.

---

# 12. Transaccionalidad

La carga debe proteger la consistencia entre:

```text
estadistica
visitante
errores
controles
```

Ante un fallo durante una operación transaccional debe ejecutarse:

```text
ROLLBACK
```

para evitar cargas parciales inconsistentes.

Las fronteras transaccionales deben estar claramente definidas.

---

# 13. Administración de archivos

Debe garantizarse el orden:

```text
procesar
   ↓
cargar
   ↓
generar backup
   ↓
validar backup
   ↓
eliminar origen
```

Nunca debe eliminarse el archivo del SFTP antes de confirmar el backup.

---

# 14. Backup

El requerimiento establece como destino:

```text
/home/etl/visitas/bckp
```

Los archivos procesados deben almacenarse comprimidos en ZIP.

Antes de producción se debe definir:

- naming convention
- estructura de directorios
- política de retención
- capacidad de almacenamiento
- monitoreo de espacio
- permisos
- estrategia de restauración

Ejemplo:

```text
/home/etl/visitas/bckp/
└── 2026/
    └── 08/
        └── report_7_<timestamp>.zip
```

---

# 15. Capacidad del storage

Como el servidor ETL será utilizado como almacenamiento de backups, debe monitorearse:

```text
espacio disponible
crecimiento diario
crecimiento mensual
retención
capacidad máxima
```

Se recomienda generar alertas cuando el filesystem alcance determinados umbrales.

Ejemplo:

```text
70% → warning
85% → critical
```

Los valores definitivos deben acordarse con Infraestructura/Operaciones.

---

# 16. Reintentos

Los errores transitorios pueden manejarse mediante retries.

Ejemplos:

```text
timeout SFTP
conexión temporal MySQL
error temporal de red
```

Los errores funcionales no deben solucionarse mediante reintentos infinitos.

Ejemplo:

```text
layout inválido
```

seguirá siendo inválido después de un retry.

Debe distinguirse:

```text
retryable error
vs
non-retryable error
```

---

# 17. Timeouts

Las operaciones externas deben tener timeouts configurados.

Principalmente:

```text
SFTP
MySQL
storage
servicios cloud
```

Esto evita ejecuciones bloqueadas indefinidamente.

---

# 18. Observabilidad

Antes de producción deben estar disponibles como mínimo:

## Airflow

```text
estado DAG
estado tasks
duración
retries
logs
```

## Bitácora ETL

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

## Archivo

```text
status
checksum
timestamps
records
backup
delete
errores
```

---

# 19. Reconciliación

Deben configurarse controles automáticos.

Ejemplo:

```text
records_read
=
records_valid + records_invalid
```

Para archivos cargados:

```text
records_valid
=
records_loaded
```

Las inconsistencias deben producir alertas y ser investigadas.

---

# 20. Alertamiento

Se recomienda alertar ante:

```text
DAG FAILED
task FAILED
files_failed > 0
SFTP unavailable
MySQL unavailable
backup FAILED
source delete FAILED
retries exhausted
SLA exceeded
reconciliation FAILED
unexpected file volume
unexpected invalid-record volume
low backup storage
```

El canal dependerá de las herramientas corporativas:

```text
email
Teams
Slack
PagerDuty
Cloud Monitoring
```

---

# 21. SLA

Antes de producción debe definirse:

```text
hora esperada de llegada
hora de inicio
tiempo máximo de ejecución
hora máxima de disponibilidad de datos
tiempo de atención de incidentes
```

Airflow puede utilizar estos valores para monitoreo y alertamiento.

---

# 22. Volumetría

Antes de liberar debe medirse:

```text
archivos por día
registros por archivo
registros por día
tamaño de archivos
crecimiento mensual
tiempo de procesamiento
```

La implementación Python/MySQL es apropiada mientras la volumetría se encuentre dentro de la capacidad definida.

Si el volumen crece significativamente, puede evolucionarse hacia procesamiento distribuido con Spark.

---

# 23. Performance MySQL

Deben revisarse los índices necesarios para consultas y cargas.

Actualmente existen índices relacionados con:

```text
email
fecha_envio
run_id
source_file
checksum
status
```

Se debe monitorear:

- duración de INSERT/UPDATE
- locks
- tamaño de tablas
- crecimiento de índices
- conexiones
- CPU
- memoria
- I/O

---

# 24. PySpark / Big Data

Para mayores volúmenes puede utilizarse la variante PySpark.

La POC demuestra:

```text
GCS
 ↓
Dataproc Serverless
 ↓
PySpark
 ↓
Parquet
 ↓
BigQuery
```

Para producción deben definirse adicionalmente:

- estrategia de particionamiento
- lifecycle de GCS
- IAM
- service accounts
- costos
- quotas
- tamaño de batches
- observabilidad
- política de reprocesos

---

# 25. Hadoop / Hive / Impala

Si el destino productivo fuera Hadoop:

```text
SFTP
 ↓
HDFS / Landing
 ↓
Spark
 ↓
Parquet
 ↓
Hive Metastore
 ↓
Hive / Impala
```

Deben considerarse:

- particionamiento
- tamaño de archivos
- compactación
- small-files problem
- estadísticas de tablas
- permisos HDFS
- metastore
- disponibilidad del cluster
- capacidad de almacenamiento
- estrategia de reproceso

---

# 26. Pruebas antes de producción

Como mínimo deben ejecutarse:

```text
unit tests
integration tests
data-quality tests
idempotency tests
reprocessing tests
failure tests
reconciliation tests
```

Casos mínimos:

```text
archivo válido
archivo con layout incorrecto
email inválido
fecha inválida
archivo duplicado
múltiples archivos
email repetido
fallo SFTP
fallo MySQL
fallo backup
reproceso
```

---

# 27. CI/CD

Se recomienda implementar un pipeline que ejecute automáticamente:

```text
lint
unit tests
integration tests
DAG import validation
security checks
build
deployment
```

El despliegue productivo no debe realizarse directamente desde la máquina de un desarrollador.

---

# 28. Versionamiento

El código debe mantenerse bajo Git.

Flujo recomendado:

```text
feature branch
      ↓
Pull Request
      ↓
Code Review
      ↓
CI
      ↓
Approval
      ↓
main
      ↓
deploy
```

Los cambios productivos deben ser trazables hasta un commit/version.

---

# 29. Rollback

Antes de desplegar debe existir una estrategia de rollback.

Debe ser posible:

```text
identificar versión anterior
detener ejecución
restaurar versión
validar DAG
reprocesar de forma controlada
```

Un rollback de código no debe implicar automáticamente eliminar datos.

La recuperación de datos debe tratarse mediante un procedimiento separado y auditado.

---

# 30. Seguridad de datos

Se debe revisar si los emails u otros campos están sujetos a políticas corporativas de protección de datos.

Se recomienda:

- cifrado en tránsito
- cifrado en reposo
- control de acceso
- auditoría
- mínimo privilegio
- políticas de retención
- evitar información sensible en logs

---

# 31. Documentación para Operaciones

Antes del go-live, Operaciones debe recibir:

```text
arquitectura
runbook
DAG
schedule
SLA
queries de diagnóstico
matriz de errores
procedimiento de reproceso
procedimiento de escalamiento
contactos responsables
```

---

# 32. Criterios de aceptación para producción

La liberación puede considerarse lista cuando:

```text
[ ] Configuración separada por ambiente
[ ] Credenciales fuera del código
[ ] SFTP productivo validado
[ ] MySQL productivo validado
[ ] Directorio backup disponible
[ ] Permisos validados
[ ] Schedule definido
[ ] SLA definido
[ ] Idempotencia probada
[ ] Reproceso probado
[ ] Validaciones probadas
[ ] Reconciliación probada
[ ] Backups probados
[ ] Eliminación segura probada
[ ] Tests exitosos
[ ] Airflow sin import errors
[ ] Monitoreo habilitado
[ ] Alertamiento configurado
[ ] Runbook entregado
[ ] Rollback documentado
[ ] Operaciones capacitado
[ ] Aprobación para go-live
```

---

# 33. Go-Live

Durante la primera ejecución productiva se recomienda acompañamiento del equipo de desarrollo.

Se debe validar:

```text
archivos detectados
      ↓
archivos procesados
      ↓
reconciliación
      ↓
tablas destino
      ↓
backup
      ↓
eliminación origen
      ↓
bitácora
      ↓
alertas
```

Una vez estabilizado el proceso, la operación normal queda bajo responsabilidad del equipo operativo siguiendo el Runbook.