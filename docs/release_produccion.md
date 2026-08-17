# Estrategia de Liberación a Producción - ETL Visitas

## 1. Objetivo

Definir los controles, validaciones y actividades requeridas para liberar
el proceso ETL de visitas desde desarrollo hacia ambientes QA y Producción.

La liberación debe garantizar:

- reproducibilidad;
- seguridad;
- trazabilidad;
- rollback;
- integridad de datos;
- observabilidad;
- segregación de ambientes;
- configuración externa al código.

---

## 2. Ambientes

El proceso considera tres ambientes:

### Development

Objetivo:

Desarrollo y pruebas locales.

Componentes:

- Airflow local sobre Docker.
- MySQL local.
- SFTP simulado.
- Storage local.
- Variables mediante `.env`.

---

### QA / Staging

Objetivo:

Validación integrada antes de producción.

Componentes propuestos en GCP:

- Cloud Composer.
- Cloud SQL for MySQL.
- Cloud Storage.
- Secret Manager.
- Cloud Logging / Monitoring.
- SFTP de pruebas o endpoint controlado.

Debe utilizar datos de prueba representativos y configuración independiente
de Producción.

---

### Production

Objetivo:

Ejecución diaria del ETL con información real.

Componentes propuestos:

- Cloud Composer.
- Cloud SQL for MySQL.
- Cloud Storage.
- Secret Manager.
- Cloud Logging.
- Cloud Monitoring.

Las credenciales, recursos y datos de Producción deben permanecer
completamente separados de Development y QA.

---

## 3. Principio de promoción

El mismo código debe promoverse entre ambientes.

No deben existir ramas o modificaciones manuales del código como:

```text
if environment == "prod":
    host = "..."

## CI/CD

El repositorio utiliza un workflow de CI ejecutado mediante GitHub Actions.

El pipeline se ejecuta en:

- pushes a `main`;
- pushes a ramas `feature/*`;
- Pull Requests hacia `main`.

Quality gates:

1. Checkout del repositorio.
2. Configuración de Python.
3. Instalación de dependencias.
4. Validación de compilación.
5. Validación de que `.env` no se encuentre versionado.
6. Ejecución de unit tests.

Una Pull Request no debe promoverse a `main` si el pipeline de CI no termina correctamente.

Las pruebas de integración y end-to-end se ejecutan sobre el ambiente Docker/QA,
donde están disponibles MySQL, SFTP y Airflow.

En producción, la estrategia propuesta es:

feature branch
→ Pull Request
→ CI
→ merge main
→ deploy QA
→ smoke test
→ aprobación
→ deploy PROD