# Re-ingesta de requisitos del profesor (versión actual)

## Fuente y alcance
- Fuente exclusiva revisada en esta regeneración:
  - `ce-ia-big-data-main/ce-ia-big-data-main/cryptolake-fases-1-2.md`
  - `ce-ia-big-data-main/ce-ia-big-data-main/cryptolake-fases-3-4.md`
  - `ce-ia-big-data-main/ce-ia-big-data-main/cryptolake-fases-5-6.md`
- `cryptolake-general-guide.md` se toma solo como contexto (no como fuente normativa).

## Checklist por fases

### Fases 1-2

| ID | Requisito (profesor) | Ruta exacta + sección | Estado alumno | ¿Cambió vs versión anterior? |
|---|---|---|---|---|
| F12-01 | Stack base con MinIO, Iceberg REST, Kafka/Kafka UI, Spark master/worker, Airflow + Postgres en `docker-compose.yml` | `ce-ia-big-data-main/ce-ia-big-data-main/cryptolake-fases-1-2.md` → **2.6 — Crear el Docker Compose completo** | Cumple | No (mismo alcance técnico) |
| F12-02 | Configuración Spark para Iceberg REST + MinIO (catalog, endpoint S3, extensiones Iceberg) | `.../cryptolake-fases-1-2.md` → **2.4 — Crear el Dockerfile de Spark con soporte para Iceberg** | Cumple | No |
| F12-03 | Comandos base de operación en Makefile (`up/down/logs/status/spark-shell`) | `.../cryptolake-fases-1-2.md` → **2.7 — Crear el Makefile** | Cumple | No |
| F12-04 | Creación/verificación de namespaces Iceberg `bronze/silver/gold` | `.../cryptolake-fases-1-2.md` → **2.10 — Probar Spark con Iceberg** | Cumple | No |
| F12-05 | Checks de salud inicial del entorno | `.../cryptolake-fases-1-2.md` → **2.9 — Verificar que todo funciona** | Cumple | No |

### Fases 3-4

| ID | Requisito (profesor) | Ruta exacta + sección | Estado alumno | ¿Cambió vs versión anterior? |
|---|---|---|---|---|
| F34-01 | Job batch `api_to_bronze.py` para cargar APIs en Bronze | `ce-ia-big-data-main/ce-ia-big-data-main/cryptolake-fases-3-4.md` → **5.3 — Script: Cargar datos de APIs en Iceberg Bronze** | Parcial | No (sigue priorizado streaming del alumno) |
| F34-02 | Tablas Bronze explícitas + estrategia `append` | `.../cryptolake-fases-3-4.md` → **5.3 — Script: Cargar datos de APIs en Iceberg Bronze** | Parcial | No |
| F34-03 | Transformación Bronze→Silver con limpieza/dedup + `MERGE INTO` incremental | `.../cryptolake-fases-3-4.md` → **6.3 — Script: Bronze → Silver** | Parcial | No |
| F34-04 | Capa Gold en star schema (dimensiones + fact table) | `.../cryptolake-fases-3-4.md` → **6.5 — Script: Silver → Gold (Modelado Dimensional)** | Cumple | No |
| F34-05 | Targets de Makefile para medallion (`bronze-load`, `silver-transform`, `gold-transform`, `pipeline`) | `.../cryptolake-fases-3-4.md` → **5.5 — Añadir comandos al Makefile** y **6.7 — Ejecutar el pipeline completo** | Parcial | No |

### Fases 5-6

| ID | Requisito (profesor) | Ruta exacta + sección | Estado alumno | ¿Cambió vs versión anterior? |
|---|---|---|---|---|
| F56-01 | Servicio `spark-thrift` en compose (puerto 10000) reutilizando imagen Spark | `ce-ia-big-data-main/ce-ia-big-data-main/cryptolake-fases-5-6.md` → **7.2 — Añadir Spark Thrift Server al Docker Compose** | Cumple | No |
| F56-02 | Proyecto dbt completo (`dbt_project.yml`, `profiles.yml`, `models/sources.yml`, staging/marts) | `.../cryptolake-fases-5-6.md` → **7.4 a 7.9** | Cumple | No |
| F56-03 | Macros dbt (`generate_schema_name`, `create_table_as`) para namespaces/LOCATION Iceberg | `.../cryptolake-fases-5-6.md` → **7.7 — Crear macros personalizadas** | Cumple | No |
| F56-04 | Tests dbt (`schema.yml` + tests SQL custom) | `.../cryptolake-fases-5-6.md` → **7.10 — Crear tests de datos** | Cumple | No |
| F56-05 | Targets Make para dbt (`dbt-run`, `dbt-test` y compuesto) | `.../cryptolake-fases-5-6.md` → **7.13 — Actualizar Makefile y hacer commit** | Cumple | No |
| F56-06 | DAG maestro `cryptolake_full_pipeline` con secuencia ingest→bronze→silver→dbt run/test→quality | `.../cryptolake-fases-5-6.md` → **8.4 — Crear el DAG Master** | Cumple | No |
| F56-07 | Trigger y consulta de estado del DAG full pipeline | `.../cryptolake-fases-5-6.md` → **8.6 — Activar y ejecutar el DAG en Airflow** y **8.7 — Actualizar Makefile y hacer commit** | Cumple | No |

## Notas de actualización frente a la versión anterior
- Se reemplaza la referencia histórica `profesor_ref/*` por la ruta vigente `ce-ia-big-data-main/ce-ia-big-data-main/*`.
- No se detectan cambios funcionales nuevos en requisitos de fases 1-6 que obliguen a alterar el código ya adaptado del alumno.
- Se mantiene como brecha conocida el track batch canónico de fases 3-4 (API batch puro), dado que el repo del alumno prioriza su flujo streaming existente.
