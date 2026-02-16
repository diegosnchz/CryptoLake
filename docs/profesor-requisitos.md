# Requisitos del profesor para CryptoLake (fases 1-6)

## Alcance y fuente
- Fuente unica usada para requisitos: repo `joseluis-m/ce-ia-big-data` clonado en `_prof_repo/`.
- Archivos base: `_prof_repo/cryptolake-general-guide.md`, `_prof_repo/cryptolake-fases-1-2.md`, `_prof_repo/cryptolake-fases-3-4.md`, `_prof_repo/cryptolake-fases-5-6.md`.
- Este documento no replica el proyecto del profesor; solo extrae requisitos accionables para evaluar el repo del alumno.

## Checklist por fases

### Fase 1-2 (infraestructura y base tecnica)

| ID | Requisito accionable | Evidencia (repo profesor) | Estado en repo alumno |
|---|---|---|---|
| F12-01 | Docker Compose con MinIO, Iceberg REST, Kafka, Kafka UI, Spark master/worker, Airflow webserver/scheduler + Postgres | `_prof_repo/cryptolake-fases-1-2.md:517`, `:556`, `:597`, `:621`, `:653`, `:670`, `:688`, `:728`, `:769` | Cumple |
| F12-02 | Spark configurado para Iceberg REST + MinIO (catalogo, S3 endpoint, extensiones Iceberg) | `_prof_repo/cryptolake-fases-1-2.md:459`, `:461`, `:470`, `:478` | Cumple |
| F12-03 | Makefile con ciclo base (`up/down/logs/status/spark-shell`) | `_prof_repo/cryptolake-fases-1-2.md:809`, `:817`, `:833`, `:840`, `:849`, `:851` | Cumple |
| F12-04 | Creacion/verificacion de namespaces Iceberg `bronze/silver/gold` | `_prof_repo/cryptolake-fases-1-2.md:942`, `:943`, `:944`, `:947` | Cumple |
| F12-05 | Comprobaciones iniciales de salud de servicios | `_prof_repo/cryptolake-fases-1-2.md:1968` | Cumple |

### Fase 3-4 (Bronze -> Silver -> Gold)

| ID | Requisito accionable | Evidencia (repo profesor) | Estado en repo alumno |
|---|---|---|---|
| F34-01 | Job de carga Bronze desde APIs batch (`api_to_bronze.py`) | `_prof_repo/cryptolake-fases-3-4.md:132`, `:488`, `:524` | Parcial |
| F34-02 | Tablas Bronze Iceberg creadas explicitamente y carga append | `_prof_repo/cryptolake-fases-3-4.md:334`, `:354`, `:399`, `:415` | Parcial |
| F34-03 | Job Bronze -> Silver con limpieza/dedup y `MERGE INTO` | `_prof_repo/cryptolake-fases-3-4.md:660`, `:787`, `:793`, `:838` | Cumple |
| F34-04 | Gold en formato star schema (dims + fact) | `_prof_repo/cryptolake-fases-3-4.md:904`, `:959`, `:1012`, `:1075` | Falta |
| F34-05 | Make targets de pipeline medallion (`bronze-load`, `silver-transform`, `gold-transform`, `pipeline`) | `_prof_repo/cryptolake-fases-3-4.md:524`, `:529`, `:534`, `:541`, `:1300` | Parcial |

### Fase 5-6 (dbt + Airflow)

| ID | Requisito accionable | Evidencia (repo profesor) | Estado en repo alumno |
|---|---|---|---|
| F56-01 | Servicio `spark-thrift` en Docker Compose (puerto `10000`) y reutilizacion de imagen Spark | `_prof_repo/cryptolake-fases-5-6.md:99`, `:103`, `:120`, `:142` | Falta |
| F56-02 | Proyecto dbt completo: `dbt_project.yml`, `profiles.yml`, `models/sources.yml`, staging + marts | `_prof_repo/cryptolake-fases-5-6.md:210`, `:232`, `:301`, `:358`, `:507`, `:617`, `:732` | Parcial |
| F56-03 | Macros dbt `generate_schema_name` y `create_table_as` para schemas/LOCATION Iceberg | `_prof_repo/cryptolake-fases-5-6.md:426`, `:463` | Falta |
| F56-04 | Tests dbt (`schema.yml` + test SQL custom) | `_prof_repo/cryptolake-fases-5-6.md:882`, `:947`, `:968` | Falta |
| F56-05 | Targets Make para dbt (`dbt-run`, `dbt-test` y equivalente compuesto) | `_prof_repo/cryptolake-fases-5-6.md:1088`, `:1091`, `:1094` | Falta |
| F56-06 | DAG maestro `cryptolake_full_pipeline` con secuencia ingestion -> bronze -> silver -> dbt run/test -> quality | `_prof_repo/cryptolake-fases-5-6.md:1303`, `:1353`, `:1460`, `:1468`, `:1501` | Falta |
| F56-07 | Comandos Airflow para trigger y estado del DAG full pipeline | `_prof_repo/cryptolake-fases-5-6.md:1582`, `:1586` | Parcial |

## Observaciones para adaptacion conservadora
- El repo del profesor usa dominio principal `daily_prices/fear_greed`; el repo del alumno usa `futures_trades` y `ohlcv_1m`.
- Se debe adaptar dbt `sources.yml` y modelos a tablas reales del alumno (sin renombrados destructivos), manteniendo compatibilidad con flujo actual.
- El requisito estricto para evaluacion de fases 5-6 es: Spark Thrift + dbt operativo + Airflow orquestando run/test dbt.
