# Requisitos del profesor para CryptoLake (fases 1-6)

## Alcance y fuente
- Fuente unica de requisitos: `profesor_ref/cryptolake-general-guide.md`, `profesor_ref/cryptolake-fases-1-2.md`, `profesor_ref/cryptolake-fases-3-4.md`, `profesor_ref/cryptolake-fases-5-6.md`.
- Este documento esta actualizado al estado post-adaptacion (2026-02-16).
- Criterio: cumplir el marco del profesor sin romper el dominio actual del alumno (`futures_trades` y `ohlcv_1m`).

## Checklist por fases

### Fase 1-2 (infraestructura y base tecnica)

| ID | Requisito accionable | Evidencia (profesor_ref) | Estado en repo alumno |
|---|---|---|---|
| F12-01 | Docker Compose con MinIO, Iceberg REST, Kafka, Kafka UI, Spark master/worker, Airflow webserver/scheduler + Postgres | `profesor_ref/cryptolake-fases-1-2.md:517`, `:556`, `:597`, `:621`, `:653`, `:670`, `:688`, `:728`, `:769` | Cumple |
| F12-02 | Spark configurado para Iceberg REST + MinIO (catalogo, S3 endpoint, extensiones Iceberg) | `profesor_ref/cryptolake-fases-1-2.md:459`, `:461`, `:470`, `:478` | Cumple |
| F12-03 | Makefile con ciclo base (`up/down/logs/status/spark-shell`) | `profesor_ref/cryptolake-fases-1-2.md:809`, `:817`, `:833`, `:840`, `:849`, `:851` | Cumple |
| F12-04 | Creacion/verificacion de namespaces Iceberg `bronze/silver/gold` | `profesor_ref/cryptolake-fases-1-2.md:942`, `:943`, `:944`, `:947` | Cumple |
| F12-05 | Comprobaciones iniciales de salud de servicios | `profesor_ref/cryptolake-fases-1-2.md:1968` | Cumple |

### Fase 3-4 (Bronze -> Silver -> Gold)

| ID | Requisito accionable | Evidencia (profesor_ref) | Estado en repo alumno |
|---|---|---|---|
| F34-01 | Job de carga Bronze desde APIs batch (`api_to_bronze.py`) | `profesor_ref/cryptolake-fases-3-4.md:132`, `:488`, `:524` | Parcial |
| F34-02 | Tablas Bronze Iceberg creadas explicitamente y carga append | `profesor_ref/cryptolake-fases-3-4.md:334`, `:354`, `:399`, `:415` | Parcial |
| F34-03 | Job Bronze -> Silver con limpieza/dedup y estrategia de upsert tipo `MERGE INTO` | `profesor_ref/cryptolake-fases-3-4.md:660`, `:787`, `:793`, `:838` | Parcial |
| F34-04 | Gold en formato star schema (dims + fact) | `profesor_ref/cryptolake-fases-3-4.md:904`, `:959`, `:1012`, `:1075` | Cumple |
| F34-05 | Make targets de pipeline medallion (`bronze-load`, `silver-transform`, `gold-transform`, `pipeline`) | `profesor_ref/cryptolake-fases-3-4.md:524`, `:529`, `:534`, `:541`, `:1300` | Parcial |

### Fase 5-6 (dbt + Airflow)

| ID | Requisito accionable | Evidencia (profesor_ref) | Estado en repo alumno |
|---|---|---|---|
| F56-01 | Servicio `spark-thrift` en Docker Compose (puerto `10000`) y reutilizacion de imagen Spark | `profesor_ref/cryptolake-fases-5-6.md:99`, `:103`, `:120`, `:142` | Cumple |
| F56-02 | Proyecto dbt completo: `dbt_project.yml`, `profiles.yml`, `models/sources.yml`, staging + marts | `profesor_ref/cryptolake-fases-5-6.md:210`, `:232`, `:301`, `:358`, `:507`, `:617`, `:732` | Cumple |
| F56-03 | Macros dbt `generate_schema_name` y `create_table_as` para schemas/LOCATION Iceberg | `profesor_ref/cryptolake-fases-5-6.md:426`, `:463` | Cumple |
| F56-04 | Tests dbt (`schema.yml` + tests SQL custom) | `profesor_ref/cryptolake-fases-5-6.md:882`, `:947`, `:968` | Cumple |
| F56-05 | Targets Make para dbt (`dbt-run`, `dbt-test` y equivalente compuesto) | `profesor_ref/cryptolake-fases-5-6.md:1088`, `:1091`, `:1094` | Cumple |
| F56-06 | DAG maestro `cryptolake_full_pipeline` con secuencia ingestion -> bronze -> silver -> dbt run/test -> quality | `profesor_ref/cryptolake-fases-5-6.md:1303`, `:1353`, `:1460`, `:1468`, `:1501` | Cumple |
| F56-07 | Comandos Airflow para trigger y estado del DAG full pipeline | `profesor_ref/cryptolake-fases-5-6.md:1582`, `:1586` | Cumple |

## Observaciones de adaptacion conservadora
- Se mantiene la arquitectura de streaming existente (Kafka -> Bronze) y se anade la ruta profesor (dbt + Thrift + DAG maestro).
- No se renombraron tablas del dominio actual; dbt se adapto a `cryptolake.silver.ohlcv_1m`.
- La carga batch de APIs del profesor (`daily_prices/fear_greed`) se mantiene como opcion de ingesta, no como reemplazo del flujo principal.
- Kafka se mantiene en modo ZooKeeper (desviacion respecto a KRaft en fase 1-2 del profesor), documentado en `docs/adr/0001-kafka-mode.md`.
- Decisiones ambiguas documentadas en `docs/adr/ADR-0001-profesor-adaptacion.md`.
