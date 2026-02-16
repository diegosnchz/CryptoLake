# Resumen de entrega: adaptacion a requisitos del profesor

## Alcance
- Fuente de requisitos usada: `profesor_ref/cryptolake-general-guide.md`, `profesor_ref/cryptolake-fases-1-2.md`, `profesor_ref/cryptolake-fases-3-4.md`, `profesor_ref/cryptolake-fases-5-6.md`.
- Criterio aplicado: integracion minima y compatible con el estado avanzado del repo del alumno.

## Que se anadio
- Servicio `spark-thrift` en `docker-compose.yml` (puerto `10000`) con configuracion Iceberg/MinIO.
- DAG maestro `cryptolake_full_pipeline` en `src/orchestration/dags/dag_full_pipeline.py`.
- Proyecto dbt completo en `src/transformation/dbt_cryptolake/`:
  - `dbt_project.yml`, `profiles.yml`, `models/sources.yml`
  - modelos `staging` y `marts` (star schema adaptado al dominio actual)
  - macros `generate_schema_name` y `create_table_as`
  - tests dbt genericos y tests SQL custom.
- Targets Make para ruta profesor:
  - `spark-thrift-up`, `spark-thrift-check`
  - `dbt-install`, `dbt-debug`, `dbt-run`, `dbt-test`, `dbt-all`
  - `airflow-trigger-full`, `airflow-status-full`
- Documentacion nueva:
  - `docs/profesor-requisitos.md`
  - `docs/profesor-gap.md`
  - `docs/validacion-local.md`
  - `docs/adr/ADR-0001-profesor-adaptacion.md`

## Que se cambio
- `README.md` actualizado con dos rutas:
  - Camino actual del alumno.
  - Ruta profesor (dbt + Airflow full pipeline).
- `src/processing/bootstrap/bootstrap_iceberg.py` extendido para asegurar namespaces necesarios por dbt.
- `docker/spark/spark-defaults.conf` ajustado para consistencia catalogo Iceberg + MinIO en entorno local.
- `docker-compose.yml` ajustado para que `spark-thrift` use 1 core (`spark.cores.max=1`) y no bloquee jobs batch.
- `docker/airflow/Dockerfile` y `docker/airflow/requirements.txt` ajustados para ejecutar dbt en contenedor Airflow.

## Que se dejo igual (y por que)
- Se conserva la arquitectura principal de streaming (`Kafka -> bronze.futures_trades`) para no romper el flujo ya operativo.
- Se mantiene la ruta Gold por PySpark existente; la ruta Gold por dbt se agrega como capa adicional para evaluacion de fases 5-6.
- No se renombraron tablas del dominio actual a nombres del ejemplo del profesor (`daily_prices`, etc.) para evitar regresiones.

## Estado final frente al profesor
- Fases 1-2: **Cumple**.
- Fases 3-4: **Parcial** en lo que depende del dominio batch original del profesor (`api_to_bronze`, `MERGE INTO daily_prices/fear_greed`), con equivalentes adaptados al dominio del alumno.
- Fases 5-6: **Cumple** (Spark Thrift + dbt operativo + Airflow DAG full pipeline + tests).

## Evidencia de validacion
- Ver `docs/validacion-local.md`.
- Resultados verificados el 2026-02-16:
  - `dbt debug`: OK
  - `dbt run`: PASS (4 modelos)
  - `dbt test`: PASS (16 tests)
  - Conteos Iceberg: Bronze `143632`, Silver `168`, Gold `168`
