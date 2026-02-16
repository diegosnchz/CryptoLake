# Gap analysis: requisitos del profesor vs estado actual del alumno

## Criterio
- Fuente de requisitos: `_prof_repo/*.md`.
- Estado actual evaluado sobre: `docker-compose.yml`, `Makefile`, `README.md`, `src/processing/*`, `src/orchestration/dags/*`, `src/transformation/dbt_cryptolake/*`.
- Objetivo de este gap: implementar solo lo faltante, minimo y compatible con el pipeline actual.

## Tabla de gap y plan de accion

| Requisito (profe) | Estado actual (alumno) | Accion concreta (archivo a tocar) | Criterio de aceptacion (comando de prueba) |
|---|---|---|---|
| Spark Thrift Server expuesto en `10000` para dbt | Falta servicio `spark-thrift` en `docker-compose.yml` | Actualizar `docker-compose.yml` para agregar `spark-thrift` y etiquetar imagen Spark reutilizable | `docker compose up -d spark-master spark-thrift` y `docker logs spark-thrift | tail -n 20` debe mostrar arranque Thrift |
| Verificacion de puerto Thrift y conexion SQL | No hay comando de verificacion dedicado | Agregar target Make (`spark-thrift-check`) y seccion README de comprobacion | `make spark-thrift-check` (o equivalente) y `docker exec spark-master /opt/spark/bin/spark-sql -e "SHOW NAMESPACES IN cryptolake;"` |
| Proyecto dbt completo con `profiles.yml` + `sources.yml` | Existe `dbt_project.yml` minimo y 2 modelos sueltos; no `profiles.yml` ni `sources.yml` | Crear/actualizar `src/transformation/dbt_cryptolake/dbt_project.yml`, `profiles.yml`, `models/sources.yml` | `cd src/transformation/dbt_cryptolake && dbt debug --profiles-dir .` exitoso |
| Modelos dbt staging/marts adaptados a tablas Silver reales | Falta estructura staging/marts y el modelo actual referencia tablas inconsistentes (`futures_ohlcv_1m` vs `ohlcv_1m`) | Crear `models/staging/*` y `models/marts/*` adaptados a `cryptolake.silver.ohlcv_1m` (y otras tablas reales) | `dbt run --profiles-dir .` crea tablas Gold sin errores |
| Macros dbt para schema naming y LOCATION Iceberg | Falta `macros/generate_schema_name.sql` y `macros/create_table_as.sql` | Crear macros en `src/transformation/dbt_cryptolake/macros/` con version conservadora para catalogo actual | `dbt run --profiles-dir . --target prod` crea objetos en schema esperado (`gold`) |
| Tests dbt minimos (`not_null`, `unique`, positivos`) | Falta `schema.yml` y tests custom | Crear `models/marts/schema.yml` y `tests/assert_positive_*.sql` | `dbt test --profiles-dir .` pasa, o reporta fallos de datos reales (sin fallos de configuracion) |
| Make targets para dbt (`install/debug/run/test`) | No existen targets dbt | Extender `Makefile` con `dbt-install`, `dbt-debug`, `dbt-run`, `dbt-test` manteniendo targets actuales | `make dbt-debug`, `make dbt-run`, `make dbt-test` |
| Airflow DAG maestro de pipeline completo | Hay DAGs separados (`ingest_realtime_to_bronze`, `bronze_to_silver_1m`, `silver_to_gold_daily`) pero no full pipeline con dbt | Crear nuevo DAG `src/orchestration/dags/dag_full_pipeline.py` o ampliar el actual para orquestar silver + dbt run/test | `docker exec airflow-webserver airflow dags list | findstr cryptolake_full_pipeline` |
| Trigger/estado del DAG full pipeline desde Make | Solo existe `airflow-trigger-silver` | Agregar `airflow-trigger-full` y `airflow-status-full` en `Makefile` (sin quitar targets viejos) | `make airflow-trigger-full` y `make airflow-status-full` |
| README con dos rutas (actual y profesor) | README solo describe ruta actual | Actualizar `README.md` con seccion "Ruta profesor (dbt + airflow full pipeline)" y comandos reproducibles | Validacion manual siguiendo pasos del README sin comandos faltantes |
| Documentacion de decisiones ambiguas | No existe `docs/adr/` | Crear `docs/adr/ADR-0001-profesor-adaptacion.md` para decisiones conservadoras (nombres de tablas, coexistencia Gold PySpark/dbt, alcance batch opcional) | Archivo ADR presente y referenciado en README/docs resumen |
| Validacion local integral | No existe documento especifico de validacion de fases profesor | Crear `docs/validacion-local.md` con arranque, checks de servicios, evidencia Bronze/Silver/Gold y dbt debug/run/test | Ejecutar secuencia de comandos del documento en local Docker Compose |

## Riesgos identificados antes de implementar
- El proyecto actual mezcla nombres `ohlcv_1m` y `futures_ohlcv_1m`; hay que normalizar referencias para no romper API/Streamlit/jobs.
- `dbt` puede requerir dependencias extras en imagen Airflow o ejecucion local con entorno Python; se resolvera con targets Make y documentacion clara.
- El DAG actual usa `docker exec` desde Airflow; se mantiene este patron para compatibilidad en local.
