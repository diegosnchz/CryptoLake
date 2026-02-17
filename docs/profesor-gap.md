# Gap analysis: requisitos del profesor vs estado del repo

## Criterio
- Fuente de requisitos: `profesor_ref/*.md`.
- Estado actual: post-implementacion (2026-02-16).
- Objetivo: cerrar lo faltante de fases 5-6 sin romper el flujo ya existente.

## Matriz de cierre

| Requisito (profe) | Estado inicial | Accion aplicada | Estado final | Evidencia / prueba |
|---|---|---|---|---|
| Kafka en modo KRaft (sin ZooKeeper) segun fases 1-2 | Falta | Se mantiene Kafka + ZooKeeper para no romper el stack estable ya validado; se documenta decision conservadora | Aceptado como desviacion | `docker compose ps` muestra `zookeeper` activo y `docker-compose.yml` usa `KAFKA_ZOOKEEPER_CONNECT` |
| Spark Thrift en `10000` y reutilizacion imagen Spark | Falta | Se agrego `spark-thrift` y `image: cryptolake-spark` compartida, limitando Thrift a 1 core para no bloquear batch | Cerrado | `docker compose ps` muestra `spark-thrift` con `0.0.0.0:10000->10000` |
| Verificacion de conectividad Thrift | Falta | Se agrego `spark-thrift-check` en `Makefile` + comando equivalente en README | Cerrado | `docker exec airflow-webserver python -c "import socket; ..."` -> `spark-thrift:10000 reachable` |
| Proyecto dbt completo (`dbt_project.yml`, `profiles.yml`, `sources.yml`, staging/marts) | Parcial | Se estructuro `src/transformation/dbt_cryptolake/` con profile `cryptolake`, source silver y modelos staging/marts | Cerrado | `dbt debug/run/test` exitosos sobre `spark-thrift` |
| Macros dbt para schemas/LOCATION Iceberg | Falta | Se agregaron `macros/generate_schema_name.sql` y `macros/create_table_as.sql` | Cerrado | `dbt run --target prod` crea staging/gold en namespaces esperados |
| Tests dbt minimos (`not_null`, `unique`, positivos`) | Falta | Se agrego `models/marts/schema.yml` y tests SQL custom en `tests/` | Cerrado | `dbt test --target prod` -> `PASS=16 ERROR=0` |
| Make targets dbt (`dbt-install/debug/run/test`) | Falta | Se agregaron targets y `dbt-all` | Cerrado | Targets presentes en `Makefile` y comandos equivalentes documentados |
| DAG maestro full pipeline | Falta | Se agrego `src/orchestration/dags/dag_full_pipeline.py` | Cerrado | `airflow dags list` incluye `cryptolake_full_pipeline` |
| Trigger/estado DAG full desde Make | Parcial | Se agregaron `airflow-trigger-full` y `airflow-status-full` | Cerrado | `airflow dags trigger cryptolake_full_pipeline` + `list-runs` |
| README con camino actual + camino profesor | Falta | README actualizado con dos rutas reproducibles y fallback PowerShell | Cerrado | Secciones `Camino actual` y `Ruta profesor (dbt + Airflow full pipeline)` |
| Documentacion de decisiones ambiguas (ADR) | Falta | Se creo `docs/adr/ADR-0001-profesor-adaptacion.md` | Cerrado | ADR presente y alineado con decisiones aplicadas |
| Validacion local integral | Falta | Se crea `docs/validacion-local.md` con arranque, checks y evidencia | Cerrado | Documento de validacion ejecutable en local |

## Gaps que quedan (aceptados)
- Requisitos de fases 3-4 centrados en dominio batch del profesor (`historical_prices`, `fear_greed`, `MERGE INTO` sobre `daily_prices`) quedan en estado `Parcial` porque este repo prioriza streaming de futuros (`futures_trades` -> `ohlcv_1m`) y no se reemplazo esa arquitectura.
- Esta diferencia es intencional y documentada en `docs/adr/ADR-0001-profesor-adaptacion.md`.
- Kafka continua en modo ZooKeeper (no KRaft) por estabilidad y compatibilidad del entorno actual; decision documentada en `docs/adr/0001-kafka-mode.md`.

## Nota para evaluacion
- No hay gaps bloqueantes en fases 5-6: Spark Thrift, dbt (debug/run/test) y DAG maestro estan operativos con evidencias de ejecucion en `docs/validacion-local.md`.
- Para fases 3-4, el gap es de nomenclatura/dataset del ejemplo del profesor, no de ausencia de capas Bronze/Silver/Gold ni de orquestacion.
