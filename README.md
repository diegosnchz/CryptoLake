# CryptoLake

Lakehouse reproducible para cripto en tiempo real con **Kafka + Spark Structured Streaming + Iceberg + MinIO + Airflow + FastAPI + Streamlit**.

## Estado actual del repo (resumen 10-15 líneas)
1. Existe infraestructura Docker Compose con MinIO, Kafka (con ZooKeeper), Spark master/worker, Airflow y catálogo Iceberg REST.
2. Ya había productor Binance por websocket, pero publicaba con naming inconsistente (`prices.realtime` vs `binance_futures_realtime`).
3. Había jobs Spark para Bronze y Silver, pero sin contrato de evento formal ni estrategia de idempotencia robusta.
4. El DAG de Airflow estaba mezclado con procesos batch legacy y rutas/configuración parcialmente desalineadas.
5. El proyecto tenía `pydantic-settings` y `structlog`, pero uso incompleto/inconsistente entre módulos.
6. No existía API operativa para consultar Silver/Gold desde clientes.
7. No existía dashboard Streamlit funcional consumiendo endpoints reales.
8. Existían extractores batch auxiliares (CoinGecko / Fear & Greed), no integrados en arquitectura Bronze/Silver/Gold principal.
9. Había dependencias dbt definidas, pero sin proyecto dbt mínimo dentro del repo.
10. Faltaba CI para lint/tests y tooling DX (pre-commit/ruff) robusto.
11. README era breve y no tenía quickstart integral, troubleshooting ni queries de validación.

## Gaps y contradicciones detectadas
- Topic de Kafka contradictorio: `KAFKA_TOPIC_PRICES_REALTIME=prices.realtime` vs `KAFKA_TOPIC_FUTURES=binance_futures_realtime`.
- Tabla Bronze previa (`realtime_prices`) no seguía contrato unificado de evento ni naming consistente por dominio futures.
- Agregación Silver previa con `overwrite` no era segura para reinicios y podía romper idempotencia.
- Configuración repartida entre defaults hardcodeados y variables de entorno no alineadas.

## Plan recomendado en PRs pequeños (prioridad)
1. **PR1 Config & Naming**: unificar ENV/topic, settings typed y logging estructurado.
2. **PR2 Contract + Producer**: contrato Binance Futures + productor websocket público + tests unitarios.
3. **PR3 Iceberg Bootstrap**: namespaces/tablas Bronze/Silver/Gold idempotentes.
4. **PR4 Bronze Streaming**: ingest Kafka->Iceberg Bronze con particionado y checkpoints.
5. **PR5 Silver/Gold**: OHLCV 1m con watermark + upsert, y métricas diarias Gold.
6. **PR6 Airflow DAGs**: 3 DAGs separados con sensores/retries/parametrización.
7. **PR7 API/UI + Observability**: FastAPI + Streamlit + health/metrics.
8. **PR8 DX/CI/Docs**: ruff/pre-commit, GitHub Actions, README operativo end-to-end.

---

## Arquitectura objetivo (ASCII)

```text
Binance Futures WS
      |
      v
Python Producer -> Kafka topic: binance_futures_realtime
      |
      v
Spark Structured Streaming
      |
      +--> Bronze (Iceberg): cryptolake.bronze.futures_trades
      |
      +--> Silver (Iceberg): cryptolake.silver.futures_ohlcv_1m (upsert 1m)
      |
      +--> Gold (Iceberg): cryptolake.gold.futures_daily_stats

Airflow DAGs orquestan bootstrap y jobs
FastAPI expone consultas analíticas
Streamlit consume FastAPI
```

## Contrato de evento (Binance Futures)
Campos normalizados:
- `symbol`
- `event_time`
- `price`
- `qty`
- `side`
- `trade_id`
- `exchange`
- `ingest_ts`

## Estructura principal
- `src/contracts`: contrato de eventos.
- `src/ingestion/streaming`: productor websocket -> Kafka.
- `src/processing/bootstrap`: bootstrap Iceberg.
- `src/processing/streaming`: Kafka -> Bronze.
- `src/processing/batch`: Bronze -> Silver 1m.
- `src/processing/gold`: Silver -> Gold diario.
- `src/orchestration/dags`: DAGs Airflow.
- `src/api`: API FastAPI.
- `src/ui`: dashboard Streamlit.
- `src/transformation/dbt_cryptolake`: proyecto dbt mínimo.

## Quickstart real
```bash
cp .env.example .env
make up
make kafka-create-topics
make bootstrap
```

> El productor corre en contenedor `producer` y publica en `KAFKA_TOPIC_FUTURES`.

## Ejecución pipelines
```bash
make run-bronze
make run-silver
make run-gold
```

## Verificación rápida
```bash
python scripts/health_check.py
curl http://localhost:8000/health
curl "http://localhost:8000/ohlcv/1m?symbol=BTCUSDT&start=2025-01-01%2000:00:00&end=2025-12-31%2023:59:59"
```

### Queries Spark SQL de validación
```sql
SELECT count(*) FROM cryptolake.bronze.futures_trades;
SELECT symbol, window_start, close, volume FROM cryptolake.silver.futures_ohlcv_1m ORDER BY window_start DESC LIMIT 20;
SELECT symbol, trade_date, total_volume, price_range, trades FROM cryptolake.gold.futures_daily_stats ORDER BY trade_date DESC LIMIT 20;
```

## Checklist de aceptación (operativo)
- Kafka UI (`localhost:8080`): topic `binance_futures_realtime` con tráfico.
- MinIO (`localhost:9001`): buckets bronze/silver/gold con datos/checkpoints.
- Iceberg REST (`localhost:8181`): namespaces/tables creados por bootstrap.
- Airflow (`localhost:8083`): DAGs `ingest_realtime_to_bronze`, `bronze_to_silver_1m`, `silver_to_gold_daily`.
- API (`localhost:8000`): `/health`, `/metrics`, `/ohlcv/1m`, `/stats/daily` respondiendo.
- Streamlit (`localhost:8501`): tabla OHLCV + gráfico.

## Troubleshooting
- Si Spark no conecta a catálogo: validar `ICEBERG_REST_URI` y `MINIO_*`.
- Si no hay datos en Kafka: revisar logs del contenedor `producer`.
- Si no hay datos en Silver/Gold: confirmar que Bronze tenga eventos y que checkpoints existan.
- Si Airflow falla en spark-submit: confirmar `SPARK_MASTER=spark://spark-master:7077`.

## dbt
Se incluye proyecto mínimo `src/transformation/dbt_cryptolake` porque `dbt-spark` ya está en stack. Se recomienda evolucionar con tests/sources en PR siguiente para entornos multi-job.

## Validate Bronze end-to-end
1. `cp .env.example .env`
2. `docker-compose --env-file .env up -d minio mc iceberg-rest spark-master spark-worker`
3. `make bootstrap-iceberg`
4. `make spark-sql-check`
5. `docker-compose --env-file .env up -d zookeeper kafka kafka-ui producer`
6. `make kafka-peek`
7. `make bronze-available-now`
8. `docker exec spark-master /opt/spark/bin/spark-sql -e "SELECT count(*) AS n FROM cryptolake.bronze.futures_trades;"`

Expected result: step 8 returns `n > 0`.

Notes:
- Spark workers run with `AWS_REGION/AWS_DEFAULT_REGION=us-east-1` so Iceberg `S3FileIO` can write to MinIO without region errors.
- Producer runs as `python -m src.ingestion.streaming.binance_producer` to avoid PYTHONPATH hacks.

## Silver data quality
- The 1m silver job drops rows where `price <= 0`.
- The 1m silver job drops rows where `qty < 0`.
- The 1m silver job drops rows with null/empty `symbol`.
- The 1m silver job drops rows with null event timestamp (`event_time`).
- Metrics are logged per run as `rows_in`, `rows_valid`, `rows_dropped`, `duration_ms` in spark-submit output.

## Airflow orchestration
- Bronze streaming runs as a service (`producer` + `stream_to_bronze.py`), not as an Airflow task.
- Airflow DAG `bronze_to_silver_1m` orchestrates the silver batch pipeline only.
- Task order: dependency healthcheck -> `bronze_to_silver_1m.py` -> `check_silver_count.py`.
- Launch with `make airflow-up` and trigger with `make airflow-trigger-silver`.
