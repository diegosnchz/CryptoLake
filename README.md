# CryptoLake

Realtime crypto lakehouse with Kafka, Spark Structured Streaming, Iceberg (REST catalog), MinIO, Airflow, and Streamlit.

## Architecture
```text
Binance Futures WS
  -> Producer (TradeEventV1)
  -> Kafka topic: binance_futures_realtime
  -> Spark stream: bronze.futures_trades (Iceberg)
  -> Spark batch: silver.ohlcv_1m (Iceberg)
  -> Streamlit inspector: localhost:8502

Airflow orchestrates silver batch + checks.
Bronze streaming runs as a service.
```

## Contract
- Canonical event: `TradeEventV1` in `src/contracts/events.py`
- Shared Spark schema: `src/contracts/spark_schema.py`
- Compatibility rules: `docs/contracts.md`
- Sample payload: `samples/trade_event_v1.json`

## Quickstart (10 steps)
1. `cp .env.example .env`
2. `docker-compose --env-file .env up -d minio mc iceberg-rest spark-master spark-worker`
3. `make bootstrap-iceberg`
4. `make spark-sql-check`
5. `docker-compose --env-file .env up -d zookeeper kafka kafka-ui producer`
6. `make kafka-peek`
7. `make bronze-available-now`
8. `make silver-1m`
9. `make airflow-up` and `make airflow-trigger-silver`
10. `make serve` and open `http://localhost:8502`

## Windows quickstart (WSL recommended)
- Recommended: run commands from WSL2 (Ubuntu) so `make`, shell scripts, and Docker CLI behave consistently.
- If you stay on PowerShell without `make`, use these equivalents:
  - `bootstrap-iceberg`:
    - `docker exec -e PYTHONPATH=/opt/spark/work-dir spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/src/processing/bootstrap/bootstrap_iceberg.py`
  - `kafka-peek`:
    - `docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic binance_futures_realtime --max-messages 3 --timeout-ms 30000`
  - `bronze-available-now`:
    - `docker exec -e PYTHONPATH=/opt/spark/work-dir spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/src/processing/streaming/stream_to_bronze.py --mode available-now`
  - `silver-1m`:
    - `docker exec -e PYTHONPATH=/opt/spark/work-dir spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/src/processing/batch/bronze_to_silver_1m.py`
  - `serve`:
    - `docker-compose up -d streamlit`

## Validation commands
- Bronze count: `docker exec spark-master /opt/spark/bin/spark-sql -e "SELECT count(*) AS n FROM cryptolake.bronze.futures_trades;"`
- Silver count: `docker exec spark-master /opt/spark/bin/spark-sql -e "SELECT count(*) AS n FROM cryptolake.silver.ohlcv_1m;"`
- Silver sample: `docker exec spark-master /opt/spark/bin/spark-sql -e "SELECT * FROM cryptolake.silver.ohlcv_1m ORDER BY window_start DESC LIMIT 5;"`

## Developer commands
- `make kafka-peek`
- `make bronze-available-now`
- `make silver-1m`
- `make spark-sql-check`
- `make spark-sql-check-silver`
- `make doctor`
- `make reset-kafka`
- `make clean-checkpoints` (dev only: remove bronze checkpoint to realign offsets after reset)

## Data quality in silver
- `price > 0`
- `qty >= 0`
- `symbol` not null/empty
- event timestamp not null
- metrics logged: `rows_in`, `rows_valid`, `rows_dropped`, `duration_ms`

## Troubleshooting
- `make: command not found` on Windows: run equivalent Docker commands or install GNU Make.
- Kafka `InconsistentClusterIdException`: run `make reset-kafka`.
- If consumer shows `LEADER_NOT_AVAILABLE`, wait 10-20 seconds and retry the command.
- Dev only: if `available-now` reads 0 rows after Kafka reset, run `make clean-checkpoints` to clear bronze checkpoint and realign offsets.
- Iceberg S3 region errors: verify `AWS_REGION` and `AWS_DEFAULT_REGION` are `us-east-1`.
- Streamlit port conflict: app is mapped to `localhost:8502`.
- If bronze writes 0 rows in available-now, verify producer traffic with `make kafka-peek` first.
