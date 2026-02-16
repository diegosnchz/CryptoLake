# Validacion local (Docker Compose)

## Fecha de validacion
- Ejecutado el **2026-02-16** en entorno local con Docker Compose.

## 1) Arranque limpio
1. `cp .env.example .env`
2. `docker compose down -v`
3. `docker compose up -d --build minio mc iceberg-rest spark-master spark-worker spark-thrift postgres airflow-init airflow-webserver airflow-scheduler zookeeper kafka kafka-ui producer api streamlit`

Comprobacion:
- `docker compose ps`
- Resultado esperado: servicios `Up` (al menos `minio`, `iceberg-rest`, `kafka`, `spark-master`, `spark-worker`, `spark-thrift`, `airflow-webserver`, `airflow-scheduler`).

## 2) Salud de servicios

### MinIO
- `curl http://localhost:9000/minio/health/live`

### Iceberg REST
- `curl http://localhost:8181/v1/config`

### Spark master
- Abrir `http://localhost:8082`

### Spark Thrift (puerto 10000)
- `docker exec airflow-webserver python -c "import socket; socket.create_connection(('spark-thrift',10000),timeout=5).close(); print('spark-thrift:10000 reachable')"`
- Resultado validado: `spark-thrift:10000 reachable`.

### Airflow
- `docker exec airflow-webserver airflow dags list | findstr cryptolake_full_pipeline`
- Resultado validado: DAG `cryptolake_full_pipeline` listado.

## 3) Pipeline de datos Bronze -> Silver -> Gold

### Bronze (streaming)
- Opcion A (ruta actual): dejar `producer` emitiendo y ejecutar `bronze-available-now`.
- Opcion B (manual):
  - `docker exec -e PYTHONPATH=/opt/spark/work-dir spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/src/processing/streaming/stream_to_bronze.py --mode available-now`

### Silver
- Comando estandar:
  - `docker exec -e PYTHONPATH=/opt/spark/work-dir spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/src/processing/batch/bronze_to_silver_1m.py`
- Fallback (si hay contencion de cores con Thrift en un entorno pequeno):
  - `docker exec spark-master bash -lc "cd /opt/spark/work-dir && export PYTHONPATH=/opt/spark/work-dir && export SPARK_MASTER='local[*]' && /opt/spark/bin/spark-submit src/processing/batch/bronze_to_silver_1m.py"`
- Nota:
  - `spark-thrift` esta limitado a 1 core (`spark.cores.max=1`) para evitar bloquear jobs batch de Spark.
- Resultado validado en esta corrida:
  - `rows_in=143632`, `rows_valid=143632`, `rows_dropped=0`.

### Gold (dbt)
- `docker exec airflow-webserver bash -lc "cd /opt/airflow/src/transformation/dbt_cryptolake && dbt debug --profiles-dir . --target prod"`
- `docker exec airflow-webserver bash -lc "cd /opt/airflow/src/transformation/dbt_cryptolake && dbt run --profiles-dir . --target prod"`
- `docker exec airflow-webserver bash -lc "cd /opt/airflow/src/transformation/dbt_cryptolake && dbt test --profiles-dir . --target prod"`
- Resultado validado:
  - `dbt debug`: `All checks passed`
  - `dbt run`: `PASS=4 ERROR=0`
  - `dbt test`: `PASS=16 ERROR=0`

## 4) Evidencia de datos en Bronze/Silver/Gold

Comando:
- `docker exec spark-master /opt/spark/bin/spark-sql -e "SELECT COUNT(*) AS bronze_count FROM cryptolake.bronze.futures_trades; SELECT COUNT(*) AS silver_count FROM cryptolake.silver.ohlcv_1m; SELECT COUNT(*) AS gold_count FROM cryptolake.gold.fact_ohlcv_1m;"`

Resultado validado (2026-02-16):
- `bronze_count = 143632`
- `silver_count = 168`
- `gold_count = 168`

## 5) Airflow full pipeline

Comandos:
1. `docker exec airflow-webserver airflow dags trigger cryptolake_full_pipeline`
2. `docker exec airflow-webserver airflow dags list-runs -d cryptolake_full_pipeline -o table`

Resultado validado:
- Se crea ejecucion manual del DAG (estado inicial `queued`/`running` segun scheduler).

## 6) Nota sobre Make en Windows
- Si `make` no esta instalado en PowerShell, usar los comandos `docker exec` equivalentes documentados en `README.md`.
- En WSL2 (recomendado) los targets Make funcionan sin cambios.
