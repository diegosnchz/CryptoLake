import os
import socket
from datetime import datetime, timedelta
from urllib.error import URLError
from urllib.request import urlopen

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.task_group import TaskGroup

KAFKA_HOST = os.getenv("KAFKA_HEALTH_HOST", "kafka")
KAFKA_PORT = int(os.getenv("KAFKA_HEALTH_PORT", "29092"))
THRIFT_HOST = os.getenv("THRIFT_HEALTH_HOST", "spark-thrift")
THRIFT_PORT = int(os.getenv("THRIFT_HEALTH_PORT", "10000"))
MINIO_HEALTH_URL = os.getenv("MINIO_HEALTH_URL", "http://minio:9000/minio/health/live")
ICEBERG_HEALTH_URL = os.getenv("ICEBERG_HEALTH_URL", "http://iceberg-rest:8181/v1/config")


def _socket_reachable(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=5):
            return True
    except OSError:
        return False


def _http_reachable(url: str) -> bool:
    try:
        with urlopen(url, timeout=5) as response:  # noqa: S310
            return 200 <= response.status < 500
    except (URLError, TimeoutError):
        return False


def _dependencies_ready() -> bool:
    return (
        _socket_reachable(KAFKA_HOST, KAFKA_PORT)
        and _socket_reachable(THRIFT_HOST, THRIFT_PORT)
        and _http_reachable(MINIO_HEALTH_URL)
        and _http_reachable(ICEBERG_HEALTH_URL)
    )


default_args = {
    "owner": "cryptolake",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}


with DAG(
    dag_id="cryptolake_full_pipeline",
    default_args=default_args,
    description="Pipeline profesor: ingestion -> bronze -> silver -> dbt gold -> quality",
    schedule_interval="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["cryptolake", "profesor", "dbt"],
) as dag:
    wait_for_dependencies = PythonSensor(
        task_id="wait_for_dependencies",
        python_callable=_dependencies_ready,
        poke_interval=15,
        timeout=300,
    )

    with TaskGroup("ingestion", tooltip="Contexto de ingesta para ruta profesor") as ingestion_group:
        prepare_ingestion = BashOperator(
            task_id="prepare_ingestion",
            bash_command=(
                "if [ \"${RUN_BATCH_INGESTION:-false}\" = \"true\" ]; then "
                "cd /opt/airflow && "
                "python -m src.ingestion.batch.coingecko_extractor && "
                "python -m src.ingestion.batch.fear_greed_extractor; "
                "else "
                "echo 'Skipping batch extractors; streaming producer remains the primary Bronze path.'; "
                "fi"
            ),
        )

    with TaskGroup("bronze_load", tooltip="Carga a Bronze") as bronze_group:
        run_bronze_available_now = BashOperator(
            task_id="bronze_available_now",
            bash_command=(
                "if [ \"${RUN_BRONZE_AVAILABLE_NOW:-false}\" = \"true\" ]; then "
                "docker exec -e PYTHONPATH=/opt/spark/work-dir "
                "spark-master /opt/spark/bin/spark-submit "
                "/opt/spark/work-dir/src/processing/streaming/stream_to_bronze.py --mode available-now; "
                "else "
                "echo 'Skipping available-now Bronze load (set RUN_BRONZE_AVAILABLE_NOW=true to enable).'; "
                "fi"
            ),
        )

    with TaskGroup("silver_processing", tooltip="Bronze to Silver") as silver_group:
        bronze_to_silver = BashOperator(
            task_id="bronze_to_silver_1m",
            bash_command=(
                "docker exec -e PYTHONPATH=/opt/spark/work-dir "
                "spark-master /opt/spark/bin/spark-submit "
                "/opt/spark/work-dir/src/processing/batch/bronze_to_silver_1m.py"
            ),
        )
        verify_silver = BashOperator(
            task_id="verify_silver_count",
            bash_command=(
                "docker exec -e PYTHONPATH=/opt/spark/work-dir "
                "spark-master /opt/spark/bin/spark-submit "
                "/opt/spark/work-dir/src/processing/batch/check_silver_count.py"
            ),
        )
        bronze_to_silver >> verify_silver

    with TaskGroup("gold_transformation", tooltip="dbt Silver to Gold") as gold_group:
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=(
                "cd /opt/airflow/src/transformation/dbt_cryptolake && "
                "dbt run --profiles-dir . --target prod"
            ),
        )
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=(
                "cd /opt/airflow/src/transformation/dbt_cryptolake && "
                "dbt test --profiles-dir . --target prod"
            ),
        )
        dbt_run >> dbt_test

    with TaskGroup("data_quality", tooltip="Resumen de checks") as quality_group:
        quality_summary = BashOperator(
            task_id="quality_summary",
            bash_command='echo "Data quality checks completed (dbt tests + silver count)."',
        )

    wait_for_dependencies >> ingestion_group >> bronze_group >> silver_group >> gold_group >> quality_group
