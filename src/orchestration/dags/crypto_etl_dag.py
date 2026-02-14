import os
import socket
from datetime import timedelta
from urllib.error import URLError
from urllib.request import urlopen

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.utils.dates import days_ago

SPARK_MASTER = os.getenv("SPARK_MASTER", "spark://spark-master:7077")
KAFKA_BROKER_HOST = os.getenv("KAFKA_HEALTH_HOST", "kafka")
KAFKA_BROKER_PORT = int(os.getenv("KAFKA_HEALTH_PORT", "29092"))
MINIO_HEALTH_URL = os.getenv("MINIO_HEALTH_URL", "http://minio:9000/minio/health/live")
ICEBERG_HEALTH_URL = os.getenv("ICEBERG_HEALTH_URL", "http://iceberg-rest:8181/v1/config")


def _kafka_reachable() -> bool:
    try:
        with socket.create_connection((KAFKA_BROKER_HOST, KAFKA_BROKER_PORT), timeout=5):
            return True
    except OSError:
        return False


def _http_ok(url: str) -> bool:
    try:
        with urlopen(url, timeout=5) as response:  # noqa: S310
            return 200 <= response.status < 500
    except (URLError, TimeoutError):
        return False


def _silver_deps_reachable() -> bool:
    return _kafka_reachable() and _http_ok(MINIO_HEALTH_URL) and _http_ok(ICEBERG_HEALTH_URL)


def _topic_hint() -> None:
    topic = os.getenv(
        "KAFKA_TOPIC_PRICES_REALTIME",
        os.getenv("KAFKA_TOPIC_FUTURES", "binance_futures_realtime"),
    )
    print(f"Using Kafka topic: {topic}")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    "ingest_realtime_to_bronze",
    default_args=default_args,
    description="Validate dependencies and launch Spark structured streaming to Bronze",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag_ingest:
    wait_for_kafka = PythonSensor(
        task_id="wait_for_kafka", python_callable=_kafka_reachable, poke_interval=15, timeout=300
    )
    print_topic = PythonOperator(task_id="print_topic", python_callable=_topic_hint)
    bootstrap = BashOperator(
        task_id="bootstrap_iceberg",
        bash_command="python /opt/airflow/src/processing/bootstrap/bootstrap_iceberg.py",
    )
    start_bronze_stream = BashOperator(
        task_id="start_bronze_stream",
        bash_command=(
            "spark-submit --master "
            + SPARK_MASTER
            + " /opt/airflow/src/processing/streaming/stream_to_bronze.py"
        ),
    )

    wait_for_kafka >> print_topic >> bootstrap >> start_bronze_stream

with DAG(
    "bronze_to_silver_1m",
    default_args=default_args,
    description="Run batch silver job and validate non-empty output table",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag_silver:
    wait_for_dependencies = PythonSensor(
        task_id="wait_for_dependencies",
        python_callable=_silver_deps_reachable,
        poke_interval=15,
        timeout=300,
    )
    run_silver = BashOperator(
        task_id="run_bronze_to_silver",
        bash_command="spark-submit --master "
        + SPARK_MASTER
        + " /opt/airflow/src/processing/batch/bronze_to_silver_1m.py",
    )
    check_silver_count = BashOperator(
        task_id="check_silver_count",
        bash_command="spark-submit --master "
        + SPARK_MASTER
        + " /opt/airflow/src/processing/batch/check_silver_count.py",
    )
    wait_for_dependencies >> run_silver >> check_silver_count

with DAG(
    "silver_to_gold_daily",
    default_args=default_args,
    description="Build daily gold stats from silver",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag_gold:
    run_gold = BashOperator(
        task_id="run_silver_to_gold_daily",
        bash_command="spark-submit --master "
        + SPARK_MASTER
        + " /opt/airflow/src/processing/gold/silver_to_gold_daily.py",
    )
