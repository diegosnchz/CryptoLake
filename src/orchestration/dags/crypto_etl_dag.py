from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
import os

# Add src to path for PythonOperator imports if needed
sys.path.append("/opt/airflow/src")

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Batch Processing DAG
with DAG(
    'cryptolake_batch_etl',
    default_args=default_args,
    description='Batch ETL from Bronze to Silver',
    schedule_interval=timedelta(minutes=15),
    start_date=days_ago(1),
    catchup=False,
) as dag_batch:

    # Task: Bronze to Silver Transformation
    # Assumes Spark Master is accessible at spark://spark-master:7077
    bronze_to_silver = SparkSubmitOperator(
        task_id='bronze_to_silver',
        application='/opt/spark/work-dir/src/processing/batch/bronze_to_silver.py',
        conn_id='spark_default', # Needs to be configured in Airflow UI or environment
        conf={
            "spark.master": "spark://spark-master:7077",
            "spark.submit.deployMode": "client"
        }
    )

# Batch Ingestion DAG (CoinGecko, Fear & Greed)
with DAG(
    'cryptolake_ingestion_batch',
    default_args=default_args,
    description='Batch Ingestion from APIs',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
) as dag_ingestion:

    def run_coingecko_extractor():
        # This is a simplifed execution. In production, consider DockerOperator or dedicated executor.
        # We are importing functions or running scripts.
        # For simplicity, we'll use os.system or similar if we haven't packaged it as a library.
        # Better: Import the function.
        # But we need dependencies. Airflow env has them.
        import src.ingestion.batch.coingecko_extractor as cg
        data = cg.fetch_coingecko_data()
        if data:
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"coingecko_markets_{timestamp}.json"
            cg.save_to_minio(data, filename)

    def run_fear_greed_extractor():
        import src.ingestion.batch.fear_greed_extractor as fg
        data = fg.fetch_fear_greed_data()
        if data:
            import datetime
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"fear_greed_{timestamp}.json"
            fg.save_to_minio(data, filename)

    task_coingecko = PythonOperator(
        task_id='ingest_coingecko',
        python_callable=run_coingecko_extractor
    )

    task_fear_greed = PythonOperator(
        task_id='ingest_fear_greed',
        python_callable=run_fear_greed_extractor
    )

    [task_coingecko, task_fear_greed]
