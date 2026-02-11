from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
import logging
import sys
import os

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from src.processing.schemas.bronze import binance_futures_aggtrade_schema
from src.config.settings import settings

# Configuration
CHECKPOINT_LOCATION = f"s3a://{settings.MINIO_BUCKET_BRONZE}/checkpoints/bronze/binance_futures"
TABLE_NAME = "cryptolake.bronze.realtime_prices"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("CryptoLake-StreamToBronze") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    logger.info("Spark Session Created")

    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", settings.KAFKA_TOPIC_PRICES_REALTIME) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON
    parsed_df = df.select(
        from_json(col("value").cast("string"), binance_futures_aggtrade_schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")

    # Add ingestion timestamp
    final_df = parsed_df.withColumn("ingestion_time", current_timestamp())

    # Create namespace if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS cryptolake.bronze")

    # Write to Iceberg
    query = final_df.writeStream \
        .format("iceberg") \
        .outputMode("append") \
        .trigger(processingTime="1 minute") \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .toTable(TABLE_NAME)

    logger.info(f"Streaming query started to table {TABLE_NAME}...")
    query.awaitTermination()

if __name__ == "__main__":
    main()
