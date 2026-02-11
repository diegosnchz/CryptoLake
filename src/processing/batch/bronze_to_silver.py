from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, sum, min, max, col
import logging
import sys
import os

# Configuration(no specific env vars needed as settings come from defaults)
SOURCE_TABLE = "cryptolake.bronze.realtime_prices"
TARGET_TABLE = "cryptolake.silver.binance_futures_1m"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    return SparkSession.builder \
        .appName("CryptoLake-BronzeToSilver") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    logger.info("Spark Session Created")

    # Read from Bronze Table
    try:
        bronze_df = spark.read \
            .format("iceberg") \
            .load(SOURCE_TABLE)
    except Exception as e:
        logger.error(f"Failed to read from {SOURCE_TABLE}: {e}")
        return

    # Transformation: Aggregate OHLCV (Open, High, Low, Close, Volume) per minute
    # Note: 'p' (price) and 'q' (quantity) are strings in raw schema, need casting
    silver_df = bronze_df \
        .withColumn("price", col("p").cast("double")) \
        .withColumn("quantity", col("q").cast("double")) \
        .withColumn("timestamp", (col("T") / 1000).cast("timestamp")) \
        .groupBy(window(col("timestamp"), "1 minute"), col("s").alias("symbol")) \
        .agg(
            col("window.start").alias("start_time"),
            col("window.end").alias("end_time"),
            max("price").alias("high"),
            min("price").alias("low"),
            avg("price").alias("avg_price"),
            sum("quantity").alias("volume")
        )

    # Create namespace if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS cryptolake.silver")

    # Write to Silver Table
    silver_df.write \
        .format("iceberg") \
        .mode("overwrite") \
        .save(TARGET_TABLE)

    logger.info(f"Batch processing completed. Data written to {TARGET_TABLE}")

if __name__ == "__main__":
    main()
