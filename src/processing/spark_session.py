from pyspark.sql import SparkSession

from src.config.settings import settings


def build_spark_session(app_name: str) -> SparkSession:
    catalog = settings.iceberg_catalog_name
    warehouse = f"s3://{settings.minio_bucket_bronze}/warehouse"
    spark_packages = ",".join(
        [
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.2",
            "org.apache.iceberg:iceberg-aws-bundle:1.4.2",
            "org.apache.hadoop:hadoop-aws:3.3.4",
            "com.amazonaws:aws-java-sdk-bundle:1.12.262",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
            "org.apache.commons:commons-pool2:2.11.1",
            "org.apache.kafka:kafka-clients:3.4.1",
        ]
    )

    return (
        SparkSession.builder.appName(app_name)
        .config("spark.master", settings.spark_master)
        .config("spark.jars.ivy", "/tmp/.ivy2")
        .config("spark.jars.packages", spark_packages)
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{catalog}.type", "rest")
        .config(f"spark.sql.catalog.{catalog}.uri", settings.iceberg_rest_uri)
        .config(f"spark.sql.catalog.{catalog}.warehouse", warehouse)
        .config(f"spark.sql.catalog.{catalog}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"spark.sql.catalog.{catalog}.s3.endpoint", settings.minio_endpoint)
        .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
        .config(f"spark.sql.catalog.{catalog}.s3.access-key-id", settings.minio_access_key)
        .config(f"spark.sql.catalog.{catalog}.s3.secret-access-key", settings.minio_secret_key)
        .config(f"spark.sql.catalog.{catalog}.s3.region", "us-east-1")
        .config("spark.hadoop.fs.s3a.endpoint", settings.minio_endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.access.key", settings.minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", settings.minio_secret_key)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .getOrCreate()
    )
