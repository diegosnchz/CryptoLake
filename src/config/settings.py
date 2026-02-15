from functools import lru_cache
from typing import List

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Don't hard-wire env_file here: unit tests should be able to control env values
    # without being overridden by a local `.env` checked into the repo.
    model_config = SettingsConfigDict(env_file_encoding="utf-8", extra="ignore")

    app_env: str = Field("dev", alias="APP_ENV")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    kafka_bootstrap_servers: str = Field("kafka:29092", alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_topic_prices_realtime: str = Field(
        "binance_futures_realtime",
        validation_alias=AliasChoices("KAFKA_TOPIC_PRICES_REALTIME", "KAFKA_TOPIC_FUTURES"),
    )
    producer_reconnect_max_backoff_seconds: int = Field(
        60,
        alias="PRODUCER_RECONNECT_MAX_BACKOFF_SECONDS",
    )

    binance_ws_base_url: str = Field("wss://fstream.binance.com/stream", alias="BINANCE_WS_BASE_URL")
    binance_symbols: str = Field("btcusdt,ethusdt,solusdt,bnbusdt", alias="BINANCE_SYMBOLS")

    minio_endpoint: str = Field("http://minio:9000", alias="MINIO_ENDPOINT")
    minio_access_key: str = Field("minioadmin", alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field("minioadmin", alias="MINIO_SECRET_KEY")
    minio_bucket_bronze: str = Field("cryptolake-bronze", alias="MINIO_BUCKET_BRONZE")
    minio_bucket_silver: str = Field("cryptolake-silver", alias="MINIO_BUCKET_SILVER")
    minio_bucket_gold: str = Field("cryptolake-gold", alias="MINIO_BUCKET_GOLD")

    iceberg_catalog_name: str = Field("cryptolake", alias="ICEBERG_CATALOG_NAME")
    iceberg_rest_uri: str = Field("http://iceberg-rest:8181", alias="ICEBERG_REST_URI")
    iceberg_namespace_bronze: str = Field("bronze", alias="ICEBERG_NAMESPACE_BRONZE")
    iceberg_namespace_silver: str = Field("silver", alias="ICEBERG_NAMESPACE_SILVER")
    iceberg_namespace_gold: str = Field("gold", alias="ICEBERG_NAMESPACE_GOLD")
    spark_master: str = Field("spark://spark-master:7077", alias="SPARK_MASTER")

    api_host: str = Field("0.0.0.0", alias="API_HOST")
    api_port: int = Field(8000, alias="API_PORT")

    @property
    def kafka_topic_futures(self) -> str:
        # Backward-compatible accessor for modules still using legacy naming.
        return self.kafka_topic_prices_realtime

    @property
    def symbols(self) -> List[str]:
        return [item.strip().lower() for item in self.binance_symbols.split(",") if item.strip()]


@lru_cache
def get_settings() -> Settings:
    # Default runtime behavior: load from `.env` if present.
    return Settings(_env_file=".env", _env_file_encoding="utf-8")


settings = get_settings()
