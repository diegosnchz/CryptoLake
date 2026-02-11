from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field

class Settings(BaseSettings):
    # Kafka
    KAFKA_BOOTSTRAP_SERVERS: str = Field("localhost:9092", alias="KAFKA_BOOTSTRAP_SERVERS")
    KAFKA_TOPIC_PRICES_REALTIME: str = Field("prices.realtime", alias="KAFKA_TOPIC_PRICES_REALTIME")

    # MinIO
    MINIO_ENDPOINT: str = Field("http://localhost:9000", alias="MINIO_ENDPOINT")
    MINIO_ACCESS_KEY: str = Field("minioadmin", alias="MINIO_ACCESS_KEY")
    MINIO_SECRET_KEY: str = Field("minioadmin", alias="MINIO_SECRET_KEY")
    MINIO_BUCKET_BRONZE: str = Field("cryptolake-bronze", alias="MINIO_BUCKET_BRONZE")
    MINIO_BUCKET_SILVER: str = Field("cryptolake-silver", alias="MINIO_BUCKET_SILVER")
    MINIO_BUCKET_GOLD: str = Field("cryptolake-gold", alias="MINIO_BUCKET_GOLD")

    # APIs
    BINANCE_WS_URL: str = "wss://fstream.binance.com/ws/btcusdt@aggTrade"
    COINGECKO_API_URL: str = "https://api.coingecko.com/api/v3/coins/markets"
    FEAR_GREED_API_URL: str = "https://api.alternative.me/fng/?limit=1"

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

settings = Settings()
