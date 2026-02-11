# CryptoLake

End-to-End Crypto Data Lakehouse using MinIO, Apache Iceberg, Spark, Kafka, and Airflow.

## Setup

1.  **Create Directories**:
    ```bash
    bash create_structure.sh
    ```

2.  **Environment Variables**:
    Copy `.env.example` to `.env` and fill in your Binance API credentials if needed (public data streams might not require them).

3.  **Deploy**:
    ```bash
    docker-compose up -d
    ```

## Components

- **MinIO**: S3-compatible storage at `localhost:9000`. Console at `localhost:9001`.
- **Kafka**: Event streaming at `localhost:9092`.
- **Spark**: Data processing master at `localhost:8080`.
- **Airflow**: Orchestrator at `localhost:8081`.
- **Iceberg REST Catalog**: `localhost:8181`.

## Monitoring

- Kafka Topic: `binance_futures_realtime`
- Iceberg Tables: `demo.bronze.binance_futures`, `demo.silver.binance_futures_1m`
