from abc import ABC, abstractmethod
import logging
import structlog
import boto3
import json
import os
from datetime import datetime

# Configure structlog
structlog.configure(
    processors=[
        structlog.processors.JSONRenderer()
    ],
    logger_factory=structlog.STDLIB_FACTORY,
)

logger = structlog.get_logger()

class BaseExtractor(ABC):
    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name
        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        
        self.s3_client = boto3.client('s3',
                                      endpoint_url=self.minio_endpoint,
                                      aws_access_key_id=self.minio_access_key,
                                      aws_secret_access_key=self.minio_secret_key)

    @abstractmethod
    def extract(self):
        """Implement the logic to fetch data from the source."""
        pass

    def save_to_datalake(self, data: dict, path: str):
        """Saves data to MinIO (S3) bucket."""
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=path,
                Body=json.dumps(data)
            )
            logger.info("data_saved", bucket=self.bucket_name, path=path, status="success")
        except Exception as e:
            logger.error("save_failed", bucket=self.bucket_name, path=path, error=str(e))
            raise e
