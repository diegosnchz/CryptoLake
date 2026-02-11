import requests
import json
import boto3
import os
import logging
from datetime import datetime

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_NAME = "bronze"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def fetch_fear_greed_data():
    url = "https://api.alternative.me/fng/?limit=1"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.error(f"Error fetching Fear & Greed Index: {e}")
        return None

def save_to_minio(data, filename):
    s3 = boto3.client('s3',
                      endpoint_url=MINIO_ENDPOINT,
                      aws_access_key_id=MINIO_ACCESS_KEY,
                      aws_secret_access_key=MINIO_SECRET_KEY)
    
    try:
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=f"fear_greed/{filename}",
            Body=json.dumps(data)
        )
        logger.info(f"Successfully saved {filename} to MinIO bucket {BUCKET_NAME}")
    except Exception as e:
        logger.error(f"Error saving to MinIO: {e}")

if __name__ == "__main__":
    data = fetch_fear_greed_data()
    if data:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"fear_greed_{timestamp}.json"
        save_to_minio(data, filename)
