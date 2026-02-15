import os
import sys
import time
from datetime import datetime

import requests
import structlog

# Add src to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))
from src.config.settings import settings
from src.ingestion.base import BaseExtractor

logger = structlog.get_logger()


class CoinGeckoExtractor(BaseExtractor):
    def __init__(self):
        super().__init__(bucket_name=settings.minio_bucket_bronze)
        self.base_url = "https://api.coingecko.com/api/v3"

    def fetch_history(self, coin_id: str, days: int = 90):
        """Fetch historical market data for a coin."""
        url = f"{self.base_url}/coins/{coin_id}/market_chart"
        params = {
            "vs_currency": "usd",
            "days": days,
            "interval": "daily",  # or hourly if needed for shorter durations
        }

        try:
            logger.info("fetching_history", coin_id=coin_id, days=days)
            response = requests.get(url, params=params)

            if response.status_code == 429:
                logger.warning("rate_limit_hit", coin_id=coin_id, retry_after=60)
                time.sleep(60)
                return self.fetch_history(coin_id, days)

            response.raise_for_status()
            data = response.json()

            # Save to Data Lake
            timestamp = datetime.now().strftime("%Y%m%d")
            path = f"coingecko/history/{coin_id}_{days}d_{timestamp}.json"
            self.save_to_datalake(data, path)

            # Respect rate limits
            time.sleep(10)  # 6-10 secs for public API

        except Exception as e:
            logger.error("fetch_failed", coin_id=coin_id, error=str(e))

    def extract(self):
        """Batch extraction for top coins."""
        # Top coins to fetch history for
        coins = ["bitcoin", "ethereum", "solana", "binancecoin"]

        for coin in coins:
            self.fetch_history(coin, days=90)


if __name__ == "__main__":
    extractor = CoinGeckoExtractor()
    extractor.extract()
