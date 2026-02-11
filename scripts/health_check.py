import socket
import requests
import logging
import sys

# Configuration
SERVICES = {
    "MinIO": ("localhost", 9000),
    "Kafka": ("localhost", 9092), # Externally mapped port
    "Spark Master": ("localhost", 8080),
    "Airflow Webserver": ("localhost", 8081),
    "Iceberg REST": ("localhost", 8181)
}

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_port(host, port):
    try:
        with socket.create_connection((host, port), timeout=3):
            return True
    except (socket.timeout, ConnectionRefusedError):
        return False

def check_http(url):
    try:
        response = requests.get(url, timeout=3)
        return response.status_code == 200
    except requests.RequestException:
        return False

def main():
    logger.info("Starting health checks...")
    all_healthy = True
    
    for name, (host, port) in SERVICES.items():
        if check_port(host, port):
            logger.info(f"✅ {name} is reachable at {host}:{port}")
        else:
            logger.error(f"❌ {name} is NOT reachable at {host}:{port}")
            all_healthy = False
            
    # Check specific HTTP endpoints if needed
    # minio_health = check_http("http://localhost:9000/minio/health/live")
    
    if all_healthy:
        logger.info("🚀 All systems compliant. Ready for takeoff.")
        sys.exit(0)
    else:
        logger.error("⚠️ Some systems are down.")
        sys.exit(1)

if __name__ == "__main__":
    main()
