GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
BLUE   := $(shell tput -Txterm setaf 4)
RESET  := $(shell tput -Txterm sgr0)
TOPIC ?= $(or $(KAFKA_TOPIC_PRICES_REALTIME),$(KAFKA_TOPIC_FUTURES),binance_futures_realtime)
SPARK_EXEC ?= docker exec -e PYTHONPATH=/opt/spark/work-dir spark-master

setup:
	@echo "${BLUE}Creating directory structure...${RESET}"
	chmod +x create_structure.sh
	./create_structure.sh

install:
	@echo "${BLUE}Installing dependencies...${RESET}"
	pip install -e .

up:
	@echo "${BLUE}Starting CryptoLake services...${RESET}"
	docker-compose up -d --build
	@echo "${GREEN}✔ CryptoLake running${RESET}"

bootstrap: bootstrap-iceberg

bootstrap-iceberg:
	$(SPARK_EXEC) /opt/spark/bin/spark-submit /opt/spark/work-dir/src/processing/bootstrap/bootstrap_iceberg.py

spark-sql:
	$(SPARK_EXEC) /opt/spark/bin/spark-sql

spark-sql-check:
	$(SPARK_EXEC) /opt/spark/bin/spark-sql -e "SHOW NAMESPACES IN cryptolake; SHOW TABLES IN cryptolake.bronze; DESCRIBE TABLE cryptolake.bronze.futures_trades;"

run-bronze:
	$(SPARK_EXEC) /opt/spark/bin/spark-submit /opt/spark/work-dir/src/processing/streaming/stream_to_bronze.py

bronze-available-now:
	$(SPARK_EXEC) /opt/spark/bin/spark-submit /opt/spark/work-dir/src/processing/streaming/stream_to_bronze.py --mode available-now

run-silver:
	docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/src/processing/batch/bronze_to_silver.py

run-gold:
	docker exec spark-master /opt/spark/bin/spark-submit /opt/spark/work-dir/src/processing/gold/silver_to_gold_daily.py

down:
	docker-compose down

logs:
	docker-compose logs -f

kafka-create-topics:
	docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic $(TOPIC)

kafka-peek:
	docker exec kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic $(TOPIC) --max-messages 3 --timeout-ms 30000

lint:
	ruff check src tests

format:
	ruff format src tests

test:
	pytest -q
