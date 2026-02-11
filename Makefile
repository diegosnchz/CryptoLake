# Colors
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
BLUE   := $(shell tput -Txterm setaf 4)
RESET  := $(shell tput -Txterm sgr0)

setup:
	@echo "${BLUE}Creating directory structure...${RESET}"
	chmod +x create_structure.sh
	./create_structure.sh

install:
	@echo "${BLUE}Installing dependencies...${RESET}"
	pip install .

up:
	@echo "${BLUE}Starting CryptoLake services...${RESET}"
	docker-compose up -d
	@echo ""
	@echo "${GREEN}✔ CryptoLake está corriendo!${RESET}"
	@echo ""
	@echo "${YELLOW}📊 Servicios disponibles:${RESET}"
	@echo "${BLUE}MinIO Console:${RESET}   http://localhost:9001 ${RESET}(user: minioadmin / pass: minioadmin)"
	@echo "${BLUE}Kafka UI:${RESET}        http://localhost:8080"
	@echo "${BLUE}Spark Master:${RESET}    http://localhost:8082"
	@echo "${BLUE}Airflow:${RESET}         http://localhost:8083 ${RESET}(user: admin / pass: admin)"
	@echo "${BLUE}Iceberg Catalog:${RESET} http://localhost:8181/health"
	@echo ""

down:
	@echo "${YELLOW}Stopping services...${RESET}"
	docker-compose down

logs:
	docker-compose logs -f

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +

kafka-create-topics:
	@echo "${BLUE}Creating Kafka topics...${RESET}"
	docker exec kafka kafka-topics --create --if-not-exists --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 --topic prices.realtime
	@echo "${GREEN}Topic 'prices.realtime' created.${RESET}"

test:
	pytest
