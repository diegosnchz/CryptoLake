#!/bin/bash

# Create Project Root (assuming we run this from inside the root or parent, but for safety we'll just create subdirs)
# mkdir -p cryptolake
# cd cryptolake

# Create directory structure
mkdir -p .github/workflows
mkdir -p docker/airflow
mkdir -p docker/spark
mkdir -p src/config
mkdir -p src/ingestion/streaming
mkdir -p src/ingestion/batch
mkdir -p src/processing/streaming
mkdir -p src/processing/batch
mkdir -p src/processing/schemas
mkdir -p src/transformation/dbt_cryptolake
mkdir -p src/orchestration/dags
mkdir -p scripts

# Create empty files to ensure git tracks them (optional, but good practice)
touch src/config/__init__.py
touch src/ingestion/__init__.py
touch src/processing/__init__.py

# Permissions (optional)
chmod +x scripts/
