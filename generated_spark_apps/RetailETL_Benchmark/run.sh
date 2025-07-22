#!/bin/bash

echo "Starting RetailETL_Project Spark Application..."

# Set PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src/main/python"

# Create necessary directories
mkdir -p data/output
mkdir -p logs

# Run the application
spark-submit \
  --master local[*] \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  src/main/python/main.py \
  --config config/application.yaml

echo "Application completed"
