#!/bin/bash

# Simple run script for the Spark application

echo "🚀 Starting Spark ETL Application..."

# Set PYTHONPATH
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src/app"

# Run the application
python src/app/main.py

echo "✅ Application completed"
