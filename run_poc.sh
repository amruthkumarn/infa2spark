#!/bin/bash

# Run the Informatica to PySpark PoC
echo "Starting Informatica to PySpark PoC..."

# Set up environment
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Create necessary directories
mkdir -p output
mkdir -p sample_data
mkdir -p input

# Copy sample project XML to input directory
if [ -f "/Users/ninad/Downloads/informatica_xsd_xml/sample_project/sample_project.xml" ]; then
    cp "/Users/ninad/Downloads/informatica_xsd_xml/sample_project/sample_project.xml" input/
    echo "Copied sample project XML"
else
    echo "Warning: Sample project XML not found"
fi

# Check if running with spark-submit or python
if command -v spark-submit &> /dev/null; then
    echo "Running with spark-submit..."
    spark-submit \
        --master local[*] \
        --conf spark.sql.adaptive.enabled=true \
        --conf spark.sql.adaptive.coalescePartitions.enabled=true \
        --conf spark.sql.warehouse.dir=./spark-warehouse \
        --packages org.apache.spark:spark-avro_2.12:3.4.0 \
        src/main.py
else
    echo "Running with python (make sure PySpark is installed)..."
    python src/main.py
fi

echo "PoC execution completed. Check informatica_poc.log for details."