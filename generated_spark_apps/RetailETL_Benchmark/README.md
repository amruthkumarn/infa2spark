# RetailETL_Project - Generated Spark Application

This Spark application was automatically generated from Informatica BDM project: **RetailETL_Project**

## Project Overview

- **Version**: 2.0
- **Description**: Simple Retail ETL Pipeline - Products and Sales Processing
- **Generated on**: 2025-07-22 08:07:43

## Quick Start

### Prerequisites
- Apache Spark 3.4+
- Python 3.8+
- Java 8 or 11

### Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Generate test data
python scripts/generate_test_data.py

# Run the application
./run.sh
```

### Using Docker
```bash
# Build and run with Docker Compose
docker-compose up --build
```

## Project Structure

```
RetailETL_Project/
├── src/main/python/          # Source code
│   ├── main.py              # Main application entry point
│   ├── base_classes.py      # Base classes for mappings and workflows
│   ├── mappings/            # Generated mapping implementations
│   ├── workflows/           # Generated workflow orchestration
│   └── transformations/     # Transformation logic
├── config/                  # Configuration files
│   └── application.yaml     # Main application configuration
├── data/                    # Data directories
│   ├── input/              # Input data files
│   └── output/             # Output data files
├── scripts/                 # Utility scripts
│   └── generate_test_data.py # Test data generation
├── logs/                   # Application logs
├── requirements.txt        # Python dependencies
├── Dockerfile             # Docker configuration
├── docker-compose.yml     # Docker Compose configuration
└── run.sh                 # Application run script
```

## Configuration

Edit `config/application.yaml` to customize:
- Spark configuration
- Database connections
- Application parameters

## Mappings

### Product_Load
- **Description**: Load product master data with basic transformations
- **Components**:
  - PRODUCT_SOURCE (HDFS)
  - VALIDATE_PRODUCT (Expression)
  - DIM_PRODUCT (HIVE)

### Sales_Processing
- **Description**: Process daily sales transactions with aggregations
- **Components**:
  - SALES_TRANSACTIONS (HDFS)
  - PRODUCT_LOOKUP (HIVE)
  - ENRICH_SALES (Lookup)
  - AGGREGATE_SALES (Aggregator)
  - FACT_DAILY_SALES (HIVE)


## Workflows

### Daily_Retail_ETL
- **Description**: Daily retail data processing workflow
- **Tasks**:
  - Load_Products (Mapping)
  - Process_Sales (Mapping)
  - Data_Quality_Check (Command)
  - Send_Success_Email (Email)


## Running in Production

1. **Cluster Deployment**:
   ```bash
   spark-submit --master yarn --deploy-mode cluster src/main/python/main.py
   ```

2. **Resource Configuration**:
   ```bash
   spark-submit \
     --master yarn \
     --num-executors 10 \
     --executor-memory 4g \
     --executor-cores 2 \
     src/main/python/main.py
   ```

## Monitoring

- Application logs: `logs/retailetl_project.log`
- Spark UI: `http://localhost:4040` (when running locally)

## Support

This application was generated automatically. For modifications:
1. Update the original Informatica BDM project
2. Regenerate the Spark application
3. Or modify the generated code directly for custom requirements
