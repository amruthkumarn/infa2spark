# FinancialDW_Project - Generated Spark Application

This Spark application was automatically generated from Informatica BDM project: **FinancialDW_Project**

## Project Overview

- **Version**: 3.1
- **Description**: Enterprise Financial Data Warehouse - Multi-source Integration with SCD
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
FinancialDW_Project/
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

### Customer_Dimension_Load
- **Description**: Load customer dimension with SCD Type 2 implementation
- **Components**:
  - CUSTOMER_SOURCE_ORACLE (Oracle)
  - EXISTING_CUSTOMER_DIM (HIVE)
  - STANDARDIZE_CUSTOMER (Expression)
  - DETECT_CHANGES (Lookup)
  - SCD_TYPE2_LOGIC (Java)
  - DIM_CUSTOMER_OUTPUT (HIVE)

### Transaction_Fact_Load
- **Description**: Load transaction facts with multiple source integration
- **Components**:
  - ACCOUNT_TRANSACTIONS_DB2 (DB2)
  - ACCOUNT_MASTER (Oracle)
  - PRODUCT_MASTER (HDFS)
  - CUSTOMER_DIM (HIVE)
  - JOIN_ACCOUNT_CUSTOMER (Joiner)
  - ENRICH_WITH_CUSTOMER (Lookup)
  - ENRICH_WITH_PRODUCT (Lookup)
  - CALCULATE_METRICS (Expression)
  - FACT_TRANSACTIONS (HIVE)

### Risk_Analytics_Aggregation
- **Description**: Generate risk analytics and compliance reports
- **Components**:
  - FACT_TRANSACTIONS_SOURCE (HIVE)
  - RISK_AGGREGATION (Aggregator)
  - COMPLIANCE_SCORING (Expression)
  - RISK_ANALYTICS_MART (HIVE)


## Workflows

### Financial_DW_ETL_Process
- **Description**: Complete Financial Data Warehouse ETL Process
- **Tasks**:
  - Load_Customer_Dimension (Mapping)
  - Load_Transaction_Facts (Mapping)
  - Generate_Risk_Analytics (Mapping)
  - Data_Quality_Validation (Command)
  - Generate_Compliance_Report (Command)
  - Archive_Processed_Files (Command)
  - Send_ETL_Summary (Email)


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

- Application logs: `logs/financialdw_project.log`
- Spark UI: `http://localhost:4040` (when running locally)

## Support

This application was generated automatically. For modifications:
1. Update the original Informatica BDM project
2. Regenerate the Spark application
3. Or modify the generated code directly for custom requirements
