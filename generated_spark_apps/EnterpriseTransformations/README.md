# Enterprise_Complete_Transformations - Generated Spark Application

This Spark application was automatically generated from Informatica BDM project: **Enterprise_Complete_Transformations**

## Project Overview

- **Version**: 3.0.1
- **Description**: Complete enterprise project demonstrating all transformation types and complex workflow patterns
- **Generated on**: 2025-07-31 08:09:33

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
Enterprise_Complete_Transformations/
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

### m_Complete_Transformation_Showcase
- **Description**: 
- **Components**:
  - EXP_Data_Standardization (ExpressionTransformation)
  - JNR_Customer_Transaction (JoinerTransformation)
  - AGG_Customer_Metrics (AggregatorTransformation)
  - LKP_Customer_Demographics (LookupTransformation)
  - SEQ_Customer_Key (SequenceTransformation)
  - SRT_Customer_Ranking (SorterTransformation)
  - RTR_Customer_Segmentation (RouterTransformation)
  - UNI_Combine_Segments (UnionTransformation)
  - SCD_Customer_Dimension (JavaTransformation)
  - EXP_Product_Enrichment (ExpressionTransformation)
  - LKP_Product_Suppliers (LookupTransformation)
  - AGG_Product_Metrics (AggregatorTransformation)
  - SRT_Product_Ranking (SorterTransformation)
  - SRC_Customer_Master (SourceDefinition)
  - SRC_Transaction_History (SourceDefinition)
  - SRC_Product_Master (SourceDefinition)
  - TGT_Customer_Data_Warehouse (TargetDefinition)


## Workflows

### wf_Enterprise_Complete_ETL
- **Description**: 
- **Tasks**:
  - START (TaskInstance)
  - CMD_Pre_Processing (TaskInstance)
  - s_m_Complete_Transformation_Showcase (TaskInstance)
  - CMD_Post_Processing_Success (TaskInstance)
  - EMAIL_Success_Notification (TaskInstance)
  - CMD_Error_Handler (TaskInstance)
  - EMAIL_Failure_Notification (TaskInstance)
  - END (TaskInstance)


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

- Application logs: `logs/enterprise_complete_transformations.log`
- Spark UI: `http://localhost:4040` (when running locally)

## Support

This application was generated automatically. For modifications:
1. Update the original Informatica BDM project
2. Regenerate the Spark application
3. Or modify the generated code directly for custom requirements
