# TransformationShowcaseProject - Generated Spark Application

This Spark application was automatically generated from Informatica BDM project: **TransformationShowcaseProject**

## Project Overview

- **Version**: 1.0
- **Description**: Showcase of all reusable transformation classes
- **Generated on**: 2025-07-23 19:56:26

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
TransformationShowcaseProject/
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

### m_Transformation_Showcase
- **Description**: Mapping showcasing all transformation types
- **Components**:
  - SRC_Sales_Data (HDFS)
  - EXP_Calculate_Fields (Expression)
  - AGG_Sales_Summary (Aggregator)
  - LKP_Customer_Info (Lookup)
  - JNR_Sales_Customer (Joiner)
  - SEQ_Row_Numbers (Sequence)
  - SRT_Sort_Data (Sorter)
  - RTR_Route_Data (Router)
  - UNI_Combine_Data (Union)
  - JAVA_SCD_Logic (Java)
  - TGT_Sales_Mart (HIVE)


## Workflows

No workflows found in the project.

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

- Application logs: `logs/transformationshowcaseproject.log`
- Spark UI: `http://localhost:4040` (when running locally)

## Support

This application was generated automatically. For modifications:
1. Update the original Informatica BDM project
2. Regenerate the Spark application
3. Or modify the generated code directly for custom requirements
