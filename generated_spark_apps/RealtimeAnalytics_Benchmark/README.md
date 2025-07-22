# RealtimeAnalytics_Platform - Generated Spark Application

This Spark application was automatically generated from Informatica BDM project: **RealtimeAnalytics_Platform**

## Project Overview

- **Version**: 4.0
- **Description**: Real-Time Analytics Platform - IoT, Streaming, and ML Pipeline
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
RealtimeAnalytics_Platform/
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

### IoT_Stream_Processing
- **Description**: Real-time IoT sensor data processing with anomaly detection
- **Components**:
  - IOT_SENSOR_STREAM (Kafka)
  - PARSE_IOT_JSON (Expression)
  - ENRICH_WITH_DEVICE_META (Lookup)
  - ANOMALY_DETECTION (Java)
  - REAL_TIME_AGGREGATIONS (Aggregator)
  - REAL_TIME_METRICS (Kafka)
  - IOT_DATA_LAKE (HDFS)

### User_Behavior_Stream
- **Description**: Real-time user behavior analytics and personalization
- **Components**:
  - CLICKSTREAM_EVENTS (Kafka)
  - USER_PROFILES (HDFS)
  - SESSION_WINDOW_ANALYTICS (Aggregator)
  - REAL_TIME_PERSONALIZATION (Java)
  - SENTIMENT_ANALYSIS (Expression)
  - USER_BEHAVIOR_STREAM (Kafka)
  - PERSONALIZATION_CACHE (Redis)

### Fraud_Detection_Pipeline
- **Description**: Real-time fraud detection with ML models
- **Components**:
  - TRANSACTION_STREAM (Kafka)
  - FEATURE_ENGINEERING (Expression)
  - ML_FRAUD_SCORING (Java)
  - COMPLEX_EVENT_PROCESSING (Aggregator)
  - RISK_SCORING (Expression)
  - FRAUD_ALERTS (Kafka)
  - TRANSACTION_SCORES (ElasticSearch)


## Workflows

### Real_Time_Analytics_Pipeline
- **Description**: Continuous real-time analytics pipeline
- **Tasks**:
  - Start_IoT_Processing (Mapping)
  - Start_User_Behavior (Mapping)
  - Start_Fraud_Detection (Mapping)
  - Monitor_Stream_Health (Command)
  - ML_Model_Refresh (Command)


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

- Application logs: `logs/realtimeanalytics_platform.log`
- Spark UI: `http://localhost:4040` (when running locally)

## Support

This application was generated automatically. For modifications:
1. Update the original Informatica BDM project
2. Regenerate the Spark application
3. Or modify the generated code directly for custom requirements
