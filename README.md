# Informatica BDM to PySpark Converter - PoC

This Proof of Concept (PoC) demonstrates the conversion of Informatica BDM (Business Data Model) objects to PySpark-based Python code that can be executed using `spark3-submit`.

## Overview

The PoC converts the sample Informatica project (`sample_project.xml`) into executable PySpark code that replicates the ETL logic, including:

- **4 Mappings**: Sales Staging, Customer Dimension Load, Fact Order Load, Aggregate Reports
- **1 Workflow**: Daily ETL Process with task dependencies
- **Multiple Transformations**: Expression, Aggregator, Lookup, Joiner, Java (SCD Type 2)
- **Various Data Sources**: HDFS, Hive, DB2, Oracle

## Project Structure

```
informatica_to_pyspark_poc/
├── README.md
├── requirements.txt
├── run_poc.sh                    # Main execution script
├── config/                       # Configuration files
│   ├── connections.yaml
│   ├── sample_project_config.yaml
│   └── spark_config.yaml
├── src/                          # Source code
│   ├── main.py                   # Main application entry point
│   ├── core/                     # Core framework
│   │   ├── base_classes.py
│   │   ├── xml_parser.py
│   │   ├── spark_manager.py
│   │   └── config_manager.py
│   ├── transformations/          # Transformation implementations
│   │   └── base_transformation.py
│   ├── mappings/                 # Mapping implementations
│   │   ├── sales_staging.py
│   │   └── customer_dim_load.py
│   ├── workflows/                # Workflow orchestration
│   │   └── daily_etl_process.py
│   ├── data_sources/             # Data source management
│   │   ├── data_source_manager.py
│   │   └── mock_data_generator.py
│   └── utils/                    # Utilities
│       └── notifications.py
├── tests/                        # Test cases
│   └── test_xml_parser.py
├── input/                        # Input XML files
├── output/                       # Output data files
└── sample_data/                  # Mock input data
```

## Features Implemented

### Core Framework
- **XML Parser**: Parses Informatica project XML files and extracts objects
- **Spark Manager**: Manages Spark session creation and configuration
- **Configuration Manager**: Handles YAML-based configuration files
- **Base Classes**: Abstract base classes for mappings, transformations, and workflows

### Transformations
- **Expression Transformation**: Filtering and calculated columns
- **Aggregator Transformation**: Group by and aggregation operations
- **Lookup Transformation**: Join operations for lookups
- **Joiner Transformation**: Multi-source joins
- **Java Transformation**: Custom logic including SCD Type 2

### Data Sources
- **Mock Data Generator**: Creates realistic test data
- **Multi-format Support**: Parquet, CSV, Avro simulation
- **Connection Management**: Abstracts different data source types

### Workflow Orchestration
- **Task Dependencies**: Maintains execution order based on dependencies
- **Error Handling**: Comprehensive error handling and logging
- **Notifications**: Email notification simulation

## Setup and Installation

### Prerequisites
- Python 3.8+
- Apache Spark 3.4+ (if using spark-submit)
- Java 8 or 11

### Installation Steps

1. **Clone/Download the PoC code**
2. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up PySpark (if not using spark-submit):**
   ```bash
   pip install pyspark[sql]==3.4.0
   ```

4. **Copy the sample project XML:**
   ```bash
   cp /Users/ninad/Downloads/informatica_xsd_xml/sample_project/sample_project.xml input/
   ```

## Running the PoC

### Option 1: Using the run script (Recommended)
```bash
./run_poc.sh
```

### Option 2: Using spark-submit directly
```bash
spark3-submit \
  --master local[*] \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \
  src/main.py
```

### Option 3: Using Python directly
```bash
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"
python src/main.py
```

## Sample Output

When executed successfully, the PoC will:

1. **Parse the XML project** and extract object definitions
2. **Generate mock data** for all sources (sales, customer, order, product data)
3. **Execute mappings in sequence:**
   - Sales Staging: Filters and aggregates sales data
   - Customer Dimension Load: Implements SCD Type 2 logic
4. **Write results** to output directory in Parquet format
5. **Send notifications** (simulated via logging)
6. **Generate execution logs** in `informatica_poc.log`

## Configuration

### Connections (`config/connections.yaml`)
```yaml
HDFS_CONN:
  type: HDFS
  host: namenode.company.com
  port: 8020
  local_path: sample_data/  # For PoC simulation
```

### Spark Settings (`config/spark_config.yaml`)
```yaml
spark:
  spark.master: "local[*]"
  spark.sql.adaptive.enabled: "true"
  spark.sql.warehouse.dir: "./spark-warehouse"
```

### Project Parameters (`config/sample_project_config.yaml`)
```yaml
parameters:
  LOAD_DATE: "$$SystemDate"
  ENV: "DEV"
  PROJECT_VERSION: "1.0"
```

## Testing

Run the test suite:
```bash
pytest tests/ -v
```

## Output Structure

After execution, check the `output/` directory for results:
```
output/
├── stg_sales/           # Sales staging results
├── dim_customer/        # Customer dimension with SCD Type 2
└── informatica_poc.log  # Detailed execution logs
```

## Conversion Logic

### XML to PySpark Mapping

| Informatica Object | PySpark Equivalent | Implementation |
|-------------------|-------------------|----------------|
| Mapping | Python Class | BaseMapping subclass |
| Source | DataFrame read | DataSourceManager |
| Expression Transform | DataFrame.withColumn() | ExpressionTransformation |
| Aggregator Transform | DataFrame.groupBy().agg() | AggregatorTransformation |
| Lookup Transform | DataFrame.join() | LookupTransformation |
| Target | DataFrame.write | DataSourceManager |
| Workflow | Python Class | BaseWorkflow orchestration |

### Transformation Examples

**Expression Filter:**
```python
# Informatica: amount > 0 AND region IS NOT NULL
df.filter("amount > 0 AND region IS NOT NULL")
```

**Aggregation:**
```python
# Informatica: GROUP BY region, product; SUM(amount)
df.groupBy("region", "product").agg(sum("amount").alias("total_amount"))
```

**SCD Type 2:**
```python
# Automatically handles effective dates, expiry dates, and current flags
scd_transformation.transform(source_df, existing_dim_df)
```

## Limitations and Next Steps

### Current Limitations
- Mock data sources (no actual DB connections)
- Simplified SCD Type 2 implementation
- Email notifications are simulated
- Limited error recovery mechanisms

### Planned Enhancements
1. **Real data source connections** (JDBC, HDFS, Hive)
2. **Advanced expression parsing** from Informatica syntax
3. **Complete workflow dependency handling**
4. **Performance optimization** and monitoring
5. **Security and authentication** integration

## Support and Development

This PoC demonstrates the feasibility of converting Informatica BDM projects to PySpark. The modular architecture allows for easy extension and customization based on specific requirements.

For questions or enhancements, refer to the detailed logging in `informatica_poc.log` and the comprehensive error handling throughout the codebase.