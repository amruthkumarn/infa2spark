"""
Pytest configuration and fixtures for the PoC tests
"""
import pytest
import tempfile
import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys

# Add src to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
src_dir = os.path.join(parent_dir, 'src')
sys.path.insert(0, src_dir)

@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    import os
    import tempfile
    
    # Set Java security manager to permissive for testing
    os.environ["JAVA_OPTS"] = "-Djava.security.manager=default"
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--master local[2] pyspark-shell"
    
    # Create temp warehouse directory
    warehouse_dir = tempfile.mkdtemp(prefix="spark-warehouse-")
    
    try:
        spark = SparkSession.builder \
            .appName("InformaticaPoC-Tests") \
            .master("local[2]") \
            .config("spark.sql.shuffle.partitions", "2") \
            .config("spark.sql.warehouse.dir", warehouse_dir) \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .config("spark.driver.host", "127.0.0.1") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")  # Reduce log noise during tests
        
        yield spark
        
    finally:
        try:
            spark.stop()
        except:
            pass
        # Clean up warehouse directory
        import shutil
        try:
            shutil.rmtree(warehouse_dir)
        except:
            pass

@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    temp_path = tempfile.mkdtemp()
    yield temp_path
    shutil.rmtree(temp_path)

@pytest.fixture
def sample_config():
    """Sample configuration for testing"""
    return {
        "connections": {
            "HDFS_CONN": {
                "type": "HDFS",
                "host": "localhost",
                "port": 8020,
                "local_path": "sample_data/"
            },
            "HIVE_CONN": {
                "type": "HIVE",
                "host": "localhost",
                "port": 10000,
                "local_path": "output/"
            }
        },
        "parameters": {
            "PROJECT_VERSION": "1.0",
            "LOAD_DATE": "2023-01-01",
            "ENV": "TEST"
        },
        "mappings": {
            "sales_staging": {
                "source_format": "parquet",
                "target_format": "parquet"
            }
        }
    }

@pytest.fixture
def sample_sales_data(spark_session):
    """Create sample sales data for testing"""
    data = [
        ("SALE_000001", "North", "Product_A", 150.0, "2023-01-15", "CUST_000001"),
        ("SALE_000002", "South", "Product_B", 75.0, "2023-01-16", "CUST_000002"),
        ("SALE_000003", "East", "Product_A", 200.0, "2023-01-17", "CUST_000003"),
        ("SALE_000004", "West", "Product_C", 0.0, "2023-01-18", "CUST_000004"),  # Invalid amount
        ("SALE_000005", None, "Product_A", 125.0, "2023-01-19", "CUST_000005"),  # Invalid region
        ("SALE_000006", "North", "Product_D", 300.0, "2022-12-31", "CUST_000006"),  # Old date
    ]
    
    schema = StructType([
        StructField("sale_id", StringType(), True),
        StructField("region", StringType(), True),
        StructField("product", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("sale_date", StringType(), True),
        StructField("customer_id", StringType(), True)
    ])
    
    return spark_session.createDataFrame(data, schema)

@pytest.fixture
def sample_customer_data(spark_session):
    """Create sample customer data for testing"""
    data = [
        ("CUST_000001", "Customer One", "New York", "Active", "2022-01-01", "customer1@email.com"),
        ("CUST_000002", "Customer Two", "Los Angeles", "Active", "2022-02-01", "customer2@email.com"),
        ("CUST_000003", "Customer Three", "Chicago", "Inactive", "2022-03-01", "customer3@email.com"),
        ("CUST_000004", "Customer Four", "Houston", "Active", "2022-04-01", "customer4@email.com"),
    ]
    
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("email", StringType(), True)
    ])
    
    return spark_session.createDataFrame(data, schema)