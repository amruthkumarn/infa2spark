"""
Shared test fixtures and configuration
"""
import pytest
import tempfile
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \
        .appName("TestSpark") \
        .master("local[2]") \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def temp_data_dir():
    """Create temporary directory for test data"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_config():
    """Sample configuration for testing"""
    return {
        "spark": {
            "app.name": "TestApp",
            "master": "local[2]"
        },
        "connections": {
            "TEST_CONN": {
                "type": "HDFS",
                "base_path": "/tmp/test_data"
            }
        }
    }
