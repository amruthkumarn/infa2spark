"""
Tests for Spark Manager functionality
"""
import pytest
from src.core.spark_manager import SparkManager

def test_spark_manager_creation():
    """Test SparkManager creation"""
    manager = SparkManager()
    assert manager.spark_session is None

def test_create_spark_session():
    """Test Spark session creation"""
    manager = SparkManager()
    
    config = {
        "spark": {
            "spark.master": "local[1]",
            "spark.sql.shuffle.partitions": "2"
        },
        "enable_hive_support": False,
        "log_level": "ERROR"
    }
    
    spark = manager.create_spark_session("TestApp", config)
    
    assert spark is not None
    assert spark.sparkContext.appName == "TestApp"
    assert manager.spark_session is not None
    
    # Test getting existing session
    same_spark = manager.get_spark_session()
    assert same_spark is spark
    
    # Cleanup
    manager.stop_spark_session()
    assert manager.spark_session is None

def test_configure_for_testing():
    """Test testing configuration"""
    manager = SparkManager()
    spark = manager.configure_for_testing()
    
    assert spark is not None
    assert "Test" in spark.sparkContext.appName
    
    # Cleanup
    manager.stop_spark_session()

def test_get_spark_session_before_creation():
    """Test error when getting session before creation"""
    manager = SparkManager()
    
    with pytest.raises(ValueError, match="Spark session not initialized"):
        manager.get_spark_session()