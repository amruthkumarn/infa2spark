"""
Tests for Configuration Manager functionality
"""
import pytest
import tempfile
import os
import yaml
from src.core.config_manager import ConfigManager

@pytest.fixture
def temp_config_dir():
    """Create temporary config directory with test files"""
    temp_dir = tempfile.mkdtemp()
    
    # Create test config files
    connections_config = {
        "HDFS_CONN": {
            "type": "HDFS",
            "host": "localhost",
            "port": 8020
        },
        "DB2_CONN": {
            "type": "DB2",
            "host": "testdb",
            "port": 50000,
            "database": "TESTDB"
        }
    }
    
    spark_config = {
        "spark": {
            "spark.master": "local[*]",
            "spark.sql.adaptive.enabled": "true"
        },
        "enable_hive_support": False,
        "log_level": "INFO"
    }
    
    # Write config files
    with open(os.path.join(temp_dir, "connections.yaml"), 'w') as f:
        yaml.dump(connections_config, f)
        
    with open(os.path.join(temp_dir, "spark_config.yaml"), 'w') as f:
        yaml.dump(spark_config, f)
    
    yield temp_dir
    
    # Cleanup
    import shutil
    shutil.rmtree(temp_dir)

def test_config_manager_creation(temp_config_dir):
    """Test ConfigManager creation"""
    manager = ConfigManager(temp_config_dir)
    assert manager.config_dir == temp_config_dir
    assert len(manager._config_cache) == 0

def test_load_config(temp_config_dir):
    """Test loading configuration files"""
    manager = ConfigManager(temp_config_dir)
    
    # Load connections config
    connections = manager.load_config("connections")
    assert "HDFS_CONN" in connections
    assert connections["HDFS_CONN"]["type"] == "HDFS"
    
    # Test caching
    connections2 = manager.load_config("connections")
    assert connections is connections2  # Should be same object from cache

def test_get_connections_config(temp_config_dir):
    """Test getting connections configuration"""
    manager = ConfigManager(temp_config_dir)
    connections = manager.get_connections_config()
    
    assert "HDFS_CONN" in connections
    assert "DB2_CONN" in connections

def test_get_spark_config(temp_config_dir):
    """Test getting Spark configuration"""
    manager = ConfigManager(temp_config_dir)
    spark_config = manager.get_spark_config()
    
    assert "spark" in spark_config
    assert spark_config["enable_hive_support"] is False

def test_resolve_parameters():
    """Test parameter resolution"""
    manager = ConfigManager()
    
    parameters = {
        "ENV": "TEST",
        "VERSION": "1.0"
    }
    
    # Test parameter substitution
    result = manager.resolve_parameters("Environment: $$ENV, Version: $$VERSION", parameters)
    assert result == "Environment: TEST, Version: 1.0"
    
    # Test system date substitution
    result = manager.resolve_parameters("Date: $$SystemDate", parameters)
    assert "Date: 202" in result  # Should contain current year
    
    # Test non-string values
    result = manager.resolve_parameters(123, parameters)
    assert result == 123

def test_merge_configs():
    """Test configuration merging"""
    manager = ConfigManager()
    
    config1 = {"a": 1, "b": 2}
    config2 = {"b": 3, "c": 4}
    config3 = {"d": 5}
    
    merged = manager.merge_configs(config1, config2, config3)
    
    assert merged["a"] == 1
    assert merged["b"] == 3  # Should be overwritten by config2
    assert merged["c"] == 4
    assert merged["d"] == 5

def test_load_nonexistent_config():
    """Test loading non-existent configuration file"""
    manager = ConfigManager("nonexistent_dir")
    
    with pytest.raises(FileNotFoundError):
        manager.load_config("nonexistent_config")