"""
Tests for mapping functionality
"""
import pytest
import tempfile
import os
import shutil
from src.mappings.sales_staging import SalesStaging
from src.mappings.customer_dim_load import CustomerDimLoad
from src.data_sources.data_source_manager import DataSourceManager

@pytest.fixture
def temp_output_dir():
    """Create temporary output directory"""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def test_config_with_temp_dir(sample_config, temp_output_dir):
    """Update config to use temporary directories"""
    config = sample_config.copy()
    config['connections']['HIVE_CONN']['local_path'] = temp_output_dir
    config['connections']['HDFS_CONN']['local_path'] = temp_output_dir
    return config

def test_data_source_manager_mock_data_generation(spark_session, test_config_with_temp_dir):
    """Test that DataSourceManager can generate mock data"""
    dsm = DataSourceManager(spark_session, test_config_with_temp_dir['connections'])
    
    # Test reading various source types (should generate mock data)
    sales_df = dsm.read_source("SALES_SOURCE", "HDFS")
    assert sales_df.count() > 0
    assert "sale_id" in sales_df.columns
    
    customer_df = dsm.read_source("CUSTOMER_SOURCE", "DB2")
    assert customer_df.count() > 0
    assert "customer_id" in customer_df.columns

def test_sales_staging_mapping_execution(spark_session, test_config_with_temp_dir, temp_output_dir):
    """Test complete Sales Staging mapping execution"""
    # Create the mapping
    mapping = SalesStaging(spark_session, test_config_with_temp_dir)
    
    # Execute the mapping
    result = mapping.execute()
    
    # Should return True for successful execution
    assert result is True
    
    # Check that output was created
    output_path = os.path.join(temp_output_dir, "stg_sales")
    assert os.path.exists(output_path)
    
    # Verify output data structure
    output_df = spark_session.read.parquet(output_path)
    assert output_df.count() > 0
    
    # Check for expected columns (aggregated data)
    expected_columns = ["region", "product", "total_amount", "total_sales", "load_date"]
    for col in expected_columns:
        assert col in output_df.columns
    
    # Verify data quality
    results = output_df.collect()
    for row in results:
        assert row.total_amount >= 0
        assert row.total_sales >= 1
        assert row.region is not None
        assert row.product is not None

def test_customer_dim_load_first_load(spark_session, test_config_with_temp_dir, temp_output_dir):
    """Test Customer Dimension Load mapping - first load"""
    # Create the mapping
    mapping = CustomerDimLoad(spark_session, test_config_with_temp_dir)
    
    # Execute the mapping (should be first load since no existing data)
    result = mapping.execute()
    
    # Should return True for successful execution
    assert result is True
    
    # Check that output was created
    output_path = os.path.join(temp_output_dir, "dim_customer")
    assert os.path.exists(output_path)
    
    # Verify output data structure
    output_df = spark_session.read.parquet(output_path)
    assert output_df.count() > 0
    
    # Check for SCD Type 2 columns
    scd_columns = ["effective_date", "expiry_date", "is_current", "record_version", "load_date"]
    for col in scd_columns:
        assert col in output_df.columns
    
    # All records should be current in first load
    current_records = output_df.filter(output_df.is_current == True).count()
    assert current_records == output_df.count()

def test_customer_dim_load_incremental_load(spark_session, test_config_with_temp_dir, temp_output_dir):
    """Test Customer Dimension Load mapping - incremental load"""
    # First, run the initial load
    mapping = CustomerDimLoad(spark_session, test_config_with_temp_dir)
    result = mapping.execute()
    assert result is True
    
    # Read the initial output
    output_path = os.path.join(temp_output_dir, "dim_customer")
    initial_df = spark_session.read.parquet(output_path)
    initial_count = initial_df.count()
    
    # Now run again (simulating incremental load)
    # Note: In a real scenario, we would have new source data
    # For this test, we're just verifying the mapping can run again
    result = mapping.execute()
    assert result is True
    
    # Verify the output still exists and has data
    final_df = spark_session.read.parquet(output_path)
    assert final_df.count() >= initial_count

def test_mapping_error_handling(spark_session, test_config_with_temp_dir):
    """Test mapping error handling"""
    # Create a mapping with invalid configuration
    invalid_config = test_config_with_temp_dir.copy()
    invalid_config['connections'] = {}  # Remove connections
    
    mapping = SalesStaging(spark_session, invalid_config)
    
    # Should handle errors gracefully
    with pytest.raises(Exception):
        mapping.execute()

def test_data_source_manager_write_operations(spark_session, test_config_with_temp_dir, temp_output_dir, sample_sales_data):
    """Test DataSourceManager write operations"""
    dsm = DataSourceManager(spark_session, test_config_with_temp_dir['connections'])
    
    # Test writing to different target types
    test_targets = [
        ("test_hive_table", "HIVE"),
        ("test_hdfs_file", "HDFS"),
    ]
    
    for target_name, target_type in test_targets:
        dsm.write_target(sample_sales_data, target_name, target_type)
        
        # Verify output was created
        if target_type == "HIVE":
            output_path = os.path.join(temp_output_dir, target_name.lower())
        else:
            output_path = os.path.join(temp_output_dir, target_name.lower())
        
        assert os.path.exists(output_path)
        
        # Verify we can read the data back
        if target_type == "HIVE":
            result_df = spark_session.read.parquet(output_path)
        else:
            result_df = spark_session.read.parquet(output_path)
        
        assert result_df.count() == sample_sales_data.count()

def test_mapping_with_real_data_files(spark_session, test_config_with_temp_dir, temp_output_dir, sample_sales_data):
    """Test mapping with actual data files instead of mock data"""
    # Create sample data files
    sales_input_path = os.path.join(temp_output_dir, "sales_source.parquet")
    customer_input_path = os.path.join(temp_output_dir, "customer_source.csv")
    
    # Write sample data
    sample_sales_data.write.mode("overwrite").parquet(sales_input_path)
    
    # Create customer data
    customer_data = [
        ("CUST_000001", "Customer One", "New York", "Active", "2022-01-01", "customer1@email.com"),
        ("CUST_000002", "Customer Two", "Los Angeles", "Active", "2022-02-01", "customer2@email.com"),
    ]
    
    from pyspark.sql.types import StructType, StructField, StringType
    customer_schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("email", StringType(), True)
    ])
    
    customer_df = spark_session.createDataFrame(customer_data, customer_schema)
    customer_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(customer_input_path)
    
    # Update config to point to actual files
    config = test_config_with_temp_dir.copy()
    
    # Test DataSourceManager with real files
    dsm = DataSourceManager(spark_session, config['connections'])
    
    # Read the sales data
    sales_df = dsm._read_hdfs("sales_source", "parquet", path=sales_input_path)
    assert sales_df.count() == sample_sales_data.count()
    
    # Read the customer data
    customer_read_df = dsm._read_jdbc("customer_source", "db2", path=customer_input_path)
    assert customer_read_df.count() == 2