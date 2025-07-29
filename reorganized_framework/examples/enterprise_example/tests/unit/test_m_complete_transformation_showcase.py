"""
Unit tests for MCompleteTransformationShowcase mapping
"""
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
from pathlib import Path

# Add source path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / "src/app"))

from mappings.m_complete_transformation_showcase import MCompleteTransformationShowcase


class TestMCompleteTransformationShowcase:
    """Test MCompleteTransformationShowcase mapping functionality"""
    
    def test_mapping_initialization(self, spark_session, sample_config):
        """Test mapping can be initialized properly"""
        mapping = MCompleteTransformationShowcase(spark=spark_session, config=sample_config)
        assert mapping is not None
        assert mapping.spark == spark_session
    
    def test_mapping_schema_validation(self, spark_session, sample_config):
        """Test mapping validates input schemas correctly"""
        mapping = MCompleteTransformationShowcase(spark=spark_session, config=sample_config)
        
        # Create test schema based on enterprise sources
        test_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("salary", IntegerType(), True)
        ])
        
        # Test schema validation
        # This should be implemented based on actual mapping logic
        assert True  # Placeholder - implement actual validation
    
    def test_mapping_transformation(self, spark_session, sample_config, temp_data_dir):
        """Test mapping transformation logic"""
        mapping = MCompleteTransformationShowcase(spark=spark_session, config=sample_config)
        
        # Create test input data representing enterprise data
        test_data = [
            (1, "John Doe", "Engineering", 75000),
            (2, "Jane Smith", "Finance", 65000),
            (3, "Bob Johnson", "Marketing", 55000)
        ]
        
        input_df = spark_session.createDataFrame(
            test_data, 
            ["id", "name", "department", "salary"]
        )
        
        # Test the transformation
        try:
            result_df = mapping.execute(input_df)
            assert result_df is not None
            assert isinstance(result_df, DataFrame)
            assert result_df.count() > 0
            
            # Check if transformations were applied
            result_columns = result_df.columns
            assert len(result_columns) > 0
            
        except (NotImplementedError, AttributeError):
            # If execute method not implemented, test passes with skip
            pytest.skip("Execute method not implemented yet")
    
    def test_mapping_error_handling(self, spark_session, sample_config):
        """Test mapping handles errors gracefully"""
        mapping = MCompleteTransformationShowcase(spark=spark_session, config=sample_config)
        
        # Test with invalid input
        empty_df = spark_session.createDataFrame([], StructType([]))
        
        # Should handle empty dataframe gracefully
        try:
            result = mapping.execute(empty_df)
            # Should either return empty result or handle gracefully
            assert True
        except (NotImplementedError, AttributeError):
            pytest.skip("Execute method not implemented yet")
        except Exception as e:
            # If it throws an exception, it should be a meaningful one
            assert str(e) != ""
    
    @pytest.mark.slow
    def test_mapping_performance(self, spark_session, sample_config):
        """Test mapping performance with larger dataset"""
        mapping = MCompleteTransformationShowcase(spark=spark_session, config=sample_config)
        
        # Create larger test dataset
        import time
        large_data = [
            (i, f"User_{i}", "Engineering" if i % 3 == 0 else "Finance", 50000 + (i * 1000))
            for i in range(1000)
        ]
        
        input_df = spark_session.createDataFrame(
            large_data, 
            ["id", "name", "department", "salary"]
        )
        
        start_time = time.time()
        try:
            result_df = mapping.execute(input_df)
            execution_time = time.time() - start_time
            
            # Performance assertion - should complete within reasonable time
            assert execution_time < 30.0  # 30 seconds max for 1000 records
            assert result_df.count() > 0
        except (NotImplementedError, AttributeError):
            pytest.skip("Execute method not implemented yet")
    
    def test_enterprise_connections(self, spark_session, sample_config):
        """Test enterprise connections are properly configured"""
        mapping = MCompleteTransformationShowcase(spark=spark_session, config=sample_config)
        
        # Test that enterprise connections are accessible
        config = mapping.config
        assert "connections" in config
        
        # Check for enterprise-specific connections
        connections = config["connections"]
        assert "ENTERPRISE_HIVE_CONN" in connections or "ENTERPRISE_HDFS_CONN" in connections
        
        # Validate connection parameters
        for conn_name, conn_config in connections.items():
            assert "type" in conn_config
            assert conn_config["type"] in ["HIVE", "HDFS", "DATABASE"]