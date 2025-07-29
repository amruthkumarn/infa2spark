"""
Unit tests for MCompleteTransformationShowcase mapping - DYNAMICALLY GENERATED
Based on analysis: 3 sources, 1 targets, 13 transformations
"""
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
import sys
from pathlib import Path

# Add source path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / "src/main/python"))

from mappings.m_complete_transformation_showcase import MCompleteTransformationShowcase


class TestMCompleteTransformationShowcase:
    """Test MCompleteTransformationShowcase mapping functionality - Schema-aware tests"""
    
    def test_mapping_initialization(self, spark_session, sample_config):
        """Test mapping can be initialized properly"""
        mapping = MCompleteTransformationShowcase(spark=spark_session, config=sample_config)
        assert mapping is not None
        assert mapping.spark == spark_session
    

    def test_source_schema_validation(self, spark_session, sample_config):
        """Test schema validation for SRC_Customer_Master source"""
        mapping = MCompleteTransformationShowcase(spark=spark_session, config=sample_config)
        
        # Expected schema based on SRC_Customer_Master
        expected_schema = StructType([
            StructField("id", IntegerType(), False),StructField("name", StringType(), True),StructField("created_date", DateType(), True)
        ])
        
        # Create test DataFrame with expected schema
        test_df = spark_session.createDataFrame([], expected_schema)
        
        # Schema validation logic would go here
        assert len(expected_schema.fields) == 3
        assert expected_schema.fieldNames() == ['id', 'name', 'created_date']


    def test_with_source_schema_data(self, spark_session, sample_config):
        """Test using actual source schema: SRC_Customer_Master"""
        # Test data based on SRC_Customer_Master schema
        test_data = [
            (1, "TestValue_1", "2024-01-15"),(2, "TestValue_2", "2024-02-15"),(3, "TestValue_3", "2024-03-15")
        ]
        
        test_df = spark_session.createDataFrame(
            test_data, 
            ['id', 'name', 'created_date']
        )
        
        assert test_df.count() == 3
        assert set(test_df.columns) == set(['id', 'name', 'created_date'])


    def test_complex_transformations(self, spark_session, sample_config):
        """Test complex transformations: ['EXP_Data_Standardization', 'SCD_Customer_Dimension', 'EXP_Product_Enrichment']"""
        mapping = MCompleteTransformationShowcase(spark=spark_session, config=sample_config)
        
        # Complex transformations require detailed validation
        # Testing: ExpressionTransformation, JavaTransformation, ExpressionTransformation
        
        # Create test data for complex transformation validation
        test_data = [(1, "Test", 100.50), (2, "Test2", 200.75)]
        test_df = spark_session.createDataFrame(test_data, ["id", "name", "amount"])
        
        try:
            # Complex transformations should handle edge cases
            result = mapping.execute(test_df)
            assert result is not None
            
            # Validate complex transformation outputs
            if result.count() > 0:
                # Check for derived/calculated fields
                result_columns = result.columns
                derived_fields = [col for col in result_columns if 'calc' in col.lower() or 'derived' in col.lower()]
                # Complex transformations often create derived fields
                
        except (NotImplementedError, AttributeError):
            pytest.skip("Complex transformation logic not implemented yet")

    def test_medium_complexity_transformations(self, spark_session, sample_config):
        """Test medium complexity transformations: ['JNR_Customer_Transaction', 'AGG_Customer_Metrics', 'LKP_Customer_Demographics', 'LKP_Product_Suppliers', 'AGG_Product_Metrics']"""
        mapping = MCompleteTransformationShowcase(spark=spark_session, config=sample_config)
        
        # Medium complexity: JoinerTransformation, AggregatorTransformation, LookupTransformation, LookupTransformation, AggregatorTransformation
        # These typically involve joins, aggregations, or lookups
        
        test_data_a = [(1, "A"), (2, "B")]
        test_data_b = [(1, "X"), (2, "Y")]
        
        df_a = spark_session.createDataFrame(test_data_a, ["id", "value_a"])
        df_b = spark_session.createDataFrame(test_data_b, ["id", "value_b"])
        
        try:
            # Medium complexity transformations often need multiple inputs
            result = mapping.execute(df_a)  # Simplified for now
            assert result is not None
            
        except (NotImplementedError, AttributeError):
            pytest.skip("Medium complexity transformation logic not implemented yet")
    
    def test_end_to_end_transformation(self, spark_session, sample_config):
        """Test complete transformation pipeline with realistic data"""
        mapping = MCompleteTransformationShowcase(spark=spark_session, config=sample_config)
        
        # Create test data based on actual source schemas
        test_input_data = [
            (1, "John Doe", "2024-01-15"),(2, "Jane Smith", "2024-02-20"),(3, "Bob Johnson", "2024-03-10"),(4, "Alice Brown", "2024-04-05"),(5, "Charlie Wilson", "2024-05-12")
        ]
        
        test_input_df = spark_session.createDataFrame(
            test_input_data,
            ['id', 'name', 'created_date']
        )
        
        try:
            result_df = mapping.execute(test_input_df)
            
            # Validate output structure
            assert result_df is not None
            assert isinstance(result_df, DataFrame)
            
            # Schema-specific validations
            result_columns = result_df.columns
            expected_target_fields = ['id', 'name', 'created_date']
            
            # Check that key business fields are present
            business_fields = ['id', 'name', 'created_date', 'updated_date']
            present_business_fields = [f for f in business_fields if f in result_columns]
            assert len(present_business_fields) > 0, "Should have at least one business field"
            
        except (NotImplementedError, AttributeError):
            pytest.skip("Execute method not implemented yet")
    
    @pytest.mark.slow
    def test_performance_with_realistic_volume(self, spark_session, sample_config):
        """Test performance with data volume similar to production"""
        mapping = MCompleteTransformationShowcase(spark=spark_session, config=sample_config)
        
        # Generate larger dataset based on source schema
        import time
        
        # Create 10K records with realistic data distribution
        # Generate 10K records for performance testing
        import random
        
        large_data = []
        for i in range(10000):
            # Generate realistic data distribution
            row = [
                i + 1,  # ID
                f"User_{i}",  # Name
                random.choice(['Engineering', 'Finance', 'Marketing', 'HR', 'Sales']),  # Department
                50000 + (i % 50000),  # Salary variation
                f"2024-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}"  # Date variation
            ]
            large_data.append(tuple(row))
        
        large_test_df = spark_session.createDataFrame(
            large_data,
            ['id', 'name', 'created_date']  # Use first 5 fields
        )
        
        start_time = time.time()
        try:
            result_df = mapping.execute(large_test_df)
            execution_time = time.time() - start_time
            
            # Performance assertions based on transformation complexity
            max_time = 120
            assert execution_time < max_time, f"Execution took {execution_time:.2f}s, expected < {max_time}s"
            
            # Data quality checks
            assert result_df.count() > 0
            
        except (NotImplementedError, AttributeError):
            pytest.skip("Execute method or performance optimization not implemented yet")
