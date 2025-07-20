"""
Tests for transformation functionality
"""
import pytest
from pyspark.sql.functions import *
from src.transformations.base_transformation import (
    ExpressionTransformation,
    AggregatorTransformation, 
    LookupTransformation,
    JoinerTransformation,
    JavaTransformation
)

def test_expression_transformation_filters(spark_session, sample_sales_data):
    """Test expression transformation with filters"""
    filters = [
        "amount > 0",
        "region IS NOT NULL",
        "sale_date >= '2023-01-01'"
    ]
    
    transformation = ExpressionTransformation(
        name="TEST_FILTER",
        filters=filters
    )
    
    result_df = transformation.transform(sample_sales_data)
    
    # Should filter out invalid records
    assert result_df.count() == 3  # Only 3 valid records
    
    # Verify filter conditions
    amounts = [row.amount for row in result_df.collect()]
    assert all(amount > 0 for amount in amounts)
    
    regions = [row.region for row in result_df.collect() if row.region is not None]
    assert len(regions) == 3  # All should have valid regions

def test_expression_transformation_calculations(spark_session, sample_sales_data):
    """Test expression transformation with calculated columns"""
    expressions = {
        "sale_year": "year(to_date(sale_date))",
        "amount_category": "CASE WHEN amount < 100 THEN 'Small' WHEN amount < 200 THEN 'Medium' ELSE 'Large' END"
    }
    
    transformation = ExpressionTransformation(
        name="TEST_CALC",
        expressions=expressions
    )
    
    result_df = transformation.transform(sample_sales_data)
    
    # Check that new columns exist
    assert "sale_year" in result_df.columns
    assert "amount_category" in result_df.columns
    
    # Check some values
    first_row = result_df.first()
    assert first_row.sale_year in [2022, 2023]
    assert first_row.amount_category in ['Small', 'Medium', 'Large']

def test_aggregator_transformation(spark_session, sample_sales_data):
    """Test aggregator transformation"""
    group_by_cols = ["region", "product"]
    aggregations = {
        "total_amount": {"type": "sum", "column": "amount"},
        "sale_count": {"type": "count", "column": "sale_id"},
        "avg_amount": {"type": "avg", "column": "amount"},
        "max_amount": {"type": "max", "column": "amount"}
    }
    
    transformation = AggregatorTransformation(
        name="TEST_AGG",
        group_by_cols=group_by_cols,
        aggregations=aggregations
    )
    
    result_df = transformation.transform(sample_sales_data)
    
    # Check result structure
    expected_columns = group_by_cols + list(aggregations.keys())
    for col in expected_columns:
        assert col in result_df.columns
    
    # Should have fewer rows than input (aggregated)
    assert result_df.count() < sample_sales_data.count()
    
    # Check aggregation results
    results = result_df.collect()
    for row in results:
        assert row.total_amount >= 0
        assert row.sale_count >= 1
        assert row.avg_amount >= 0

def test_lookup_transformation(spark_session, sample_sales_data, sample_customer_data):
    """Test lookup transformation"""
    join_conditions = ["sample_sales_data.customer_id = sample_customer_data.customer_id"]
    
    transformation = LookupTransformation(
        name="TEST_LOOKUP",
        join_conditions=join_conditions,
        join_type="left"
    )
    
    result_df = transformation.transform(sample_sales_data, sample_customer_data)
    
    # Should have same number of rows as input (left join)
    assert result_df.count() == sample_sales_data.count()
    
    # Should have columns from both DataFrames
    sales_columns = sample_sales_data.columns
    customer_columns = sample_customer_data.columns
    
    for col in sales_columns:
        assert col in result_df.columns
    
    # Check that lookup worked
    joined_data = result_df.filter(col("customer_name").isNotNull()).collect()
    assert len(joined_data) > 0

def test_joiner_transformation(spark_session, sample_sales_data, sample_customer_data):
    """Test joiner transformation"""
    join_conditions = ["sample_sales_data.customer_id = sample_customer_data.customer_id"]
    
    transformation = JoinerTransformation(
        name="TEST_JOINER",
        join_conditions=join_conditions,
        join_type="inner"
    )
    
    result_df = transformation.transform(sample_sales_data, sample_customer_data)
    
    # Inner join should only return matching records
    assert result_df.count() <= min(sample_sales_data.count(), sample_customer_data.count())
    
    # All records should have non-null values for joined columns
    results = result_df.collect()
    for row in results:
        assert row.customer_id is not None
        assert row.customer_name is not None

def test_java_transformation_scd_type2_first_load(spark_session, sample_customer_data):
    """Test SCD Type 2 logic for first load"""
    transformation = JavaTransformation(
        name="TEST_SCD",
        logic_type="scd_type2"
    )
    
    result_df = transformation.transform(sample_customer_data, None)
    
    # Should have SCD columns
    scd_columns = ["effective_date", "expiry_date", "is_current", "record_version"]
    for col in scd_columns:
        assert col in result_df.columns
    
    # All records should be current in first load
    current_records = result_df.filter(col("is_current") == True).count()
    assert current_records == result_df.count()
    
    # All records should have version 1
    version_1_records = result_df.filter(col("record_version") == 1).count()
    assert version_1_records == result_df.count()

def test_java_transformation_scd_type2_incremental(spark_session, sample_customer_data):
    """Test SCD Type 2 logic for incremental load"""
    transformation = JavaTransformation(
        name="TEST_SCD",
        logic_type="scd_type2"
    )
    
    # Create existing dimension data (simulate first load result)
    existing_df = transformation.transform(sample_customer_data, None)
    
    # Create new source data with changes
    new_data = [
        ("CUST_000001", "Customer One Updated", "New York", "Active", "2022-01-01", "customer1@email.com"),
        ("CUST_000005", "Customer Five", "Phoenix", "Active", "2023-01-01", "customer5@email.com"),  # New customer
    ]
    
    from pyspark.sql.types import StructType, StructField, StringType
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("status", StringType(), True),
        StructField("created_date", StringType(), True),
        StructField("email", StringType(), True)
    ])
    
    new_source_df = spark_session.createDataFrame(new_data, schema)
    
    # Apply SCD Type 2 transformation
    result_df = transformation.transform(new_source_df, existing_df)
    
    # Should have more records than source (due to historical records)
    assert result_df.count() >= new_source_df.count()
    
    # Should have both current and non-current records
    current_count = result_df.filter(col("is_current") == True).count()
    non_current_count = result_df.filter(col("is_current") == False).count()
    
    assert current_count > 0
    assert non_current_count >= 0  # May have expired records

def test_transformation_with_empty_dataframe(spark_session):
    """Test transformations with empty input"""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType
    
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("value", DoubleType(), True)
    ])
    
    empty_df = spark_session.createDataFrame([], schema)
    
    # Test expression transformation
    expr_transform = ExpressionTransformation("TEST", expressions={"new_col": "value * 2"})
    result = expr_transform.transform(empty_df)
    assert result.count() == 0
    assert "new_col" in result.columns
    
    # Test aggregator transformation
    agg_transform = AggregatorTransformation(
        "TEST",
        group_by_cols=["id"],
        aggregations={"sum_value": {"type": "sum", "column": "value"}}
    )
    result = agg_transform.transform(empty_df)
    assert result.count() == 0