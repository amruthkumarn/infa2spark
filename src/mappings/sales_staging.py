"""
Sales Staging Mapping Implementation
Converts: SALES_SOURCE -> FILTER_SALES -> AGG_SALES -> STG_SALES
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from ..core.base_classes import BaseMapping
from ..transformations.base_transformation import ExpressionTransformation, AggregatorTransformation
from ..data_sources.data_source_manager import DataSourceManager

class SalesStaging(BaseMapping):
    """Sales Staging mapping implementation"""
    
    def __init__(self, spark_session, config):
        super().__init__("Sales_Staging", spark_session, config)
        self.data_source_manager = DataSourceManager(spark_session, config.get('connections', {}))
        
    def execute(self) -> bool:
        """Execute the Sales Staging mapping"""
        try:
            self.logger.info("Starting Sales Staging mapping execution")
            
            # Step 1: Read from SALES_SOURCE (HDFS Parquet)
            sales_df = self._read_sales_source()
            
            # Step 2: Apply FILTER_SALES transformation
            filtered_df = self._apply_filter_transformation(sales_df)
            
            # Step 3: Apply AGG_SALES transformation
            aggregated_df = self._apply_aggregation_transformation(filtered_df)
            
            # Step 4: Write to STG_SALES target (Hive)
            self._write_to_staging_target(aggregated_df)
            
            self.logger.info("Sales Staging mapping executed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in Sales Staging mapping: {str(e)}")
            raise
            
    def _read_sales_source(self) -> DataFrame:
        """Read from SALES_SOURCE"""
        self.logger.info("Reading from SALES_SOURCE")
        return self.data_source_manager.read_source("SALES_SOURCE", "HDFS", format="PARQUET")
        
    def _apply_filter_transformation(self, input_df: DataFrame) -> DataFrame:
        """Apply FILTER_SALES transformation"""
        self.logger.info("Applying FILTER_SALES transformation")
        
        # Define filter conditions (example business logic)
        filters = [
            "amount > 0",  # Filter out zero or negative amounts
            "sale_date >= '2023-01-01'",  # Only recent sales
            "region IS NOT NULL"  # Valid region data
        ]
        
        # Add calculated columns
        expressions = {
            "sale_year": "year(to_date(sale_date))",
            "sale_month": "month(to_date(sale_date))",
            "amount_category": "CASE WHEN amount < 100 THEN 'Small' WHEN amount < 500 THEN 'Medium' ELSE 'Large' END"
        }
        
        filter_transformation = ExpressionTransformation(
            name="FILTER_SALES",
            expressions=expressions,
            filters=filters
        )
        
        return filter_transformation.transform(input_df)
        
    def _apply_aggregation_transformation(self, input_df: DataFrame) -> DataFrame:
        """Apply AGG_SALES transformation"""
        self.logger.info("Applying AGG_SALES transformation")
        
        # Group by region and product for aggregation
        group_by_cols = ["region", "product", "sale_year", "sale_month"]
        
        # Define aggregations
        aggregations = {
            "total_amount": {"type": "sum", "column": "amount"},
            "total_sales": {"type": "count", "column": "sale_id"},
            "avg_amount": {"type": "avg", "column": "amount"},
            "max_amount": {"type": "max", "column": "amount"},
            "min_amount": {"type": "min", "column": "amount"}
        }
        
        agg_transformation = AggregatorTransformation(
            name="AGG_SALES",
            group_by_cols=group_by_cols,
            aggregations=aggregations
        )
        
        return agg_transformation.transform(input_df)
        
    def _write_to_staging_target(self, output_df: DataFrame):
        """Write to STG_SALES target"""
        self.logger.info("Writing to STG_SALES target")
        
        # Add audit columns
        final_df = output_df.withColumn("load_date", current_date()) \
                           .withColumn("load_timestamp", current_timestamp()) \
                           .withColumn("source_system", lit("SALES_SYSTEM"))
        
        self.data_source_manager.write_target(final_df, "STG_SALES", "HIVE", mode="overwrite")