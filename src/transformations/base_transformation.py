"""
Base transformation classes for PySpark implementations
"""
from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

class BaseTransformation(ABC):
    """Base class for all transformations"""
    
    def __init__(self, name: str, transformation_type: str):
        self.name = name
        self.transformation_type = transformation_type
        self.logger = logging.getLogger(f"Transformation.{name}")
        
    @abstractmethod
    def transform(self, input_df: DataFrame) -> DataFrame:
        """Apply transformation to input DataFrame"""
        pass
        
    def log_dataframe_info(self, df: DataFrame, stage: str):
        """Log DataFrame information for debugging"""
        count = df.count()
        columns = len(df.columns)
        self.logger.info(f"{stage} - Rows: {count}, Columns: {columns}")
        
class ExpressionTransformation(BaseTransformation):
    """Handles expression-based transformations (filtering, calculations)"""
    
    def __init__(self, name: str, expressions: dict = None, filters: list = None):
        super().__init__(name, "Expression")
        self.expressions = expressions or {}
        self.filters = filters or []
        
    def transform(self, input_df: DataFrame) -> DataFrame:
        """Apply expressions and filters"""
        result_df = input_df
        
        # Apply filters
        for filter_expr in self.filters:
            result_df = result_df.filter(filter_expr)
            self.logger.info(f"Applied filter: {filter_expr}")
            
        # Apply expressions (new columns or transformations)
        for col_name, expression in self.expressions.items():
            result_df = result_df.withColumn(col_name, expr(expression))
            self.logger.info(f"Added column {col_name} with expression: {expression}")
            
        self.log_dataframe_info(result_df, "Expression Result")
        return result_df

class AggregatorTransformation(BaseTransformation):
    """Handles aggregation operations"""
    
    def __init__(self, name: str, group_by_cols: list = None, aggregations: dict = None):
        super().__init__(name, "Aggregator")
        self.group_by_cols = group_by_cols or []
        self.aggregations = aggregations or {}
        
    def transform(self, input_df: DataFrame) -> DataFrame:
        """Apply aggregation"""
        if not self.group_by_cols:
            self.logger.warning("No group by columns specified for aggregation")
            return input_df
            
        # Build aggregation expressions
        agg_exprs = []
        for agg_name, agg_config in self.aggregations.items():
            agg_type = agg_config.get('type', 'sum')
            agg_column = agg_config.get('column')
            
            if agg_type.lower() == 'sum':
                agg_exprs.append(sum(agg_column).alias(agg_name))
            elif agg_type.lower() == 'count':
                agg_exprs.append(count(agg_column).alias(agg_name))
            elif agg_type.lower() == 'avg':
                agg_exprs.append(avg(agg_column).alias(agg_name))
            elif agg_type.lower() == 'max':
                agg_exprs.append(max(agg_column).alias(agg_name))
            elif agg_type.lower() == 'min':
                agg_exprs.append(min(agg_column).alias(agg_name))
                
        result_df = input_df.groupBy(*self.group_by_cols).agg(*agg_exprs)
        
        self.log_dataframe_info(result_df, "Aggregation Result")
        return result_df

class LookupTransformation(BaseTransformation):
    """Handles lookup operations (joins)"""
    
    def __init__(self, name: str, lookup_table: str = None, join_conditions: list = None, 
                 join_type: str = "left"):
        super().__init__(name, "Lookup")
        self.lookup_table = lookup_table
        self.join_conditions = join_conditions or []
        self.join_type = join_type
        
    def transform(self, input_df: DataFrame, lookup_df: DataFrame = None) -> DataFrame:
        """Perform lookup (join) operation"""
        if lookup_df is None:
            self.logger.error("Lookup DataFrame not provided")
            return input_df
            
        if not self.join_conditions:
            self.logger.error("No join conditions specified")
            return input_df
            
        # Build join condition
        join_expr = None
        for condition in self.join_conditions:
            condition_expr = expr(condition)
            if join_expr is None:
                join_expr = condition_expr
            else:
                join_expr = join_expr & condition_expr
                
        result_df = input_df.join(lookup_df, join_expr, self.join_type)
        
        self.log_dataframe_info(result_df, "Lookup Result")
        return result_df

class JoinerTransformation(BaseTransformation):
    """Handles join operations between multiple sources"""
    
    def __init__(self, name: str, join_conditions: list = None, join_type: str = "inner"):
        super().__init__(name, "Joiner")
        self.join_conditions = join_conditions or []
        self.join_type = join_type
        
    def transform(self, left_df: DataFrame, right_df: DataFrame) -> DataFrame:
        """Join two DataFrames"""
        if not self.join_conditions:
            self.logger.error("No join conditions specified")
            return left_df
            
        # Build join condition
        join_expr = None
        for condition in self.join_conditions:
            condition_expr = expr(condition)
            if join_expr is None:
                join_expr = condition_expr
            else:
                join_expr = join_expr & condition_expr
                
        result_df = left_df.join(right_df, join_expr, self.join_type)
        
        self.log_dataframe_info(result_df, "Join Result")
        return result_df

class JavaTransformation(BaseTransformation):
    """Handles custom Java transformation logic"""
    
    def __init__(self, name: str, logic_type: str = "scd_type2"):
        super().__init__(name, "Java")
        self.logic_type = logic_type
        
    def transform(self, input_df: DataFrame, existing_df: DataFrame = None) -> DataFrame:
        """Apply custom transformation logic"""
        if self.logic_type == "scd_type2":
            return self._apply_scd_type2(input_df, existing_df)
        else:
            self.logger.warning(f"Unknown logic type: {self.logic_type}")
            return input_df
            
    def _apply_scd_type2(self, source_df: DataFrame, existing_df: DataFrame = None) -> DataFrame:
        """Apply SCD Type 2 logic"""
        if existing_df is None:
            # First load - add SCD columns
            result_df = source_df.withColumn("effective_date", current_date()) \
                               .withColumn("expiry_date", lit(None).cast(DateType())) \
                               .withColumn("is_current", lit(True)) \
                               .withColumn("record_version", lit(1))
        else:
            # Incremental load - implement SCD Type 2 logic
            # This is a simplified implementation
            # Mark existing records as expired
            expired_df = existing_df.withColumn("expiry_date", current_date()) \
                                   .withColumn("is_current", lit(False))
            
            # Add new records
            new_df = source_df.withColumn("effective_date", current_date()) \
                             .withColumn("expiry_date", lit(None).cast(DateType())) \
                             .withColumn("is_current", lit(True)) \
                             .withColumn("record_version", lit(1))
                             
            result_df = expired_df.union(new_df)
            
        self.log_dataframe_info(result_df, "SCD Type 2 Result")
        return result_df