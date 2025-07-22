"""
Generated Transformation Classes
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ..base_classes import BaseTransformation


class ExpressionTransformation(BaseTransformation):
    """Expression-based transformations (filtering, calculations)"""
    
    def __init__(self, name: str, expressions: dict = None, filters: list = None):
        super().__init__(name, "Expression")
        self.expressions = expressions or {}
        self.filters = filters or []
        
    def transform(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """Apply expressions and filters"""
        result_df = input_df
        
        # Apply filters
        for filter_expr in self.filters:
            result_df = result_df.filter(filter_expr)
            self.logger.info(f"Applied filter: {filter_expr}")
            
        # Apply expressions
        for col_name, expression in self.expressions.items():
            result_df = result_df.withColumn(col_name, expr(expression))
            self.logger.info(f"Added column {col_name}")
            
        return result_df


class AggregatorTransformation(BaseTransformation):
    """Aggregation operations"""
    
    def __init__(self, name: str, group_by_cols: list = None, aggregations: dict = None):
        super().__init__(name, "Aggregator")
        self.group_by_cols = group_by_cols or []
        self.aggregations = aggregations or {}
        
    def transform(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """Apply aggregation"""
        if not self.group_by_cols:
            return input_df
            
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
                
        return input_df.groupBy(*self.group_by_cols).agg(*agg_exprs)


class LookupTransformation(BaseTransformation):
    """Lookup operations (joins)"""
    
    def __init__(self, name: str, join_conditions: list = None, join_type: str = "left"):
        super().__init__(name, "Lookup")
        self.join_conditions = join_conditions or []
        self.join_type = join_type
        
    def transform(self, input_df: DataFrame, lookup_df: DataFrame = None, **kwargs) -> DataFrame:
        """Perform lookup operation"""
        if lookup_df is None:
            return input_df
            
        if not self.join_conditions:
            return input_df
            
        join_expr = None
        for condition in self.join_conditions:
            condition_expr = expr(condition)
            if join_expr is None:
                join_expr = condition_expr
            else:
                join_expr = join_expr & condition_expr
                
        return input_df.join(lookup_df, join_expr, self.join_type)


class JoinerTransformation(BaseTransformation):
    """Join operations between sources"""
    
    def __init__(self, name: str, join_conditions: list = None, join_type: str = "inner"):
        super().__init__(name, "Joiner")
        self.join_conditions = join_conditions or []
        self.join_type = join_type
        
    def transform(self, left_df: DataFrame, right_df: DataFrame, **kwargs) -> DataFrame:
        """Join two DataFrames"""
        if not self.join_conditions:
            return left_df
            
        join_expr = None
        for condition in self.join_conditions:
            condition_expr = expr(condition)
            if join_expr is None:
                join_expr = condition_expr
            else:
                join_expr = join_expr & condition_expr
                
        return left_df.join(right_df, join_expr, self.join_type)


class JavaTransformation(BaseTransformation):
    """Custom transformation logic"""
    
    def __init__(self, name: str, logic_type: str = "custom"):
        super().__init__(name, "Java")
        self.logic_type = logic_type
        
    def transform(self, input_df: DataFrame, existing_df: DataFrame = None, **kwargs) -> DataFrame:
        """Apply custom transformation logic"""
        if self.logic_type == "scd_type2":
            return self._apply_scd_type2(input_df, existing_df)
        else:
            return input_df
            
    def _apply_scd_type2(self, source_df: DataFrame, existing_df: DataFrame = None) -> DataFrame:
        """Apply SCD Type 2 logic"""
        if existing_df is None:
            return source_df.withColumn("effective_date", current_date()) \
                           .withColumn("expiry_date", lit(None).cast("date")) \
                           .withColumn("is_current", lit(True)) \
                           .withColumn("record_version", lit(1))
        else:
            # Simplified SCD Type 2 implementation
            expired_df = existing_df.withColumn("expiry_date", current_date()) \
                                   .withColumn("is_current", lit(False))
            
            new_df = source_df.withColumn("effective_date", current_date()) \
                             .withColumn("expiry_date", lit(None).cast("date")) \
                             .withColumn("is_current", lit(True)) \
                             .withColumn("record_version", lit(1))
                             
            return expired_df.union(new_df)
