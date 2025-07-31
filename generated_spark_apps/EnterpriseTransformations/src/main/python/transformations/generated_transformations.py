"""
Generated Transformation Classes
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ..runtime.base_classes import BaseTransformation


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


class SequenceTransformation(BaseTransformation):
    """Sequence number generation"""
    
    def __init__(self, name: str, start_value: int = 1, increment_value: int = 1, **kwargs):
        super().__init__(name, "Sequence")
        self.start_value = start_value
        self.increment_value = increment_value
        
    def transform(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """Generate sequence numbers"""
        from pyspark.sql.functions import monotonically_increasing_id, row_number, lit
        from pyspark.sql.window import Window
        
        if self.increment_value == 1:
            # Use monotonic ID for performance
            return input_df.withColumn("NEXTVAL", 
                monotonically_increasing_id() + self.start_value)
        else:
            # Use row_number for custom increment
            window_spec = Window.orderBy(lit(1))
            return input_df.withColumn("NEXTVAL",
                (row_number().over(window_spec) - 1) * self.increment_value + self.start_value)


class SorterTransformation(BaseTransformation):
    """Data sorting operations"""
    
    def __init__(self, name: str, sort_keys: list = None):
        super().__init__(name, "Sorter")
        self.sort_keys = sort_keys or []
        
    def transform(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """Sort data based on sort keys"""
        from pyspark.sql.functions import col
        
        if not self.sort_keys:
            return input_df
            
        sort_exprs = []
        for key in self.sort_keys:
            field_name = key['field_name']
            direction = key.get('direction', 'ASC')
            
            if direction.upper() == 'ASC':
                sort_exprs.append(col(field_name).asc())
            else:
                sort_exprs.append(col(field_name).desc())
                
        return input_df.orderBy(*sort_exprs)


class RouterTransformation(BaseTransformation):
    """Conditional data routing"""
    
    def __init__(self, name: str, output_groups: list = None):
        super().__init__(name, "Router")
        self.output_groups = output_groups or []
        
    def transform(self, input_df: DataFrame, **kwargs) -> dict:
        """Route data to multiple outputs"""
        results = {}
        
        for group in self.output_groups:
            group_name = group['name']
            condition = group['condition']
            results[group_name] = input_df.filter(condition)
            
        # Default group gets remaining records
        all_conditions = " OR ".join([f"({g['condition']})" for g in self.output_groups])
        results['DEFAULT'] = input_df.filter(f"NOT ({all_conditions})")
        
        return results


class UnionTransformation(BaseTransformation):
    """Union operations for combining DataFrames"""
    
    def __init__(self, name: str, union_type: str = "UNION_ALL"):
        super().__init__(name, "Union")
        self.union_type = union_type
        
    def transform(self, *input_dataframes, **kwargs) -> DataFrame:
        """Combine multiple DataFrames"""
        if len(input_dataframes) < 2:
            return input_dataframes[0] if input_dataframes else None
            
        result_df = input_dataframes[0]
        for df in input_dataframes[1:]:
            result_df = result_df.union(df)
            
        if self.union_type == "UNION_DISTINCT":
            result_df = result_df.distinct()
            
        return result_df


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
