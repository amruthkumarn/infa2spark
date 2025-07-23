"""
Base classes for Comprehensive_Complex_Project Spark Application
Generated from Informatica BDM Project
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
import logging
from datetime import datetime


class BaseMapping(ABC):
    """Base class for all mapping implementations"""
    
    def __init__(self, name: str, spark: SparkSession, config: Dict[str, Any]):
        self.name = name
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(f"Mapping.{name}")
        
    @abstractmethod
    def execute(self) -> bool:
        """Execute the mapping logic"""
        pass
        
    def log_dataframe_info(self, df: DataFrame, stage: str):
        """Log DataFrame information for debugging"""
        try:
            count = df.count()
            columns = len(df.columns)
            self.logger.info(f"{stage} - Rows: {count}, Columns: {columns}")
        except Exception as e:
            self.logger.warning(f"Could not get DataFrame info for {stage}: {str(e)}")


class BaseTransformation(ABC):
    """Base class for all transformations"""
    
    def __init__(self, name: str, transformation_type: str):
        self.name = name
        self.transformation_type = transformation_type
        self.logger = logging.getLogger(f"Transformation.{name}")
        
    @abstractmethod
    def transform(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """Apply transformation to input DataFrame"""
        pass


class BaseWorkflow(ABC):
    """Base class for workflow orchestration"""
    
    def __init__(self, name: str, spark: SparkSession, config: Dict[str, Any]):
        self.name = name
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(f"Workflow.{name}")
        
    @abstractmethod
    def execute(self) -> bool:
        """Execute the workflow"""
        pass
        
    def validate_workflow(self) -> bool:
        """Validate workflow configuration"""
        return True


class DataSourceManager:
    """Manages data source connections and operations"""
    
    def __init__(self, spark: SparkSession, connections_config: Dict[str, Any]):
        self.spark = spark
        self.connections = connections_config
        self.logger = logging.getLogger("DataSourceManager")
        
    def read_source(self, source_name: str, source_type: str, **kwargs) -> DataFrame:
        """Read data from source"""
        self.logger.info(f"Reading from source: {source_name} (type: {source_type})")
        
        if source_type.upper() == "HDFS":
            return self._read_hdfs_source(source_name, **kwargs)
        elif source_type.upper() == "HIVE":
            return self._read_hive_source(source_name, **kwargs)
        elif source_type.upper() in ["DB2", "ORACLE"]:
            return self._read_jdbc_source(source_name, source_type, **kwargs)
        else:
            # Generate mock data for unknown sources
            return self._generate_mock_data(source_name)
            
    def write_target(self, df: DataFrame, target_name: str, target_type: str, **kwargs):
        """Write data to target"""
        self.logger.info(f"Writing to target: {target_name} (type: {target_type})")
        
        mode = kwargs.get('mode', 'overwrite')
        
        if target_type.upper() == "HIVE":
            self._write_hive_target(df, target_name, mode)
        elif target_type.upper() == "HDFS":
            self._write_hdfs_target(df, target_name, mode, **kwargs)
        else:
            # Default to parquet files
            output_path = f"data/output/{target_name}"
            df.write.mode(mode).parquet(output_path)
            
    def _read_hdfs_source(self, source_name: str, **kwargs) -> DataFrame:
        """Read from HDFS source"""
        format_type = kwargs.get('format', 'parquet').lower()
        path = f"data/input/{source_name.lower()}"
        
        if format_type == 'parquet':
            return self.spark.read.parquet(path)
        elif format_type == 'csv':
            return self.spark.read.option("header", "true").csv(path)
        elif format_type == 'avro':
            return self.spark.read.format("avro").load(path)
        else:
            return self.spark.read.parquet(path)
            
    def _read_hive_source(self, source_name: str, **kwargs) -> DataFrame:
        """Read from Hive table"""
        return self.spark.table(source_name.lower())
        
    def _read_jdbc_source(self, source_name: str, source_type: str, **kwargs) -> DataFrame:
        """Read from JDBC source"""
        # For demo purposes, generate mock data
        # In production, would use actual JDBC connections
        return self._generate_mock_data(source_name)
        
    def _write_hive_target(self, df: DataFrame, target_name: str, mode: str):
        """Write to Hive table"""
        df.write.mode(mode).saveAsTable(target_name.lower())
        
    def _write_hdfs_target(self, df: DataFrame, target_name: str, mode: str, **kwargs):
        """Write to HDFS target"""
        format_type = kwargs.get('format', 'parquet').lower()
        path = f"data/output/{target_name.lower()}"
        
        if format_type == 'parquet':
            df.write.mode(mode).parquet(path)
        elif format_type == 'csv':
            df.write.mode(mode).option("header", "true").csv(path)
        else:
            df.write.mode(mode).parquet(path)
            
    def _generate_mock_data(self, source_name: str) -> DataFrame:
        """Generate mock data for testing"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
        from pyspark.sql.functions import lit, current_date
        
        # Define schema based on source name
        if 'sales' in source_name.lower():
            schema = StructType([
                StructField("sale_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("amount", DoubleType(), False),
                StructField("sale_date", DateType(), False),
                StructField("region", StringType(), False)
            ])
            data = [
                ("S001", "C001", "P001", 100.0, "2023-01-01", "North"),
                ("S002", "C002", "P002", 250.0, "2023-01-02", "South"),
                ("S003", "C003", "P001", 75.0, "2023-01-03", "East")
            ]
        elif 'customer' in source_name.lower():
            schema = StructType([
                StructField("customer_id", StringType(), False),
                StructField("customer_name", StringType(), False),
                StructField("city", StringType(), False),
                StructField("status", StringType(), False),
                StructField("email", StringType(), False)
            ])
            data = [
                ("C001", "John Doe", "New York", "Active", "john@email.com"),
                ("C002", "Jane Smith", "Los Angeles", "Active", "jane@email.com"),
                ("C003", "Bob Johnson", "Chicago", "Inactive", "bob@email.com")
            ]
        else:
            # Generic schema
            schema = StructType([
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
                StructField("value", DoubleType(), False),
                StructField("created_date", DateType(), False)
            ])
            data = [
                ("1", "Record 1", 100.0, "2023-01-01"),
                ("2", "Record 2", 200.0, "2023-01-02"),
                ("3", "Record 3", 300.0, "2023-01-03")
            ]
            
        return self.spark.createDataFrame(data, schema)