"""
Data source management for different connection types
"""
from pyspark.sql import SparkSession, DataFrame
from typing import Dict, Any, Optional
import logging
import os

class DataSourceManager:
    """Manages data sources and connections"""
    
    def __init__(self, spark_session: SparkSession, connections_config: Dict[str, Any]):
        self.spark = spark_session
        self.connections = connections_config
        self.logger = logging.getLogger("DataSourceManager")
        
    def read_source(self, source_name: str, source_type: str, **kwargs) -> DataFrame:
        """Read data from various source types"""
        try:
            if source_type.upper() == "HDFS":
                return self._read_hdfs(source_name, **kwargs)
            elif source_type.upper() == "HIVE":
                return self._read_hive(source_name, **kwargs)
            elif source_type.upper() == "DB2":
                return self._read_jdbc(source_name, "db2", **kwargs)
            elif source_type.upper() == "ORACLE":
                return self._read_jdbc(source_name, "oracle", **kwargs)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
                
        except Exception as e:
            self.logger.error(f"Error reading from {source_name} ({source_type}): {str(e)}")
            raise
            
    def write_target(self, df: DataFrame, target_name: str, target_type: str, **kwargs):
        """Write data to various target types"""
        try:
            if target_type.upper() == "HIVE":
                self._write_hive(df, target_name, **kwargs)
            elif target_type.upper() == "HDFS":
                self._write_hdfs(df, target_name, **kwargs)
            elif target_type.upper() in ["DB2", "ORACLE"]:
                self._write_jdbc(df, target_name, target_type.lower(), **kwargs)
            else:
                raise ValueError(f"Unsupported target type: {target_type}")
                
        except Exception as e:
            self.logger.error(f"Error writing to {target_name} ({target_type}): {str(e)}")
            raise
            
    def _read_hdfs(self, source_name: str, format_type: str = "parquet", **kwargs) -> DataFrame:
        """Read from HDFS"""
        # For PoC, use local files
        file_path = kwargs.get('path', f"sample_data/{source_name.lower()}.{format_type}")
        
        if not os.path.exists(file_path):
            self.logger.warning(f"File not found: {file_path}, creating mock data")
            return self._create_mock_data(source_name)
            
        if format_type.lower() == "parquet":
            return self.spark.read.parquet(file_path)
        elif format_type.lower() == "avro":
            return self.spark.read.format("avro").load(file_path)
        elif format_type.lower() == "csv":
            return self.spark.read.option("header", "true").csv(file_path)
        else:
            return self.spark.read.format(format_type).load(file_path)
            
    def _read_hive(self, table_name: str, **kwargs) -> DataFrame:
        """Read from Hive table"""
        # For PoC, simulate with local files or create mock data
        try:
            return self.spark.table(table_name)
        except Exception:
            self.logger.warning(f"Hive table {table_name} not found, creating mock data")
            return self._create_mock_data(table_name)
            
    def _read_jdbc(self, source_name: str, db_type: str, **kwargs) -> DataFrame:
        """Read from JDBC sources"""
        # For PoC, simulate with CSV files
        csv_path = f"sample_data/{source_name.lower()}.csv"
        
        if not os.path.exists(csv_path):
            self.logger.warning(f"CSV file not found: {csv_path}, creating mock data")
            return self._create_mock_data(source_name)
            
        return self.spark.read.option("header", "true").csv(csv_path)
        
    def _write_hive(self, df: DataFrame, table_name: str, **kwargs):
        """Write to Hive table"""
        mode = kwargs.get('mode', 'overwrite')
        # For PoC, write to local parquet files
        output_path = f"output/{table_name.lower()}"
        df.write.mode(mode).parquet(output_path)
        self.logger.info(f"Written data to {output_path} (simulating Hive table {table_name})")
        
    def _write_hdfs(self, df: DataFrame, target_name: str, format_type: str = "parquet", **kwargs):
        """Write to HDFS"""
        mode = kwargs.get('mode', 'overwrite')
        output_path = f"output/{target_name.lower()}"
        
        if format_type.lower() == "parquet":
            df.write.mode(mode).parquet(output_path)
        elif format_type.lower() == "avro":
            df.write.mode(mode).format("avro").save(output_path)
        else:
            df.write.mode(mode).format(format_type).save(output_path)
            
        self.logger.info(f"Written data to {output_path}")
        
    def _write_jdbc(self, df: DataFrame, target_name: str, db_type: str, **kwargs):
        """Write to JDBC targets"""
        # For PoC, write to CSV files
        mode = kwargs.get('mode', 'overwrite')
        output_path = f"output/{target_name.lower()}.csv"
        df.coalesce(1).write.mode(mode).option("header", "true").csv(output_path)
        self.logger.info(f"Written data to {output_path} (simulating {db_type} table {target_name})")
        
    def _create_mock_data(self, source_name: str) -> DataFrame:
        """Create mock data for testing"""
        from .mock_data_generator import MockDataGenerator
        generator = MockDataGenerator(self.spark)
        return generator.generate_mock_data(source_name)