"""
Spark Session Manager for PySpark operations
"""
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from typing import Dict, Any, Optional
import logging

class SparkManager:
    """Manages Spark session creation and configuration"""
    
    def __init__(self):
        self.spark_session: Optional[SparkSession] = None
        self.logger = logging.getLogger("SparkManager")
        
    def create_spark_session(self, app_name: str, config: Dict[str, Any] = None) -> SparkSession:
        """Create and configure Spark session"""
        try:
            # Default Spark configuration
            spark_conf = SparkConf()
            spark_conf.setAppName(app_name)
            
            # Set default configurations for local development
            default_config = {
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true", 
                "spark.sql.warehouse.dir": "/tmp/spark-warehouse",
                "spark.sql.execution.arrow.pyspark.enabled": "true",
                "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
                "spark.sql.adaptive.skewJoin.enabled": "true"
            }
            
            # Apply default configuration
            for key, value in default_config.items():
                spark_conf.set(key, value)
                
            # Apply custom configuration if provided
            if config:
                spark_config = config.get('spark', {})
                for key, value in spark_config.items():
                    spark_conf.set(key, str(value))
                    
            # Create Spark session
            builder = SparkSession.builder.config(conf=spark_conf)
            
            # Enable Hive support if needed
            if config and config.get('enable_hive_support', True):
                builder = builder.enableHiveSupport()
                
            self.spark_session = builder.getOrCreate()
            
            # Set log level
            log_level = config.get('log_level', 'WARN') if config else 'WARN'
            self.spark_session.sparkContext.setLogLevel(log_level)
            
            self.logger.info(f"Spark session created successfully for app: {app_name}")
            return self.spark_session
            
        except Exception as e:
            self.logger.error(f"Failed to create Spark session: {str(e)}")
            raise
            
    def get_spark_session(self) -> SparkSession:
        """Get current Spark session"""
        if self.spark_session is None:
            raise ValueError("Spark session not initialized. Call create_spark_session first.")
        return self.spark_session
        
    def stop_spark_session(self):
        """Stop Spark session"""
        if self.spark_session:
            self.spark_session.stop()
            self.spark_session = None
            self.logger.info("Spark session stopped")
            
    def configure_for_testing(self) -> SparkSession:
        """Create minimal Spark session for testing"""
        test_config = {
            "spark": {
                "spark.master": "local[2]",
                "spark.sql.shuffle.partitions": "2",
                "spark.sql.warehouse.dir": "/tmp/test-warehouse"
            },
            "enable_hive_support": False,
            "log_level": "ERROR"
        }
        
        return self.create_spark_session("InformaticaPoC-Test", test_config)