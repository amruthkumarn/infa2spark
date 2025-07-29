"""
Main application entry point for the Spark ETL application
"""

import sys
import yaml
from pathlib import Path
from pyspark.sql import SparkSession


def load_config(config_path: str = "config/application.yaml"):
    """Load application configuration"""
    config_file = Path(config_path)
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)


def create_spark_session(config):
    """Create Spark session with configuration"""
    builder = SparkSession.builder
    
    # Apply Spark configurations
    spark_config = config.get("spark", {})
    for key, value in spark_config.items():
        builder = builder.config(key, value)
    
    return builder.getOrCreate()


def main():
    """Main application entry point"""
    try:
        # Load configuration
        config = load_config()
        print(f"🚀 Starting {config['application']['name']}")
        
        # Create Spark session
        spark = create_spark_session(config)
        print(f"✅ Spark session created: {spark.sparkContext.appName}")
        
        # Import and execute mappings
        from mappings.customer_etl import execute_customer_etl
        
        # Execute ETL process
        success = execute_customer_etl(spark, config)
        
        if success:
            print("✅ ETL process completed successfully")
            return 0
        else:
            print("❌ ETL process failed")
            return 1
            
    except Exception as e:
        print(f"❌ Application error: {e}")
        return 1
    finally:
        if 'spark' in locals():
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
