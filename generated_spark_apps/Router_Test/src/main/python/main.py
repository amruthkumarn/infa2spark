"""
Main Application for Router_Test
Generated from Informatica BDM Project

Usage:
    python main.py [--config config.yaml]
    spark-submit main.py [--config config.yaml]
"""
import sys
import os
import logging
import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import yaml

# Add current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))


def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/router_test.log')
        ]
    )


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Error loading config: {str(e)}")
        return {}


def create_spark_session(app_name: str, config: dict) -> SparkSession:
    """Create Spark session with configuration"""
    spark_config = config.get('spark', {})
    
    # Default Spark configuration
    conf = SparkConf()
    conf.setAppName(app_name)
    
    # Apply custom configuration
    for key, value in spark_config.items():
        conf.set(key, str(value))
    
    # Create Spark session
    spark = SparkSession.builder \
        .config(conf=conf) \
        .enableHiveSupport() \
        .getOrCreate()
    
    # Set log level
    log_level = config.get('log_level', 'WARN')
    spark.sparkContext.setLogLevel(log_level)
    
    return spark


def main():
    """Main application function"""
    parser = argparse.ArgumentParser(description='Router_Test Spark Application')
    parser.add_argument('--config', default='config/application.yaml', 
                       help='Path to configuration file')
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger("Router_Test")
    
    try:
        logger.info("Starting Router_Test Spark Application")
        
        # Load configuration
        config = load_config(args.config)
        
        # Create Spark session
        spark = create_spark_session("Router_Test", config)
        
        # Create output directories
        os.makedirs("data/output", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        # No workflow defined - run basic application
        logger.info("No workflow defined, running basic application")
        exit_code = 0
        
        # Cleanup
        spark.stop()
        
        logger.info("Application completed")
        sys.exit(exit_code)
        
    except Exception as e:
        logger.error(f"Application failed with error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()