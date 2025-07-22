"""
Main Application for RealtimeAnalytics_Platform
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
from workflows.real_time_analytics_pipeline import Realtimeanalyticspipeline


def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/realtimeanalytics_platform.log')
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
    parser = argparse.ArgumentParser(description='RealtimeAnalytics_Platform Spark Application')
    parser.add_argument('--config', default='config/application.yaml', 
                       help='Path to configuration file')
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger("RealtimeAnalytics_Platform")
    
    try:
        logger.info("Starting RealtimeAnalytics_Platform Spark Application")
        
        # Load configuration
        config = load_config(args.config)
        
        # Create Spark session
        spark = create_spark_session("RealtimeAnalytics_Platform", config)
        
        # Create output directories
        os.makedirs("data/output", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        # Execute main workflow
        workflow = Realtimeanalyticspipeline(spark, config)
        
        if workflow.validate_workflow():
            logger.info("Workflow validation passed")
            success = workflow.execute()
            
            if success:
                logger.info("RealtimeAnalytics_Platform completed successfully")
                exit_code = 0
            else:
                logger.error("RealtimeAnalytics_Platform execution failed")
                exit_code = 1
        else:
            logger.error("Workflow validation failed")
            exit_code = 1
        
        # Cleanup
        spark.stop()
        
        logger.info("Application completed")
        sys.exit(exit_code)
        
    except Exception as e:
        logger.error(f"Application failed with error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()