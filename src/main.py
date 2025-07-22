"""
Main application entry point for Informatica to PySpark PoC
"""
import sys
import os
import logging
from pathlib import Path

# Add src directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from core.spark_manager import SparkManager
from core.config_manager import ConfigManager
from core.xml_parser import InformaticaXMLParser
from core.spark_code_generator import SparkCodeGenerator  # Add this import
from workflows.daily_etl_process import DailyETLProcess

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('informatica_poc.log')
        ]
    )

def generate_spark_app():
    """Generate standalone Spark application from XML"""
    setup_logging()
    logger = logging.getLogger("SparkGenerator")
    
    try:
        logger.info("Starting Spark application generation")
        
        # Initialize code generator
        generator = SparkCodeGenerator("generated_spark_apps")
        
        # Generate application from sample XML
        app_path = generator.generate_spark_application(
            "input/sample_project.xml",
            "MyBDMProject_SparkApp"
        )
        
        logger.info(f"Spark application generated at: {app_path}")
        return app_path
        
    except Exception as e:
        logger.error(f"Error generating Spark application: {str(e)}")
        raise

def main():
    """Main application function"""
    setup_logging()
    logger = logging.getLogger("Main")
    
    # Check if user wants to generate Spark app
    if len(sys.argv) > 1 and sys.argv[1] == "--generate-spark-app":
        app_path = generate_spark_app()
        logger.info(f"Generated Spark application available at: {app_path}")
        logger.info("To run the generated app:")
        logger.info(f"  cd {app_path}")
        logger.info("  ./run.sh")
        return
    
    try:
        logger.info("Starting Informatica to PySpark PoC")
        
        # Initialize configuration manager
        config_manager = ConfigManager("config")
        
        # Load configurations
        spark_config = config_manager.get_spark_config()
        connections_config = config_manager.get_connections_config()
        project_config = config_manager.get_sample_project_config()
        
        # Merge all configurations
        full_config = config_manager.merge_configs(
            spark_config,
            {"connections": connections_config},
            project_config
        )
        
        # Parse Informatica XML project (optional - for reference)
        xml_parser = InformaticaXMLParser()
        try:
            project = xml_parser.parse_project("input/sample_project.xml")
            logger.info(f"Parsed project: {project.name} v{project.version}")
            logger.info(f"Found {len(project.folders)} folder types")
        except Exception as e:
            logger.warning(f"Could not parse XML project: {str(e)}")
            
        # Initialize Spark
        spark_manager = SparkManager()
        spark_session = spark_manager.create_spark_session("InformaticaPoC", full_config)
        
        # Create output directories
        os.makedirs("output", exist_ok=True)
        
        # Execute workflow
        workflow = DailyETLProcess(spark_session, full_config)
        
        # Validate workflow before execution
        if workflow.validate_workflow():
            logger.info("Workflow validation passed")
            success = workflow.execute()
            
            if success:
                logger.info("Workflow executed successfully")
                exit_code = 0
            else:
                logger.error("Workflow execution failed")
                exit_code = 1
        else:
            logger.error("Workflow validation failed")
            exit_code = 1
            
        # Cleanup
        spark_manager.stop_spark_session()
        
        logger.info("Application completed")
        sys.exit(exit_code)
        
    except Exception as e:
        logger.error(f"Application failed with error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()