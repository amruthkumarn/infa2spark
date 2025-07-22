#!/usr/bin/env python3
"""
Standalone test for Spark Code Generator
Tests the code generation functionality without requiring PySpark
"""
import sys
import os
from pathlib import Path
import logging

# Add src to path
sys.path.insert(0, 'src')

def setup_logging():
    """Setup basic logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def test_spark_code_generation():
    """Test the Spark code generation"""
    logger = logging.getLogger("TestGenerator")
    
    try:
        # Import only what we need for code generation (avoiding PySpark imports)
        from core.spark_code_generator import SparkCodeGenerator
        
        logger.info("Starting Spark application generation test")
        
        # Initialize code generator
        generator = SparkCodeGenerator("generated_spark_apps")
        
        # Generate application from sample XML
        input_xml = "input/sample_project.xml"
        project_name = "MyBDMProject_SparkApp"
        
        if not os.path.exists(input_xml):
            logger.error(f"Input XML file not found: {input_xml}")
            return False
        
        logger.info(f"Generating Spark application from {input_xml}")
        app_path = generator.generate_spark_application(input_xml, project_name)
        
        logger.info(f"✅ Spark application generated successfully!")
        logger.info(f"📁 Location: {app_path}")
        logger.info(f"🚀 To run the generated app:")
        logger.info(f"   cd {app_path}")
        logger.info(f"   ./run.sh")
        
        # Verify key files were generated
        app_dir = Path(app_path)
        expected_files = [
            "src/main/python/main.py",
            "src/main/python/base_classes.py", 
            "config/application.yaml",
            "requirements.txt",
            "run.sh",
            "README.md"
        ]
        
        missing_files = []
        for file_path in expected_files:
            if not (app_dir / file_path).exists():
                missing_files.append(file_path)
        
        if missing_files:
            logger.warning(f"⚠️  Missing files: {missing_files}")
        else:
            logger.info("✅ All expected files generated successfully")
        
        return True
        
    except ImportError as e:
        logger.error(f"Import error (missing dependencies): {str(e)}")
        logger.info("This is expected if PySpark is not installed")
        logger.info("The code generation should still work for basic functionality")
        return False
    except Exception as e:
        logger.error(f"❌ Error generating Spark application: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    setup_logging()
    success = test_spark_code_generation()
    sys.exit(0 if success else 1) 