#!/usr/bin/env python3
"""
Multi-Project Spark Generation Demo
Demonstrates generating Spark applications from different XSD-compliant XML projects
"""
import sys
import os
import logging
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, 'src')

from core.spark_code_generator import SparkCodeGenerator

def setup_logging():
    """Setup demo logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def generate_demo_projects():
    """Generate Spark applications from all demo XML projects"""
    
    logger = logging.getLogger("MultiProjectDemo")
    logger.info("🚀 Starting Multi-Project Spark Generation Demo")
    
    # Initialize the generator
    generator = SparkCodeGenerator("generated_spark_apps")
    
    # Define our demo projects
    projects = [
        {
            'xml_file': 'input/retail_etl_project.xml',
            'name': 'RetailETL_SparkApp', 
            'description': 'Simple Retail ETL with basic transformations'
        },
        {
            'xml_file': 'input/financial_dw_project.xml',
            'name': 'FinancialDW_SparkApp',
            'description': 'Complex Financial DW with SCD Type 2 and multi-source integration'
        },
        {
            'xml_file': 'input/realtime_analytics_project.xml', 
            'name': 'RealtimeAnalytics_SparkApp',
            'description': 'Modern Real-time Analytics with Streaming, IoT, and ML'
        }
    ]
    
    generated_apps = []
    
    for i, project in enumerate(projects, 1):
        logger.info(f"\n{'='*80}")
        logger.info(f"📋 PROJECT {i}/3: {project['description']}")
        logger.info(f"{'='*80}")
        
        try:
            # Generate the Spark application
            app_path = generator.generate_spark_application(
                project['xml_file'],
                project['name']
            )
            
            generated_apps.append({
                'name': project['name'],
                'path': app_path,
                'description': project['description']
            })
            
            # Analyze the generated application
            analyze_generated_app(app_path, project['name'])
            
            logger.info(f"✅ Successfully generated: {project['name']}")
            
        except Exception as e:
            logger.error(f"❌ Failed to generate {project['name']}: {str(e)}")
            continue
    
    # Print summary
    print_generation_summary(generated_apps)
    
    return generated_apps

def analyze_generated_app(app_path, app_name):
    """Analyze and report on generated application structure"""
    
    logger = logging.getLogger("AppAnalyzer")
    app_dir = Path(app_path)
    
    if not app_dir.exists():
        logger.error(f"Generated app directory not found: {app_path}")
        return
    
    # Count generated files
    mappings_dir = app_dir / "src/main/python/mappings"
    workflows_dir = app_dir / "src/main/python/workflows" 
    
    mapping_count = len(list(mappings_dir.glob("*.py"))) if mappings_dir.exists() else 0
    workflow_count = len(list(workflows_dir.glob("*.py"))) if workflows_dir.exists() else 0
    
    # Check for key files
    key_files = [
        "src/main/python/main.py",
        "src/main/python/base_classes.py",
        "config/application.yaml", 
        "requirements.txt",
        "run.sh",
        "README.md"
    ]
    
    existing_files = []
    missing_files = []
    
    for file_path in key_files:
        if (app_dir / file_path).exists():
            existing_files.append(file_path)
        else:
            missing_files.append(file_path)
    
    # Get file sizes for key components
    main_py_size = (app_dir / "src/main/python/main.py").stat().st_size if (app_dir / "src/main/python/main.py").exists() else 0
    readme_size = (app_dir / "README.md").stat().st_size if (app_dir / "README.md").exists() else 0
    
    logger.info(f"📊 Generated Application Analysis:")
    logger.info(f"   • Mappings Generated: {mapping_count}")
    logger.info(f"   • Workflows Generated: {workflow_count}")
    logger.info(f"   • Core Files Present: {len(existing_files)}/{len(key_files)}")
    logger.info(f"   • Main.py Size: {main_py_size:,} bytes")
    logger.info(f"   • README Size: {readme_size:,} bytes")
    
    if missing_files:
        logger.warning(f"   • Missing Files: {missing_files}")

def print_generation_summary(generated_apps):
    """Print a comprehensive summary of all generated applications"""
    
    print("\n" + "="*100)
    print("🎯 SPARK CODE GENERATION SUMMARY")
    print("="*100)
    
    if not generated_apps:
        print("❌ No applications were successfully generated")
        return
    
    print(f"✅ Successfully generated {len(generated_apps)} Spark applications:\n")
    
    for i, app in enumerate(generated_apps, 1):
        print(f"{i}. **{app['name']}**")
        print(f"   📁 Location: {app['path']}")
        print(f"   📝 Description: {app['description']}")
        
        # Show directory structure
        app_dir = Path(app['path'])
        if app_dir.exists():
            src_dir = app_dir / "src/main/python"
            if src_dir.exists():
                mapping_files = list((src_dir / "mappings").glob("*.py")) if (src_dir / "mappings").exists() else []
                workflow_files = list((src_dir / "workflows").glob("*.py")) if (src_dir / "workflows").exists() else []
                
                print(f"   🗂️  Generated:")
                print(f"      • {len(mapping_files)} mapping classes")
                print(f"      • {len(workflow_files)} workflow classes")
                print(f"      • Complete project structure with Docker support")
                print("")
    
    print("🔧 **How to run any generated application:**")
    print("   cd <app_directory>")  
    print("   pip install -r requirements.txt")
    print("   python scripts/generate_test_data.py")
    print("   ./run.sh")
    print("")
    
    print("🏗️ **Key Features of Generated Applications:**")
    print("   ✅ Production-ready PySpark code")
    print("   ✅ Complete Docker deployment setup")  
    print("   ✅ Comprehensive logging and error handling")
    print("   ✅ Test data generation scripts")
    print("   ✅ Documentation and README files")
    print("   ✅ XSD-compliant transformation logic")
    
    print("\n" + "="*100)
    print("🎉 **Multi-Project Generation Complete!**")
    print("="*100)

if __name__ == "__main__":
    setup_logging()
    
    try:
        generated_apps = generate_demo_projects()
        
        if generated_apps:
            print(f"\n🎯 Demo completed successfully! Generated {len(generated_apps)} Spark applications.")
        else:
            print("\n❌ Demo failed - no applications were generated.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n💥 Demo failed with error: {str(e)}")
        sys.exit(1) 