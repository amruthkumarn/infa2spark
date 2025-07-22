#!/usr/bin/env python3
"""
Demonstration: Common Utilities for XML-Specific Spark Applications
=================================================================

This script demonstrates the powerful common utilities that are now automatically
included in ALL generated Spark applications, with special focus on XML processing.

Key Features Demonstrated:
✅ XML parsing and metadata extraction from Informatica projects
✅ Data type conversion from XML schema to Spark types
✅ Configuration management with parameter resolution
✅ Common transformation patterns (lookups, SCD, data quality)
✅ Performance optimizations
✅ Logging and monitoring utilities
"""

import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, 'src')

def demo_xml_utilities():
    """Demonstrate XML processing utilities"""
    
    print("🔍 XML Utilities Demonstration")
    print("-" * 40)
    
    try:
        from core.spark_common_utilities import XMLUtilities
        
        # Parse Informatica XML project
        xml_file = "input/financial_dw_project.xml"
        
        if Path(xml_file).exists():
            print(f"📄 Parsing Informatica XML: {xml_file}")
            project_info = XMLUtilities.parse_informatica_xml(xml_file)
            
            print(f"✅ Project Name: {project_info['name']}")
            print(f"📊 Sources Found: {len(project_info['sources'])}")
            print(f"🔄 Transformations Found: {len(project_info['transformations'])}")
            print(f"🎯 Targets Found: {len(project_info['targets'])}")
            print(f"⚙️  Parameters Found: {len(project_info['parameters'])}")
            
            # Show sample source metadata
            if project_info['sources']:
                sample_source = project_info['sources'][0]
                print(f"\n📋 Sample Source: {sample_source['name']}")
                print(f"   Type: {sample_source['type']}")
                print(f"   Database: {sample_source['database_type']}")
                print(f"   Schema Fields: {len(sample_source['schema'])}")
                
                # Show schema details
                if sample_source['schema']:
                    print(f"   📈 Schema Preview:")
                    for field in sample_source['schema'][:3]:
                        print(f"     • {field['name']}: {field['datatype']} "
                              f"({'nullable' if field['nullable'] else 'not null'})")
        else:
            print(f"⚠️  XML file not found: {xml_file}")
            
        return True
        
    except Exception as e:
        print(f"❌ Error in XML utilities demo: {str(e)}")
        return False

def demo_data_type_utilities():
    """Demonstrate data type conversion utilities"""
    
    print(f"\n🔧 Data Type Conversion Utilities")
    print("-" * 40)
    
    try:
                          from core.spark_common_utilities import DataTypeUtilities
         from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
        
        # Test Informatica to Spark type conversions
        test_cases = [
            ("STRING", "", "", "String field"),
            ("INTEGER", "", "", "Integer field"), 
            ("DECIMAL", "10", "2", "Decimal with precision"),
            ("DATE", "", "", "Date field"),
            ("TIMESTAMP", "", "", "Timestamp field"),
            ("VARCHAR", "255", "", "Variable character field")
        ]
        
        print("🔄 Informatica → Spark Type Conversions:")
        for informatica_type, precision, scale, description in test_cases:
            spark_type = DataTypeUtilities.convert_informatica_type_to_spark(
                informatica_type, precision, scale
            )
            print(f"   • {informatica_type:12} → {str(spark_type):20} ({description})")
        
        # Demonstrate schema creation from metadata
        sample_metadata = {
            'name': 'sample_source',
            'schema': [
                {'name': 'customer_id', 'datatype': 'INTEGER', 'nullable': False},
                {'name': 'customer_name', 'datatype': 'VARCHAR', 'precision': '100', 'nullable': True},
                {'name': 'balance', 'datatype': 'DECIMAL', 'precision': '10', 'scale': '2', 'nullable': True},
                {'name': 'created_date', 'datatype': 'DATE', 'nullable': False}
            ]
        }
        
        spark_schema = DataTypeUtilities.create_spark_schema_from_xml_metadata(sample_metadata)
        print(f"\n📊 Generated Spark Schema:")
        for field in spark_schema.fields:
            nullable_str = "nullable" if field.nullable else "not null"
            print(f"   • {field.name:15}: {str(field.dataType):20} ({nullable_str})")
        
        return True
        
    except Exception as e:
        print(f"❌ Error in data type utilities demo: {str(e)}")
        return False

def demo_transformation_utilities():
    """Demonstrate transformation utilities with mock data"""
    
    print(f"\n🎯 Transformation Utilities Demonstration")
    print("-" * 40)
    
    try:
        from core.spark_common_utilities import TransformationUtilities, LoggingUtilities
        from pyspark.sql import SparkSession
        from pyspark.sql.types import *
        from pyspark.sql.functions import *
        
        # Create Spark session
        spark = SparkSession.builder \
            .appName("UtilitiesDemo") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        logger = LoggingUtilities.setup_application_logging("UtilitiesDemo")
        
        # Create sample data
        input_schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("balance", DecimalType(10,2), True)
        ])
        
        input_data = [
            (1, "Alice Johnson", 1500.50),
            (2, "Bob Smith", 2750.25),
            (3, "Carol Davis", 850.00),
            (4, None, 1200.75),  # Test null handling
            (1, "Alice Johnson Updated", 1600.00)  # Test duplicate
        ]
        
        input_df = spark.createDataFrame(input_data, input_schema)
        
        # Create lookup data
        lookup_schema = StructType([
            StructField("customer_id", IntegerType(), False),
            StructField("region", StringType(), True),
            StructField("status", StringType(), True)
        ])
        
        lookup_data = [
            (1, "North", "Premium"),
            (2, "South", "Standard"),
            (3, "East", "Standard"),
            (4, "West", "Basic")
        ]
        
        lookup_df = spark.createDataFrame(lookup_data, lookup_schema)
        
        print("📊 Sample Input Data:")
        LoggingUtilities.log_dataframe_metrics(input_df, "Input", logger)
        
        print(f"\n🔍 Lookup Data:")
        LoggingUtilities.log_dataframe_metrics(lookup_df, "Lookup", logger)
        
        # Demonstrate lookup transformation
        print(f"\n🔗 Applying Lookup Transformation...")
        enriched_df = TransformationUtilities.apply_lookup_with_cache(
            input_df, lookup_df, 
            join_keys=['customer_id'],
            lookup_columns=['region', 'status'],
            cache_lookup=True
        )
        
        LoggingUtilities.log_dataframe_metrics(enriched_df, "Enriched", logger)
        
        # Demonstrate data quality checks
        print(f"\n✅ Applying Data Quality Checks...")
        quality_rules = {
            'not_null_columns': ['customer_id', 'name'],
            'remove_duplicates': True,
            'range_checks': {
                'balance': {'min': 0.0, 'max': 10000.0}
            }
        }
        
        clean_df, metrics = TransformationUtilities.apply_data_quality_checks(
            enriched_df, quality_rules
        )
        
        print(f"📊 Data Quality Metrics:")
        for metric, value in metrics.items():
            print(f"   • {metric}: {value}")
        
        # Show final results
        print(f"\n📋 Final Clean Data Sample:")
        clean_df.show(5, truncate=False)
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ Error in transformation utilities demo: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def demo_configuration_utilities():
    """Demonstrate configuration management utilities"""
    
    print(f"\n⚙️  Configuration Utilities Demonstration")
    print("-" * 40)
    
    try:
        from core.spark_common_utilities import ConfigurationUtilities
        import tempfile
        import os
        
        # Create a sample YAML configuration with parameters
        sample_config = """
application:
  name: "DemoApp"
  version: "1.0"
  load_date: "$$SystemDate"
  batch_size: "${BATCH_SIZE}"
  
connections:
  database:
    url: "jdbc:postgresql://localhost:5432/testdb"
    driver: "org.postgresql.Driver"
    user: "${DB_USER}"
    password: "${DB_PASSWORD}"

parameters:
  ERROR_THRESHOLD: "0.05"
  REGION: "US"
"""
        
        # Write to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(sample_config)
            temp_config_path = f.name
        
        try:
            # Set some environment variables for demo
            os.environ['BATCH_SIZE'] = '50000'
            os.environ['DB_USER'] = 'demo_user'
            os.environ['DB_PASSWORD'] = 'demo_password'
            
            print("📄 Loading YAML configuration with environment variables...")
            config = ConfigurationUtilities.load_yaml_config(temp_config_path)
            
            print("✅ Configuration loaded successfully!")
            print(f"   Application: {config['application']['name']} v{config['application']['version']}")
            print(f"   Batch Size: {config['application']['batch_size']}")
            print(f"   Database User: {config['connections']['database']['user']}")
            
            # Demonstrate parameter resolution
            print(f"\n🔄 Resolving Informatica parameters...")
            xml_parameters = {
                'PROJECT_NAME': 'FinancialDW',
                'ENV': 'PRODUCTION'
            }
            
            resolved_config = ConfigurationUtilities.resolve_informatica_parameters(
                config, xml_parameters
            )
            
            print(f"✅ Parameters resolved!")
            print(f"   Load Date: {resolved_config['application']['load_date']}")
            
            # Validate connection configuration
            print(f"\n🔍 Validating connection configuration...")
            conn_config = resolved_config['connections']['database']
            conn_config['type'] = 'jdbc'  # Add required type field
            
            is_valid, errors = ConfigurationUtilities.validate_connection_config(conn_config)
            
            if is_valid:
                print("✅ Connection configuration is valid!")
            else:
                print(f"❌ Configuration errors: {errors}")
        
        finally:
            # Clean up temp file
            os.unlink(temp_config_path)
        
        return True
        
    except Exception as e:
        print(f"❌ Error in configuration utilities demo: {str(e)}")
        return False

def demo_performance_utilities():
    """Demonstrate performance optimization utilities"""
    
    print(f"\n🚀 Performance Utilities Demonstration")
    print("-" * 40)
    
    try:
        from core.spark_common_utilities import PerformanceUtilities
        from pyspark.sql import SparkSession
        from pyspark.sql.types import *
        
        # Create Spark session
        spark = SparkSession.builder \
            .appName("PerformanceDemo") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "false") \
            .getOrCreate()
        
        # Create sample DataFrames
        large_df = spark.range(1000000).select(
            col("id").alias("customer_id"),
            (col("id") % 100).alias("region_id"),
            (col("id") * 1.5).alias("balance")
        )
        
        small_df = spark.range(100).select(
            col("id").alias("region_id"),
            col("id").cast(StringType()).alias("region_name")
        )
        
        print("📊 DataFrames created:")
        print(f"   Large DataFrame: {large_df.count():,} rows")
        print(f"   Small DataFrame: {small_df.count():,} rows")
        
        # Demonstrate auto-broadcasting
        print(f"\n📡 Auto-broadcasting small DataFrame...")
        broadcasted_df = PerformanceUtilities.auto_broadcast_small_tables(
            small_df, threshold=1000
        )
        print("✅ Small DataFrame will be broadcasted for joins")
        
        # Demonstrate join optimization
        print(f"\n🔗 Optimizing large DataFrame for joins...")
        optimized_df = PerformanceUtilities.optimize_dataframe_for_joins(
            large_df, ['customer_id']
        )
        print("✅ Large DataFrame optimized with partitioning and caching")
        
        # Calculate optimal partitions
        optimal_partitions = PerformanceUtilities.get_optimal_partition_count(large_df)
        print(f"📊 Optimal partition count: {optimal_partitions}")
        
        # Perform optimized join
        print(f"\n⚡ Performing optimized join...")
        join_result = optimized_df.join(
            broadcasted_df,
            optimized_df.region_id == broadcasted_df.region_id,
            'left_outer'
        )
        
        result_count = join_result.count()
        print(f"✅ Join completed: {result_count:,} rows")
        
        spark.stop()
        return True
        
    except Exception as e:
        print(f"❌ Error in performance utilities demo: {str(e)}")
        return False

def demo_utilities_in_generated_app():
    """Show how utilities are used in generated applications"""
    
    print(f"\n🎯 Utilities in Generated Applications")
    print("-" * 40)
    
    try:
        # Check generated application structure
        app_path = "generated_spark_apps/RetailETL_WithUtilities"
        if not Path(app_path).exists():
            print(f"❌ Generated app not found at {app_path}")
            return False
        
        # Check utilities file
        utils_file = Path(app_path) / "src/main/python/utils/common_utilities.py"
        if utils_file.exists():
            print(f"✅ Utilities available in generated app: {utils_file}")
            
            # Show how they're imported in base classes
            base_classes_file = Path(app_path) / "src/main/python/base_classes.py"
            if base_classes_file.exists():
                with open(base_classes_file, 'r') as f:
                    content = f.read()
                    if 'from .utils.common_utilities import' in content:
                        print("✅ Utilities automatically imported in base classes")
                    else:
                        print("⚠️  Utilities import not found in base classes")
            
            # List available utility classes
            with open(utils_file, 'r') as f:
                content = f.read()
                utility_classes = [
                    'XMLUtilities',
                    'DataTypeUtilities', 
                    'TransformationUtilities',
                    'ConfigurationUtilities',
                    'PerformanceUtilities',
                    'LoggingUtilities'
                ]
                
                available_classes = []
                for util_class in utility_classes:
                    if f'class {util_class}' in content:
                        available_classes.append(util_class)
                
                print(f"📚 Available utility classes in generated app:")
                for util_class in available_classes:
                    print(f"   ✅ {util_class}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error checking utilities in generated app: {str(e)}")
        return False

if __name__ == "__main__":
    print("🎯 Common Utilities Demonstration for XML-Specific Spark Applications")
    print("=" * 80)
    print("This demo shows how common utilities enhance generated Spark applications")
    print("with powerful XML processing and transformation capabilities.")
    print("")
    
    # Run all demonstrations
    demos = [
        ("XML Processing", demo_xml_utilities),
        ("Data Type Conversion", demo_data_type_utilities),
        ("Transformations", demo_transformation_utilities),
        ("Configuration Management", demo_configuration_utilities),
        ("Performance Optimization", demo_performance_utilities),
        ("Generated App Integration", demo_utilities_in_generated_app)
    ]
    
    passed = 0
    for demo_name, demo_func in demos:
        try:
            if demo_func():
                passed += 1
                print(f"✅ {demo_name} demo completed successfully!")
            else:
                print(f"❌ {demo_name} demo failed!")
        except Exception as e:
            print(f"❌ {demo_name} demo error: {str(e)}")
        
        print("")  # Spacing between demos
    
    print("=" * 80)
    print(f"📊 Demo Results: {passed}/{len(demos)} demos passed")
    
    if passed == len(demos):
        print("🎉 ALL DEMOS PASSED!")
        print("\n🚀 Key Benefits Demonstrated:")
        print("   ✅ XML parsing and metadata extraction from Informatica projects")
        print("   ✅ Automatic data type conversion from XML schema to Spark") 
        print("   ✅ Intelligent lookup transformations with caching")
        print("   ✅ Data quality validation with comprehensive metrics")
        print("   ✅ Performance optimizations (broadcasting, partitioning, caching)")
        print("   ✅ Standardized logging and monitoring")
        print("   ✅ Configuration management with parameter resolution")
        print("   ✅ Automatic integration into ALL generated Spark applications")
    else:
        print("⚠️  Some demos failed - check error messages above") 