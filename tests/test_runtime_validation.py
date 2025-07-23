#!/usr/bin/env python3
"""
Runtime Validation Test for Generated Spark Code
Tests instantiation and method calls without requiring full Spark execution
"""
import sys
import os
from pathlib import Path
from unittest.mock import Mock, MagicMock
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_mock_spark_session():
    """Create a mock Spark session for testing"""
    mock_spark = Mock()
    mock_spark.sparkContext = Mock()
    mock_spark.sparkContext.setLogLevel = Mock()
    mock_spark.read = Mock()
    mock_spark.createDataFrame = Mock()
    mock_spark.sql = Mock()
    mock_spark.stop = Mock()
    return mock_spark

def create_mock_dataframe():
    """Create a mock DataFrame for testing"""
    mock_df = Mock()
    mock_df.withColumn = Mock(return_value=mock_df)
    mock_df.filter = Mock(return_value=mock_df)
    mock_df.groupBy = Mock(return_value=mock_df)
    mock_df.agg = Mock(return_value=mock_df)
    mock_df.join = Mock(return_value=mock_df)
    mock_df.union = Mock(return_value=mock_df)
    mock_df.orderBy = Mock(return_value=mock_df)
    mock_df.count = Mock(return_value=100)
    mock_df.show = Mock()
    mock_df.write = Mock()
    return mock_df

def test_transformation_instantiation(app_dir: Path) -> bool:
    """Test if transformation classes can be instantiated"""
    try:
        # Add app directory to Python path
        sys.path.insert(0, str(app_dir / "src" / "main" / "python"))
        
        # Mock PySpark modules
        sys.modules['pyspark'] = Mock()
        sys.modules['pyspark.sql'] = Mock()
        sys.modules['pyspark.sql.functions'] = Mock()
        sys.modules['pyspark.sql.types'] = Mock()
        sys.modules['pyspark.sql.window'] = Mock()
        
        # Import and test base classes
        from base_classes import BaseTransformation, BaseMapping, BaseWorkflow
        logger.info("✅ Base classes imported successfully")
        
        # Import and test transformations
        from transformations.generated_transformations import (
            SequenceTransformation, SorterTransformation, RouterTransformation, 
            UnionTransformation, ExpressionTransformation, AggregatorTransformation
        )
        logger.info("✅ Transformation classes imported successfully")
        
        # Test instantiation
        mock_df = create_mock_dataframe()
        
        # Test SequenceTransformation
        seq_transform = SequenceTransformation("test_seq", start_value=1, increment_value=1)
        result = seq_transform.transform(mock_df)
        logger.info("✅ SequenceTransformation: instantiation and transform OK")
        
        # Test SorterTransformation
        sort_keys = [{'field_name': 'id', 'direction': 'ASC'}]
        sort_transform = SorterTransformation("test_sort", sort_keys=sort_keys)
        result = sort_transform.transform(mock_df)
        logger.info("✅ SorterTransformation: instantiation and transform OK")
        
        # Test RouterTransformation
        output_groups = [{'name': 'group1', 'condition': 'amount > 100'}]
        router_transform = RouterTransformation("test_router", output_groups=output_groups)
        result = router_transform.transform(mock_df)
        logger.info("✅ RouterTransformation: instantiation and transform OK")
        
        # Test UnionTransformation
        union_transform = UnionTransformation("test_union")
        result = union_transform.transform(mock_df, mock_df)
        logger.info("✅ UnionTransformation: instantiation and transform OK")
        
        # Test ExpressionTransformation
        expressions = {'calculated_field': 'amount * 1.1'}
        filters = ['amount > 0']
        expr_transform = ExpressionTransformation("test_expr", expressions=expressions, filters=filters)
        result = expr_transform.transform(mock_df)
        logger.info("✅ ExpressionTransformation: instantiation and transform OK")
        
        # Test AggregatorTransformation
        group_by_cols = ['region']
        aggregations = {'total_amount': {'type': 'sum', 'column': 'amount'}}
        agg_transform = AggregatorTransformation("test_agg", group_by_cols=group_by_cols, aggregations=aggregations)
        result = agg_transform.transform(mock_df)
        logger.info("✅ AggregatorTransformation: instantiation and transform OK")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Transformation testing failed: {e}")
        return False
    finally:
        # Clean up sys.path
        if str(app_dir / "src" / "main" / "python") in sys.path:
            sys.path.remove(str(app_dir / "src" / "main" / "python"))

def test_mapping_instantiation(app_dir: Path) -> bool:
    """Test if mapping classes can be instantiated"""
    try:
        # Add app directory to Python path
        sys.path.insert(0, str(app_dir / "src" / "main" / "python"))
        
        # Mock Spark session
        mock_spark = create_mock_spark_session()
        mock_config = {'connections': {}, 'parameters': {}}
        
        # Find mapping files
        mappings_dir = app_dir / "src" / "main" / "python" / "mappings"
        if not mappings_dir.exists():
            logger.info("⚠️  No mappings directory found")
            return True
            
        mapping_files = [f for f in mappings_dir.glob("*.py") if f.name != "__init__.py"]
        
        for mapping_file in mapping_files:
            try:
                # Import mapping module
                module_name = mapping_file.stem
                spec = importlib.util.spec_from_file_location(module_name, mapping_file)
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                
                # Find mapping class (should be the one that inherits from BaseMapping)
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (isinstance(attr, type) and 
                        hasattr(attr, '__bases__') and 
                        any('BaseMapping' in str(base) for base in attr.__bases__)):
                        
                        # Test instantiation
                        mapping_instance = attr(mock_spark, mock_config)
                        logger.info(f"✅ {attr_name}: instantiation OK")
                        
                        # Test execute method exists
                        if hasattr(mapping_instance, 'execute'):
                            logger.info(f"✅ {attr_name}: execute method found")
                        else:
                            logger.warning(f"⚠️  {attr_name}: execute method missing")
                        
                        break
                        
            except Exception as e:
                logger.error(f"❌ Mapping {mapping_file.name} testing failed: {e}")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Mapping testing failed: {e}")
        return False
    finally:
        # Clean up sys.path
        if str(app_dir / "src" / "main" / "python") in sys.path:
            sys.path.remove(str(app_dir / "src" / "main" / "python"))

def test_workflow_instantiation(app_dir: Path) -> bool:
    """Test if workflow classes can be instantiated"""
    try:
        # Add app directory to Python path
        sys.path.insert(0, str(app_dir / "src" / "main" / "python"))
        
        # Mock Spark session
        mock_spark = create_mock_spark_session()
        mock_config = {'workflow_parameters': {}}
        
        # Find workflow files
        workflows_dir = app_dir / "src" / "main" / "python" / "workflows"
        if not workflows_dir.exists():
            logger.info("⚠️  No workflows directory found")
            return True
            
        workflow_files = [f for f in workflows_dir.glob("*.py") if f.name != "__init__.py"]
        
        for workflow_file in workflow_files:
            try:
                # Import workflow module
                module_name = workflow_file.stem
                spec = importlib.util.spec_from_file_location(module_name, workflow_file)
                module = importlib.util.module_from_spec(spec)
                sys.modules[module_name] = module
                spec.loader.exec_module(module)
                
                # Find workflow class
                for attr_name in dir(module):
                    attr = getattr(module, attr_name)
                    if (isinstance(attr, type) and 
                        hasattr(attr, '__bases__') and 
                        any('BaseWorkflow' in str(base) for base in attr.__bases__)):
                        
                        # Test instantiation
                        workflow_instance = attr(mock_spark, mock_config)
                        logger.info(f"✅ {attr_name}: instantiation OK")
                        
                        # Test execute method exists
                        if hasattr(workflow_instance, 'execute'):
                            logger.info(f"✅ {attr_name}: execute method found")
                        else:
                            logger.warning(f"⚠️  {attr_name}: execute method missing")
                        
                        break
                        
            except Exception as e:
                logger.error(f"❌ Workflow {workflow_file.name} testing failed: {e}")
                return False
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Workflow testing failed: {e}")
        return False
    finally:
        # Clean up sys.path
        if str(app_dir / "src" / "main" / "python") in sys.path:
            sys.path.remove(str(app_dir / "src" / "main" / "python"))

def main():
    """Main runtime validation function"""
    print("🧪 Starting Runtime Validation Tests...")
    
    generated_apps_dir = Path("generated_spark_apps")
    
    if not generated_apps_dir.exists():
        logger.error("Generated apps directory not found")
        sys.exit(1)
    
    total_apps = 0
    passed_apps = 0
    
    for app_dir in generated_apps_dir.iterdir():
        if app_dir.is_dir():
            total_apps += 1
            logger.info(f"\n🚀 Testing app: {app_dir.name}")
            
            # Test transformations
            transform_ok = test_transformation_instantiation(app_dir)
            
            # Test mappings  
            mapping_ok = test_mapping_instantiation(app_dir)
            
            # Test workflows
            workflow_ok = test_workflow_instantiation(app_dir)
            
            if transform_ok and mapping_ok and workflow_ok:
                passed_apps += 1
                logger.info(f"✅ {app_dir.name}: All runtime tests PASSED")
            else:
                logger.error(f"❌ {app_dir.name}: Some runtime tests FAILED")
    
    print(f"\n📊 RUNTIME VALIDATION SUMMARY:")
    print(f"   Total Applications: {total_apps}")
    print(f"   ✅ Passed: {passed_apps}")
    print(f"   ❌ Failed: {total_apps - passed_apps}")
    print(f"   Success Rate: {(passed_apps/total_apps*100):.1f}%" if total_apps > 0 else "   Success Rate: N/A")
    
    if passed_apps == total_apps:
        print(f"\n🎉 All applications passed runtime validation!")
        sys.exit(0)
    else:
        print(f"\n❌ {total_apps - passed_apps} applications failed runtime validation")
        sys.exit(1)

if __name__ == "__main__":
    # Import required modules for testing
    import importlib.util
    main() 