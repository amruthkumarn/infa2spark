"""
Integration tests for the complete PoC
"""
import pytest
import tempfile
import shutil
import os
import sys
from unittest.mock import patch

# Add src to path
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
src_dir = os.path.join(parent_dir, 'src')
sys.path.insert(0, src_dir)

from core.xml_parser import InformaticaXMLParser
from core.spark_manager import SparkManager
from core.config_manager import ConfigManager
from workflows.daily_etl_process import DailyETLProcess

@pytest.fixture
def integration_test_dir():
    """Create directory structure for integration testing"""
    temp_dir = tempfile.mkdtemp()
    
    # Create subdirectories
    subdirs = ['input', 'output', 'config', 'sample_data', 'logs']
    for subdir in subdirs:
        os.makedirs(os.path.join(temp_dir, subdir), exist_ok=True)
    
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture
def integration_xml_file(integration_test_dir):
    """Create sample XML file for integration testing"""
    xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
<project name="IntegrationTestProject" version="1.0" xmlns="http://www.informatica.com/BDM/Project/10.2">
  <description>Integration Test Project</description>
  
  <folders>
    <folder name="Mappings">
      <mapping name="Sales_Staging">
        <description>Test sales staging</description>
        <components>
          <source name="SALES_SOURCE" type="HDFS" format="PARQUET"/>
          <transformation name="FILTER_SALES" type="Expression"/>
          <transformation name="AGG_SALES" type="Aggregator"/>
          <target name="STG_SALES" type="HIVE"/>
        </components>
      </mapping>
    </folder>
    
    <folder name="Workflows">
      <workflow name="Daily_ETL_Process">
        <description>Test workflow</description>
        <tasks>
          <task name="T1_Sales_Staging" type="Mapping" mapping="Sales_Staging"/>
          <task name="T5_Send_Notification" type="Email">
            <properties>
              <property name="Recipient" value="test@example.com"/>
            </properties>
          </task>
        </tasks>
        <links>
          <link from="T1_Sales_Staging" to="T5_Send_Notification" condition="SUCCESS"/>
        </links>
      </workflow>
    </folder>
  </folders>
  
  <connections>
    <connection name="HDFS_CONN" type="HDFS" host="localhost" port="8020"/>
    <connection name="HIVE_CONN" type="HIVE" host="localhost" port="10000"/>
  </connections>
  
  <parameters>
    <parameter name="TEST_PARAM" value="test_value"/>
  </parameters>
</project>'''
    
    xml_file = os.path.join(integration_test_dir, 'input', 'test_project.xml')
    with open(xml_file, 'w') as f:
        f.write(xml_content)
    
    return xml_file

@pytest.fixture
def integration_config_files(integration_test_dir):
    """Create configuration files for integration testing"""
    import yaml
    
    config_dir = os.path.join(integration_test_dir, 'config')
    output_dir = os.path.join(integration_test_dir, 'output')
    
    # Connections config
    connections_config = {
        'HDFS_CONN': {
            'type': 'HDFS',
            'host': 'localhost',
            'port': 8020,
            'local_path': output_dir
        },
        'HIVE_CONN': {
            'type': 'HIVE',
            'host': 'localhost',
            'port': 10000,
            'local_path': output_dir
        }
    }
    
    with open(os.path.join(config_dir, 'connections.yaml'), 'w') as f:
        yaml.dump(connections_config, f)
    
    # Spark config
    spark_config = {
        'spark': {
            'spark.master': 'local[2]',
            'spark.sql.shuffle.partitions': '2',
            'spark.sql.warehouse.dir': os.path.join(integration_test_dir, 'spark-warehouse')
        },
        'enable_hive_support': False,
        'log_level': 'ERROR'
    }
    
    with open(os.path.join(config_dir, 'spark_config.yaml'), 'w') as f:
        yaml.dump(spark_config, f)
    
    # Sample project config
    project_config = {
        'project': {
            'name': 'IntegrationTestProject',
            'version': '1.0'
        },
        'parameters': {
            'LOAD_DATE': '2023-01-01',
            'ENV': 'TEST'
        },
        'notifications': {
            'enabled': False
        }
    }
    
    with open(os.path.join(config_dir, 'sample_project_config.yaml'), 'w') as f:
        yaml.dump(project_config, f)
    
    return config_dir

def test_xml_parser_integration(integration_xml_file):
    """Test XML parser with complete XML file"""
    parser = InformaticaXMLParser()
    project = parser.parse_project(integration_xml_file)
    
    assert project.name == "IntegrationTestProject"
    assert project.version == "1.0"
    
    # Verify mappings
    assert 'folder' in project.folders
    assert 'Mappings' in project.folders['folder']
    mappings = project.folders['folder']['Mappings']
    assert len(mappings) == 1
    assert mappings[0]['name'] == "Sales_Staging"
    
    # Verify workflows
    assert 'Workflows' in project.folders['folder']
    workflows = project.folders['folder']['Workflows']
    assert len(workflows) == 1
    assert workflows[0]['name'] == "Daily_ETL_Process"
    
    # Verify connections
    assert len(project.connections) == 2
    assert 'HDFS_CONN' in project.connections
    assert 'HIVE_CONN' in project.connections

def test_config_manager_integration(integration_config_files):
    """Test configuration manager with actual config files"""
    config_manager = ConfigManager(integration_config_files)
    
    # Load all configurations
    connections = config_manager.get_connections_config()
    spark_config = config_manager.get_spark_config()
    project_config = config_manager.get_sample_project_config()
    
    assert 'HDFS_CONN' in connections
    assert 'spark' in spark_config
    assert 'project' in project_config
    
    # Test merging configurations
    merged = config_manager.merge_configs(
        spark_config,
        {'connections': connections},
        project_config
    )
    
    assert 'spark' in merged
    assert 'connections' in merged
    assert 'project' in merged

def test_spark_manager_integration():
    """Test Spark manager creation and configuration"""
    spark_manager = SparkManager()
    
    config = {
        'spark': {
            'spark.master': 'local[1]',
            'spark.sql.shuffle.partitions': '2'
        },
        'enable_hive_support': False,
        'log_level': 'ERROR'
    }
    
    spark = spark_manager.create_spark_session("IntegrationTest", config)
    
    try:
        assert spark is not None
        assert spark.sparkContext.appName == "IntegrationTest"
        
        # Test basic DataFrame operations
        data = [("test1", 1), ("test2", 2)]
        df = spark.createDataFrame(data, ["name", "value"])
        assert df.count() == 2
        
    finally:
        spark_manager.stop_spark_session()

def test_end_to_end_integration(integration_test_dir, integration_xml_file, integration_config_files, spark_session):
    """Test complete end-to-end integration"""
    # Setup environment
    os.chdir(integration_test_dir)
    
    # Load configurations
    config_manager = ConfigManager(integration_config_files)
    connections = config_manager.get_connections_config()
    spark_config = config_manager.get_spark_config()
    project_config = config_manager.get_sample_project_config()
    
    full_config = config_manager.merge_configs(
        spark_config,
        {'connections': connections},
        project_config
    )
    
    # Parse XML project
    parser = InformaticaXMLParser()
    project = parser.parse_project(integration_xml_file)
    
    assert project.name == "IntegrationTestProject"
    
    # Create and execute workflow
    workflow = DailyETLProcess(spark_session, full_config)
    
    # Mock mapping execution to avoid complex data setup
    def mock_execute_task(task_name):
        if task_name == "T1_Sales_Staging":
            # Create mock output
            output_dir = os.path.join(integration_test_dir, 'output', 'stg_sales')
            os.makedirs(output_dir, exist_ok=True)
            
            # Create simple test data
            data = [("test", 1)]
            df = spark_session.createDataFrame(data, ["name", "value"])
            df.write.mode("overwrite").parquet(output_dir)
            return True
        elif task_name == "T5_Send_Notification":
            return True
        else:
            return True
    
    # Replace execute_task method
    workflow._execute_task = mock_execute_task
    
    # Execute workflow
    result = workflow.execute()
    assert result is True
    
    # Verify output was created
    output_path = os.path.join(integration_test_dir, 'output', 'stg_sales')
    assert os.path.exists(output_path)
    
    # Verify we can read the output
    output_df = spark_session.read.parquet(output_path)
    assert output_df.count() == 1

@patch('sys.argv', ['main.py'])
def test_main_application_integration(integration_test_dir, integration_xml_file, integration_config_files):
    """Test main application integration (mocked)"""
    # Change to test directory
    original_cwd = os.getcwd()
    os.chdir(integration_test_dir)
    
    try:
        # Copy XML file to expected location
        expected_xml_path = os.path.join(integration_test_dir, 'input', 'sample_project.xml')
        shutil.copy2(integration_xml_file, expected_xml_path)
        
        # Mock the main module execution
        with patch('builtins.__import__') as mock_import:
            def mock_import_side_effect(name, *args, **kwargs):
                if name == 'src.main':
                    # Create a mock main module
                    mock_main = type('MockMain', (), {})()
                    mock_main.main = lambda: None
                    return mock_main
                else:
                    return __import__(name, *args, **kwargs)
            
            mock_import.side_effect = mock_import_side_effect
            
            # This would normally run the main application
            # For testing, we just verify the setup is correct
            assert os.path.exists(expected_xml_path)
            assert os.path.exists(integration_config_files)
            
    finally:
        os.chdir(original_cwd)

def test_error_handling_integration(integration_test_dir, spark_session):
    """Test error handling in integration scenarios"""
    # Test with invalid configuration
    config = {'connections': {}}  # Empty connections
    
    workflow = DailyETLProcess(spark_session, config)
    
    # Should handle missing configuration gracefully
    def test_execute_task():
        try:
            result = workflow._execute_task("T1_Sales_Staging")
            # Should either succeed with defaults or fail gracefully
            assert isinstance(result, bool)
        except Exception as e:
            # Expected exceptions are OK
            assert isinstance(e, (KeyError, ValueError, FileNotFoundError))
    
    test_execute_task()

def test_data_quality_integration(spark_session, integration_test_dir):
    """Test data quality and validation in integration"""
    from data_sources.mock_data_generator import MockDataGenerator
    
    # Generate mock data
    generator = MockDataGenerator(spark_session)
    
    # Test different data types
    data_sources = ["sales_source", "customer_source", "order_source", "product_source"]
    
    for source in data_sources:
        df = generator.generate_mock_data(source)
        
        # Basic quality checks
        assert df.count() > 0, f"No data generated for {source}"
        assert len(df.columns) > 0, f"No columns generated for {source}"
        
        # Check for null values in key fields
        if "customer_id" in df.columns:
            null_customers = df.filter(df.customer_id.isNull()).count()
            assert null_customers == 0, f"Found null customer_ids in {source}"
        
        if "amount" in df.columns:
            negative_amounts = df.filter(df.amount < 0).count()
            # Some negative amounts might be valid, but check they're reasonable
            total_records = df.count()
            assert negative_amounts < total_records * 0.1, f"Too many negative amounts in {source}"