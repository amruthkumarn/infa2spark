"""
Tests for IMX XML parser functionality
"""
import pytest
import tempfile
import os
from src.core.xml_parser import InformaticaXMLParser

@pytest.fixture
def sample_imx_content():
    """Sample IMX XML content for testing"""
    return '''<?xml version="1.0" encoding="UTF-8"?>
<IMX xmlns="http://com.informatica.imx"
     xmlns:project="http://com.informatica.metadata.common.project/2"
     xmlns:folder="http://com.informatica.metadata.common.folder/2"
     xmlns:powercenter="http://com.informatica.powercenter/1">

    <!-- Project Structure -->
    <project:Project id="PROJECT_1" name="TestIMXProject" shared="false">
        <description>Test IMX project</description>
        <contents>
            <folder:Folder id="FOLDER_1" name="Mappings">
                <contents>
                    <powercenter:TLoaderMapping id="MAPPING_1" name="Test_Mapping">
                        <description>Test mapping</description>
                        <components>
                            <source name="TEST_SOURCE" type="HDFS" format="PARQUET"/>
                            <transformation name="TEST_FILTER" type="Expression"/>
                            <target name="TEST_TARGET" type="HIVE"/>
                        </components>
                    </powercenter:TLoaderMapping>
                </contents>
            </folder:Folder>
            
            <folder:Folder id="FOLDER_2" name="Workflows">
                <contents>
                    <powercenter:TWorkflow id="WORKFLOW_1" name="Test_Workflow">
                        <description>Test workflow</description>
                        <tasks>
                            <task name="T1_Test" type="Mapping" mapping="Test_Mapping"/>
                        </tasks>
                        <links>
                            <link from="START" to="T1_Test" condition="SUCCESS"/>
                        </links>
                    </powercenter:TWorkflow>
                </contents>
            </folder:Folder>
        </contents>
    </project:Project>
    
    <!-- Repository Connections -->
    <powercenter:TRepConnection id="CONN_1" 
                               connectionName="TEST_CONN"
                               connectionType="HDFS" 
                               host="localhost" 
                               port="8020"/>
                               
    <!-- Project parameters -->
    <project:Parameter name="TEST_PARAM" value="test_value"/>

</IMX>'''

@pytest.fixture
def imx_file(sample_imx_content):
    """Create temporary IMX file for testing"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
        f.write(sample_imx_content)
        f.flush()
        yield f.name
    os.unlink(f.name)

def test_imx_detection(imx_file):
    """Test IMX format detection"""
    parser = InformaticaXMLParser()
    project = parser.parse_project(imx_file)
    
    assert project.name == "TestIMXProject"
    assert project.description == "Test IMX project"

def test_imx_project_parsing(imx_file):
    """Test IMX project structure parsing"""
    parser = InformaticaXMLParser()
    project = parser.parse_project(imx_file)
    
    # Check basic project info
    assert project.name == "TestIMXProject"
    assert project.description == "Test IMX project"
    
    # Check folders
    assert 'Mappings' in project.folders
    assert 'Workflows' in project.folders
    
    # Check mappings
    mappings = project.folders['Mappings']
    assert len(mappings) == 1
    
    mapping = mappings[0]
    assert mapping['name'] == "Test_Mapping"
    assert mapping['id'] == "MAPPING_1"
    assert mapping['description'] == "Test mapping"
    assert len(mapping['components']) == 3
    
    # Check workflows
    workflows = project.folders['Workflows']
    assert len(workflows) == 1
    
    workflow = workflows[0]
    assert workflow['name'] == "Test_Workflow"
    assert workflow['id'] == "WORKFLOW_1"
    assert len(workflow['tasks']) == 1
    assert len(workflow['links']) == 1

def test_imx_connections_parsing(imx_file):
    """Test IMX connection parsing"""
    parser = InformaticaXMLParser()
    project = parser.parse_project(imx_file)
    
    assert len(project.connections) == 1
    
    conn_name = list(project.connections.keys())[0]
    connection = project.connections[conn_name]
    
    assert connection.name == "TEST_CONN"
    assert connection.connection_type == "HDFS"
    assert connection.host == "localhost"
    assert connection.port == 8020

def test_imx_parameters_parsing(imx_file):
    """Test IMX parameter parsing"""
    parser = InformaticaXMLParser()
    project = parser.parse_project(imx_file)
    
    assert len(project.parameters) == 1
    assert "TEST_PARAM" in project.parameters
    assert project.parameters["TEST_PARAM"] == "test_value"

def test_backward_compatibility_direct_project():
    """Test that direct project format still works"""
    direct_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<project name="DirectProject" version="1.0" xmlns="http://www.informatica.com/BDM/Project/10.2">
  <description>Direct Project</description>
  <folders>
    <folder name="Mappings">
      <mapping name="Direct_Mapping">
        <description>Direct mapping</description>
        <components>
          <source name="DIRECT_SOURCE" type="HDFS"/>
          <target name="DIRECT_TARGET" type="HIVE"/>
        </components>
      </mapping>
    </folder>
  </folders>
  <connections>
    <connection name="DIRECT_CONN" type="HDFS" host="localhost" port="8020"/>
  </connections>
  <parameters>
    <parameter name="DIRECT_PARAM" value="direct_value"/>
  </parameters>
</project>'''
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
        f.write(direct_xml)
        f.flush()
        
        try:
            parser = InformaticaXMLParser()
            project = parser.parse_project(f.name)
            
            assert project.name == "DirectProject"
            assert project.description == "Direct Project"
            assert 'Mappings' in project.folders
            assert len(project.connections) == 1
            assert len(project.parameters) == 1
            
        finally:
            os.unlink(f.name)

def test_imx_namespace_handling():
    """Test namespace handling in IMX parsing"""
    minimal_imx = '''<?xml version="1.0" encoding="UTF-8"?>
<IMX xmlns="http://com.informatica.imx">
    <!-- Minimal IMX without project -->
</IMX>'''
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
        f.write(minimal_imx)
        f.flush()
        
        try:
            parser = InformaticaXMLParser()
            project = parser.parse_project(f.name)
            
            # Should create default project
            assert project.name == "DefaultIMXProject"
            assert project.description == "Generated from IMX file"
            
        finally:
            os.unlink(f.name)

def test_local_name_extraction():
    """Test local name extraction utility"""
    parser = InformaticaXMLParser()
    
    # Test namespaced tag
    assert parser._get_local_name("{http://example.com/ns}Element") == "Element"
    
    # Test non-namespaced tag
    assert parser._get_local_name("Element") == "Element"
    
    # Test namespace extraction
    assert parser._get_namespace("{http://example.com/ns}Element") == "http://example.com/ns"
    assert parser._get_namespace("Element") == ""