"""
Tests for XML parser functionality
"""
import pytest
import tempfile
import os
from src.core.xml_parser import InformaticaXMLParser

@pytest.fixture
def sample_xml_content():
    """Sample XML content for testing"""
    return '''<?xml version="1.0" encoding="UTF-8"?>
<project name="TestProject" version="1.0" xmlns="http://www.informatica.com/BDM/Project/10.2">
  <description>Test Project</description>
  <folders>
    <folder name="Mappings">
      <mapping name="Test_Mapping">
        <description>Test mapping</description>
        <components>
          <source name="TEST_SOURCE" type="HDFS" format="PARQUET"/>
          <transformation name="TEST_FILTER" type="Expression"/>
          <target name="TEST_TARGET" type="HIVE"/>
        </components>
      </mapping>
    </folder>
  </folders>
  <connections>
    <connection name="TEST_CONN" type="HDFS" host="localhost" port="8020"/>
  </connections>
  <parameters>
    <parameter name="TEST_PARAM" value="test_value"/>
  </parameters>
</project>'''

@pytest.fixture
def xml_file(sample_xml_content):
    """Create temporary XML file for testing"""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
        f.write(sample_xml_content)
        f.flush()
        yield f.name
    os.unlink(f.name)

def test_parse_project_basic(xml_file):
    """Test basic project parsing"""
    parser = InformaticaXMLParser()
    project = parser.parse_project(xml_file)
    
    assert project.name == "TestProject"
    assert project.version == "1.0"
    assert project.description == "Test Project"

def test_parse_mappings(xml_file):
    """Test mapping parsing"""
    parser = InformaticaXMLParser()
    project = parser.parse_project(xml_file)
    
    assert 'Mappings' in project.folders
    mappings = project.folders['Mappings']
    assert len(mappings) == 1
    
    mapping = mappings[0]
    assert mapping['name'] == "Test_Mapping"
    assert mapping['description'] == "Test mapping"
    assert len(mapping['components']) == 3

def test_parse_connections(xml_file):
    """Test connection parsing"""
    parser = InformaticaXMLParser()
    project = parser.parse_project(xml_file)
    
    assert len(project.connections) == 1
    conn = project.connections['TEST_CONN']
    assert conn.name == "TEST_CONN"
    assert conn.connection_type == "HDFS"
    assert conn.host == "localhost"
    assert conn.port == 8020

def test_parse_parameters(xml_file):
    """Test parameter parsing"""
    parser = InformaticaXMLParser()
    project = parser.parse_project(xml_file)
    
    assert len(project.parameters) == 1
    assert project.parameters['TEST_PARAM'] == "test_value"