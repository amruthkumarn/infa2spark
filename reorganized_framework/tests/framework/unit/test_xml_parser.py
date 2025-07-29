"""
Unit tests for XML Parser component of the framework.
Tests the framework's ability to parse Informatica XML files.
"""
import pytest
from pathlib import Path
from unittest.mock import Mock, patch
import xml.etree.ElementTree as ET

# Import framework components
import sys
sys.path.append(str(Path(__file__).parent.parent.parent.parent / "src"))

from informatica_spark.core.parsers.xml_parser import InformaticaXMLParser
from informatica_spark.core.models.base_classes import Project, Connection, Task


class TestXMLParserUnit:
    """Unit tests for InformaticaXMLParser."""
    
    @pytest.fixture
    def parser(self):
        """Create parser instance for testing."""
        return InformaticaXMLParser()
    
    @pytest.fixture
    def simple_xml_content(self):
        """Simple XML content for basic parsing tests."""
        return '''<?xml version="1.0" encoding="UTF-8"?>
<REPO:Project xmlns:REPO="http://www.informatica.com/repository">
    <REPO:IObject projectName="TestProject">
        <REPO:IObject folderName="TestFolder" folderType="MAPPING">
            <REPO:IObject mappingName="SimpleMapping">
                <REPO:IObject sourceName="TestSource" sourceType="SourceDefinition"/>
                <REPO:IObject targetName="TestTarget" targetType="TargetDefinition"/>
            </REPO:IObject>
        </REPO:IObject>
    </REPO:IObject>
</REPO:Project>'''
    
    @pytest.fixture
    def complex_xml_content(self):
        """Complex XML with transformations for advanced parsing tests."""
        return '''<?xml version="1.0" encoding="UTF-8"?>
<REPO:Project xmlns:REPO="http://www.informatica.com/repository" 
               xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <REPO:IObject xsi:type="REPO:PowerMartProject" projectName="ComplexProject">
        <REPO:IObject xsi:type="REPO:Folder" folderName="TransformationFolder" folderType="MAPPING">
            <REPO:IObject xsi:type="REPO:Mapping" mappingName="ComplexMapping">
                <REPO:IObject xsi:type="REPO:SourceDefinition" sourceName="CustomerSource">
                    <REPO:IObject xsi:type="REPO:SourceField" fieldName="customer_id" dataType="integer"/>
                    <REPO:IObject xsi:type="REPO:SourceField" fieldName="customer_name" dataType="string"/>
                </REPO:IObject>
                <REPO:IObject xsi:type="REPO:Transformation" transformationName="CustomerTransformation" 
                             transformationType="Expression">
                    <REPO:IObject xsi:type="REPO:TransformField" fieldName="customer_id" expression="customer_id"/>
                    <REPO:IObject xsi:type="REPO:TransformField" fieldName="customer_name_upper" 
                                 expression="UPPER(customer_name)"/>
                </REPO:IObject>
                <REPO:IObject xsi:type="REPO:TargetDefinition" targetName="CustomerTarget">
                    <REPO:IObject xsi:type="REPO:TargetField" fieldName="customer_id" dataType="integer"/>
                    <REPO:IObject xsi:type="REPO:TargetField" fieldName="customer_name_upper" dataType="string"/>
                </REPO:IObject>
            </REPO:IObject>
        </REPO:IObject>
    </REPO:IObject>
</REPO:Project>'''

    def test_parser_initialization(self, parser):
        """Test parser initializes correctly."""
        assert parser is not None
        assert hasattr(parser, 'parse_xml_file')
        assert hasattr(parser, 'parse_xml_content')
    
    def test_simple_xml_parsing(self, parser, simple_xml_content, temp_directory):
        """Test parsing of simple XML content."""
        # Write XML to temporary file
        xml_file = temp_directory / "simple_test.xml"
        xml_file.write_text(simple_xml_content)
        
        # Parse XML
        project = parser.parse_xml_file(str(xml_file))
        
        # Assertions
        assert project is not None
        assert project.name == "TestProject"
        assert len(project.folders) > 0
        
        # Check folder parsing
        folder = project.folders["MAPPING"]["TestFolder"]
        assert folder is not None
        assert len(folder) > 0
        
    def test_complex_xml_parsing(self, parser, complex_xml_content, temp_directory):
        """Test parsing of complex XML with transformations."""
        # Write XML to temporary file
        xml_file = temp_directory / "complex_test.xml"
        xml_file.write_text(complex_xml_content)
        
        # Parse XML
        project = parser.parse_xml_file(str(xml_file))
        
        # Assertions
        assert project is not None
        assert project.name == "ComplexProject"
        
        # Check mappings were parsed
        assert len(project.mappings) > 0
        mapping = project.mappings["ComplexMapping"]
        assert mapping.name == "ComplexMapping"
        
        # Check components were extracted
        assert len(mapping.components) >= 3  # Source, Transformation, Target
        
    def test_invalid_xml_handling(self, parser, temp_directory):
        """Test handling of invalid XML files."""
        # Create invalid XML file
        invalid_xml = temp_directory / "invalid.xml"
        invalid_xml.write_text("This is not valid XML content")
        
        # Test that parser handles invalid XML gracefully
        with pytest.raises(Exception):  # Should raise XML parsing exception
            parser.parse_xml_file(str(invalid_xml))
    
    def test_missing_file_handling(self, parser):
        """Test handling of missing XML files."""
        with pytest.raises(FileNotFoundError):
            parser.parse_xml_file("nonexistent_file.xml")
    
    def test_namespace_handling(self, parser, complex_xml_content):
        """Test proper handling of XML namespaces."""
        project = parser.parse_xml_content(complex_xml_content)
        
        # Verify namespace-aware parsing worked
        assert project is not None
        assert project.name == "ComplexProject"
        
    @pytest.mark.parametrize("xml_type,expected_project_name", [
        ("simple", "TestProject"),
        ("complex", "ComplexProject")
    ])
    def test_different_xml_formats(self, parser, xml_type, expected_project_name, 
                                   simple_xml_content, complex_xml_content):
        """Test parsing different XML format variations."""
        xml_content = simple_xml_content if xml_type == "simple" else complex_xml_content
        
        project = parser.parse_xml_content(xml_content)
        assert project.name == expected_project_name
    
    def test_connection_parsing(self, parser):
        """Test parsing of connection definitions."""
        xml_with_connections = '''<?xml version="1.0" encoding="UTF-8"?>
<REPO:Project xmlns:REPO="http://www.informatica.com/repository">
    <REPO:IObject projectName="ConnectionTest">
        <REPO:IObject connectionName="TestConnection" connectionType="ODBC">
            <REPO:IObject attributeName="ConnectionString" attributeValue="DSN=TestDB"/>
            <REPO:IObject attributeName="Username" attributeValue="testuser"/>
        </REPO:IObject>
    </REPO:IObject>
</REPO:Project>'''
        
        project = parser.parse_xml_content(xml_with_connections)
        
        # Check connections were parsed
        assert len(project.connections) > 0
        connection = project.connections.get("TestConnection")
        assert connection is not None
        assert connection.name == "TestConnection"


@pytest.mark.integration
class TestXMLParserIntegration:
    """Integration tests for XML Parser with other framework components."""
    
    def test_parser_with_code_generator_integration(self, parser, simple_xml_content):
        """Test that parsed XML can be used by code generator."""
        project = parser.parse_xml_content(simple_xml_content)
        
        # Verify project structure is suitable for code generation
        assert project.name is not None
        assert len(project.folders) > 0
        assert isinstance(project.folders, dict)
        
    def test_real_informatica_xml_samples(self, parser):
        """Test parsing with real Informatica XML samples if available."""
        # This would test with actual Informatica export files
        # For now, we'll skip if no real samples are available
        pytest.skip("Real Informatica XML samples not available for testing")


if __name__ == "__main__":
    # Run tests if executed directly
    pytest.main([__file__, "-v"])