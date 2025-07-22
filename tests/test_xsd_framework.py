"""
Comprehensive tests for XSD-compliant framework
Tests the new object model, reference management, and enhanced parser
"""
import pytest
import tempfile
import os
from typing import List

from src.core.xsd_base_classes import (
    Element, NamedElement, ObjectReference, ElementReference, TypedElement,
    PMDataType, Annotation, ElementCollection, TypeValidator,
    XSDComplianceError, InvalidReferenceError
)
from src.core.reference_manager import ReferenceManager, ReferenceType, PendingReference
from src.core.xsd_xml_parser import XSDXMLParser, ParsingMode, ElementFactory
from src.core.xsd_project_model import XSDProject, XSDFolder

class TestXSDBaseClasses:
    """Test XSD base classes functionality"""
    
    def test_element_creation(self):
        """Test basic Element creation"""
        element = Element(id="test_id", iid=123)
        assert element.id == "test_id"
        assert element.iid == 123
        assert len(element.annotations) == 0
        
    def test_named_element_creation(self):
        """Test NamedElement creation"""
        element = NamedElement(name="TestElement", description="Test description", id="named_1")
        assert element.name == "TestElement"
        assert element.description == "Test description"
        assert element.id == "named_1"
        
    def test_object_reference_creation(self):
        """Test ObjectReference creation"""
        ref = ObjectReference(idref="target_id", id="ref_1")
        assert ref.idref == "target_id"
        assert ref.id == "ref_1"
        assert not ref.is_resolved()
        
    def test_element_reference_creation(self):
        """Test ElementReference creation"""
        ref = ElementReference(iidref=456, id="elem_ref_1")
        assert ref.iidref == 456
        assert ref.id == "elem_ref_1"
        assert not ref.is_resolved()
        
    def test_typed_element_creation(self):
        """Test TypedElement creation with data types"""
        typed_elem = TypedElement(
            data_type=PMDataType.SQL_VARCHAR,
            length=255,
            nullable=True,
            id="typed_1"
        )
        assert typed_elem.data_type == PMDataType.SQL_VARCHAR
        assert typed_elem.length == 255
        assert typed_elem.nullable is True
        assert typed_elem.is_string_type()
        assert not typed_elem.is_numeric_type()
        
    def test_annotation_management(self):
        """Test annotation add/get/remove functionality"""
        element = Element(id="annotated_elem")
        
        # Add annotation
        annotation = Annotation(name="test_annotation", value="test_value")
        element.add_annotation(annotation)
        assert len(element.annotations) == 1
        
        # Get annotation
        retrieved = element.get_annotation("test_annotation")
        assert retrieved is not None
        assert retrieved.value == "test_value"
        
        # Remove annotation
        removed = element.remove_annotation("test_annotation")
        assert removed is True
        assert len(element.annotations) == 0
        
    def test_type_validation(self):
        """Test data type validation"""
        # Valid integer
        assert TypeValidator.validate_data_type("123", PMDataType.SQL_INTEGER) is True
        assert TypeValidator.validate_data_type(123, PMDataType.SQL_INTEGER) is True
        
        # Invalid integer
        assert TypeValidator.validate_data_type("abc", PMDataType.SQL_INTEGER) is False
        
        # Valid string
        assert TypeValidator.validate_data_type("test", PMDataType.SQL_VARCHAR) is True
        
        # Valid boolean
        assert TypeValidator.validate_data_type(True, PMDataType.SQL_BOOLEAN) is True
        assert TypeValidator.validate_data_type("true", PMDataType.SQL_BOOLEAN) is True
        
    def test_type_conversion(self):
        """Test data type conversion"""
        # Convert to integer
        result = TypeValidator.convert_value("123", PMDataType.SQL_INTEGER)
        assert result == 123
        assert isinstance(result, int)
        
        # Convert to float
        result = TypeValidator.convert_value("123.45", PMDataType.SQL_DECIMAL)
        assert result == 123.45
        assert isinstance(result, float)
        
        # Convert to string
        result = TypeValidator.convert_value(123, PMDataType.SQL_VARCHAR)
        assert result == "123"
        assert isinstance(result, str)

class TestElementCollection:
    """Test ElementCollection functionality"""
    
    def test_collection_operations(self):
        """Test basic collection operations"""
        collection = ElementCollection()
        
        # Add elements
        elem1 = NamedElement(name="Element1", id="elem_1")
        elem2 = NamedElement(name="Element2", id="elem_2")
        
        collection.add(elem1)
        collection.add(elem2)
        
        assert collection.count() == 2
        
        # Get by ID
        retrieved = collection.get_by_id("elem_1")
        assert retrieved is elem1
        
        # Get by name
        retrieved = collection.get_by_name("Element2")
        assert retrieved is elem2
        
        # Get by type
        named_elements = collection.list_by_type(NamedElement)
        assert len(named_elements) == 2
        
        # Remove element
        collection.remove(elem1)
        assert collection.count() == 1
        assert collection.get_by_id("elem_1") is None

class TestReferenceManager:
    """Test reference management system"""
    
    def test_object_registration(self):
        """Test object registration with IDs"""
        ref_manager = ReferenceManager()
        
        # Register object with ID
        element = NamedElement(name="TestElement", id="test_id")
        success = ref_manager.register_object(element)
        assert success is True
        
        # Resolve reference
        resolved = ref_manager.resolve_id_reference("test_id")
        assert resolved is element
        
        # Test IID registration
        iid_element = Element(iid=123)
        success = ref_manager.register_object(iid_element)
        assert success is True
        
        resolved = ref_manager.resolve_iid_reference(123)
        assert resolved is iid_element
        
    def test_pending_references(self):
        """Test pending reference resolution"""
        ref_manager = ReferenceManager()
        
        # Create referring object
        referring_obj = ObjectReference(idref="target_id", id="ref_1")
        
        # Add pending reference
        pending = ref_manager.add_pending_reference(
            referring_obj, "resolved_object", "target_id", ReferenceType.ID_REF
        )
        assert pending is True  # Should be pending
        
        # Register target object
        target_obj = NamedElement(name="Target", id="target_id")
        ref_manager.register_object(target_obj)
        
        # Reference should now be resolved
        assert referring_obj.resolved_object is target_obj
        
    def test_reference_validation(self):
        """Test reference validation"""
        ref_manager = ReferenceManager()
        
        # Add unresolved reference
        referring_obj = ObjectReference(idref="missing_id", id="ref_1")
        ref_manager.add_pending_reference(
            referring_obj, "resolved_object", "missing_id", ReferenceType.ID_REF
        )
        
        # Validate should find errors
        errors = ref_manager.validate_references()
        assert len(errors) > 0
        assert "unresolved" in errors[0].lower()
        
    def test_circular_reference_detection(self):
        """Test circular reference detection"""
        ref_manager = ReferenceManager()
        
        # Create circular reference: A -> B -> A
        elem_a = NamedElement(name="ElementA", id="elem_a")
        elem_b = NamedElement(name="ElementB", id="elem_b")
        
        # Add references before registering objects to create the circular dependency
        ref_manager.add_pending_reference(elem_a, "ref_to_b", "elem_b", ReferenceType.ID_REF)
        ref_manager.add_pending_reference(elem_b, "ref_to_a", "elem_a", ReferenceType.ID_REF)
        
        # Now register objects (references will be resolved but graph will be built)
        ref_manager.register_object(elem_a)
        ref_manager.register_object(elem_b)
        
        # Validation should detect circular reference
        errors = ref_manager.validate_references()
        circular_found = any("circular" in error.lower() for error in errors)
        assert circular_found
        
    def test_reference_statistics(self):
        """Test reference statistics"""
        ref_manager = ReferenceManager()
        
        # Register some objects
        elem1 = NamedElement(name="Element1", id="elem_1")
        elem2 = NamedElement(name="Element2", id="elem_2")
        
        ref_manager.register_object(elem1)
        ref_manager.register_object(elem2)
        
        stats = ref_manager.get_reference_statistics()
        assert stats['registered_objects'] == 2
        assert stats['total_registrations'] == 2

class TestXSDXMLParser:
    """Test XSD XML parser functionality"""
    
    def test_parser_creation(self):
        """Test parser creation with different modes"""
        parser = XSDXMLParser(ParsingMode.STRICT)
        assert parser.parsing_mode == ParsingMode.STRICT
        assert isinstance(parser.reference_manager, ReferenceManager)
        assert isinstance(parser.element_factory, ElementFactory)
        
    def test_simple_xml_parsing(self):
        """Test parsing simple XML"""
        xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
        <project name="TestProject" id="proj_1">
            <description>Test project description</description>
        </project>'''
        
        parser = XSDXMLParser(ParsingMode.LENIENT)
        root_element = parser.parse_string(xml_content)
        
        assert root_element is not None
        assert hasattr(root_element, 'name')
        
    def test_namespace_detection(self):
        """Test namespace detection"""
        xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
        <IMX xmlns="http://com.informatica.imx"
             xmlns:project="http://com.informatica.metadata.common.project/2">
            <project:Project name="TestProject" id="proj_1">
                <description>Test</description>
            </project:Project>
        </IMX>'''
        
        parser = XSDXMLParser(ParsingMode.LENIENT)
        root_element = parser.parse_string(xml_content)
        
        assert root_element is not None
        assert len(parser.namespaces) >= 2  # Default namespace + project namespace
        
    def test_reference_resolution_in_parsing(self):
        """Test that references are resolved during parsing"""
        xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
        <project name="TestProject" id="proj_1">
            <transformation name="TestTransform" id="trans_1"/>
            <instance name="TestInstance" transformation="trans_1" id="inst_1"/>
        </project>'''
        
        parser = XSDXMLParser(ParsingMode.LENIENT)
        root_element = parser.parse_string(xml_content)
        
        # Check that references were processed
        stats = parser.get_parsing_statistics()
        assert stats['parsed_elements'] > 0
        
    def test_validation_errors(self):
        """Test validation error handling"""
        # Invalid XML
        xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
        <project name="TestProject">
            <unclosed_tag>
        </project>'''
        
        parser = XSDXMLParser(ParsingMode.STRICT)
        
        with pytest.raises(XSDComplianceError):
            parser.parse_string(xml_content)
            
    def test_parsing_statistics(self):
        """Test parsing statistics collection"""
        xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
        <project name="TestProject" id="proj_1">
            <folder name="TestFolder" id="folder_1"/>
            <transformation name="TestTransform" id="trans_1"/>
        </project>'''
        
        parser = XSDXMLParser(ParsingMode.LENIENT)
        parser.parse_string(xml_content)
        
        stats = parser.get_parsing_statistics()
        assert 'parsed_elements' in stats
        assert 'validation_errors' in stats
        assert 'reference_stats' in stats
        
    def test_validation_report(self):
        """Test comprehensive validation report"""
        xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
        <project name="TestProject" id="proj_1">
            <instance transformation="missing_transform" id="inst_1"/>
        </project>'''
        
        parser = XSDXMLParser(ParsingMode.LENIENT)
        parser.parse_string(xml_content)
        
        report = parser.get_validation_report()
        assert 'parsing_errors' in report
        assert 'reference_errors' in report
        assert 'statistics' in report

class TestXSDProjectModel:
    """Test XSD project model classes"""
    
    def test_project_creation(self):
        """Test project creation and basic operations"""
        project = XSDProject(
            name="TestProject",
            version="2.0",
            shared=True,
            description="Test project"
        )
        
        assert project.name == "TestProject"
        assert project.version == "2.0"
        assert project.shared is True
        assert project.description == "Test project"
        
    def test_folder_operations(self):
        """Test folder creation and management"""
        project = XSDProject(name="TestProject")
        
        # Create folder
        folder = project.create_folder("TestFolder", "Test folder description")
        assert folder.name == "TestFolder"
        assert project.get_folder("TestFolder") is folder
        
    def test_parameter_management(self):
        """Test project parameter management"""
        project = XSDProject(name="TestProject")
        
        # Add parameters
        project.add_parameter("PARAM1", "value1")
        project.add_parameter("PARAM2", "value2")
        
        assert project.get_parameter("PARAM1") == "value1"
        assert project.get_parameter("PARAM2") == "value2"
        assert project.get_parameter("PARAM3", "default") == "default"
        
    def test_project_summary(self):
        """Test project summary export"""
        project = XSDProject(name="TestProject", version="1.0")
        project.create_folder("Mappings")
        project.create_folder("Sessions")
        project.add_parameter("ENV", "TEST")
        
        summary = project.export_summary()
        assert summary['name'] == "TestProject"
        assert summary['version'] == "1.0"
        assert summary['folders_count'] == 2
        assert summary['parameters_count'] == 1

class TestXSDFolderModel:
    """Test XSD folder model"""
    
    def test_folder_creation(self):
        """Test folder creation"""
        folder = XSDFolder(name="TestFolder", description="Test description")
        assert folder.name == "TestFolder"
        assert folder.description == "Test description"
        assert folder.is_empty()
        
    def test_folder_contents(self):
        """Test folder contents management"""
        folder = XSDFolder(name="Mappings")
        
        # Add mock elements
        mapping1 = NamedElement(name="Mapping1", id="map_1")
        mapping1.__class__.__name__ = "XSDMapping"  # Mock class name
        
        folder.add_to_contents(mapping1)
        assert not folder.is_empty()
        assert len(folder.mappings) == 1
        
        # Get by name
        retrieved = folder.get_element_by_name("Mapping1")
        assert retrieved is mapping1
        
    def test_folder_summary(self):
        """Test folder summary"""
        folder = XSDFolder(name="TestFolder")
        
        # Add mock content
        elem = NamedElement(name="TestElement", id="elem_1")
        folder.add_to_contents(elem)
        
        summary = folder.get_folder_summary()
        assert summary['name'] == "TestFolder"
        assert summary['total_contents'] == 1

# Integration test that combines multiple components
class TestXSDFrameworkIntegration:
    """Integration tests for the complete XSD framework"""
    
    def test_end_to_end_parsing(self):
        """Test complete parsing workflow with all components"""
        xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
        <IMX xmlns="http://com.informatica.imx"
             xmlns:project="http://com.informatica.metadata.common.project/2"
             xmlns:folder="http://com.informatica.metadata.common.folder/2">
             
            <project:Project name="IntegrationTest" id="proj_1" shared="false">
                <description>Integration test project</description>
                <contents>
                    <folder:Folder name="Mappings" id="folder_1">
                        <description>Mappings folder</description>
                        <contents>
                            <!-- Folder contents would go here -->
                        </contents>
                    </folder:Folder>
                </contents>
            </project:Project>
            
        </IMX>'''
        
        # Parse with XSD parser
        parser = XSDXMLParser(ParsingMode.LENIENT)
        root_element = parser.parse_string(xml_content)
        
        # Verify parsing
        assert root_element is not None
        
        # Check statistics
        stats = parser.get_parsing_statistics()
        assert stats['parsed_elements'] > 0
        assert len(parser.validation_errors) == 0
        
        # Check reference resolution
        ref_stats = parser.reference_manager.get_reference_statistics()
        assert ref_stats['registered_objects'] > 0
        
        # Validate references
        validation_errors = parser.reference_manager.validate_references()
        # Should have no unresolved references for this simple case
        unresolved_errors = [e for e in validation_errors if 'unresolved' in e.lower()]
        assert len(unresolved_errors) == 0

@pytest.fixture
def sample_xsd_xml():
    """Sample XSD-compliant XML for testing"""
    return '''<?xml version="1.0" encoding="UTF-8"?>
    <IMX xmlns="http://com.informatica.imx"
         xmlns:project="http://com.informatica.metadata.common.project/2"
         xmlns:folder="http://com.informatica.metadata.common.folder/2">
         
        <project:Project name="SampleProject" id="proj_1" shared="false">
            <description>Sample XSD-compliant project</description>
            <contents>
                <folder:Folder name="Mappings" id="folder_1">
                    <contents>
                        <!-- Mapping content -->
                    </contents>
                </folder:Folder>
            </contents>
        </project:Project>
        
    </IMX>'''

def test_parsing_with_fixture(sample_xsd_xml):
    """Test parsing using the sample XML fixture"""
    parser = XSDXMLParser(ParsingMode.LENIENT)
    root_element = parser.parse_string(sample_xsd_xml)
    
    assert root_element is not None
    
    # Verify no critical errors
    report = parser.get_validation_report()
    critical_errors = [e for e in report['parsing_errors'] if 'error' in e.lower()]
    assert len(critical_errors) == 0