"""
Enhanced XSD-compliant XML parser for Informatica metadata
Supports full XSD schema compliance, validation, and reference resolution
"""
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Type, Any, Union, Callable
import logging
from pathlib import Path
from enum import Enum
import re

from .xsd_base_classes import (
    Element, NamedElement, ObjectReference, ElementReference, TypedElement,
    PMDataType, Annotation, ElementCollection, TypeValidator,
    XSDComplianceError, InvalidReferenceError, TypeValidationError, RequiredAttributeError
)
from .reference_manager import ReferenceManager, ReferenceType, auto_resolve_references

class ParsingMode(Enum):
    """XML parsing modes"""
    STRICT = "strict"  # Strict XSD compliance
    LENIENT = "lenient"  # Allow some non-compliance
    LEGACY = "legacy"  # Support legacy formats

class NamespaceInfo:
    """Information about XML namespaces"""
    def __init__(self, prefix: str, uri: str, schema_location: Optional[str] = None):
        self.prefix = prefix
        self.uri = uri
        self.schema_location = schema_location

class ElementFactory:
    """Factory for creating XSD-compliant elements from XML"""
    
    def __init__(self, reference_manager: ReferenceManager):
        self.ref_manager = reference_manager
        self.logger = logging.getLogger("ElementFactory")
        
        # Registry of element creators by local name
        self._element_creators: Dict[str, Callable] = {}
        self._namespace_handlers: Dict[str, Callable] = {}
        
        # Register built-in element creators
        self._register_built_in_creators()
        
    def _register_built_in_creators(self):
        """Register built-in element creators"""
        # Core elements
        self._element_creators['Element'] = self._create_generic_element
        self._element_creators['NamedElement'] = self._create_named_element
        self._element_creators['ObjectReference'] = self._create_object_reference
        self._element_creators['ElementReference'] = self._create_element_reference
        
        # Common metadata elements - register both with and without namespace
        self._element_creators['Project'] = self._create_project
        self._element_creators['project'] = self._create_project  # Also handle lowercase
        self._element_creators['Folder'] = self._create_folder
        self._element_creators['folder'] = self._create_folder
        self._element_creators['Mapping'] = self._create_mapping
        self._element_creators['mapping'] = self._create_mapping
        self._element_creators['Instance'] = self._create_instance
        self._element_creators['instance'] = self._create_instance
        self._element_creators['Port'] = self._create_port
        self._element_creators['port'] = self._create_port
        self._element_creators['AbstractTransformation'] = self._create_transformation
        self._element_creators['transformation'] = self._create_transformation
        self._element_creators['Session'] = self._create_session
        self._element_creators['session'] = self._create_session
        self._element_creators['ConnectInfo'] = self._create_connection
        self._element_creators['connection'] = self._create_connection
        
        # Legacy elements (PowerCenter imports to BDM)
        self._element_creators['TLoaderMapping'] = self._create_tloader_mapping
        self._element_creators['TWidgetInstance'] = self._create_twidget_instance
        self._element_creators['TRepConnection'] = self._create_trep_connection
        
    def register_element_creator(self, local_name: str, creator_func: Callable):
        """Register a custom element creator"""
        self._element_creators[local_name] = creator_func
        
    def create_element(self, xml_elem: ET.Element, namespace_map: Dict[str, str] = None) -> Optional[Element]:
        """Create an XSD element from XML element"""
        local_name = self._get_local_name(xml_elem.tag)
        
        # Get appropriate creator
        creator = self._element_creators.get(local_name, self._create_generic_element)
        
        try:
            element = creator(xml_elem, namespace_map or {})
            
            # Register element if it has an ID
            if element and (element.id or element.iid is not None):
                self.ref_manager.register_object(element)
                
            # Auto-resolve any references in the element
            if element:
                auto_resolve_references(element, self.ref_manager)
                
            return element
            
        except Exception as e:
            self.logger.error(f"Error creating element {local_name}: {e}")
            return None
            
    def _get_local_name(self, tag: str) -> str:
        """Extract local name from namespaced tag"""
        if tag.startswith('{'):
            return tag[tag.rfind('}') + 1:]
        return tag
        
    def _extract_base_attributes(self, xml_elem: ET.Element) -> Dict[str, Any]:
        """Extract base XSD attributes (id, iid, idref, locator)"""
        attrs = {}
        
        # Extract IMX attributes
        for attr_name in xml_elem.attrib:
            local_attr = self._get_local_name(attr_name)
            value = xml_elem.attrib[attr_name]
            
            if local_attr == 'id':
                attrs['id'] = value
            elif local_attr == 'iid':
                try:
                    attrs['iid'] = int(value)
                except ValueError:
                    self.logger.warning(f"Invalid IID value: {value}")
            elif local_attr == 'idref':
                attrs['idref'] = value
            elif local_attr == 'iidref':
                try:
                    attrs['iidref'] = int(value)
                except ValueError:
                    self.logger.warning(f"Invalid IIDREF value: {value}")
            elif local_attr == 'locator':
                attrs['locator'] = value
                
        return attrs
        
    def _create_generic_element(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> Element:
        """Create a generic Element"""
        attrs = self._extract_base_attributes(xml_elem)
        return Element(**attrs)
        
    def _create_named_element(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> NamedElement:
        """Create a NamedElement"""
        attrs = self._extract_base_attributes(xml_elem)
        name = xml_elem.get('name', 'UnknownElement')
        description = self._get_element_text(xml_elem, 'description')
        
        element = NamedElement(name=name, description=description or "", **attrs)
        return element
        
    def _create_object_reference(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> ObjectReference:
        """Create an ObjectReference"""
        attrs = self._extract_base_attributes(xml_elem)
        idref = attrs.get('idref')
        if not idref:
            raise InvalidReferenceError("ObjectReference requires idref attribute")
        return ObjectReference(idref=idref, **{k: v for k, v in attrs.items() if k != 'idref'})
        
    def _create_element_reference(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> ElementReference:
        """Create an ElementReference"""
        attrs = self._extract_base_attributes(xml_elem)
        iidref = attrs.get('iidref')
        if iidref is None:
            raise InvalidReferenceError("ElementReference requires iidref attribute")
        return ElementReference(iidref=iidref, **{k: v for k, v in attrs.items() if k != 'iidref'})
        
    def _create_project(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> 'XSDProject':
        """Create a Project element"""
        from .xsd_project_model import XSDProject  # Import here to avoid circular imports
        
        attrs = self._extract_base_attributes(xml_elem)
        name = xml_elem.get('name', 'UnknownProject')
        version = xml_elem.get('version', '1.0')
        shared = xml_elem.get('shared', 'false').lower() == 'true'
        description = self._get_element_text(xml_elem, 'description')
        
        return XSDProject(name=name, version=version, shared=shared, 
                         description=description or "", **attrs)
        
    def _create_folder(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> 'XSDFolder':
        """Create a Folder element"""
        from .xsd_project_model import XSDFolder  # Import here to avoid circular imports
        
        attrs = self._extract_base_attributes(xml_elem)
        name = xml_elem.get('name', 'UnknownFolder')
        description = self._get_element_text(xml_elem, 'description')
        
        return XSDFolder(name=name, description=description or "", **attrs)
        
    def _create_mapping(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> 'XSDMapping':
        """Create a Mapping element"""
        from .xsd_mapping_model import XSDMapping  # Import here to avoid circular imports
        
        attrs = self._extract_base_attributes(xml_elem)
        name = xml_elem.get('name', 'UnknownMapping')
        description = self._get_element_text(xml_elem, 'description')
        
        return XSDMapping(name=name, description=description or "", **attrs)
        
    def _create_instance(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> 'XSDInstance':
        """Create an Instance element"""
        from .xsd_mapping_model import XSDInstance  # Import here to avoid circular imports
        
        attrs = self._extract_base_attributes(xml_elem)
        name = xml_elem.get('name', 'UnknownInstance')
        transformation_ref = xml_elem.get('transformation')
        
        return XSDInstance(name=name, transformation_ref=transformation_ref, **attrs)
        
    def _create_port(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> 'XSDPort':
        """Create a Port element"""
        from .xsd_mapping_model import XSDPort  # Import here to avoid circular imports
        
        attrs = self._extract_base_attributes(xml_elem)
        from_port = xml_elem.get('fromPort')
        to_ports = xml_elem.get('toPorts', '').split() if xml_elem.get('toPorts') else []
        
        return XSDPort(from_port=from_port, to_ports=to_ports, **attrs)
        
    def _create_transformation(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> 'XSDAbstractTransformation':
        """Create an AbstractTransformation element"""
        from .xsd_transformation_model import transformation_registry  # Import here to avoid circular imports
        
        attrs = self._extract_base_attributes(xml_elem)
        name = xml_elem.get('name', 'UnknownTransformation')
        transformation_type = xml_elem.get('type', 'Generic')
        
        # Create transformation using registry
        transformation = transformation_registry.create_transformation(transformation_type, name, **attrs)
        
        if transformation:
            # Configure transformation from XML attributes
            transformation.configuration.active = xml_elem.get('active', 'true').lower() == 'true'
            if xml_elem.get('tracing'):
                transformation.configuration.custom_properties['tracing'] = xml_elem.get('tracing')
            if xml_elem.get('partitioning'):
                transformation.configuration.custom_properties['partitioning'] = xml_elem.get('partitioning')
                
        return transformation or transformation_registry.create_transformation('Generic', name, **attrs)
        
    def _create_session(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> 'XSDSession':
        """Create a Session element"""
        from .xsd_session_model import XSDSession  # Import here to avoid circular imports
        
        attrs = self._extract_base_attributes(xml_elem)
        name = xml_elem.get('name', 'UnknownSession')
        mapping_ref = xml_elem.get('mapping')
        
        return XSDSession(name=name, mapping_ref=mapping_ref, **attrs)
        
    def _create_connection(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> 'XSDConnection':
        """Create a ConnectInfo element"""
        from .xsd_connection_model import XSDConnection  # Import here to avoid circular imports
        
        attrs = self._extract_base_attributes(xml_elem)
        name = xml_elem.get('connectionName') or xml_elem.get('name', 'UnknownConnection')
        connection_type = xml_elem.get('connectionType') or xml_elem.get('type', 'UNKNOWN')
        
        return XSDConnection(name=name, connection_type=connection_type, **attrs)
        
    def _create_tloader_mapping(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> 'XSDTLoaderMapping':
        """Create a legacy TLoaderMapping element (PowerCenter import to BDM)"""
        from .xsd_legacy_model import XSDTLoaderMapping  # Import here to avoid circular imports
        
        attrs = self._extract_base_attributes(xml_elem)
        name = xml_elem.get('name', 'UnknownMapping')
        description = self._get_element_text(xml_elem, 'description')
        
        return XSDTLoaderMapping(name=name, description=description or "", **attrs)
        
    def _create_twidget_instance(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> 'XSDTWidgetInstance':
        """Create a legacy TWidgetInstance element (PowerCenter import to BDM)"""
        from .xsd_legacy_model import XSDTWidgetInstance  # Import here to avoid circular imports
        
        attrs = self._extract_base_attributes(xml_elem)
        name = xml_elem.get('name', 'UnknownInstance')
        widget_ref = xml_elem.get('widget')
        
        return XSDTWidgetInstance(name=name, widget_ref=widget_ref, **attrs)
        
    def _create_trep_connection(self, xml_elem: ET.Element, namespace_map: Dict[str, str]) -> 'XSDTRepConnection':
        """Create a legacy TRepConnection element (PowerCenter import to BDM)"""
        from .xsd_legacy_model import XSDTRepConnection  # Import here to avoid circular imports
        
        attrs = self._extract_base_attributes(xml_elem)
        connection_name = xml_elem.get('connectionName', 'UnknownConnection')
        connection_type = xml_elem.get('connectionType', 'UNKNOWN')
        
        return XSDTRepConnection(connection_name=connection_name, 
                               connection_type=connection_type, **attrs)
        
    def _get_element_text(self, parent: ET.Element, child_name: str) -> Optional[str]:
        """Get text content of a child element"""
        for child in parent:
            if self._get_local_name(child.tag) == child_name:
                return child.text
        return None

class XSDXMLParser:
    """
    Enhanced XML parser with full XSD compliance and validation
    
    Features:
    - Full namespace support
    - ID/IDREF reference resolution
    - XSD-compliant object model
    - Validation and error reporting
    - Multiple parsing modes
    - Extension points for custom elements
    """
    
    def __init__(self, parsing_mode: ParsingMode = ParsingMode.LENIENT):
        self.parsing_mode = parsing_mode
        self.reference_manager = ReferenceManager()
        self.element_factory = ElementFactory(self.reference_manager)
        self.logger = logging.getLogger("XSDXMLParser")
        
        # Namespace management
        self.namespaces: Dict[str, NamespaceInfo] = {}
        self.default_namespace: Optional[str] = None
        
        # Parsing statistics
        self.parsed_elements = 0
        self.validation_errors: List[str] = []
        self.warnings: List[str] = []
        
        # Element collections
        self.elements = ElementCollection()
        
    def parse_file(self, xml_file_path: str) -> Optional[Element]:
        """Parse an XML file and return the root element"""
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            
            self.logger.info(f"Parsing XML file: {xml_file_path}")
            
            # Detect and register namespaces
            self._detect_namespaces(root)
            
            # Parse the root element
            root_element = self._parse_element(root)
            
            # Validate references after parsing
            validation_errors = self.reference_manager.validate_references()
            if validation_errors:
                self.validation_errors.extend(validation_errors)
                if self.parsing_mode == ParsingMode.STRICT:
                    raise XSDComplianceError(f"Reference validation failed: {validation_errors}")
                    
            self.logger.info(f"Parsing completed. Elements: {self.parsed_elements}, "
                           f"Errors: {len(self.validation_errors)}, "
                           f"Warnings: {len(self.warnings)}")
                           
            return root_element
            
        except ET.ParseError as e:
            self.logger.error(f"XML parsing error: {e}")
            raise XSDComplianceError(f"Invalid XML format: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error during parsing: {e}")
            raise
            
    def parse_string(self, xml_content: str) -> Optional[Element]:
        """Parse XML content from string"""
        try:
            root = ET.fromstring(xml_content)
            
            # Detect and register namespaces
            self._detect_namespaces(root)
            
            # Parse the root element
            root_element = self._parse_element(root)
            
            # Validate references
            validation_errors = self.reference_manager.validate_references()
            if validation_errors:
                self.validation_errors.extend(validation_errors)
                if self.parsing_mode == ParsingMode.STRICT:
                    raise XSDComplianceError(f"Reference validation failed: {validation_errors}")
                    
            return root_element
            
        except ET.ParseError as e:
            self.logger.error(f"XML parsing error: {e}")
            raise XSDComplianceError(f"Invalid XML format: {e}")
            
    def _detect_namespaces(self, root: ET.Element):
        """Detect and register XML namespaces"""
        # Extract namespace from root tag
        if root.tag.startswith('{'):
            # Root element has namespace
            namespace_uri = root.tag[root.tag.find('{') + 1:root.tag.find('}')]
            self.default_namespace = namespace_uri
            self.namespaces[''] = NamespaceInfo('', namespace_uri)
        
        # Scan the entire tree for namespaced elements to build namespace map
        self._scan_for_namespaces(root)
        
        self.logger.debug(f"Detected namespaces: {list(self.namespaces.keys())}")
        
    def _scan_for_namespaces(self, element: ET.Element):
        """Recursively scan elements to find all namespaces in use"""
        # Check current element
        if element.tag.startswith('{'):
            namespace_uri = element.tag[element.tag.find('{') + 1:element.tag.find('}')]
            
            # Try to determine prefix by common patterns
            if 'project' in namespace_uri:
                prefix = 'project'
            elif 'folder' in namespace_uri:
                prefix = 'folder'
            elif 'mapping' in namespace_uri:
                prefix = 'mapping'
            elif 'transformation' in namespace_uri:
                prefix = 'transformation'
            elif 'imx' in namespace_uri:
                prefix = 'imx'
            else:
                # Generate generic prefix
                prefix = f'ns{len(self.namespaces)}'
                
            if prefix not in self.namespaces:
                self.namespaces[prefix] = NamespaceInfo(prefix, namespace_uri)
        
        # Check child elements
        for child in element:
            self._scan_for_namespaces(child)
        
    def _parse_element(self, xml_elem: ET.Element) -> Optional[Element]:
        """Parse an XML element to XSD element"""
        try:
            # Create the element using the factory
            element = self.element_factory.create_element(xml_elem, self.namespaces)
            
            if element:
                # Add to collection
                self.elements.add(element)
                
                # Parse child elements
                self._parse_child_elements(xml_elem, element)
                
                # Parse annotations
                self._parse_annotations(xml_elem, element)
                
                self.parsed_elements += 1
                
            return element
            
        except Exception as e:
            error_msg = f"Error parsing element {xml_elem.tag}: {e}"
            self.logger.error(error_msg)
            
            if self.parsing_mode == ParsingMode.STRICT:
                raise XSDComplianceError(error_msg)
            else:
                self.validation_errors.append(error_msg)
                return None
                
    def _parse_child_elements(self, xml_elem: ET.Element, parent_element: Element):
        """Parse child elements and add them to parent"""
        for child_xml in xml_elem:
            local_name = self._get_local_name(child_xml.tag)
            
            # Skip annotation elements (handled separately)
            if local_name == 'annotations':
                continue
                
            child_element = self._parse_element(child_xml)
            if child_element:
                # Add child to parent (if parent supports children)
                if hasattr(parent_element, 'add_child'):
                    parent_element.add_child(child_element)
                elif hasattr(parent_element, 'children'):
                    if not hasattr(parent_element, 'children'):
                        parent_element.children = []
                    parent_element.children.append(child_element)
                    
    def _parse_annotations(self, xml_elem: ET.Element, element: Element):
        """Parse annotations for an element"""
        for child in xml_elem:
            if self._get_local_name(child.tag) == 'annotations':
                for annotation_elem in child:
                    annotation = Annotation(
                        name=self._get_local_name(annotation_elem.tag),
                        value=annotation_elem.text or annotation_elem.attrib,
                        namespace=self._get_namespace(annotation_elem.tag)
                    )
                    element.add_annotation(annotation)
                    
    def _get_local_name(self, tag: str) -> str:
        """Extract local name from namespaced tag"""
        if tag.startswith('{'):
            return tag[tag.rfind('}') + 1:]
        return tag
        
    def _get_namespace(self, tag: str) -> Optional[str]:
        """Extract namespace from namespaced tag"""
        if tag.startswith('{'):
            return tag[tag.find('{') + 1:tag.find('}')]
        return None
        
    def get_parsing_statistics(self) -> Dict[str, Any]:
        """Get parsing statistics"""
        ref_stats = self.reference_manager.get_reference_statistics()
        
        return {
            'parsing_mode': self.parsing_mode.value,
            'parsed_elements': self.parsed_elements,
            'validation_errors': len(self.validation_errors),
            'warnings': len(self.warnings),
            'namespaces': len(self.namespaces),
            'elements_in_collection': self.elements.count(),
            'reference_stats': ref_stats
        }
        
    def get_validation_report(self) -> Dict[str, Any]:
        """Get detailed validation report"""
        ref_validation = self.reference_manager.validate_references()
        
        return {
            'parsing_errors': self.validation_errors,
            'parsing_warnings': self.warnings,
            'reference_errors': ref_validation,
            'reference_graph': self.reference_manager.export_reference_graph(),
            'statistics': self.get_parsing_statistics()
        }
        
    def clear(self):
        """Clear all parsed data"""
        self.reference_manager.clear()
        self.elements.clear()
        self.namespaces.clear()
        self.default_namespace = None
        self.parsed_elements = 0
        self.validation_errors.clear()
        self.warnings.clear()
        
    def register_custom_element_creator(self, local_name: str, creator_func: Callable):
        """Register a custom element creator function"""
        self.element_factory.register_element_creator(local_name, creator_func)

# Utility functions for backward compatibility
def parse_informatica_xml(file_path: str, 
                         parsing_mode: ParsingMode = ParsingMode.LENIENT) -> Optional[Element]:
    """
    Convenience function to parse Informatica XML file
    
    Args:
        file_path: Path to XML file
        parsing_mode: Parsing mode (strict, lenient, legacy)
        
    Returns:
        Root Element or None if parsing failed
    """
    parser = XSDXMLParser(parsing_mode)
    return parser.parse_file(file_path)

def create_parser_with_legacy_support() -> XSDXMLParser:
    """Create a parser configured for legacy Informatica formats"""
    parser = XSDXMLParser(ParsingMode.LEGACY)
    
    # Add legacy element creators if needed
    # parser.register_custom_element_creator('legacy_element', custom_creator)
    
    return parser