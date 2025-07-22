"""
XSD-compliant project and folder model classes
Based on com.informatica.metadata.common.project.xsd and com.informatica.metadata.common.folder.xsd
"""
from typing import List, Dict, Optional, Any
from dataclasses import dataclass, field

from .xsd_base_classes import NamedElement, ElementCollection

class XSDProject(NamedElement):
    """
    XSD-compliant Project class
    Based on com.informatica.metadata.common.project.Project
    """
    
    def __init__(self, 
                 name: str,
                 version: str = "1.0",
                 shared: bool = False,
                 description: str = "",
                 **kwargs):
        super().__init__(name=name, description=description, **kwargs)
        self.version = version
        self.shared = shared  # Project shared flag from XSD
        
        # Project contents - collection of all first-class objects
        self.contents = ElementCollection()
        
        # Organized collections for easy access
        self.folders: Dict[str, 'XSDFolder'] = {}
        self.mappings: Dict[str, Any] = {}  # Will be XSDMapping objects
        self.sessions: Dict[str, Any] = {}  # Will be XSDSession objects
        self.connections: Dict[str, Any] = {}  # Will be XSDConnection objects
        self.transformations: Dict[str, Any] = {}  # Will be XSDAbstractTransformation objects
        
        # Project-level parameters and outputs
        self.parameters: Dict[str, str] = {}
        self.outputs: Dict[str, Any] = {}
        
        # Project metadata
        self.creation_date: Optional[str] = None
        self.modified_date: Optional[str] = None
        self.created_by: Optional[str] = None
        self.modified_by: Optional[str] = None
        
    def add_to_contents(self, element):
        """Add an element to project contents"""
        self.contents.add(element)
        
        # Also add to organized collections
        if isinstance(element, XSDFolder):
            self.folders[element.name] = element
        # Add other types as they are implemented
        
    def get_folder(self, name: str) -> Optional['XSDFolder']:
        """Get folder by name"""
        return self.folders.get(name)
        
    def create_folder(self, name: str, description: str = "") -> 'XSDFolder':
        """Create a new folder in the project"""
        folder = XSDFolder(name=name, description=description)
        self.add_to_contents(folder)
        return folder
        
    def add_parameter(self, name: str, value: str):
        """Add a project parameter"""
        self.parameters[name] = value
        
    def get_parameter(self, name: str, default: str = None) -> Optional[str]:
        """Get project parameter value"""
        return self.parameters.get(name, default)
        
    def get_all_elements_by_type(self, element_type: type) -> List:
        """Get all elements of a specific type from project contents"""
        return self.contents.list_by_type(element_type)
        
    def export_summary(self) -> Dict[str, Any]:
        """Export project summary for reporting"""
        return {
            'name': self.name,
            'version': self.version,
            'shared': self.shared,
            'description': self.description,
            'folders_count': len(self.folders),
            'mappings_count': len(self.mappings),
            'sessions_count': len(self.sessions),
            'connections_count': len(self.connections),
            'transformations_count': len(self.transformations),
            'parameters_count': len(self.parameters),
            'total_contents': self.contents.count(),
            'created_by': self.created_by,
            'creation_date': self.creation_date
        }

class XSDFolder(NamedElement):
    """
    XSD-compliant Folder class
    Based on com.informatica.metadata.common.folder.Folder
    """
    
    def __init__(self, 
                 name: str,
                 description: str = "",
                 **kwargs):
        super().__init__(name=name, description=description, **kwargs)
        
        # Folder contents - collection of objects in this folder
        self.contents = ElementCollection()
        
        # Organized collections by type for easy access
        self.mappings: List[Any] = []  # XSDMapping objects
        self.sessions: List[Any] = []  # XSDSession objects  
        self.workflows: List[Any] = []  # XSDWorkflow objects
        self.applications: List[Any] = []  # XSDApplication objects
        self.sources: List[Any] = []  # Source objects
        self.targets: List[Any] = []  # Target objects
        self.transformations: List[Any] = []  # Transformation objects
        
        # Folder metadata
        self.folder_type: Optional[str] = None  # e.g., "Mappings", "Sessions", etc.
        self.permissions: Dict[str, Any] = {}
        
    def add_to_contents(self, element):
        """Add an element to folder contents"""
        self.contents.add(element)
        
        # Also add to type-specific collections
        element_type_name = element.__class__.__name__
        
        if 'Mapping' in element_type_name:
            self.mappings.append(element)
        elif 'Session' in element_type_name:
            self.sessions.append(element)
        elif 'Workflow' in element_type_name:
            self.workflows.append(element)
        elif 'Application' in element_type_name:
            self.applications.append(element)
        elif 'Source' in element_type_name:
            self.sources.append(element)
        elif 'Target' in element_type_name:
            self.targets.append(element)
        elif 'Transformation' in element_type_name:
            self.transformations.append(element)
            
    def get_contents_by_type(self, element_type: type) -> List:
        """Get folder contents by element type"""
        return self.contents.list_by_type(element_type)
        
    def get_element_by_name(self, name: str):
        """Get element from folder by name"""
        return self.contents.get_by_name(name)
        
    def remove_element(self, element):
        """Remove element from folder"""
        self.contents.remove(element)
        
        # Also remove from type-specific collections
        element_type_name = element.__class__.__name__
        
        if 'Mapping' in element_type_name and element in self.mappings:
            self.mappings.remove(element)
        elif 'Session' in element_type_name and element in self.sessions:
            self.sessions.remove(element)
        elif 'Workflow' in element_type_name and element in self.workflows:
            self.workflows.remove(element)
        elif 'Application' in element_type_name and element in self.applications:
            self.applications.remove(element)
        elif 'Source' in element_type_name and element in self.sources:
            self.sources.remove(element)
        elif 'Target' in element_type_name and element in self.targets:
            self.targets.remove(element)
        elif 'Transformation' in element_type_name and element in self.transformations:
            self.transformations.remove(element)
            
    def is_empty(self) -> bool:
        """Check if folder is empty"""
        return self.contents.count() == 0
        
    def get_folder_summary(self) -> Dict[str, Any]:
        """Get folder summary information"""
        return {
            'name': self.name,
            'description': self.description,
            'folder_type': self.folder_type,
            'total_contents': self.contents.count(),
            'mappings_count': len(self.mappings),
            'sessions_count': len(self.sessions),
            'workflows_count': len(self.workflows),
            'applications_count': len(self.applications),
            'sources_count': len(self.sources),
            'targets_count': len(self.targets),
            'transformations_count': len(self.transformations)
        }
        
    def export_contents_list(self) -> List[Dict[str, Any]]:
        """Export list of all contents with basic info"""
        contents_list = []
        
        for element in self.contents.list_all():
            element_info = {
                'name': getattr(element, 'name', 'Unknown'),
                'type': element.__class__.__name__,
                'id': element.id,
                'description': getattr(element, 'description', '')
            }
            contents_list.append(element_info)
            
        return contents_list