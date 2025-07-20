"""
XML Parser for Informatica project files
"""
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional
import logging
from .base_classes import Project, Connection, Task, WorkflowLink

class InformaticaXMLParser:
    """Parser for Informatica project XML files"""
    
    def __init__(self):
        self.logger = logging.getLogger("XMLParser")
        
    def parse_project(self, xml_file_path: str) -> Project:
        """Parse Informatica project XML file"""
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            
            # Handle namespace if present
            namespace = ""
            if root.tag.startswith('{'):
                namespace = root.tag[root.tag.find('{')+1:root.tag.find('}')]
                
            # Extract project info
            project_name = root.get('name', 'UnknownProject')
            project_version = root.get('version', '1.0')
            
            project = Project(project_name, project_version)
            
            # Parse description - handle namespace
            desc_elem = root.find('./description') if not namespace else root.find('./{%s}description' % namespace)
            if desc_elem is not None:
                project.description = desc_elem.text or ""
                
            # Parse folders and their contents
            self._parse_folders(root, project, namespace)
            
            # Parse connections
            self._parse_connections(root, project, namespace)
            
            # Parse parameters
            self._parse_parameters(root, project, namespace)
            
            self.logger.info(f"Successfully parsed project: {project_name}")
            return project
            
        except Exception as e:
            self.logger.error(f"Error parsing XML file {xml_file_path}: {str(e)}")
            raise
            
    def _parse_folders(self, root: ET.Element, project: Project, namespace: str = ""):
        """Parse folders section"""
        folders_elem = root.find('./folders') if not namespace else root.find('./{%s}folders' % namespace)
        if folders_elem is None:
            return
            
        for folder in folders_elem:
            folder_name = folder.get('name')
            if folder_name:
                if folder_name not in project.folders:
                    project.folders[folder_name] = []
                
                if folder_name == 'Mappings':
                    self._parse_mappings(folder, project, namespace)
                elif folder_name == 'Workflows':
                    self._parse_workflows(folder, project, namespace)
                elif folder_name == 'Applications':
                    self._parse_applications(folder, project, namespace)
                
    def _parse_mappings(self, mappings_folder: ET.Element, project: Project, namespace: str = ""):
        """Parse mappings from folder"""
        mapping_tag = 'mapping' if not namespace else '{%s}mapping' % namespace
        for mapping_elem in mappings_folder.findall(mapping_tag):
            mapping_info = self._extract_mapping_info(mapping_elem, namespace)
            project.folders['Mappings'].append(mapping_info)
            
    def _extract_mapping_info(self, mapping_elem: ET.Element, namespace: str = "") -> Dict:
        """Extract mapping information"""
        mapping_info = {
            'name': mapping_elem.get('name'),
            'description': '',
            'components': []
        }
        
        # Description
        desc_tag = 'description' if not namespace else '{%s}description' % namespace
        desc_elem = mapping_elem.find(desc_tag)
        if desc_elem is not None:
            mapping_info['description'] = desc_elem.text or ""
            
        # Components
        components_tag = 'components' if not namespace else '{%s}components' % namespace
        components_elem = mapping_elem.find(components_tag)
        if components_elem is not None:
            for component in components_elem:
                component_info = {
                    'name': component.get('name'),
                    'type': component.get('type'),
                    'component_type': component.tag  # source, transformation, target
                }
                
                # Additional attributes
                for attr in component.attrib:
                    if attr not in ['name', 'type']:
                        component_info[attr] = component.get(attr)
                        
                mapping_info['components'].append(component_info)
                
        return mapping_info
        
    def _parse_workflows(self, workflows_folder: ET.Element, project: Project):
        """Parse workflows from folder"""
        for workflow_elem in workflows_folder.findall('workflow'):
            workflow_info = self._extract_workflow_info(workflow_elem)
            project.folders['folder']['Workflows'].append(workflow_info)
            
    def _extract_workflow_info(self, workflow_elem: ET.Element) -> Dict:
        """Extract workflow information"""
        workflow_info = {
            'name': workflow_elem.get('name'),
            'description': '',
            'tasks': [],
            'links': []
        }
        
        # Description
        desc_elem = workflow_elem.find('description')
        if desc_elem is not None:
            workflow_info['description'] = desc_elem.text or ""
            
        # Tasks
        tasks_elem = workflow_elem.find('tasks')
        if tasks_elem is not None:
            for task_elem in tasks_elem.findall('task'):
                task_info = {
                    'name': task_elem.get('name'),
                    'type': task_elem.get('type'),
                    'mapping': task_elem.get('mapping'),
                    'properties': {}
                }
                
                # Properties
                props_elem = task_elem.find('properties')
                if props_elem is not None:
                    for prop_elem in props_elem.findall('property'):
                        prop_name = prop_elem.get('name')
                        prop_value = prop_elem.get('value')
                        task_info['properties'][prop_name] = prop_value
                        
                workflow_info['tasks'].append(task_info)
                
        # Links
        links_elem = workflow_elem.find('links')
        if links_elem is not None:
            for link_elem in links_elem.findall('link'):
                link_info = {
                    'from': link_elem.get('from'),
                    'to': link_elem.get('to'),
                    'condition': link_elem.get('condition', 'SUCCESS')
                }
                workflow_info['links'].append(link_info)
                
        return workflow_info
        
    def _parse_applications(self, applications_folder: ET.Element, project: Project):
        """Parse applications from folder"""
        for app_elem in applications_folder.findall('application'):
            app_info = self._extract_application_info(app_elem)
            project.folders['folder']['Applications'].append(app_info)
            
    def _extract_application_info(self, app_elem: ET.Element) -> Dict:
        """Extract application information"""
        app_info = {
            'name': app_elem.get('name'),
            'description': '',
            'workflows': [],
            'parameters': {}
        }
        
        # Description
        desc_elem = app_elem.find('description')
        if desc_elem is not None:
            app_info['description'] = desc_elem.text or ""
            
        # Workflows
        workflows_elem = app_elem.find('workflows')
        if workflows_elem is not None:
            for wf_ref in workflows_elem.findall('workflow-ref'):
                app_info['workflows'].append(wf_ref.get('name'))
                
        # Parameters
        params_elem = app_elem.find('parameters')
        if params_elem is not None:
            for param_elem in params_elem.findall('parameter'):
                param_name = param_elem.get('name')
                param_value = param_elem.get('value')
                app_info['parameters'][param_name] = param_value
                
        return app_info
        
    def _parse_connections(self, root: ET.Element, project: Project, namespace: str = ""):
        """Parse connections section"""
        connections_tag = 'connections' if not namespace else '{%s}connections' % namespace
        connections_elem = root.find(connections_tag)
        if connections_elem is None:
            return
            
        conn_tag = 'connection' if not namespace else '{%s}connection' % namespace
        for conn_elem in connections_elem.findall(conn_tag):
            connection = Connection(
                name=conn_elem.get('name'),
                connection_type=conn_elem.get('type'),
                host=conn_elem.get('host'),
                port=int(conn_elem.get('port', 0))
            )
            
            # Add additional properties
            for attr in conn_elem.attrib:
                if attr not in ['name', 'type', 'host', 'port']:
                    connection.properties[attr] = conn_elem.get(attr)
                    
            project.add_connection(connection)
            
    def _parse_parameters(self, root: ET.Element, project: Project, namespace: str = ""):
        """Parse project parameters"""
        params_tag = 'parameters' if not namespace else '{%s}parameters' % namespace
        params_elem = root.find(params_tag)
        if params_elem is None:
            return
            
        param_tag = 'parameter' if not namespace else '{%s}parameter' % namespace
        for param_elem in params_elem.findall(param_tag):
            param_name = param_elem.get('name')
            param_value = param_elem.get('value')
            project.add_parameter(param_name, param_value)