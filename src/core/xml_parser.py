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
        """Parse Informatica project XML file (supports both direct project and IMX wrapper)"""
        try:
            tree = ET.parse(xml_file_path)
            root = tree.getroot()
            
            # Check if this is an IMX wrapper file
            if self._is_imx_root(root):
                self.logger.info("Detected IMX wrapper format")
                return self._parse_imx_project(root)
            else:
                self.logger.info("Detected direct project format")
                return self._parse_direct_project(root)
                
        except Exception as e:
            self.logger.error(f"Error parsing XML file {xml_file_path}: {str(e)}")
            raise
            
    def _is_imx_root(self, root: ET.Element) -> bool:
        """Check if root element is IMX wrapper"""
        local_name = self._get_local_name(root.tag)
        imx_namespace = "http://www.informatica.com/imx"
        
        # Check for IMX Document or IMX root
        return (local_name in ["IMX", "Document"] or 
                imx_namespace in root.tag)
        
    def _get_local_name(self, tag: str) -> str:
        """Extract local name from namespaced tag"""
        if tag.startswith('{'):
            return tag[tag.rfind('}') + 1:]
        return tag
        
    def _get_namespace(self, tag: str) -> str:
        """Extract namespace from namespaced tag"""
        if tag.startswith('{'):
            return tag[tag.find('{') + 1:tag.find('}')]
        return ""
        
    def _parse_imx_project(self, imx_root: ET.Element) -> Project:
        """Parse project from IMX wrapper format"""
        # Look for Project element within IMX
        project_elem = None
        
        # Search for project elements with different namespace patterns
        for child in imx_root:
            local_name = self._get_local_name(child.tag)
            if local_name == "Project":
                project_elem = child
                break
                
        if project_elem is None:
            # Create default project if no Project element found
            self.logger.warning("No Project element found in IMX, creating default project")
            project = Project("DefaultIMXProject", "1.0")
            project.description = "Generated from IMX file"
        else:
            # Parse project element
            project_name = project_elem.get('name', 'IMXProject')
            project_version = project_elem.get('version', '1.0')
            project = Project(project_name, project_version)
            
            # Parse project description - try multiple formats
            # First try direct description element (with namespace handling)
            desc_elem = project_elem.find('.//{*}description')
            if desc_elem is not None and desc_elem.text:
                project.description = desc_elem.text.strip()
            else:
                # Try without namespace
                desc_elem = project_elem.find('.//description')
                if desc_elem is not None and desc_elem.text:
                    project.description = desc_elem.text.strip()
                else:
                    # Fallback to annotations format
                    for anno in project_elem.findall('.//{*}lGenericAnnotations/{*}Annotation'):
                        if anno.get('name') == 'Description':
                            project.description = anno.get('value', '')
                            break
                
            # Parse project contents - first try direct contents element
            contents_elem = project_elem.find('./{*}contents')
            if contents_elem is not None:
                self.logger.debug("Found project contents element")
                self._parse_imx_contents(contents_elem, project)
            else:
                self.logger.debug("No direct contents element found, looking for folders")
                # Look for folders directly under project (common pattern)
                folders_found = False
                for child in project_elem:
                    local_name = self._get_local_name(child.tag)
                    if local_name == "Folder":
                        self.logger.debug(f"Found direct folder: {child.get('name')}")
                        self._parse_imx_folder(child, project)
                        folders_found = True
                
                # If no folders found, parse project element directly as contents
                if not folders_found:
                    self.logger.debug("No folders found, parsing project as contents")
                    self._parse_imx_contents(project_elem, project)
        
        # Parse top-level IMX connections and parameters
        self._parse_imx_connections(imx_root, project)
        self._parse_imx_parameters(imx_root, project)
        
        self.logger.info(f"Successfully parsed IMX project: {project.name}")
        return project
        
    def _parse_imx_contents(self, contents_elem: ET.Element, project: Project):
        """Parse contents section of IMX project, now more flexible."""
        self.logger.debug(f"Parsing IMX contents with {len(list(contents_elem))} children")
        for child in contents_elem:
            local_name = self._get_local_name(child.tag)
            self.logger.debug(f"Processing contents child: {local_name} with attributes {child.attrib}")
            
            # Handle XSD-compliant IObject elements first
            if local_name == "IObject":
                self.logger.debug("Found IObject in contents")
                self._parse_iobject_element(child, project, "Root")
            elif local_name == "Folder":
                self.logger.debug(f"Found Folder in contents: {child.get('name')}")
                self._parse_imx_folder(child, project)
            elif local_name in ["TLoaderMapping", "mapping", "Mapping"]:
                # Handle mappings that might be directly in contents
                self.logger.debug(f"Found direct mapping in contents: {child.get('name')}")
                if "Mappings" not in project.folders:
                    project.folders["Mappings"] = []
                mapping_info = self._extract_imx_mapping_info(child)
                project.folders["Mappings"].append(mapping_info)
            elif local_name in ["TWorkflow", "workflow", "Workflow"]:
                # Handle workflows that might be directly in contents
                self.logger.debug(f"Found direct workflow in contents: {child.get('name')}")
                if "Workflows" not in project.folders:
                    project.folders["Workflows"] = []
                workflow_info = self._extract_imx_workflow_info(child)
                project.folders["Workflows"].append(workflow_info)
            else:
                self.logger.debug(f"Skipping unknown contents child: {local_name}")
                
    def _parse_imx_folder(self, folder_elem: ET.Element, project: Project):
        """Parse folder within IMX structure, supporting generic lObject tags."""
        folder_name = folder_elem.get('name', 'UnknownFolder')
        self.logger.debug(f"Parsing folder: {folder_name}")
        
        if folder_name not in project.folders:
            project.folders[folder_name] = []
            
        # Standard contents parsing
        contents_elem = folder_elem.find('.//{*}contents')
        if contents_elem is not None:
            self._parse_folder_contents(contents_elem, project, folder_name)

        # Also check for lObject tags directly within the folder, which is a common pattern
        self._parse_folder_contents(folder_elem, project, folder_name)

    def _parse_folder_contents(self, parent_elem: ET.Element, project: Project, folder_name: str):
        """Generic folder content parser for both <contents> and <lObject> patterns."""
        self.logger.debug(f"Parsing contents of <{parent_elem.tag}> in folder '{folder_name}'")
        for child in parent_elem:
            local_name = self._get_local_name(child.tag)
            self.logger.debug(f"Found child element: <{local_name}>")
            
            # Handle XSD-compliant IObject elements
            if local_name == "IObject":
                self._parse_iobject_element(child, project, folder_name)
            else:
                # Legacy handling for lObject and direct elements
                self._parse_legacy_object_element(child, project, folder_name)
    
    def _parse_iobject_element(self, iobject_elem: ET.Element, project: Project, folder_name: str):
        """Parse XSD-compliant IObject element with xsi:type attribute"""
        # Determine object type from xsi:type attribute
        xsi_type_attr = '{http://www.w3.org/2001/XMLSchema-instance}type'
        object_type = None
        
        if xsi_type_attr in iobject_elem.attrib:
            # Handle types like "mapping:Mapping", "workflow:Workflow", "folder:Folder"
            full_type = iobject_elem.attrib[xsi_type_attr]
            object_type = full_type.split(':')[-1]  # Extract the local type name
            self.logger.debug(f"Detected IObject xsi:type = {full_type} -> {object_type}")
        else:
            self.logger.warning(f"IObject element missing xsi:type attribute")
            return
            
        # Route to appropriate handler based on object type
        if object_type in ["TLoaderMapping", "Mapping"]:
            self.logger.info(f"Found Mapping IObject: {iobject_elem.get('name', 'Unknown')}")
            if "Mappings" not in project.folders:
                project.folders["Mappings"] = []
            mapping_info = self._extract_imx_mapping_info(iobject_elem)
            project.folders["Mappings"].append(mapping_info)
            # Also add to project.mappings for SparkCodeGenerator
            project.mappings[mapping_info['name']] = mapping_info
        elif object_type in ["TWorkflow", "Workflow"]:
            self.logger.info(f"Found Workflow IObject: {iobject_elem.get('name', 'Unknown')}")
            if "Workflows" not in project.folders:
                project.folders["Workflows"] = []
            workflow_info = self._extract_imx_workflow_info(iobject_elem)
            project.folders["Workflows"].append(workflow_info)
        elif object_type in ["Folder"]:
            self.logger.info(f"Found Folder IObject: {iobject_elem.get('name', 'Unknown')}")
            # Recursively parse nested folder
            self._parse_imx_folder(iobject_elem, project)
        elif object_type == "Application":
            self.logger.info(f"Found Application IObject: {iobject_elem.get('name', 'Unknown')}")
            if "Applications" not in project.folders:
                project.folders["Applications"] = []
            app_info = self._extract_application_info(iobject_elem)
            project.folders["Applications"].append(app_info)
        else:
            self.logger.debug(f"Skipping unknown IObject type '{object_type}' (full type: {full_type})")
    
    def _parse_legacy_object_element(self, child: ET.Element, project: Project, folder_name: str):
        """Parse legacy lObject and direct elements (backwards compatibility)"""
        local_name = self._get_local_name(child.tag)
        
        # Determine object type from xsi:type or local tag name
        xsi_type_attr = '{http://www.w3.org/2001/XMLSchema-instance}type'
        object_type = None
        if xsi_type_attr in child.attrib:
            # Handle types like "mapping:Mapping"
            object_type = child.attrib[xsi_type_attr].split(':')[-1]
            self.logger.debug(f"Detected xsi:type = {object_type}")
        else:
            object_type = local_name
            self.logger.debug(f"Using local name as object type: {object_type}")

        if object_type in ["TLoaderMapping", "Mapping"]:
            self.logger.info(f"Found Mapping: {child.get('name', 'Unknown')}")
            if "Mappings" not in project.folders:
                project.folders["Mappings"] = []
            mapping_info = self._extract_imx_mapping_info(child)
            project.folders["Mappings"].append(mapping_info)
            # Also add to project.mappings for SparkCodeGenerator
            project.mappings[mapping_info['name']] = mapping_info
        elif object_type in ["TWorkflow", "Workflow"]:
            self.logger.info(f"Found Workflow: {child.get('name', 'Unknown')}")
            if "Workflows" not in project.folders:
                project.folders["Workflows"] = []
            workflow_info = self._extract_imx_workflow_info(child)
            project.folders["Workflows"].append(workflow_info)
        elif object_type == "Application":
            self.logger.info(f"Found Application: {child.get('name', 'Unknown')}")
            if "Applications" not in project.folders:
                project.folders["Applications"] = []
            app_info = self._extract_application_info(child)
            project.folders["Applications"].append(app_info)
        else:
            self.logger.debug(f"Skipping unknown object type '{object_type}'")
                    
    def _extract_imx_mapping_info(self, mapping_elem: ET.Element) -> Dict:
        """Extract mapping information from IMX format"""
        mapping_info = {
            'name': mapping_elem.get('name', 'UnknownMapping'),
            'id': mapping_elem.get('id', ''),
            'description': '',
            'components': []
        }
        
        # Description
        desc_elem = mapping_elem.find('.//{*}description')
        if desc_elem is not None:
            mapping_info['description'] = desc_elem.text or ""
            
        # Parameters (added for complex project)
        params_elem = mapping_elem.find('.//{*}parameters')
        if params_elem is not None:
            mapping_info['parameters'] = {}
            for param in params_elem:
                param_name = param.get('name')
                param_value = param.get('value')
                if param_name:
                    mapping_info['parameters'][param_name] = param_value

        # Components (sources, transformations, targets)
        # Handle both <transformations> and direct <components> tags
        components_container = mapping_elem.find('.//{*}transformations')
        if components_container is None:
            components_container = mapping_elem.find('.//{*}components')
        if components_container is None:
            components_container = mapping_elem

        # Look for actual transformation elements
        for transformation in components_container.findall('.//{*}AbstractTransformation'):
            component_info = self._extract_transformation_details(transformation)
            mapping_info['components'].append(component_info)
            
        # Also handle simple component elements (for test XML)
        # Use iterative approach to handle namespaced elements  
        for child in components_container:
            local_name = self._get_local_name(child.tag)
            if local_name in ['source', 'transformation', 'target']:
                component_info = {
                    'name': child.get('name', 'Unknown'),
                    'type': child.get('type', local_name),
                    'format': child.get('format', ''),
                    'category': local_name
                }
                mapping_info['components'].append(component_info)
                
        return mapping_info
        
    def _extract_transformation_details(self, transformation_elem: ET.Element) -> Dict:
        """Extract detailed transformation information including ports and expressions"""
        
        # Basic info
        transform_type = transformation_elem.get('type', '').lower()
        
        # Classify component type for template filtering
        if transform_type in ['source', 'flatfilesource', 'relationalsource']:
            component_type = 'source'
        elif transform_type in ['target', 'flatfiletarget', 'relationaltarget']:
            component_type = 'target'
        else:
            component_type = 'transformation'
        
        transform_info = {
            'name': transformation_elem.get('name', ''),
            'type': transformation_elem.get('type', ''),
            'component_type': component_type,
            'ports': [],
            'expressions': [],
            'characteristics': {}
        }
        
        # Extract TransformationFieldPort elements
        ports_elem = transformation_elem.find('ports')
        if ports_elem is not None:
            for port in ports_elem.findall('TransformationFieldPort'):
                port_info = {
                    'name': port.get('name'),
                    'type': port.get('type'),
                    'direction': port.get('direction'),
                    'length': port.get('length'),
                    'precision': port.get('precision'),
                    'scale': port.get('scale')
                }
                transform_info['ports'].append(port_info)
        
        # Extract ExpressionField elements (for Expression transformations)
        expr_interface = transformation_elem.find('.//expressioninterface')
        if expr_interface is not None:
            expr_fields = expr_interface.find('expressionFields')
            if expr_fields is not None:
                for expr_field in expr_fields.findall('ExpressionField'):
                    expr_info = {
                        'name': expr_field.get('name'),
                        'expression': expr_field.get('expression'),
                        'type': expr_field.get('type', 'GENERAL')
                    }
                    transform_info['expressions'].append(expr_info)
        
        # Extract characteristics
        characteristics = transformation_elem.find('characteristics')
        if characteristics is not None:
            for char in characteristics.findall('Characteristic'):
                char_name = char.get('name')
                char_value = char.get('value')
                if char_name:
                    transform_info['characteristics'][char_name] = char_value
        
        return transform_info
        
    def _extract_imx_workflow_info(self, workflow_elem: ET.Element) -> Dict:
        """Extract workflow information from IMX format"""
        workflow_info = {
            'name': workflow_elem.get('name', 'UnknownWorkflow'),
            'id': workflow_elem.get('id', ''),
            'description': '',
            'tasks': [],
            'links': []
        }
        
        # Description
        desc_elem = workflow_elem.find('.//{*}description')
        if desc_elem is not None:
            workflow_info['description'] = desc_elem.text or ""
            
        # Tasks (handle both <tasks> and <taskinstances>)
        tasks_container = workflow_elem.find('.//{*}tasks')
        if tasks_container is None:
            tasks_container = workflow_elem.find('.//{*}taskinstances')
        
        if tasks_container is not None:
            for task_elem in tasks_container:
                task_info = {
                    'name': task_elem.get('name', ''),
                    'type': task_elem.get('type', self._get_local_name(task_elem.tag)),
                    'mapping': '',
                    'properties': {}
                }
                
                # Extract mapping name if it's a mapping task
                mapping_task_config = task_elem.find('.//{*}mappingTaskConfig')
                if mapping_task_config is not None:
                    task_info['mapping'] = mapping_task_config.get('mapping', '')
                            
                workflow_info['tasks'].append(task_info)
                
        # Links (handle both <links> and <outgoingSequenceFlows>)
        links_container = workflow_elem.find('.//{*}links')
        if links_container is None:
            links_container = workflow_elem.find('.//{*}outgoingSequenceFlows')

        if links_container is not None:
            for link_elem in links_container:
                link_info = {
                    'from': link_elem.get('from', ''),
                    'to': link_elem.get('to', ''),
                    'condition': link_elem.get('condition', 'SUCCESS')
                }
                workflow_info['links'].append(link_info)
                
        return workflow_info
        
    def _parse_imx_connections(self, imx_root: ET.Element, project: Project):
        """Parse connections from IMX root level"""
        for child in imx_root:
            local_name = self._get_local_name(child.tag)
            
            if local_name in ["TRepConnection", "connection"]:
                connection = Connection(
                    name=child.get('connectionName') or child.get('name', 'UnknownConnection'),
                    connection_type=child.get('connectionType') or child.get('type', 'UNKNOWN'),
                    host=child.get('host', ''),
                    port=int(child.get('port', 0))
                )
                
                # Add additional properties
                for attr in child.attrib:
                    if attr not in ['connectionName', 'name', 'connectionType', 'type', 'host', 'port']:
                        connection.properties[attr] = child.get(attr)
                        
                project.add_connection(connection)
                
    def _parse_imx_parameters(self, imx_root: ET.Element, project: Project):
        """Parse parameters from IMX root level"""
        for child in imx_root:
            local_name = self._get_local_name(child.tag)
            
            if local_name == "Parameter":
                param_name = child.get('name')
                param_value = child.get('value')
                if param_name:
                    project.add_parameter(param_name, param_value)
                    
    def _parse_direct_project(self, root: ET.Element) -> Project:
        """Parse direct project format (original implementation)"""
        # Handle namespace if present
        namespace = self._get_namespace(root.tag)
            
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
        
        self.logger.info(f"Successfully parsed direct project: {project_name}")
        return project
            
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
        
    def _parse_workflows(self, workflows_folder: ET.Element, project: Project, namespace: str = ""):
        """Parse workflows from folder"""
        workflow_tag = 'workflow' if not namespace else '{%s}workflow' % namespace
        for workflow_elem in workflows_folder.findall(workflow_tag):
            workflow_info = self._extract_workflow_info(workflow_elem, namespace)
            project.folders['Workflows'].append(workflow_info)
            
    def _extract_workflow_info(self, workflow_elem: ET.Element, namespace: str = "") -> Dict:
        """Extract workflow information"""
        workflow_info = {
            'name': workflow_elem.get('name'),
            'description': '',
            'tasks': [],
            'links': []
        }
        
        # Description
        desc_tag = 'description' if not namespace else '{%s}description' % namespace
        desc_elem = workflow_elem.find(desc_tag)
        if desc_elem is not None:
            workflow_info['description'] = desc_elem.text or ""
            
        # Tasks
        tasks_tag = 'tasks' if not namespace else '{%s}tasks' % namespace
        tasks_elem = workflow_elem.find(tasks_tag)
        if tasks_elem is not None:
            task_tag = 'task' if not namespace else '{%s}task' % namespace
            for task_elem in tasks_elem.findall(task_tag):
                task_info = {
                    'name': task_elem.get('name'),
                    'type': task_elem.get('type'),
                    'mapping': task_elem.get('mapping'),
                    'properties': {}
                }
                
                # Properties
                props_tag = 'properties' if not namespace else '{%s}properties' % namespace
                props_elem = task_elem.find(props_tag)
                if props_elem is not None:
                    prop_tag = 'property' if not namespace else '{%s}property' % namespace
                    for prop_elem in props_elem.findall(prop_tag):
                        prop_name = prop_elem.get('name')
                        prop_value = prop_elem.get('value')
                        task_info['properties'][prop_name] = prop_value
                        
                workflow_info['tasks'].append(task_info)
                
        # Links
        links_tag = 'links' if not namespace else '{%s}links' % namespace
        links_elem = workflow_elem.find(links_tag)
        if links_elem is not None:
            link_tag = 'link' if not namespace else '{%s}link' % namespace
            for link_elem in links_elem.findall(link_tag):
                link_info = {
                    'from': link_elem.get('from'),
                    'to': link_elem.get('to'),
                    'condition': link_elem.get('condition', 'SUCCESS')
                }
                workflow_info['links'].append(link_info)
                
        return workflow_info
        
    def _parse_applications(self, applications_folder: ET.Element, project: Project, namespace: str = ""):
        """Parse applications from folder"""
        app_tag = 'application' if not namespace else '{%s}application' % namespace
        for app_elem in applications_folder.findall(app_tag):
            app_info = self._extract_application_info(app_elem, namespace)
            project.folders['Applications'].append(app_info)
            
    def _extract_application_info(self, app_elem: ET.Element, namespace: str = "") -> Dict:
        """Extract application information"""
        app_info = {
            'name': app_elem.get('name'),
            'description': '',
            'workflows': [],
            'parameters': {}
        }
        
        # Description
        desc_tag = 'description' if not namespace else '{%s}description' % namespace
        desc_elem = app_elem.find(desc_tag)
        if desc_elem is not None:
            app_info['description'] = desc_elem.text or ""
            
        # Workflows
        workflows_tag = 'workflows' if not namespace else '{%s}workflows' % namespace
        workflows_elem = app_elem.find(workflows_tag)
        if workflows_elem is not None:
            wf_ref_tag = 'workflow-ref' if not namespace else '{%s}workflow-ref' % namespace
            for wf_ref in workflows_elem.findall(wf_ref_tag):
                app_info['workflows'].append(wf_ref.get('name'))
                
        # Parameters
        params_tag = 'parameters' if not namespace else '{%s}parameters' % namespace
        params_elem = app_elem.find(params_tag)
        if params_elem is not None:
            param_tag = 'parameter' if not namespace else '{%s}parameter' % namespace
            for param_elem in params_elem.findall(param_tag):
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