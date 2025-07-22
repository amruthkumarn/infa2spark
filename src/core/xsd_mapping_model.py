"""
XSD-compliant mapping model classes
Based on com.informatica.metadata.common.mapping.xsd

This module implements the sophisticated mapping model used by Informatica:
- Instance-based transformation representation
- Port system for data flow
- FieldMapLinkage for complex data routing
- Load order strategies and constraints
"""
from typing import List, Dict, Optional, Any, Set, Union
from enum import Enum
from dataclasses import dataclass, field
import logging

from .xsd_base_classes import NamedElement, Element, ElementCollection, PMDataType, TypedElement
from .reference_manager import ReferenceManager, ReferenceType

class PortDirection(Enum):
    """Port direction enumeration"""
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"
    VARIABLE = "VARIABLE"
    INPUT_OUTPUT = "INPUT_OUTPUT"

class LinkPolicyType(Enum):
    """Link policy types for field mapping"""
    STRICT = "STRICT"
    FLEXIBLE = "FLEXIBLE"
    PASSTHROUGH = "PASSTHROUGH"

class LoadOrderType(Enum):
    """Load order constraint types"""
    ONE_AFTER_ANOTHER = "ONE_AFTER_ANOTHER"
    PARALLEL = "PARALLEL"
    CONDITIONAL = "CONDITIONAL"

@dataclass
class MappingCharacteristics:
    """Mapping characteristics and metadata"""
    version: str = "1.0"
    is_template: bool = False
    is_reusable: bool = False
    description: str = ""
    created_by: Optional[str] = None
    modified_by: Optional[str] = None
    created_date: Optional[str] = None
    modified_date: Optional[str] = None

class XSDMapping(NamedElement):
    """
    XSD-compliant Mapping class representing a complete data transformation
    
    A mapping in Informatica is a composite transformation that defines:
    - Instances of transformations connected via ports
    - Data flow through FieldMapLinkages
    - Load order strategies for targets
    - User-defined parameters and outputs
    """
    
    def __init__(self, 
                 name: str,
                 description: str = "",
                 **kwargs):
        super().__init__(name=name, description=description, **kwargs)
        
        # Core mapping components
        self.instances: List['XSDInstance'] = []
        self.field_map_spec: Optional['XSDFieldMapSpec'] = None
        self.load_order_strategy: Optional['XSDLoadOrderStrategy'] = None
        
        # Mapping characteristics and metadata
        self.characteristics = MappingCharacteristics()
        
        # Embedded objects (for self-contained mappings)
        self.transformations: ElementCollection = ElementCollection()  # Embedded transformation definitions
        self.parameters: Dict[str, 'XSDUserDefinedParameter'] = {}  # User-defined parameters
        self.outputs: Dict[str, 'XSDUserDefinedOutput'] = {}  # User-defined outputs
        
        # Lookup tables for efficient access
        self._instances_by_name: Dict[str, 'XSDInstance'] = {}
        self._instances_by_id: Dict[str, 'XSDInstance'] = {}
        
        self.logger = logging.getLogger(f"XSDMapping.{name}")
        
    def add_instance(self, instance: 'XSDInstance'):
        """Add a transformation instance to the mapping"""
        self.instances.append(instance)
        self._instances_by_name[instance.name] = instance
        if instance.id:
            self._instances_by_id[instance.id] = instance
        self.logger.debug(f"Added instance: {instance.name}")
        
    def get_instance(self, name_or_id: str) -> Optional['XSDInstance']:
        """Get instance by name or ID"""
        # Try by name first
        instance = self._instances_by_name.get(name_or_id)
        if instance:
            return instance
        # Try by ID
        return self._instances_by_id.get(name_or_id)
        
    def remove_instance(self, instance: 'XSDInstance'):
        """Remove instance from mapping"""
        if instance in self.instances:
            self.instances.remove(instance)
            if instance.name in self._instances_by_name:
                del self._instances_by_name[instance.name]
            if instance.id and instance.id in self._instances_by_id:
                del self._instances_by_id[instance.id]
            self.logger.debug(f"Removed instance: {instance.name}")
            
    def get_source_instances(self) -> List['XSDInstance']:
        """Get all source instances in the mapping"""
        return [inst for inst in self.instances if inst.is_source()]
        
    def get_target_instances(self) -> List['XSDInstance']:
        """Get all target instances in the mapping"""
        return [inst for inst in self.instances if inst.is_target()]
        
    def get_transformation_instances(self) -> List['XSDInstance']:
        """Get all transformation instances (non-source, non-target)"""
        return [inst for inst in self.instances if inst.is_transformation()]
        
    def validate_data_flow(self) -> List[str]:
        """Validate the data flow integrity of the mapping"""
        errors = []
        
        # Check for disconnected instances
        connected_instances = set()
        if self.field_map_spec:
            for linkage in self.field_map_spec.field_map_linkages:
                if linkage.from_instance_ref:
                    connected_instances.add(linkage.from_instance_ref)
                if linkage.to_instance_ref:
                    connected_instances.add(linkage.to_instance_ref)
                    
        for instance in self.instances:
            if instance.id not in connected_instances:
                errors.append(f"Instance {instance.name} is not connected to data flow")
                
        # Check for cycles in data flow
        cycles = self._detect_data_flow_cycles()
        if cycles:
            errors.append(f"Detected {len(cycles)} cycles in data flow")
            
        return errors
        
    def _detect_data_flow_cycles(self) -> List[List[str]]:
        """Detect cycles in the data flow graph"""
        # Build adjacency graph from field map linkages
        graph = {}
        if self.field_map_spec:
            for linkage in self.field_map_spec.field_map_linkages:
                from_inst = linkage.from_instance_ref
                to_inst = linkage.to_instance_ref
                if from_inst and to_inst:
                    if from_inst not in graph:
                        graph[from_inst] = []
                    graph[from_inst].append(to_inst)
                    
        # DFS cycle detection
        visited = set()
        rec_stack = set()
        cycles = []
        
        def dfs(node, path):
            if node in rec_stack:
                # Found cycle
                cycle_start = path.index(node)
                cycles.append(path[cycle_start:] + [node])
                return
            if node in visited:
                return
                
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in graph.get(node, []):
                dfs(neighbor, path.copy())
                
            rec_stack.remove(node)
            
        for node in graph:
            if node not in visited:
                dfs(node, [])
                
        return cycles
        
    def export_summary(self) -> Dict[str, Any]:
        """Export mapping summary for analysis"""
        return {
            'name': self.name,
            'description': self.description,
            'instance_count': len(self.instances),
            'source_instances': len(self.get_source_instances()),
            'target_instances': len(self.get_target_instances()),
            'transformation_instances': len(self.get_transformation_instances()),
            'has_field_map_spec': self.field_map_spec is not None,
            'has_load_order_strategy': self.load_order_strategy is not None,
            'parameters_count': len(self.parameters),
            'outputs_count': len(self.outputs),
            'characteristics': {
                'version': self.characteristics.version,
                'is_template': self.characteristics.is_template,
                'is_reusable': self.characteristics.is_reusable
            }
        }

class XSDInstance(NamedElement):
    """
    XSD-compliant Instance class representing a transformation instance in a mapping
    
    An instance represents the use of a transformation within a specific mapping context.
    It includes:
    - Reference to the transformation definition
    - Ports for data flow connections
    - Parameter bindings
    - Instance-specific configuration
    """
    
    def __init__(self, 
                 name: str,
                 transformation_ref: str = None,
                 **kwargs):
        super().__init__(name=name, **kwargs)
        self.transformation_ref = transformation_ref  # idref to transformation
        
        # Port collections
        self.ports: List['XSDPort'] = []
        self.input_ports: List['XSDPort'] = []
        self.output_ports: List['XSDPort'] = []
        self.variable_ports: List['XSDPort'] = []
        
        # Parameter and output bindings
        self.input_bindings: List['XSDInputBinding'] = []
        self.output_bindings: List['XSDOutputBinding'] = []
        
        # Linkage ordering for field mappings
        self.linkage_orders: List['XSDLinkageOrder'] = []
        
        # Field mapping specification (for instances that support dynamic mapping)
        self.field_map_spec: Optional['XSDFieldMapSpec'] = None
        
        # Instance metadata
        self.instance_type: Optional[str] = None  # SOURCE, TARGET, TRANSFORMATION
        self.is_active: bool = True
        
        # Lookup tables
        self._ports_by_name: Dict[str, 'XSDPort'] = {}
        self._ports_by_id: Dict[str, 'XSDPort'] = {}
        
        self.logger = logging.getLogger(f"XSDInstance.{name}")
        
    def add_port(self, port: 'XSDPort'):
        """Add a port to the instance"""
        self.ports.append(port)
        
        # Add to direction-specific collections
        if port.direction == PortDirection.INPUT:
            self.input_ports.append(port)
        elif port.direction == PortDirection.OUTPUT:
            self.output_ports.append(port)
        elif port.direction == PortDirection.VARIABLE:
            self.variable_ports.append(port)
            
        # Add to lookup tables
        if port.name:
            self._ports_by_name[port.name] = port
        if port.id:
            self._ports_by_id[port.id] = port
            
        self.logger.debug(f"Added {port.direction.value} port: {port.name}")
        
    def get_port(self, name_or_id: str) -> Optional['XSDPort']:
        """Get port by name or ID"""
        port = self._ports_by_name.get(name_or_id)
        if port:
            return port
        return self._ports_by_id.get(name_or_id)
        
    def get_ports_by_direction(self, direction: PortDirection) -> List['XSDPort']:
        """Get all ports of a specific direction"""
        return [port for port in self.ports if port.direction == direction]
        
    def is_source(self) -> bool:
        """Check if this is a source instance"""
        return (self.instance_type == "SOURCE" or 
                (len(self.input_ports) == 0 and len(self.output_ports) > 0))
        
    def is_target(self) -> bool:
        """Check if this is a target instance"""
        return (self.instance_type == "TARGET" or
                (len(self.input_ports) > 0 and len(self.output_ports) == 0))
        
    def is_transformation(self) -> bool:
        """Check if this is a transformation instance"""
        return (self.instance_type == "TRANSFORMATION" or
                (len(self.input_ports) > 0 and len(self.output_ports) > 0))
        
    def validate_port_connections(self) -> List[str]:
        """Validate port connections within the instance"""
        errors = []
        
        # Check for unconnected input ports (may be optional)
        unconnected_inputs = [port for port in self.input_ports if not port.is_connected()]
        if unconnected_inputs:
            errors.append(f"Instance {self.name} has {len(unconnected_inputs)} unconnected input ports")
            
        # Check for unconnected output ports
        unconnected_outputs = [port for port in self.output_ports if not port.is_connected()]
        if unconnected_outputs:
            errors.append(f"Instance {self.name} has {len(unconnected_outputs)} unconnected output ports")
            
        return errors

class XSDPort(NamedElement):
    """
    XSD-compliant Port class for data flow connections
    
    Ports represent connection points for data flow between transformation instances.
    Different port types handle different kinds of data:
    - TransformationFieldPort: Regular field data
    - NestedPort: Complex/structured data
    - FieldMapPort: Dynamic field mapping
    """
    
    def __init__(self,
                 name: str = None,
                 direction: PortDirection = PortDirection.INPUT,
                 port_type: str = "TransformationFieldPort",
                 **kwargs):
        super().__init__(name=name or f"Port_{id(self)}", **kwargs)
        self.direction = direction
        self.port_type = port_type  # TransformationFieldPort, NestedPort, FieldMapPort
        
        # Port connections
        self.from_port_ref: Optional[str] = None  # idref to source port
        self.to_port_refs: List[str] = []  # idrefs to target ports
        
        # Associated typed element (for data type information)
        self.typed_element_ref: Optional[str] = None  # idref to TypedElement
        
        # Port metadata
        self.is_required: bool = True
        self.default_value: Optional[Any] = None
        self.is_key: bool = False  # For key fields in lookups/joins
        
        # Connection tracking
        self._connected_ports: Set[str] = set()  # Track actual connections
        
    def connect_to(self, target_port: 'XSDPort'):
        """Connect this port to a target port"""
        if target_port.id:
            self.to_port_refs.append(target_port.id)
            target_port.from_port_ref = self.id
            self._connected_ports.add(target_port.id)
            target_port._connected_ports.add(self.id)
            
    def disconnect_from(self, target_port: 'XSDPort'):
        """Disconnect this port from a target port"""
        if target_port.id in self.to_port_refs:
            self.to_port_refs.remove(target_port.id)
            target_port.from_port_ref = None
            self._connected_ports.discard(target_port.id)
            target_port._connected_ports.discard(self.id)
            
    def is_connected(self) -> bool:
        """Check if port has any connections"""
        return len(self._connected_ports) > 0 or self.from_port_ref is not None
        
    def get_connection_count(self) -> int:
        """Get total number of connections"""
        return len(self._connected_ports)

class XSDFieldMapSpec(Element):
    """
    XSD-compliant FieldMapSpec for dynamic field mapping
    
    Handles complex field mapping scenarios where fields are mapped
    dynamically based on rules or runtime conditions.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.field_map_linkages: List['XSDFieldMapLinkage'] = []
        self.field_map_ports: List['XSDFieldMapPort'] = []
        
    def add_linkage(self, linkage: 'XSDFieldMapLinkage'):
        """Add a field map linkage"""
        self.field_map_linkages.append(linkage)
        
    def add_field_map_port(self, port: 'XSDFieldMapPort'):
        """Add a field map port"""
        self.field_map_ports.append(port)

class XSDFieldMapLinkage(Element):
    """
    XSD-compliant FieldMapLinkage for data flow connections
    
    Represents the connection between data interfaces of transformation instances,
    defining how data flows through the mapping.
    """
    
    def __init__(self,
                 from_data_interface_ref: str = None,
                 to_data_interface_ref: str = None,
                 from_instance_ref: str = None,
                 to_instance_ref: str = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.from_data_interface_ref = from_data_interface_ref  # idref
        self.to_data_interface_ref = to_data_interface_ref      # idref
        self.from_instance_ref = from_instance_ref              # idref
        self.to_instance_ref = to_instance_ref                  # idref
        
        # Link policy configuration
        self.link_policy_enabled: bool = False
        self.link_policy: Optional['XSDLinkPolicy'] = None
        
        # Parameter handling
        self.parameter_enabled: bool = False
        self.param_field_map_wrapper_ref: Optional[str] = None  # idref
        
    def is_valid(self) -> bool:
        """Check if linkage has valid configuration"""
        return (self.from_data_interface_ref is not None and 
                self.to_data_interface_ref is not None)

class XSDLinkPolicy(Element):
    """Link policy for field mapping behavior"""
    
    def __init__(self, policy_type: LinkPolicyType = LinkPolicyType.FLEXIBLE, **kwargs):
        super().__init__(**kwargs)
        self.policy_type = policy_type
        self.strict_matching: bool = False
        self.case_sensitive: bool = True

class XSDFieldMapPort(XSDPort):
    """Specialized port for field mapping operations"""
    
    def __init__(self, **kwargs):
        super().__init__(port_type="FieldMapPort", **kwargs)
        self.mapping_rules: List[str] = []  # Field mapping expressions

class XSDLoadOrderStrategy(Element):
    """
    XSD-compliant LoadOrderStrategy for target loading order
    
    Defines the order in which target instances are loaded,
    including constraints and dependencies.
    """
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.constraints: List['XSDLoadOrderConstraint'] = []
        
    def add_constraint(self, constraint: 'XSDLoadOrderConstraint'):
        """Add a load order constraint"""
        self.constraints.append(constraint)
        
    def get_loading_order(self) -> List[str]:
        """Calculate the loading order based on constraints"""
        # Simplified implementation - would need topological sort for complex cases
        ordered_instances = []
        processed = set()
        
        for constraint in self.constraints:
            if constraint.primary_instance_ref not in processed:
                ordered_instances.append(constraint.primary_instance_ref)
                processed.add(constraint.primary_instance_ref)
            if constraint.secondary_instance_ref not in processed:
                ordered_instances.append(constraint.secondary_instance_ref)
                processed.add(constraint.secondary_instance_ref)
                
        return ordered_instances

class XSDLoadOrderConstraint(Element):
    """
    Abstract base for load order constraints
    
    Defines dependencies between target instances for loading order.
    """
    
    def __init__(self,
                 primary_instance_ref: str = None,
                 secondary_instance_ref: str = None,
                 constraint_type: LoadOrderType = LoadOrderType.ONE_AFTER_ANOTHER,
                 **kwargs):
        super().__init__(**kwargs)
        self.primary_instance_ref = primary_instance_ref    # idref
        self.secondary_instance_ref = secondary_instance_ref # idref
        self.constraint_type = constraint_type

class XSDOneAfterAnotherConstraint(XSDLoadOrderConstraint):
    """Constraint requiring sequential loading of instances"""
    
    def __init__(self, **kwargs):
        super().__init__(constraint_type=LoadOrderType.ONE_AFTER_ANOTHER, **kwargs)

class XSDLinkageOrder(Element):
    """
    XSD-compliant LinkageOrder for field mapping order
    
    Defines the order in which field map linkages are processed
    for a specific data interface.
    """
    
    def __init__(self,
                 to_data_interface_ref: str = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.to_data_interface_ref = to_data_interface_ref  # idref
        self.field_map_linkage_refs: List[str] = []  # idrefs to FieldMapLinkages
        
    def add_linkage_ref(self, linkage_ref: str):
        """Add a linkage reference to the order"""
        self.field_map_linkage_refs.append(linkage_ref)

# Parameter and binding classes for mapping-level configuration
class XSDUserDefinedParameter(NamedElement):
    """User-defined parameter for mapping configuration"""
    
    def __init__(self, name: str, data_type: PMDataType, default_value: Any = None, **kwargs):
        super().__init__(name=name, **kwargs)
        self.data_type = data_type
        self.default_value = default_value
        self.is_required: bool = default_value is None

class XSDUserDefinedOutput(NamedElement):
    """User-defined output for mapping results"""
    
    def __init__(self, name: str, data_type: PMDataType, **kwargs):
        super().__init__(name=name, **kwargs)
        self.data_type = data_type
        self.expression: Optional[str] = None  # Output calculation expression

class XSDInputBinding(Element):
    """Binding for mapping parameter inputs"""
    
    def __init__(self, parameter_ref: str = None, value: Any = None, **kwargs):
        super().__init__(**kwargs)
        self.parameter_ref = parameter_ref  # idref to parameter
        self.value = value

class XSDOutputBinding(Element):
    """Binding for mapping outputs"""
    
    def __init__(self, output_ref: str = None, **kwargs):
        super().__init__(**kwargs)
        self.output_ref = output_ref  # idref to output