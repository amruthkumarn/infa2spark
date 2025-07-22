"""
XSD-compliant transformation model classes
Based on com.informatica.metadata.common.transformation.xsd

This module implements the complete Informatica transformation hierarchy with:
- Field-level metadata and configuration
- Data interface specifications  
- Transformation configuration framework
- Support for all Informatica transformation types
"""
from typing import List, Dict, Optional, Any, Union, Set
from enum import Enum
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import logging

from .xsd_base_classes import NamedElement, Element, TypedElement, PMDataType, ElementCollection
from .reference_manager import ReferenceManager

class LanguageKind(Enum):
    """Programming language for transformation implementation"""
    JAVA = "java"
    C = "c"
    CPP = "cpp"
    PYTHON = "python"  # Extension for PySpark

class PartitioningKind(Enum):
    """Partitioning capability of transformation"""
    NOT_PARTITIONABLE = "notPartitionable"
    LOCALLY_PARTITIONABLE = "locallyPartitionable"
    GRID_PARTITIONABLE = "gridPartitionable"
    PROCESS_PARTITIONABLE = "processPartitionable"

class TransformationScope(Enum):
    """Scope of transformation execution"""
    ROW = "row"
    TRANSACTION = "transaction"
    ALL_INPUT = "allInput"

class OrderingKind(Enum):
    """Output ordering characteristics"""
    NEVER_SAME = "neverSame"
    INPUT_DEPENDENT = "inputDependent"
    ALWAYS_SAME = "alwaysSame"

class FieldDirection(Enum):
    """Field direction for ports"""
    INPUT = "INPUT"
    OUTPUT = "OUTPUT"
    BOTH = "BOTH"
    RETURN = "RETURN"
    LOOKUP = "LOOKUP"
    GENERATED = "GENERATED"

class MatchResolutionKind(Enum):
    """Lookup multiple match resolution"""
    RETURN_FIRST = "returnFirst"
    RETURN_LAST = "returnLast"
    RETURN_ANY = "returnAny"
    REPORT_ERROR = "reportError"
    RETURN_ALL = "returnAll"

class LoadScope(Enum):
    """Target load scope"""
    ROW = "row"
    TRANSACTION = "transaction"
    ALL = "all"

@dataclass
class FieldConstraints:
    """Field constraints and validation rules"""
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    lower_bound: Optional[int] = None
    upper_bound: Optional[int] = None
    required: bool = False
    unique: bool = False
    pattern: Optional[str] = None

@dataclass
class FieldConfiguration:
    """Field-level configuration and metadata"""
    expression: Optional[str] = None
    default_value: Optional[Any] = None
    variable_field: bool = False
    parameterizable: bool = False
    constraints: Optional[FieldConstraints] = None
    custom_properties: Dict[str, Any] = field(default_factory=dict)

class XSDTransformationField(TypedElement):
    """
    XSD-compliant transformation field with comprehensive metadata
    
    Represents a field within a transformation interface with:
    - Data type and constraints
    - Input/output direction
    - Expression and configuration
    - Runtime metadata
    """
    
    def __init__(self,
                 name: str = None,
                 data_type: PMDataType = PMDataType.SQL_VARCHAR,
                 direction: FieldDirection = FieldDirection.INPUT,
                 **kwargs):
        # Extract name from kwargs to handle it separately
        if 'name' in kwargs:
            kwargs.pop('name')
        super().__init__(data_type=data_type, **kwargs)
        self.name = name
        self.direction = direction
        
        # Field capabilities
        self.input: bool = direction in [FieldDirection.INPUT, FieldDirection.BOTH]
        self.output: bool = direction in [FieldDirection.OUTPUT, FieldDirection.BOTH, FieldDirection.RETURN]
        
        # Field configuration
        self.configuration = FieldConfiguration()
        
        # Expression and calculation
        self.expression: Optional[str] = None
        self.expression_type: Optional[str] = None  # "GENERAL", "CONDITION", "AGGREGATION"
        
        # Derived field properties
        self.is_derived: bool = False
        self.source_fields: List[str] = []  # References to source fields
        
        # Port mapping information
        self.port_type: Optional[str] = None  # "TransformationFieldPort", "NestedPort", etc.
        self.group_ref: Optional[str] = None  # Reference to data group if applicable
        
        self.logger = logging.getLogger(f"TransformationField.{name or 'unnamed'}")
        
    def set_expression(self, expression: str, expression_type: str = "GENERAL"):
        """Set field expression for derived calculations"""
        self.expression = expression
        self.expression_type = expression_type
        self.is_derived = True
        self.configuration.expression = expression
        
    def add_source_field(self, field_ref: str):
        """Add reference to source field for dependency tracking"""
        if field_ref not in self.source_fields:
            self.source_fields.append(field_ref)
            
    def validate_constraints(self, value: Any) -> List[str]:
        """Validate value against field constraints"""
        errors = []
        constraints = self.configuration.constraints
        
        if constraints:
            if constraints.required and (value is None or value == ""):
                errors.append(f"Field {self.name} is required")
                
            # Only validate min/max if value is not None
            if value is not None:
                if constraints.min_value is not None and value < constraints.min_value:
                    errors.append(f"Field {self.name} value {value} below minimum {constraints.min_value}")
                    
                if constraints.max_value is not None and value > constraints.max_value:
                    errors.append(f"Field {self.name} value {value} above maximum {constraints.max_value}")
                
        return errors
        
    def get_field_metadata(self) -> Dict[str, Any]:
        """Get comprehensive field metadata"""
        return {
            'name': self.name,
            'data_type': self.data_type.value if self.data_type else None,
            'direction': self.direction.value,
            'length': self.length,
            'precision': self.precision,
            'scale': self.scale,
            'nullable': self.nullable,
            'input': self.input,
            'output': self.output,
            'is_derived': self.is_derived,
            'expression': self.expression,
            'expression_type': self.expression_type,
            'source_fields': self.source_fields,
            'port_type': self.port_type,
            'configuration': self.configuration.custom_properties
        }

class XSDFieldSelector(Element):
    """
    Field selector for rule-based field selection
    
    Supports multiple selection strategies:
    - Type-based selection
    - Pattern-based selection  
    - Name list-based selection
    - Scoped selection within transformation contexts
    """
    
    def __init__(self, name: str = None, **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.selection_type: str = "EXPLICIT"  # "EXPLICIT", "TYPE_BASED", "PATTERN_BASED"
        self.field_names: List[str] = []
        self.data_types: List[PMDataType] = []
        self.name_patterns: List[str] = []
        self.exclude_patterns: List[str] = []
        self.scope_ref: Optional[str] = None  # Reference to transformation scope
        
    def add_field_name(self, field_name: str):
        """Add explicit field name to selection"""
        if field_name not in self.field_names:
            self.field_names.append(field_name)
            
    def add_data_type(self, data_type: PMDataType):
        """Add data type for type-based selection"""
        if data_type not in self.data_types:
            self.data_types.append(data_type)
            
    def add_name_pattern(self, pattern: str):
        """Add name pattern for pattern-based selection"""
        if pattern not in self.name_patterns:
            self.name_patterns.append(pattern)
            
    def matches_field(self, field: XSDTransformationField) -> bool:
        """Check if field matches selector criteria"""
        if self.selection_type == "EXPLICIT":
            return field.name in self.field_names
        elif self.selection_type == "TYPE_BASED":
            return field.data_type in self.data_types
        elif self.selection_type == "PATTERN_BASED":
            import re
            for pattern in self.name_patterns:
                if re.match(pattern, field.name or ""):
                    # Check exclude patterns
                    for exclude_pattern in self.exclude_patterns:
                        if re.match(exclude_pattern, field.name or ""):
                            return False
                    return True
        return False

class XSDDataInterface(NamedElement):
    """
    XSD-compliant data interface for transformations
    
    Represents a data interface with:
    - Field collections and metadata
    - Input/output direction
    - Data groups for complex structures
    - Field selectors for dynamic configuration
    """
    
    def __init__(self, 
                 name: str,
                 interface_type: str = "TransformationDataInterface",
                 **kwargs):
        super().__init__(name=name, **kwargs)
        self.interface_type = interface_type
        
        # Interface direction
        self.input: bool = False
        self.output: bool = False
        
        # Field collections
        self.fields: List[XSDTransformationField] = []
        self.field_groups: Dict[str, List[XSDTransformationField]] = {}
        self.field_selectors: List[XSDFieldSelector] = []
        
        # Interface metadata
        self.display_name: Optional[str] = None
        self.sort_fields: List[str] = []
        self.distribution_fields: List[str] = []
        
        # Lookup tables for efficient access
        self._fields_by_name: Dict[str, XSDTransformationField] = {}
        self._fields_by_id: Dict[str, XSDTransformationField] = {}
        
        self.logger = logging.getLogger(f"DataInterface.{name}")
        
    def add_field(self, field: XSDTransformationField):
        """Add field to interface"""
        self.fields.append(field)
        
        if field.name:
            self._fields_by_name[field.name] = field
        if field.id:
            self._fields_by_id[field.id] = field
            
        self.logger.debug(f"Added field: {field.name} ({field.direction.value})")
        
    def get_field(self, name_or_id: str) -> Optional[XSDTransformationField]:
        """Get field by name or ID"""
        field = self._fields_by_name.get(name_or_id)
        if field:
            return field
        return self._fields_by_id.get(name_or_id)
        
    def get_input_fields(self) -> List[XSDTransformationField]:
        """Get all input fields"""
        return [f for f in self.fields if f.input]
        
    def get_output_fields(self) -> List[XSDTransformationField]:
        """Get all output fields"""
        return [f for f in self.fields if f.output]
        
    def get_derived_fields(self) -> List[XSDTransformationField]:
        """Get all derived/calculated fields"""
        return [f for f in self.fields if f.is_derived]
        
    def add_field_group(self, group_name: str, fields: List[XSDTransformationField]):
        """Add field group for complex data structures"""
        self.field_groups[group_name] = fields
        for field in fields:
            field.group_ref = group_name
            
    def add_field_selector(self, selector: XSDFieldSelector):
        """Add field selector for dynamic configuration"""
        self.field_selectors.append(selector)
        
    def get_selected_fields(self, selector_name: str) -> List[XSDTransformationField]:
        """Get fields matching a specific selector"""
        for selector in self.field_selectors:
            if selector.name == selector_name:
                return [f for f in self.fields if selector.matches_field(f)]
        return []
        
    def validate_interface(self) -> List[str]:
        """Validate interface configuration"""
        errors = []
        
        # Check for duplicate field names
        field_names = [f.name for f in self.fields if f.name]
        if len(field_names) != len(set(field_names)):
            errors.append(f"Interface {self.name} has duplicate field names")
            
        # Validate field constraints
        for field in self.fields:
            field_errors = field.validate_constraints(None)  # Structural validation
            errors.extend(field_errors)
            
        return errors
        
    def get_interface_summary(self) -> Dict[str, Any]:
        """Get interface summary for analysis"""
        return {
            'name': self.name,
            'interface_type': self.interface_type,
            'input': self.input,
            'output': self.output,
            'total_fields': len(self.fields),
            'input_fields': len(self.get_input_fields()),
            'output_fields': len(self.get_output_fields()),
            'derived_fields': len(self.get_derived_fields()),
            'field_groups': len(self.field_groups),
            'field_selectors': len(self.field_selectors)
        }

@dataclass
class TransformationConfiguration:
    """Transformation configuration and metadata"""
    active: bool = True
    class_name: Optional[str] = None
    function_name: Optional[str] = None
    language: LanguageKind = LanguageKind.PYTHON
    partitioning_type: PartitioningKind = PartitioningKind.LOCALLY_PARTITIONABLE
    scope: TransformationScope = TransformationScope.ROW
    output_ordering: OrderingKind = OrderingKind.INPUT_DEPENDENT
    output_deterministic: bool = True
    thread_per_partition: bool = False
    generate_transaction: bool = False
    input_blocking_mandatory: bool = False
    custom_properties: Dict[str, Any] = field(default_factory=dict)

class XSDAbstractTransformation(NamedElement):
    """
    XSD-compliant base class for all transformations
    
    Provides comprehensive transformation framework with:
    - Configuration management
    - Data interface handling
    - Field metadata support
    - Extension points for specific transformation types
    """
    
    def __init__(self, 
                 name: str,
                 transformation_type: str = "Generic",
                 **kwargs):
        super().__init__(name=name, **kwargs)
        self.transformation_type = transformation_type
        
        # Configuration
        self.configuration = TransformationConfiguration()
        
        # Data interfaces
        self.data_interfaces: List[XSDDataInterface] = []
        self.input_interfaces: List[XSDDataInterface] = []
        self.output_interfaces: List[XSDDataInterface] = []
        
        # Interface lookup tables
        self._interfaces_by_name: Dict[str, XSDDataInterface] = {}
        self._interfaces_by_id: Dict[str, XSDDataInterface] = {}
        
        # Transformation metadata
        self.version: str = "1.0"
        self.vendor: Optional[str] = None
        self.category: Optional[str] = None
        self.keywords: List[str] = []
        
        # Extension points
        self.extensions: Dict[str, Any] = {}
        self.custom_properties: Dict[str, Any] = {}
        
        self.logger = logging.getLogger(f"Transformation.{name}")
        
    def add_data_interface(self, interface: XSDDataInterface):
        """Add data interface to transformation"""
        self.data_interfaces.append(interface)
        
        if interface.input:
            self.input_interfaces.append(interface)
        if interface.output:
            self.output_interfaces.append(interface)
            
        if interface.name:
            self._interfaces_by_name[interface.name] = interface
        if interface.id:
            self._interfaces_by_id[interface.id] = interface
            
        self.logger.debug(f"Added interface: {interface.name} ({interface.interface_type})")
        
    def get_interface(self, name_or_id: str) -> Optional[XSDDataInterface]:
        """Get interface by name or ID"""
        interface = self._interfaces_by_name.get(name_or_id)
        if interface:
            return interface
        return self._interfaces_by_id.get(name_or_id)
        
    def get_all_fields(self) -> List[XSDTransformationField]:
        """Get all fields from all interfaces"""
        all_fields = []
        for interface in self.data_interfaces:
            all_fields.extend(interface.fields)
        return all_fields
        
    def get_field_by_name(self, field_name: str) -> Optional[XSDTransformationField]:
        """Get field by name across all interfaces"""
        for interface in self.data_interfaces:
            field = interface.get_field(field_name)
            if field:
                return field
        return None
        
    def validate_transformation(self) -> List[str]:
        """Validate transformation configuration"""
        errors = []
        
        # Validate interfaces
        for interface in self.data_interfaces:
            interface_errors = interface.validate_interface()
            errors.extend(interface_errors)
            
        # Check for required interfaces based on transformation type
        if not self.input_interfaces and self.transformation_type not in ["Source"]:
            errors.append(f"Transformation {self.name} has no input interfaces")
            
        if not self.output_interfaces and self.transformation_type not in ["Target"]:
            errors.append(f"Transformation {self.name} has no output interfaces")
            
        return errors
        
    def get_transformation_metadata(self) -> Dict[str, Any]:
        """Get comprehensive transformation metadata"""
        return {
            'name': self.name,
            'transformation_type': self.transformation_type,
            'version': self.version,
            'vendor': self.vendor,
            'category': self.category,
            'keywords': self.keywords,
            'configuration': {
                'active': self.configuration.active,
                'language': self.configuration.language.value,
                'partitioning_type': self.configuration.partitioning_type.value,
                'scope': self.configuration.scope.value,
                'output_ordering': self.configuration.output_ordering.value,
                'output_deterministic': self.configuration.output_deterministic
            },
            'interfaces': [interface.get_interface_summary() for interface in self.data_interfaces],
            'total_fields': len(self.get_all_fields()),
            'custom_properties': self.custom_properties
        }

# Specialized transformation classes for common Informatica patterns

class XSDSourceTransformation(XSDAbstractTransformation):
    """Source transformation for data reading"""
    
    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, transformation_type="Source", **kwargs)
        
        # Source-specific configuration
        self.connection_ref: Optional[str] = None
        self.source_definition_ref: Optional[str] = None
        self.sql_override: Optional[str] = None
        self.source_filter: Optional[str] = None
        
        # Create default output interface
        output_interface = XSDDataInterface(f"{name}_OUTPUT", "SourceDataInterface")
        output_interface.output = True
        self.add_data_interface(output_interface)

class XSDTargetTransformation(XSDAbstractTransformation):
    """Target transformation for data writing"""
    
    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, transformation_type="Target", **kwargs)
        
        # Target-specific configuration
        self.connection_ref: Optional[str] = None
        self.target_definition_ref: Optional[str] = None
        self.load_scope: LoadScope = LoadScope.ALL
        self.truncate_target: bool = False
        self.bulk_mode: bool = True
        
        # Create default input interface
        input_interface = XSDDataInterface(f"{name}_INPUT", "TargetDataInterface")
        input_interface.input = True
        self.add_data_interface(input_interface)

class XSDLookupTransformation(XSDAbstractTransformation):
    """Lookup transformation with caching support"""
    
    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, transformation_type="Lookup", **kwargs)
        
        # Lookup-specific configuration
        self.lookup_source_ref: Optional[str] = None
        self.dynamic_cache: bool = False
        self.cache_persistent: bool = False
        self.cache_directory: Optional[str] = None
        self.multiple_match: MatchResolutionKind = MatchResolutionKind.RETURN_FIRST
        self.lookup_conditions: List[str] = []
        self.return_fields: List[str] = []
        
        # Create interfaces
        input_interface = XSDDataInterface(f"{name}_INPUT", "GenericDataInterface")
        input_interface.input = True
        
        lookup_interface = XSDDataInterface(f"{name}_LOOKUP", "LookupDataInterface")
        lookup_interface.input = True
        
        output_interface = XSDDataInterface(f"{name}_OUTPUT", "GenericDataInterface")
        output_interface.output = True
        
        self.add_data_interface(input_interface)
        self.add_data_interface(lookup_interface)
        self.add_data_interface(output_interface)

class XSDExpressionTransformation(XSDAbstractTransformation):
    """Expression transformation for calculations and filtering"""
    
    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, transformation_type="Expression", **kwargs)
        
        # Expression-specific configuration
        self.expressions: Dict[str, str] = {}
        self.filter_conditions: List[str] = []
        
        # Create interfaces
        input_interface = XSDDataInterface(f"{name}_INPUT", "GenericDataInterface")
        input_interface.input = True
        
        output_interface = XSDDataInterface(f"{name}_OUTPUT", "GenericDataInterface")
        output_interface.output = True
        
        self.add_data_interface(input_interface)
        self.add_data_interface(output_interface)
        
    def add_expression(self, field_name: str, expression: str):
        """Add expression for field calculation"""
        self.expressions[field_name] = expression
        
        # Add derived field to output interface
        output_interface = self.get_interface(f"{self.name}_OUTPUT")
        if output_interface:
            derived_field = XSDTransformationField(
                name=field_name,
                direction=FieldDirection.OUTPUT
            )
            derived_field.set_expression(expression)
            output_interface.add_field(derived_field)
            
    def add_filter_condition(self, condition: str):
        """Add filter condition"""
        self.filter_conditions.append(condition)

class XSDJoinerTransformation(XSDAbstractTransformation):
    """Joiner transformation for multi-source joins"""
    
    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, transformation_type="Joiner", **kwargs)
        
        # Joiner-specific configuration
        self.join_type: str = "INNER"  # INNER, LEFT, RIGHT, FULL
        self.join_conditions: List[str] = []
        self.master_source: Optional[str] = None
        self.detail_source: Optional[str] = None
        
        # Create interfaces
        master_interface = XSDDataInterface(f"{name}_MASTER", "GenericDataInterface")
        master_interface.input = True
        
        detail_interface = XSDDataInterface(f"{name}_DETAIL", "GenericDataInterface")
        detail_interface.input = True
        
        output_interface = XSDDataInterface(f"{name}_OUTPUT", "GenericDataInterface")
        output_interface.output = True
        
        self.add_data_interface(master_interface)
        self.add_data_interface(detail_interface)
        self.add_data_interface(output_interface)

class XSDAggregatorTransformation(XSDAbstractTransformation):
    """Aggregator transformation for grouping and aggregation"""
    
    def __init__(self, name: str, **kwargs):
        super().__init__(name=name, transformation_type="Aggregator", **kwargs)
        
        # Aggregator-specific configuration
        self.group_by_fields: List[str] = []
        self.aggregation_expressions: Dict[str, str] = {}
        self.sorted_input: bool = False
        
        # Create interfaces
        input_interface = XSDDataInterface(f"{name}_INPUT", "GenericDataInterface")
        input_interface.input = True
        
        output_interface = XSDDataInterface(f"{name}_OUTPUT", "GenericDataInterface")
        output_interface.output = True
        
        self.add_data_interface(input_interface)
        self.add_data_interface(output_interface)
        
    def add_group_by_field(self, field_name: str):
        """Add field to group by clause"""
        if field_name not in self.group_by_fields:
            self.group_by_fields.append(field_name)
            
    def add_aggregation(self, field_name: str, aggregation_expr: str):
        """Add aggregation expression"""
        self.aggregation_expressions[field_name] = aggregation_expr
        
        # Add aggregated field to output interface
        output_interface = self.get_interface(f"{self.name}_OUTPUT")
        if output_interface:
            agg_field = XSDTransformationField(
                name=field_name,
                direction=FieldDirection.OUTPUT
            )
            agg_field.set_expression(aggregation_expr, "AGGREGATION")
            output_interface.add_field(agg_field)

# Utility functions for transformation management

class TransformationRegistry:
    """Registry for transformation types and factory methods"""
    
    def __init__(self):
        self._transformation_types: Dict[str, type] = {}
        self._register_built_in_types()
        
    def _register_built_in_types(self):
        """Register built-in transformation types"""
        self._transformation_types.update({
            "Source": XSDSourceTransformation,
            "Target": XSDTargetTransformation,
            "Lookup": XSDLookupTransformation,
            "Expression": XSDExpressionTransformation,
            "Joiner": XSDJoinerTransformation,
            "Aggregator": XSDAggregatorTransformation,
            "Generic": XSDAbstractTransformation
        })
        
    def register_transformation_type(self, type_name: str, transformation_class: type):
        """Register custom transformation type"""
        self._transformation_types[type_name] = transformation_class
        
    def create_transformation(self, type_name: str, name: str, **kwargs) -> Optional[XSDAbstractTransformation]:
        """Create transformation instance by type"""
        transformation_class = self._transformation_types.get(type_name)
        if transformation_class:
            return transformation_class(name=name, **kwargs)
        return None
        
    def get_supported_types(self) -> List[str]:
        """Get list of supported transformation types"""
        return list(self._transformation_types.keys())

# Global transformation registry instance
transformation_registry = TransformationRegistry()