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

class XSDSequenceTransformation(XSDAbstractTransformation):
    """
    XSD-compliant Sequence transformation for unique ID generation
    
    Generates sequential numbers with configurable parameters:
    - Start value, end value, increment
    - Cycling behavior when reaching limits
    - State management for consistency across sessions
    """
    
    def __init__(self, name: str, **kwargs):
        # Extract transformation-specific kwargs before calling super
        self.start_value: int = kwargs.pop('start_value', 1)
        self.end_value: int = kwargs.pop('end_value', 999999999)
        self.increment_value: int = kwargs.pop('increment_value', 1)
        self.cycle: bool = kwargs.pop('cycle', False)
        self.current_value: Optional[int] = None
        self.state_identifier: str = kwargs.pop('state_identifier', f"seq_{name}")
        
        super().__init__(name, "Sequence", **kwargs)
        
        # Sequence field configuration
        self.sequence_field_name: str = kwargs.get('sequence_field_name', 'NEXTVAL')
        self.sequence_field_precision: int = kwargs.get('sequence_field_precision', 10)
        
        # Add sequence output field
        sequence_field = XSDTransformationField(
            name=self.sequence_field_name,
            data_type=PMDataType.SQL_BIGINT,
            direction=FieldDirection.OUTPUT,
            precision=self.sequence_field_precision,
            scale=0
        )
        
        # Create output interface
        output_interface = XSDDataInterface(
            name=f"{name}_OUT",
            interface_type="Output"
        )
        output_interface.add_field(sequence_field)
        self.add_data_interface(output_interface)
        
    def generate_spark_code(self) -> str:
        """Generate Spark code for sequence transformation"""
        return f'''
    def apply_sequence_transformation(self, input_df: DataFrame) -> DataFrame:
        """
        Generate sequential numbers using optimized Spark operations
        
        Configuration:
        - Start Value: {self.start_value}
        - End Value: {self.end_value}
        - Increment: {self.increment_value}
        - Cycle: {self.cycle}
        """
        from pyspark.sql.functions import monotonically_increasing_id, row_number, lit
        from pyspark.sql.window import Window
        
        self.logger.info("Applying Sequence transformation: {self.name}")
        
        # Method 1: Fast monotonic ID (for simple cases)
        if {self.increment_value} == 1 and not {str(self.cycle).lower()}:
            # Use Spark's built-in monotonic ID for best performance
            result_df = input_df.withColumn(
                "{self.sequence_field_name}",
                monotonically_increasing_id() + {self.start_value}
            )
        else:
            # Method 2: Precise sequence with row_number (handles increment and cycling)
            window_spec = Window.orderBy(lit(1))  # Deterministic ordering
            
            if {self.increment_value} == 1:
                # Simple row number sequence
                result_df = input_df.withColumn(
                    "{self.sequence_field_name}",
                    row_number().over(window_spec) + {self.start_value} - 1
                )
            else:
                # Custom increment sequence
                result_df = input_df.withColumn(
                    "{self.sequence_field_name}",
                    (row_number().over(window_spec) - 1) * {self.increment_value} + {self.start_value}
                )
            
            # Handle cycling if enabled
            if {str(self.cycle).lower()}:
                from pyspark.sql.functions import when, col
                range_size = {self.end_value} - {self.start_value} + 1
                result_df = result_df.withColumn(
                    "{self.sequence_field_name}",
                    when(col("{self.sequence_field_name}") > {self.end_value},
                         ((col("{self.sequence_field_name}") - {self.start_value}) % range_size) + {self.start_value}
                    ).otherwise(col("{self.sequence_field_name}"))
                )
        
        # Log sequence statistics
        total_records = input_df.count()
        self.logger.info(f"Generated {{total_records}} sequence values")
        
        return result_df
        '''
    
    def validate_configuration(self) -> List[str]:
        """Validate sequence configuration"""
        errors = super().validate_transformation()
        
        if self.start_value >= self.end_value:
            errors.append(f"Start value ({self.start_value}) must be less than end value ({self.end_value})")
        
        if self.increment_value <= 0:
            errors.append(f"Increment value ({self.increment_value}) must be positive")
            
        if not self.cycle and self.increment_value > 1:
            max_records = (self.end_value - self.start_value) // self.increment_value + 1
            self.logger.warning(f"Non-cycling sequence can generate maximum {max_records} values")
        
        return errors


class XSDSorterTransformation(XSDAbstractTransformation):
    """
    XSD-compliant Sorter transformation for data ordering
    
    Sorts input data based on multiple sort keys with configurable:
    - Sort directions (ascending/descending)
    - Null handling (first/last)
    - Case sensitivity for string sorting
    """
    
    def __init__(self, name: str, **kwargs):
        # Extract transformation-specific kwargs before calling super
        self.sort_keys: List[Dict[str, Any]] = kwargs.pop('sort_keys', [])
        self.case_sensitive: bool = kwargs.pop('case_sensitive', True)
        self.distinct_records: bool = kwargs.pop('distinct_records', False)
        self.num_partitions: Optional[int] = kwargs.pop('num_partitions', None)
        self.sort_buffer_size: str = kwargs.pop('sort_buffer_size', "64MB")
        
        super().__init__(name, "Sorter", **kwargs)
        
    def add_sort_key(self, field_name: str, direction: str = "ASC", null_treatment: str = "LAST"):
        """Add a sort key configuration"""
        self.sort_keys.append({
            'field_name': field_name,
            'direction': direction.upper(),
            'null_treatment': null_treatment.upper()
        })
    
    def generate_spark_code(self) -> str:
        """Generate Spark code for sorter transformation"""
        sort_expressions = []
        
        for sort_key in self.sort_keys:
            field_name = sort_key['field_name']
            direction = sort_key.get('direction', 'ASC')
            null_treatment = sort_key.get('null_treatment', 'LAST')
            
            if direction == 'ASC':
                if null_treatment == 'FIRST':
                    sort_expressions.append(f'col("{field_name}").asc_nulls_first()')
                else:
                    sort_expressions.append(f'col("{field_name}").asc_nulls_last()')
            else:
                if null_treatment == 'FIRST':
                    sort_expressions.append(f'col("{field_name}").desc_nulls_first()')
                else:
                    sort_expressions.append(f'col("{field_name}").desc_nulls_last()')
        
        sort_expr_str = ', '.join(sort_expressions)
        
        return f'''
    def apply_sorter_transformation(self, input_df: DataFrame) -> DataFrame:
        """
        Sort data based on configured sort keys
        
        Sort Keys: {self.sort_keys}
        Case Sensitive: {self.case_sensitive}
        Distinct Records: {self.distinct_records}
        """
        from pyspark.sql.functions import col, lower
        
        self.logger.info("Applying Sorter transformation: {self.name}")
        
        if not {self.sort_keys}:
            self.logger.warning("No sort keys defined, returning unsorted data")
            return input_df
        
        # Apply sorting
        result_df = input_df.orderBy({sort_expr_str})
        
        # Apply distinct if requested
        if {str(self.distinct_records).lower()}:
            result_df = result_df.distinct()
            self.logger.info("Applied distinct operation")
        
        # Optimize partitioning if specified
        if {self.num_partitions}:
            result_df = result_df.coalesce({self.num_partitions})
            self.logger.info(f"Coalesced to {{self.num_partitions}} partitions")
        
        return result_df
        '''


class XSDRouterTransformation(XSDAbstractTransformation):
    """
    XSD-compliant Router transformation for conditional data routing
    
    Routes input data to multiple output groups based on conditions:
    - Multiple output groups with filter conditions
    - Default group for unmatched records
    - Priority-based condition evaluation
    """
    
    def __init__(self, name: str, **kwargs):
        # Extract transformation-specific kwargs before calling super
        self.output_groups: List[Dict[str, Any]] = kwargs.pop('output_groups', [])
        self.default_group: str = kwargs.pop('default_group', 'DEFAULT')
        self.evaluation_order: str = kwargs.pop('evaluation_order', 'PRIORITY')  # PRIORITY or SEQUENTIAL
        
        super().__init__(name, "Router", **kwargs)
        
        # Create output interfaces for each group
        for group in self.output_groups:
            group_name = group.get('name', f'GROUP_{len(self.output_interfaces)}')
            output_interface = XSDDataInterface(
                name=f"{name}_{group_name}_OUT",
                interface_type="Output"
            )
            self.add_data_interface(output_interface)
        
        # Add default group interface
        default_interface = XSDDataInterface(
            name=f"{name}_{self.default_group}_OUT",
            interface_type="Output"
        )
        self.add_data_interface(default_interface)
    
    def add_output_group(self, name: str, condition: str, priority: int = 1):
        """Add an output group with routing condition"""
        self.output_groups.append({
            'name': name,
            'condition': condition,
            'priority': priority
        })
    
    def generate_spark_code(self) -> str:
        """Generate Spark code for router transformation"""
        return f'''
    def apply_router_transformation(self, input_df: DataFrame) -> Dict[str, DataFrame]:
        """
        Route data to multiple outputs based on conditions
        
        Output Groups: {self.output_groups}
        Default Group: {self.default_group}
        Evaluation Order: {self.evaluation_order}
        """
        from pyspark.sql.functions import when, lit, col
        
        self.logger.info("Applying Router transformation: {self.name}")
        
        router_results = {{}}
        remaining_df = input_df
        
        # Sort groups by priority if using priority-based evaluation
        groups = {self.output_groups}
        if "{self.evaluation_order}" == "PRIORITY":
            groups = sorted(groups, key=lambda x: x.get('priority', 1))
        
        # Process each output group
        for group in groups:
            group_name = group['name']
            condition = group['condition']
            
            self.logger.info(f"Processing router group: {{group_name}}")
            
            # Filter records matching the condition
            matched_df = remaining_df.filter(condition)
            router_results[group_name] = matched_df
            
            # Remove matched records from remaining data (for sequential evaluation)
            if "{self.evaluation_order}" == "SEQUENTIAL":
                remaining_df = remaining_df.filter(f"NOT ({{condition}})")
            
            # Log group statistics
            matched_count = matched_df.count()
            self.logger.info(f"Group {{group_name}} matched {{matched_count}} records")
        
        # Add remaining records to default group
        if "{self.evaluation_order}" == "SEQUENTIAL":
            router_results["{self.default_group}"] = remaining_df
        else:
            # For priority evaluation, default gets records not matching any condition
            default_condition = " AND ".join([f"NOT ({{g['condition']}})" for g in groups])
            router_results["{self.default_group}"] = input_df.filter(default_condition)
        
        default_count = router_results["{self.default_group}"].count()
        self.logger.info(f"Default group {{'{self.default_group}'}} received {{default_count}} records")
        
        return router_results
        '''


class XSDUnionTransformation(XSDAbstractTransformation):
    """
    XSD-compliant Union transformation for combining multiple data sources
    
    Combines multiple input DataFrames with:
    - Schema alignment and column mapping
    - Union All vs Union Distinct operations
    - Data type harmonization
    - Source tracking columns
    """
    
    def __init__(self, name: str, **kwargs):
        # Extract transformation-specific kwargs before calling super
        self.union_type: str = kwargs.pop('union_type', 'UNION_ALL')  # UNION_ALL or UNION_DISTINCT
        self.schema_alignment: str = kwargs.pop('schema_alignment', 'BY_NAME')  # BY_NAME or BY_POSITION
        self.add_source_info: bool = kwargs.pop('add_source_info', True)
        self.source_field_name: str = kwargs.pop('source_field_name', 'SOURCE_SYSTEM')
        self.input_sources: List[Dict[str, Any]] = kwargs.pop('input_sources', [])
        
        super().__init__(name, "Union", **kwargs)
        
        # Create multiple input interfaces
        for i, source in enumerate(self.input_sources):
            source_name = source.get('name', f'INPUT_{i+1}')
            input_interface = XSDDataInterface(
                name=f"{name}_{source_name}_IN",
                interface_type="Input"
            )
            self.add_data_interface(input_interface)
    
    def add_input_source(self, name: str, column_mapping: Dict[str, str] = None):
        """Add an input source with optional column mapping"""
        self.input_sources.append({
            'name': name,
            'column_mapping': column_mapping or {},
            'source_identifier': len(self.input_sources) + 1
        })
    
    def generate_spark_code(self) -> str:
        """Generate Spark code for union transformation"""
        return f'''
    def apply_union_transformation(self, *input_dataframes) -> DataFrame:
        """
        Combine multiple DataFrames using union operation
        
        Union Type: {self.union_type}
        Schema Alignment: {self.schema_alignment}
        Add Source Info: {self.add_source_info}
        Input Sources: {len(self.input_sources)}
        """
        from pyspark.sql.functions import lit, col
        from pyspark.sql.types import StructType, StructField, StringType
        
        self.logger.info("Applying Union transformation: {self.name}")
        
        if len(input_dataframes) < 2:
            self.logger.warning("Union requires at least 2 input DataFrames")
            return input_dataframes[0] if input_dataframes else None
        
        # Analyze schemas and create unified schema
        unified_schema = self._create_unified_schema(*input_dataframes)
        aligned_dataframes = []
        
        # Process each input DataFrame
        for i, df in enumerate(input_dataframes):
            source_config = {self.input_sources}[i] if i < len({self.input_sources}) else {{'name': f'INPUT_{{i+1}}'}}
            source_name = source_config.get('name', f'INPUT_{{i+1}}')
            column_mapping = source_config.get('column_mapping', {{}})
            
            self.logger.info(f"Processing input source: {{source_name}}")
            
            # Apply column mapping if specified
            mapped_df = df
            if column_mapping:
                for source_col, target_col in column_mapping.items():
                    if source_col in df.columns:
                        mapped_df = mapped_df.withColumnRenamed(source_col, target_col)
            
            # Align schema with unified schema
            aligned_df = self._align_dataframe_schema(mapped_df, unified_schema)
            
            # Add source tracking column if requested
            if {str(self.add_source_info).lower()}:
                aligned_df = aligned_df.withColumn("{self.source_field_name}", lit(source_name))
            
            aligned_dataframes.append(aligned_df)
            
            # Log source statistics
            source_count = df.count()
            self.logger.info(f"Source {{source_name}} contributed {{source_count}} records")
        
        # Perform union operation
        result_df = aligned_dataframes[0]
        for df in aligned_dataframes[1:]:
            result_df = result_df.union(df)
        
        # Apply distinct if union distinct is requested
        if "{self.union_type}" == "UNION_DISTINCT":
            result_df = result_df.distinct()
            self.logger.info("Applied distinct operation for UNION_DISTINCT")
        
        # Log final statistics
        final_count = result_df.count()
        self.logger.info(f"Union transformation produced {{final_count}} total records")
        
        return result_df
    
    def _create_unified_schema(self, *dataframes) -> StructType:
        """Create unified schema from all input DataFrames"""
        all_columns = set()
        for df in dataframes:
            all_columns.update(df.columns)
        
        # Create unified schema (simplified - in production would handle data types)
        unified_fields = [StructField(col_name, StringType(), True) for col_name in sorted(all_columns)]
        return StructType(unified_fields)
    
    def _align_dataframe_schema(self, df: DataFrame, target_schema: StructType) -> DataFrame:
        """Align DataFrame schema to target unified schema"""
        aligned_df = df
        
        # Add missing columns with null values
        for field in target_schema.fields:
            if field.name not in df.columns:
                aligned_df = aligned_df.withColumn(field.name, lit(None).cast(field.dataType))
        
        # Select columns in target schema order
        target_columns = [field.name for field in target_schema.fields]
        aligned_df = aligned_df.select(*target_columns)
        
        return aligned_df
        '''

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
            "Sequence": XSDSequenceTransformation,
            "Sorter": XSDSorterTransformation,
            "Router": XSDRouterTransformation,
            "Union": XSDUnionTransformation,
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