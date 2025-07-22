"""
Enhanced Parameter System for Informatica-to-Spark Framework
===========================================================

Implements high-priority improvements:
- Type-Aware Parameters: Proper data types (int, float, bool, date, etc.)
- Transformation Scoping: Parameter isolation per transformation
- Parameter Validation: Min/max, regex, and constraint validation
"""

from typing import Dict, Any, Optional, Union, List, Type, Callable
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, date
import re
import logging
from pathlib import Path

class ParameterType(Enum):
    """Enhanced parameter data types"""
    STRING = "string"
    INTEGER = "integer" 
    FLOAT = "float"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"
    LIST = "list"
    DICT = "dict"
    PATH = "path"

class ParameterScope(Enum):
    """Parameter scope hierarchy"""
    GLOBAL = "global"
    PROJECT = "project" 
    WORKFLOW = "workflow"
    MAPPING = "mapping"
    TRANSFORMATION = "transformation"
    SESSION = "session"
    RUNTIME = "runtime"

@dataclass
class ParameterValidation:
    """Parameter validation rules"""
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    regex_pattern: Optional[str] = None
    allowed_values: Optional[List[Any]] = None
    custom_validator: Optional[Callable[[Any], bool]] = None
    error_message: Optional[str] = None

@dataclass 
class EnhancedParameter:
    """Type-aware parameter with validation and scoping"""
    name: str
    param_type: ParameterType
    scope: ParameterScope
    default_value: Any = None
    description: str = ""
    required: bool = False
    validation: Optional[ParameterValidation] = None
    environment_specific: bool = False
    transformation_id: Optional[str] = None  # For transformation-scoped parameters
    depends_on: List[str] = field(default_factory=list)
    
    def __post_init__(self):
        """Validate parameter definition"""
        if self.validation is None:
            self.validation = ParameterValidation()
        
        # Validate default value if provided
        if self.default_value is not None:
            if not self.validate_value(self.default_value):
                raise ValueError(f"Invalid default value for parameter {self.name}: {self.default_value}")
    
    def validate_value(self, value: Any) -> bool:
        """Validate parameter value against type and constraints"""
        try:
            # Type conversion and validation
            converted_value = self._convert_to_type(value)
            
            # Apply validation rules
            return self._apply_validation_rules(converted_value)
            
        except (ValueError, TypeError) as e:
            logging.getLogger("ParameterValidation").error(
                f"Validation failed for parameter {self.name}: {e}"
            )
            return False
    
    def _convert_to_type(self, value: Any) -> Any:
        """Convert value to the correct type"""
        if value is None:
            return None
            
        if self.param_type == ParameterType.STRING:
            return str(value)
        elif self.param_type == ParameterType.INTEGER:
            if isinstance(value, str) and value.isdigit():
                return int(value)
            return int(float(value))  # Handle "100.0" -> 100
        elif self.param_type == ParameterType.FLOAT:
            return float(value)
        elif self.param_type == ParameterType.BOOLEAN:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.lower() in ['true', '1', 'yes', 'on', 'y']
            return bool(value)
        elif self.param_type == ParameterType.DATE:
            if isinstance(value, date):
                return value
            if isinstance(value, str):
                # Try common date formats
                for fmt in ["%Y-%m-%d", "%m/%d/%Y", "%d-%m-%Y"]:
                    try:
                        return datetime.strptime(value, fmt).date()
                    except ValueError:
                        continue
                raise ValueError(f"Invalid date format: {value}")
        elif self.param_type == ParameterType.DATETIME:
            if isinstance(value, datetime):
                return value
            if isinstance(value, str):
                for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                    try:
                        return datetime.strptime(value, fmt)
                    except ValueError:
                        continue
                raise ValueError(f"Invalid datetime format: {value}")
        elif self.param_type == ParameterType.PATH:
            return Path(str(value))
        elif self.param_type == ParameterType.LIST:
            if isinstance(value, list):
                return value
            if isinstance(value, str):
                # Handle comma-separated values
                return [item.strip() for item in value.split(',')]
            return [value]
        elif self.param_type == ParameterType.DICT:
            if isinstance(value, dict):
                return value
            raise ValueError(f"Cannot convert {type(value)} to dict")
        
        return value
    
    def _apply_validation_rules(self, value: Any) -> bool:
        """Apply validation rules to converted value"""
        if value is None:
            return not self.required
            
        validation = self.validation
        
        # Numeric range validation
        if validation.min_value is not None and value < validation.min_value:
            return False
        if validation.max_value is not None and value > validation.max_value:
            return False
            
        # String length validation
        if isinstance(value, str):
            if validation.min_length is not None and len(value) < validation.min_length:
                return False
            if validation.max_length is not None and len(value) > validation.max_length:
                return False
        
        # List length validation
        if isinstance(value, list):
            if validation.min_length is not None and len(value) < validation.min_length:
                return False
            if validation.max_length is not None and len(value) > validation.max_length:
                return False
        
        # Regex pattern validation
        if validation.regex_pattern and isinstance(value, str):
            if not re.match(validation.regex_pattern, value):
                return False
        
        # Allowed values validation
        if validation.allowed_values and value not in validation.allowed_values:
            return False
        
        # Custom validator
        if validation.custom_validator and not validation.custom_validator(value):
            return False
            
        return True
    
    def get_typed_value(self, raw_value: Any) -> Any:
        """Get properly typed and validated value"""
        if not self.validate_value(raw_value):
            if self.validation.error_message:
                raise ValueError(f"Parameter {self.name}: {self.validation.error_message}")
            raise ValueError(f"Invalid value for parameter {self.name}: {raw_value}")
        
        return self._convert_to_type(raw_value)

class TransformationParameterScope:
    """Parameter scope isolation for transformations"""
    
    def __init__(self, transformation_id: str, transformation_name: str):
        self.transformation_id = transformation_id
        self.transformation_name = transformation_name
        self.parameters: Dict[str, EnhancedParameter] = {}
        self.parameter_values: Dict[str, Any] = {}
        self.logger = logging.getLogger(f"TransformationScope.{transformation_name}")
    
    def add_parameter(self, parameter: EnhancedParameter):
        """Add parameter to transformation scope"""
        parameter.transformation_id = self.transformation_id
        parameter.scope = ParameterScope.TRANSFORMATION
        self.parameters[parameter.name] = parameter
    
    def set_parameter_value(self, name: str, value: Any):
        """Set parameter value with validation"""
        if name not in self.parameters:
            raise ValueError(f"Parameter {name} not defined for transformation {self.transformation_name}")
        
        parameter = self.parameters[name]
        typed_value = parameter.get_typed_value(value)
        self.parameter_values[name] = typed_value
        
        self.logger.debug(f"Set parameter {name} = {typed_value} (type: {parameter.param_type.value})")
    
    def get_parameter_value(self, name: str) -> Any:
        """Get typed parameter value"""
        if name in self.parameter_values:
            return self.parameter_values[name]
        
        if name in self.parameters:
            parameter = self.parameters[name]
            if parameter.default_value is not None:
                return parameter.get_typed_value(parameter.default_value)
            if parameter.required:
                raise ValueError(f"Required parameter {name} not set for transformation {self.transformation_name}")
        
        raise KeyError(f"Parameter {name} not found in transformation scope {self.transformation_name}")

class EnhancedParameterManager:
    """Enhanced parameter management with type-awareness and scoping"""
    
    def __init__(self):
        self.logger = logging.getLogger("EnhancedParameterManager")
        
        # Global parameter definitions
        self.global_parameters: Dict[str, EnhancedParameter] = {}
        self.project_parameters: Dict[str, EnhancedParameter] = {}
        self.workflow_parameters: Dict[str, EnhancedParameter] = {}
        self.mapping_parameters: Dict[str, EnhancedParameter] = {}
        
        # Transformation-scoped parameters
        self.transformation_scopes: Dict[str, TransformationParameterScope] = {}
        
        # Parameter values by scope
        self.parameter_values: Dict[ParameterScope, Dict[str, Any]] = {
            scope: {} for scope in ParameterScope
        }
        
        # Initialize built-in parameters
        self._initialize_builtin_parameters()
    
    def _initialize_builtin_parameters(self):
        """Initialize built-in system parameters"""
        
        # System parameters
        self.add_parameter(EnhancedParameter(
            name="SYSTEM_DATE",
            param_type=ParameterType.DATE,
            scope=ParameterScope.GLOBAL,
            default_value=date.today(),
            description="Current system date"
        ))
        
        self.add_parameter(EnhancedParameter(
            name="SYSTEM_DATETIME", 
            param_type=ParameterType.DATETIME,
            scope=ParameterScope.GLOBAL,
            default_value=datetime.now(),
            description="Current system datetime"
        ))
        
        # Common ETL parameters with validation
        self.add_parameter(EnhancedParameter(
            name="BATCH_SIZE",
            param_type=ParameterType.INTEGER,
            scope=ParameterScope.PROJECT,
            default_value=10000,
            description="Batch size for data processing",
            validation=ParameterValidation(
                min_value=100,
                max_value=1000000,
                error_message="Batch size must be between 100 and 1,000,000"
            )
        ))
        
        self.add_parameter(EnhancedParameter(
            name="ERROR_THRESHOLD",
            param_type=ParameterType.FLOAT,
            scope=ParameterScope.PROJECT,
            default_value=0.01,
            description="Maximum error rate (0.0-1.0)",
            validation=ParameterValidation(
                min_value=0.0,
                max_value=1.0,
                error_message="Error threshold must be between 0.0 and 1.0"
            )
        ))
        
        self.add_parameter(EnhancedParameter(
            name="ENVIRONMENT",
            param_type=ParameterType.STRING,
            scope=ParameterScope.GLOBAL,
            default_value="DEVELOPMENT",
            description="Execution environment",
            validation=ParameterValidation(
                allowed_values=["DEVELOPMENT", "TESTING", "STAGING", "PRODUCTION"],
                error_message="Environment must be DEV, TEST, STAGE, or PROD"
            )
        ))
    
    def add_parameter(self, parameter: EnhancedParameter):
        """Add parameter to appropriate scope"""
        if parameter.scope == ParameterScope.GLOBAL:
            self.global_parameters[parameter.name] = parameter
        elif parameter.scope == ParameterScope.PROJECT:
            self.project_parameters[parameter.name] = parameter
        elif parameter.scope == ParameterScope.WORKFLOW:
            self.workflow_parameters[parameter.name] = parameter
        elif parameter.scope == ParameterScope.MAPPING:
            self.mapping_parameters[parameter.name] = parameter
    
    def create_transformation_scope(self, transformation_id: str, transformation_name: str) -> TransformationParameterScope:
        """Create isolated parameter scope for transformation"""
        scope = TransformationParameterScope(transformation_id, transformation_name)
        self.transformation_scopes[transformation_id] = scope
        return scope
    
    def get_transformation_scope(self, transformation_id: str) -> Optional[TransformationParameterScope]:
        """Get transformation parameter scope"""
        return self.transformation_scopes.get(transformation_id)
    
    def set_parameter_value(self, name: str, value: Any, scope: ParameterScope, transformation_id: Optional[str] = None):
        """Set parameter value with type validation"""
        
        # Handle transformation-scoped parameters
        if scope == ParameterScope.TRANSFORMATION and transformation_id:
            if transformation_id in self.transformation_scopes:
                self.transformation_scopes[transformation_id].set_parameter_value(name, value)
                return
            else:
                raise ValueError(f"Transformation scope {transformation_id} not found")
        
        # Find parameter definition
        parameter = self._find_parameter_definition(name, scope)
        if not parameter:
            raise ValueError(f"Parameter {name} not defined in scope {scope.value}")
        
        # Validate and convert value
        typed_value = parameter.get_typed_value(value)
        
        # Store typed value
        self.parameter_values[scope][name] = typed_value
        
        self.logger.debug(f"Set parameter {name} = {typed_value} (type: {parameter.param_type.value}, scope: {scope.value})")
    
    def get_parameter_value(self, name: str, context_scope: ParameterScope = ParameterScope.RUNTIME, transformation_id: Optional[str] = None) -> Any:
        """Get parameter value with scope-based resolution"""
        
        # Handle transformation-scoped lookup first
        if transformation_id and transformation_id in self.transformation_scopes:
            try:
                return self.transformation_scopes[transformation_id].get_parameter_value(name)
            except KeyError:
                # Fall through to hierarchical resolution
                pass
        
        # Parameter resolution hierarchy (highest to lowest priority)
        resolution_order = [
            ParameterScope.RUNTIME,
            ParameterScope.SESSION,
            ParameterScope.TRANSFORMATION,
            ParameterScope.MAPPING, 
            ParameterScope.WORKFLOW,
            ParameterScope.PROJECT,
            ParameterScope.GLOBAL
        ]
        
        # Resolve parameter value hierarchically
        for scope in resolution_order:
            if name in self.parameter_values[scope]:
                return self.parameter_values[scope][name]
        
        # Check for default values
        parameter = self._find_parameter_definition(name)
        if parameter and parameter.default_value is not None:
            return parameter.get_typed_value(parameter.default_value)
        
        # Handle system parameters dynamically
        if name == "SYSTEM_DATE":
            return date.today()
        elif name == "SYSTEM_DATETIME":
            return datetime.now()
        
        raise KeyError(f"Parameter {name} not found in any scope")
    
    def _find_parameter_definition(self, name: str, preferred_scope: Optional[ParameterScope] = None) -> Optional[EnhancedParameter]:
        """Find parameter definition across scopes"""
        
        # Search in preferred scope first
        if preferred_scope:
            scope_map = {
                ParameterScope.GLOBAL: self.global_parameters,
                ParameterScope.PROJECT: self.project_parameters,
                ParameterScope.WORKFLOW: self.workflow_parameters,
                ParameterScope.MAPPING: self.mapping_parameters
            }
            
            if preferred_scope in scope_map and name in scope_map[preferred_scope]:
                return scope_map[preferred_scope][name]
        
        # Search all scopes
        all_scopes = [self.global_parameters, self.project_parameters, 
                     self.workflow_parameters, self.mapping_parameters]
        
        for scope_dict in all_scopes:
            if name in scope_dict:
                return scope_dict[name]
        
        return None
    
    def get_parameters_by_scope(self, scope: ParameterScope) -> Dict[str, Any]:
        """Get all parameters for a specific scope"""
        return self.parameter_values[scope].copy()
    
    def validate_all_parameters(self) -> List[str]:
        """Validate all parameter values, return list of errors"""
        errors = []
        
        for scope, params in self.parameter_values.items():
            for name, value in params.items():
                param_def = self._find_parameter_definition(name)
                if param_def and not param_def.validate_value(value):
                    errors.append(f"Invalid value for {name} in {scope.value}: {value}")
        
        # Validate transformation scopes
        for scope in self.transformation_scopes.values():
            for name, value in scope.parameter_values.items():
                param_def = scope.parameters.get(name)
                if param_def and not param_def.validate_value(value):
                    errors.append(f"Invalid value for {name} in transformation {scope.transformation_name}: {value}")
        
        return errors
    
    def export_typed_config(self) -> Dict[str, Any]:
        """Export parameters as typed configuration dictionary"""
        config = {}
        
        # Export by scope with proper typing
        for scope in ParameterScope:
            scope_params = {}
            for name, value in self.parameter_values[scope].items():
                param_def = self._find_parameter_definition(name, scope)
                if param_def:
                    # Convert dates/datetimes to ISO strings for serialization
                    if isinstance(value, (date, datetime)):
                        scope_params[name] = value.isoformat()
                    else:
                        scope_params[name] = value
                else:
                    scope_params[name] = value
            
            if scope_params:
                config[scope.value] = scope_params
        
        # Export transformation parameters
        transformation_params = {}
        for trans_id, scope in self.transformation_scopes.items():
            trans_params = {}
            for name, value in scope.parameter_values.items():
                if isinstance(value, (date, datetime)):
                    trans_params[name] = value.isoformat()
                else:
                    trans_params[name] = value
            
            if trans_params:
                transformation_params[scope.transformation_name] = trans_params
        
        if transformation_params:
            config['transformations'] = transformation_params
        
        return config 