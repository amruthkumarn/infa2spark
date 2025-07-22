"""
Session configuration framework for Informatica BDM
Manages session lifecycle, configuration validation, and runtime parameter resolution
"""
from typing import List, Dict, Optional, Any, Union, Callable
from enum import Enum
from dataclasses import dataclass
import logging
from datetime import datetime
import copy
import re

from .xsd_session_model import (
    XSDSession, SessionCollection, ExecutionEnvironment, 
    SparkExecutionEnvironment, NativeExecutionEnvironment,
    CommitKind, SessionRecoveryKind, PushdownOptimizationKind
)
from .xsd_base_classes import Element
from .reference_manager import ReferenceManager

class SessionValidationSeverity(Enum):
    """Session validation severity levels"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

class ConfigurationScope(Enum):
    """Configuration parameter scope"""
    SESSION = "session"
    MAPPING = "mapping"
    TRANSFORMATION = "transformation"
    CONNECTION = "connection"
    GLOBAL = "global"

@dataclass
class ValidationResult:
    """Session validation result"""
    severity: SessionValidationSeverity
    message: str
    category: str
    scope: ConfigurationScope
    suggested_fix: Optional[str] = None

@dataclass
class ParameterDefinition:
    """Parameter definition with metadata"""
    name: str
    data_type: str
    default_value: Any = None
    description: str = ""
    required: bool = False
    scope: ConfigurationScope = ConfigurationScope.SESSION
    validation_pattern: Optional[str] = None
    allowed_values: Optional[List[Any]] = None

@dataclass
class SessionExecutionContext:
    """Runtime execution context for sessions"""
    session_id: str
    execution_id: str
    start_time: datetime
    parameters: Dict[str, Any]
    connection_overrides: Dict[str, str]
    environment_config: Dict[str, Any]
    runtime_variables: Dict[str, Any]
    execution_mode: str = "normal"
    
    def get_effective_parameter(self, parameter_name: str, default_value: Any = None) -> Any:
        """Get effective parameter value with fallback"""
        return self.parameters.get(parameter_name, 
                                 self.runtime_variables.get(parameter_name, default_value))

class SessionConfigurationManager:
    """
    Manages session configuration lifecycle and validation
    
    Provides comprehensive session configuration management including:
    - Configuration validation and defaults
    - Parameter resolution and override handling
    - Environment configuration management
    - Configuration templates and inheritance
    """
    
    def __init__(self):
        self.logger = logging.getLogger("SessionConfigurationManager")
        self.parameter_definitions: Dict[str, ParameterDefinition] = {}
        self.configuration_templates: Dict[str, Dict[str, Any]] = {}
        self.validation_rules: List[Callable[[XSDSession], List[ValidationResult]]] = []
        
        # Initialize built-in parameter definitions
        self._initialize_builtin_parameters()
        self._initialize_builtin_templates()
        self._initialize_validation_rules()
        
    def _initialize_builtin_parameters(self):
        """Initialize built-in session parameter definitions"""
        builtin_params = [
            ParameterDefinition(
                name="DTM_BUFFER_POOL_SIZE",
                data_type="integer",
                default_value=12000000,
                description="DTM buffer pool size in bytes",
                scope=ConfigurationScope.SESSION,
                validation_pattern=r"^\d+$"
            ),
            ParameterDefinition(
                name="BUFFER_BLOCK_SIZE",
                data_type="integer", 
                default_value=64000,
                description="Buffer block size in bytes",
                scope=ConfigurationScope.SESSION,
                validation_pattern=r"^\d+$"
            ),
            ParameterDefinition(
                name="COMMIT_INTERVAL",
                data_type="integer",
                default_value=10000,
                description="Commit interval for target commits",
                scope=ConfigurationScope.SESSION,
                validation_pattern=r"^\d+$"
            ),
            ParameterDefinition(
                name="PUSHDOWN_OPTIMIZATION",
                data_type="string",
                default_value="none",
                description="Pushdown optimization strategy",
                scope=ConfigurationScope.SESSION,
                allowed_values=["none", "toSource", "toTarget", "full", "auto"]
            ),
            ParameterDefinition(
                name="SESSION_LOG_LEVEL",
                data_type="string",
                default_value="normal",
                description="Session logging level",
                scope=ConfigurationScope.SESSION,
                allowed_values=["terse", "normal", "verbose"]
            ),
            ParameterDefinition(
                name="SPARK_MASTER",
                data_type="string",
                default_value="local[*]",
                description="Spark master URL",
                scope=ConfigurationScope.SESSION,
                validation_pattern=r"^(local\[\*?\]|local\[\d+\]|spark://.+|yarn|mesos://.+)$"
            ),
            ParameterDefinition(
                name="SPARK_DEPLOY_MODE",
                data_type="string",
                default_value="client",
                description="Spark deployment mode",
                scope=ConfigurationScope.SESSION,
                allowed_values=["client", "cluster"]
            )
        ]
        
        for param in builtin_params:
            self.parameter_definitions[param.name] = param
            
    def _initialize_builtin_templates(self):
        """Initialize built-in configuration templates"""
        self.configuration_templates.update({
            "default": {
                "performance_config.buffer_block_size": 64000,
                "performance_config.dtm_buffer_pool_size": 12000000,
                "performance_config.pushdown_strategy": "none",
                "commit_config.commit_type": "target",
                "commit_config.commit_interval": 10000,
                "error_config.recovery_strategy": "failTask",
                "error_config.maximum_data_errors": 0,
                "error_config.stop_on_errors": True
            },
            
            "high_performance": {
                "performance_config.buffer_block_size": 128000,
                "performance_config.dtm_buffer_pool_size": 64000000,
                "performance_config.pushdown_strategy": "auto",
                "performance_config.constraint_based_load_ordering": True,
                "commit_config.commit_interval": 50000,
                "error_config.maximum_data_errors": 10
            },
            
            "development": {
                "performance_config.buffer_block_size": 32000,
                "performance_config.dtm_buffer_pool_size": 4000000,
                "error_config.recovery_strategy": "failTask",
                "error_config.stop_on_errors": True,
                "error_config.maximum_data_errors": 0,
                "execution_mode": "debug"
            },
            
            "production": {
                "performance_config.pushdown_strategy": "auto",
                "performance_config.constraint_based_load_ordering": True,
                "commit_config.commit_type": "target",
                "error_config.recovery_strategy": "resumeFromLastCheckpoint",
                "error_config.maximum_data_errors": 100,
                "error_config.retry_on_deadlock": True,
                "error_config.retry_attempts": 5
            },
            
            "spark_optimized": {
                "performance_config.pushdown_strategy": "full",
                "performance_config.buffer_block_size": 256000,
                "performance_config.dtm_buffer_pool_size": 128000000,
                "runtime_characteristic.selected_execution_environment_name": "Spark"
            }
        })
        
    def _initialize_validation_rules(self):
        """Initialize built-in validation rules"""
        self.validation_rules.extend([
            self._validate_basic_configuration,
            self._validate_performance_configuration,
            self._validate_commit_configuration,
            self._validate_error_handling_configuration,
            self._validate_environment_configuration,
            self._validate_parameter_consistency
        ])
        
    def register_parameter_definition(self, parameter_def: ParameterDefinition):
        """Register custom parameter definition"""
        self.parameter_definitions[parameter_def.name] = parameter_def
        self.logger.debug(f"Registered parameter definition: {parameter_def.name}")
        
    def register_configuration_template(self, template_name: str, template_config: Dict[str, Any]):
        """Register configuration template"""
        self.configuration_templates[template_name] = template_config
        self.logger.debug(f"Registered configuration template: {template_name}")
        
    def register_validation_rule(self, validation_func: Callable[[XSDSession], List[ValidationResult]]):
        """Register custom validation rule"""
        self.validation_rules.append(validation_func)
        self.logger.debug("Registered custom validation rule")
        
    def create_session_from_template(self, 
                                   session_name: str,
                                   mapping_ref: str,
                                   template_name: str = "default",
                                   overrides: Optional[Dict[str, Any]] = None) -> XSDSession:
        """Create session from configuration template"""
        if template_name not in self.configuration_templates:
            raise ValueError(f"Unknown template: {template_name}")
            
        # Create base session
        session = XSDSession(name=session_name, mapping_ref=mapping_ref)
        
        # Apply template configuration
        template_config = self.configuration_templates[template_name]
        self._apply_configuration_dict(session, template_config)
        
        # Apply overrides
        if overrides:
            self._apply_configuration_dict(session, overrides)
            
        self.logger.info(f"Created session '{session_name}' from template '{template_name}'")
        return session
        
    def _apply_configuration_dict(self, session: XSDSession, config_dict: Dict[str, Any]):
        """Apply configuration dictionary to session"""
        for key, value in config_dict.items():
            if '.' in key:
                # Handle nested property setting (e.g., 'performance_config.buffer_block_size')
                try:
                    obj_path, prop_name = key.rsplit('.', 1)
                    if hasattr(session, obj_path):
                        config_obj = getattr(session, obj_path)
                        if hasattr(config_obj, prop_name):
                            setattr(config_obj, prop_name, value)
                        else:
                            self.logger.warning(f"Property {prop_name} not found in {obj_path}")
                    else:
                        self.logger.warning(f"Configuration object {obj_path} not found")
                except Exception as e:
                    self.logger.error(f"Error setting configuration {key}: {e}")
            else:
                # Handle direct property setting
                if hasattr(session, key):
                    # Handle special enum conversions
                    if key == 'execution_mode' and isinstance(value, str):
                        from .xsd_session_model import ExecutionMode
                        setattr(session, key, ExecutionMode(value))
                    else:
                        setattr(session, key, value)
                else:
                    # Add to custom properties
                    session.custom_properties[key] = value
                    
    def validate_session_configuration(self, session: XSDSession) -> List[ValidationResult]:
        """Comprehensive session configuration validation"""
        all_results = []
        
        # Run all validation rules
        for validation_rule in self.validation_rules:
            try:
                results = validation_rule(session)
                all_results.extend(results)
            except Exception as e:
                self.logger.error(f"Validation rule failed: {e}")
                all_results.append(ValidationResult(
                    severity=SessionValidationSeverity.ERROR,
                    message=f"Validation rule failed: {e}",
                    category="validation_framework",
                    scope=ConfigurationScope.SESSION
                ))
                
        return all_results
        
    def _validate_basic_configuration(self, session: XSDSession) -> List[ValidationResult]:
        """Validate basic session configuration"""
        results = []
        
        if not session.mapping_ref:
            results.append(ValidationResult(
                severity=SessionValidationSeverity.CRITICAL,
                message="Session must reference a mapping",
                category="basic_config",
                scope=ConfigurationScope.SESSION,
                suggested_fix="Set session.mapping_ref to a valid mapping ID"
            ))
            
        if not session.name:
            results.append(ValidationResult(
                severity=SessionValidationSeverity.ERROR,
                message="Session must have a name",
                category="basic_config",
                scope=ConfigurationScope.SESSION
            ))
            
        return results
        
    def _validate_performance_configuration(self, session: XSDSession) -> List[ValidationResult]:
        """Validate performance configuration"""
        results = []
        perf_config = session.performance_config
        
        if perf_config.buffer_block_size <= 0:
            results.append(ValidationResult(
                severity=SessionValidationSeverity.ERROR,
                message="Buffer block size must be positive",
                category="performance",
                scope=ConfigurationScope.SESSION,
                suggested_fix="Set buffer_block_size to a positive value (recommended: 64000)"
            ))
            
        if perf_config.dtm_buffer_pool_size < perf_config.buffer_block_size:
            results.append(ValidationResult(
                severity=SessionValidationSeverity.WARNING,
                message="DTM buffer pool size should be larger than buffer block size",
                category="performance",
                scope=ConfigurationScope.SESSION,
                suggested_fix="Increase dtm_buffer_pool_size or decrease buffer_block_size"
            ))
            
        if perf_config.maximum_auto_mem_total_percent > 90:
            results.append(ValidationResult(
                severity=SessionValidationSeverity.WARNING,
                message="Maximum auto memory percentage is very high",
                category="performance",
                scope=ConfigurationScope.SESSION,
                suggested_fix="Consider reducing maximum_auto_mem_total_percent to 60-80%"
            ))
            
        return results
        
    def _validate_commit_configuration(self, session: XSDSession) -> List[ValidationResult]:
        """Validate commit configuration"""
        results = []
        commit_config = session.commit_config
        
        if (commit_config.commit_type == CommitKind.USER_DEFINED and 
            commit_config.commit_interval <= 0):
            results.append(ValidationResult(
                severity=SessionValidationSeverity.ERROR,
                message="User-defined commit requires positive commit interval",
                category="commit_strategy",
                scope=ConfigurationScope.SESSION,
                suggested_fix="Set commit_interval to a positive value or change commit_type"
            ))
            
        if commit_config.commit_interval > 1000000:
            results.append(ValidationResult(
                severity=SessionValidationSeverity.WARNING,
                message="Very large commit interval may cause memory issues",
                category="commit_strategy", 
                scope=ConfigurationScope.SESSION,
                suggested_fix="Consider reducing commit_interval for better memory management"
            ))
            
        return results
        
    def _validate_error_handling_configuration(self, session: XSDSession) -> List[ValidationResult]:
        """Validate error handling configuration"""
        results = []
        error_config = session.error_config
        
        if error_config.maximum_data_errors < 0:
            results.append(ValidationResult(
                severity=SessionValidationSeverity.ERROR,
                message="Maximum data errors cannot be negative",
                category="error_handling",
                scope=ConfigurationScope.SESSION,
                suggested_fix="Set maximum_data_errors to 0 or positive value"
            ))
            
        if (error_config.recovery_strategy == SessionRecoveryKind.RESUME_FROM_LAST_CHECKPOINT and
            not session.runtime_characteristic.state_store_enabled):
            results.append(ValidationResult(
                severity=SessionValidationSeverity.WARNING,
                message="Checkpoint recovery requires state store to be enabled",
                category="error_handling",
                scope=ConfigurationScope.SESSION,
                suggested_fix="Enable state_store_enabled in runtime_characteristic"
            ))
            
        return results
        
    def _validate_environment_configuration(self, session: XSDSession) -> List[ValidationResult]:
        """Validate execution environment configuration"""
        results = []
        runtime_char = session.runtime_characteristic
        
        if not runtime_char.environments:
            results.append(ValidationResult(
                severity=SessionValidationSeverity.ERROR,
                message="At least one execution environment must be configured",
                category="environment",
                scope=ConfigurationScope.SESSION,
                suggested_fix="Add at least one execution environment"
            ))
            
        selected_env = runtime_char.get_selected_environment()
        if runtime_char.selected_execution_environment_name and not selected_env:
            results.append(ValidationResult(
                severity=SessionValidationSeverity.ERROR,
                message=f"Selected environment '{runtime_char.selected_execution_environment_name}' not found",
                category="environment",
                scope=ConfigurationScope.SESSION,
                suggested_fix="Select an existing environment or add the missing environment"
            ))
            
        return results
        
    def _validate_parameter_consistency(self, session: XSDSession) -> List[ValidationResult]:
        """Validate parameter consistency and definitions"""
        results = []
        
        # Validate parameter overrides against definitions
        for param_name, param_value in session.parameter_overrides.items():
            if param_name in self.parameter_definitions:
                param_def = self.parameter_definitions[param_name]
                
                # Validate allowed values
                if param_def.allowed_values and param_value not in param_def.allowed_values:
                    results.append(ValidationResult(
                        severity=SessionValidationSeverity.ERROR,
                        message=f"Parameter '{param_name}' value '{param_value}' not in allowed values: {param_def.allowed_values}",
                        category="parameter_validation",
                        scope=param_def.scope,
                        suggested_fix=f"Use one of: {', '.join(map(str, param_def.allowed_values))}"
                    ))
                    
                # Validate pattern
                if param_def.validation_pattern and isinstance(param_value, str):
                    if not re.match(param_def.validation_pattern, param_value):
                        results.append(ValidationResult(
                            severity=SessionValidationSeverity.ERROR,
                            message=f"Parameter '{param_name}' value '{param_value}' does not match pattern '{param_def.validation_pattern}'",
                            category="parameter_validation",
                            scope=param_def.scope
                        ))
                        
        return results
        
    def resolve_session_parameters(self, 
                                 session: XSDSession,
                                 runtime_parameters: Optional[Dict[str, Any]] = None) -> SessionExecutionContext:
        """Resolve all session parameters and create execution context"""
        execution_id = f"{session.name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Start with parameter definitions defaults
        resolved_params = {}
        for param_name, param_def in self.parameter_definitions.items():
            if param_def.default_value is not None:
                resolved_params[param_name] = param_def.default_value
                
        # Apply session parameter overrides
        resolved_params.update(session.parameter_overrides)
        
        # Apply runtime parameter overrides
        if runtime_parameters:
            resolved_params.update(runtime_parameters)
            
        # Create runtime variables
        runtime_variables = {
            'SESSION_NAME': session.name,
            'SESSION_ID': session.id or session.name,
            'EXECUTION_ID': execution_id,
            'EXECUTION_TIME': datetime.now().isoformat(),
            'MAPPING_REF': session.mapping_ref
        }
        
        # Get environment configuration
        environment_config = {}
        selected_env = session.runtime_characteristic.get_selected_environment()
        if selected_env:
            environment_config = selected_env.get_environment_summary()
            
        return SessionExecutionContext(
            session_id=session.id or session.name,
            execution_id=execution_id,
            start_time=datetime.now(),
            parameters=resolved_params,
            connection_overrides=session.connection_overrides.copy(),
            environment_config=environment_config,
            runtime_variables=runtime_variables,
            execution_mode=session.execution_mode.value
        )
        
    def get_configuration_summary(self, session: XSDSession) -> Dict[str, Any]:
        """Get comprehensive configuration summary"""
        validation_results = self.validate_session_configuration(session)
        error_count = len([r for r in validation_results if r.severity in [SessionValidationSeverity.ERROR, SessionValidationSeverity.CRITICAL]])
        warning_count = len([r for r in validation_results if r.severity == SessionValidationSeverity.WARNING])
        
        return {
            'session_name': session.name,
            'session_id': session.id,
            'mapping_ref': session.mapping_ref,
            'configuration_valid': error_count == 0,
            'validation_summary': {
                'total_issues': len(validation_results),
                'errors': error_count,
                'warnings': warning_count,
                'categories': list(set(r.category for r in validation_results))
            },
            'effective_configuration': session.get_effective_configuration(),
            'parameter_definitions_used': len([p for p in session.parameter_overrides.keys() if p in self.parameter_definitions]),
            'custom_parameters': len([p for p in session.parameter_overrides.keys() if p not in self.parameter_definitions]),
            'execution_environments': len(session.runtime_characteristic.environments),
            'selected_environment': session.runtime_characteristic.selected_execution_environment_name
        }

class SessionLifecycleManager:
    """
    Manages session lifecycle operations
    
    Handles session creation, configuration, validation, and execution preparation
    """
    
    def __init__(self, reference_manager: ReferenceManager):
        self.reference_manager = reference_manager
        self.config_manager = SessionConfigurationManager()
        self.session_collection = SessionCollection()
        self.logger = logging.getLogger("SessionLifecycleManager")
        
    def create_session(self,
                      session_name: str,
                      mapping_ref: str,
                      template: str = "default",
                      overrides: Optional[Dict[str, Any]] = None) -> XSDSession:
        """Create and register new session"""
        session = self.config_manager.create_session_from_template(
            session_name, mapping_ref, template, overrides
        )
        
        # Set default execution environment if none configured
        if not session.runtime_characteristic.environments:
            default_env = SparkExecutionEnvironment()
            session.runtime_characteristic.add_execution_environment(default_env)
            
        # Register session
        self.session_collection.add(session)
        self.reference_manager.register_object(session)
        
        self.logger.info(f"Created and registered session: {session_name}")
        return session
        
    def validate_session(self, session: XSDSession) -> List[ValidationResult]:
        """Validate session configuration"""
        return self.config_manager.validate_session_configuration(session)
        
    def prepare_session_for_execution(self,
                                    session: XSDSession,
                                    runtime_parameters: Optional[Dict[str, Any]] = None) -> SessionExecutionContext:
        """Prepare session for execution with full validation"""
        # Validate configuration
        validation_results = self.validate_session(session)
        errors = [r for r in validation_results if r.severity in [SessionValidationSeverity.ERROR, SessionValidationSeverity.CRITICAL]]
        
        if errors:
            error_messages = [f"{r.category}: {r.message}" for r in errors]
            raise ValueError(f"Session validation failed: {'; '.join(error_messages)}")
            
        # Create execution context
        context = self.config_manager.resolve_session_parameters(session, runtime_parameters)
        
        self.logger.info(f"Prepared session '{session.name}' for execution: {context.execution_id}")
        return context
        
    def get_session_by_name(self, session_name: str) -> Optional[XSDSession]:
        """Get session by name"""
        return self.session_collection.get_by_name(session_name)
        
    def get_sessions_for_mapping(self, mapping_ref: str) -> List[XSDSession]:
        """Get all sessions for a mapping"""
        return self.session_collection.get_sessions_for_mapping(mapping_ref)
        
    def validate_all_sessions(self) -> Dict[str, List[ValidationResult]]:
        """Validate all registered sessions"""
        validation_results = {}
        
        for session in self.session_collection.list_by_type(XSDSession):
            results = self.validate_session(session)
            if results:
                validation_results[session.name] = results
                
        return validation_results