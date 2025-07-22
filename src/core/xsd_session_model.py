"""
XSD-compliant session model classes
Based on com.informatica.metadata.common.session.xsd

This module implements the comprehensive Informatica session framework with:
- Complete session configuration management
- Runtime environment specifications
- Commit strategy implementation
- Performance and buffer management
- Error handling and recovery strategies
"""
from typing import List, Dict, Optional, Any, Union
from enum import Enum
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import logging
from datetime import datetime

from .xsd_base_classes import NamedElement, Element, ElementCollection
from .reference_manager import ReferenceManager

class CommitKind(Enum):
    """Session commit strategy types"""
    SOURCE = "source"
    TARGET = "target"
    USER_DEFINED = "userDefined"

class SessionRecoveryKind(Enum):
    """Session recovery strategy types"""
    RESUME_FROM_LAST_CHECKPOINT = "resumeFromLastCheckpoint"
    RESTART_TASK = "restartTask"
    FAIL_TASK = "failTask"
    SKIP_TO_NEXT_TASK = "skipToNextTask"

class PushdownOptimizationKind(Enum):
    """Pushdown optimization strategy types"""
    NONE = "none"
    TO_SOURCE = "toSource"
    TO_TARGET = "toTarget"
    FULL = "full"
    AUTO = "auto"

class DefaultUpdateStrategyKind(Enum):
    """Default update strategy for target operations"""
    INSERT_ROW = "insertRow"
    UPDATE_ROW = "updateRow"
    DELETE_ROW = "deleteRow"
    REJECT_ROW = "rejectRow"
    DATA_DRIVEN = "dataDriven"

class UnicodeSortOrderKind(Enum):
    """Unicode sort order types"""
    BINARY = "binary"
    LINGUISTIC = "linguistic"
    AUTO = "auto"

class SparkDeployMode(Enum):
    """Spark deployment modes"""
    CLIENT = "client"
    CLUSTER = "cluster"

class ExecutionMode(Enum):
    """Session execution modes"""
    NORMAL = "normal"
    DEBUG = "debug"
    RECOVERY = "recovery"
    VALIDATION = "validation"

@dataclass
class SessionPerformanceConfig:
    """Session performance configuration settings"""
    buffer_block_size: int = 64000
    dtm_buffer_pool_size: int = 12000000
    dtm_buffer_size: int = 64000
    maximum_auto_mem_bytes: Optional[int] = None
    maximum_auto_mem_total_percent: int = 60
    constraint_based_load_ordering: bool = False
    
    # Advanced performance settings
    cache_compute_time: bool = True
    pipeline_buffer_size: int = 32768
    pushdown_strategy: PushdownOptimizationKind = PushdownOptimizationKind.NONE
    
@dataclass
class SessionCommitConfig:
    """Session commit strategy configuration"""
    commit_type: CommitKind = CommitKind.TARGET
    commit_interval: int = 10000
    commit_on_end_of_file: bool = True
    target_commit_interval: Optional[int] = None
    
    # Transaction handling
    enable_transactions: bool = True
    transaction_timeout: Optional[int] = None

@dataclass
class SessionErrorConfig:
    """Session error handling and recovery configuration"""
    recovery_strategy: SessionRecoveryKind = SessionRecoveryKind.FAIL_TASK
    maximum_data_errors: int = 0
    stop_on_errors: bool = True
    retry_on_deadlock: bool = True
    retry_attempts: int = 3
    retry_delay_seconds: int = 30
    
    # Pre/post session task error handling
    fail_on_any_pre_session_task_error: bool = False
    fail_on_any_post_session_task_error: bool = False
    fail_on_any_tx_handler_setup_error: bool = False
    fail_on_any_tx_handler_teardown_error: bool = False

@dataclass
class SessionDataConfig:
    """Session data processing configuration"""
    default_update_strategy: DefaultUpdateStrategyKind = DefaultUpdateStrategyKind.INSERT_ROW
    default_date_format: str = "MM/DD/YYYY HH24:MI:SS.US"
    high_precision_enabled: bool = False
    sort_order: UnicodeSortOrderKind = UnicodeSortOrderKind.BINARY
    
    # Data quality settings
    enable_null_value_processing: bool = True
    null_character_replacement: str = ""
    
class ExecutionEnvironment(Element):
    """Base execution environment for mapping execution"""
    
    def __init__(self, 
                 environment_name: str,
                 version: str = "1.0",
                 **kwargs):
        super().__init__(**kwargs)
        self.environment_name = environment_name
        self.version = version
        self.description: Optional[str] = None
        self.is_default: bool = False
        
        # Resource configuration
        self.memory_allocation: Optional[int] = None
        self.cpu_cores: Optional[int] = None
        self.temporary_directory: Optional[str] = None
        
        self.logger = logging.getLogger(f"ExecutionEnvironment.{environment_name}")
        
    @abstractmethod
    def validate_configuration(self) -> List[str]:
        """Validate environment configuration"""
        pass
        
    @abstractmethod
    def get_environment_summary(self) -> Dict[str, Any]:
        """Get environment configuration summary"""
        pass

class NativeExecutionEnvironment(ExecutionEnvironment):
    """Native Informatica execution environment"""
    
    def __init__(self, environment_name: str = "Native", **kwargs):
        super().__init__(environment_name=environment_name, **kwargs)
        
        # Native-specific configuration
        self.partitioning_enabled: bool = False
        self.max_partitions: int = 1
        self.load_balancing_enabled: bool = True
        
        # Integration service configuration
        self.integration_service_name: Optional[str] = None
        self.grid_configuration: Optional[str] = None
        
    def validate_configuration(self) -> List[str]:
        """Validate native environment configuration"""
        errors = []
        
        if self.max_partitions < 1:
            errors.append("Max partitions must be at least 1")
            
        if self.partitioning_enabled and self.max_partitions == 1:
            errors.append("Partitioning enabled but max partitions is 1")
            
        return errors
        
    def get_environment_summary(self) -> Dict[str, Any]:
        """Get native environment summary"""
        return {
            'environment_name': self.environment_name,
            'environment_type': 'Native',
            'version': self.version,
            'partitioning_enabled': self.partitioning_enabled,
            'max_partitions': self.max_partitions,
            'load_balancing_enabled': self.load_balancing_enabled,
            'integration_service': self.integration_service_name
        }

class SparkExecutionEnvironment(ExecutionEnvironment):
    """Spark-based execution environment"""
    
    def __init__(self, environment_name: str = "Spark", **kwargs):
        super().__init__(environment_name=environment_name, **kwargs)
        
        # Spark configuration
        self.deploy_mode: SparkDeployMode = SparkDeployMode.CLIENT
        self.master: str = "local[*]"
        self.application_name: Optional[str] = None
        
        # Spark-specific settings
        self.event_log_enabled: bool = True
        self.event_log_dir: Optional[str] = None
        self.hdfs_staging_dir: Optional[str] = None
        self.dynamic_allocation_enabled: bool = False
        
        # Spark configuration properties
        self.spark_conf: Dict[str, str] = {}
        
        # Default Spark configurations for Informatica workloads
        self._set_default_spark_config()
        
    def _set_default_spark_config(self):
        """Set default Spark configuration for Informatica workloads"""
        self.spark_conf.update({
            'spark.sql.adaptive.enabled': 'true',
            'spark.sql.adaptive.coalescePartitions.enabled': 'true',
            'spark.sql.execution.arrow.pyspark.enabled': 'true',
            'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
            'spark.sql.session.timeZone': 'UTC',
            'spark.sql.parquet.int96RebaseModeInWrite': 'CORRECTED'
        })
        
    def add_spark_config(self, key: str, value: str):
        """Add Spark configuration property"""
        self.spark_conf[key] = value
        
    def remove_spark_config(self, key: str):
        """Remove Spark configuration property"""
        if key in self.spark_conf:
            del self.spark_conf[key]
            
    def validate_configuration(self) -> List[str]:
        """Validate Spark environment configuration"""
        errors = []
        
        if not self.master:
            errors.append("Spark master must be specified")
            
        if self.event_log_enabled and not self.event_log_dir:
            errors.append("Event log directory required when event logging is enabled")
            
        if self.deploy_mode == SparkDeployMode.CLUSTER and self.master.startswith("local"):
            errors.append("Cluster deploy mode incompatible with local master")
            
        return errors
        
    def get_environment_summary(self) -> Dict[str, Any]:
        """Get Spark environment summary"""
        return {
            'environment_name': self.environment_name,
            'environment_type': 'Spark',
            'version': self.version,
            'deploy_mode': self.deploy_mode.value,
            'master': self.master,
            'application_name': self.application_name,
            'event_log_enabled': self.event_log_enabled,
            'dynamic_allocation': self.dynamic_allocation_enabled,
            'spark_config_count': len(self.spark_conf)
        }

class RuntimeCharacteristic(Element):
    """Runtime configuration for mappings and sessions"""
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        
        # Execution environments
        self.environments: List[ExecutionEnvironment] = []
        self.selected_execution_environment_name: Optional[str] = None
        
        # Runtime performance settings
        self.maximum_data_errors: int = 0
        self.maximum_rows_read_per_partition: Optional[int] = None
        self.maximum_runtime_interval_per_source: Optional[int] = None
        self.target_commit_interval: Optional[int] = None
        
        # State and audit configuration
        self.state_store_enabled: bool = False
        self.audit_enabled: bool = False
        self.audit_level: str = "normal"  # normal, verbose, terse
        
        # Streaming configuration
        self.streaming_enabled: bool = False
        self.streaming_window_size: Optional[int] = None
        self.streaming_checkpoint_dir: Optional[str] = None
        
        self.logger = logging.getLogger("RuntimeCharacteristic")
        
    def add_execution_environment(self, environment: ExecutionEnvironment):
        """Add execution environment"""
        self.environments.append(environment)
        if not self.selected_execution_environment_name:
            self.selected_execution_environment_name = environment.environment_name
            
    def get_selected_environment(self) -> Optional[ExecutionEnvironment]:
        """Get currently selected execution environment"""
        for env in self.environments:
            if env.environment_name == self.selected_execution_environment_name:
                return env
        return None
        
    def set_selected_environment(self, environment_name: str) -> bool:
        """Set selected execution environment"""
        for env in self.environments:
            if env.environment_name == environment_name:
                self.selected_execution_environment_name = environment_name
                return True
        return False
        
    def validate_runtime_characteristic(self) -> List[str]:
        """Validate runtime characteristic configuration"""
        errors = []
        
        if not self.environments:
            errors.append("At least one execution environment must be configured")
            
        if self.selected_execution_environment_name:
            selected_env = self.get_selected_environment()
            if not selected_env:
                errors.append(f"Selected environment '{self.selected_execution_environment_name}' not found")
            else:
                env_errors = selected_env.validate_configuration()
                errors.extend([f"Environment '{selected_env.environment_name}': {err}" for err in env_errors])
                
        if self.streaming_enabled and not self.streaming_checkpoint_dir:
            errors.append("Streaming checkpoint directory required when streaming is enabled")
            
        return errors

class XSDSession(NamedElement):
    """
    XSD-compliant Session class with comprehensive configuration support
    
    Represents a complete Informatica session with all runtime configuration,
    performance tuning, error handling, and execution environment settings.
    """
    
    def __init__(self, 
                 name: str,
                 mapping_ref: str = None,
                 **kwargs):
        super().__init__(name=name, **kwargs)
        
        # Core session references
        self.mapping_ref = mapping_ref  # idref to mapping
        self.workflow_ref: Optional[str] = None  # idref to parent workflow
        
        # Session configuration components
        self.performance_config = SessionPerformanceConfig()
        self.commit_config = SessionCommitConfig()
        self.error_config = SessionErrorConfig()
        self.data_config = SessionDataConfig()
        
        # Runtime characteristics
        self.runtime_characteristic = RuntimeCharacteristic()
        
        # Session metadata
        self.version: str = "1.0"
        self.is_enabled: bool = True
        self.execution_mode: ExecutionMode = ExecutionMode.NORMAL
        
        # Session timing and scheduling
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.estimated_duration: Optional[int] = None  # in seconds
        
        # Custom properties and overrides
        self.parameter_overrides: Dict[str, Any] = {}
        self.connection_overrides: Dict[str, str] = {}  # connection name -> override connection
        self.custom_properties: Dict[str, Any] = {}
        
        # Session status tracking
        self.last_execution_status: Optional[str] = None
        self.last_execution_time: Optional[datetime] = None
        self.execution_count: int = 0
        
        self.logger = logging.getLogger(f"Session.{name}")
        
    def add_parameter_override(self, parameter_name: str, value: Any):
        """Add session-level parameter override"""
        self.parameter_overrides[parameter_name] = value
        self.logger.debug(f"Added parameter override: {parameter_name} = {value}")
        
    def add_connection_override(self, original_connection: str, override_connection: str):
        """Add session-level connection override"""
        self.connection_overrides[original_connection] = override_connection
        self.logger.debug(f"Added connection override: {original_connection} -> {override_connection}")
        
    def get_effective_configuration(self) -> Dict[str, Any]:
        """Get complete effective configuration for session"""
        return {
            'session_name': self.name,
            'session_id': self.id,
            'mapping_ref': self.mapping_ref,
            'version': self.version,
            'enabled': self.is_enabled,
            'execution_mode': self.execution_mode.value,
            
            # Configuration sections
            'performance': {
                'buffer_block_size': self.performance_config.buffer_block_size,
                'dtm_buffer_pool_size': self.performance_config.dtm_buffer_pool_size,
                'pushdown_strategy': self.performance_config.pushdown_strategy.value,
                'constraint_based_load_ordering': self.performance_config.constraint_based_load_ordering
            },
            
            'commit': {
                'commit_type': self.commit_config.commit_type.value,
                'commit_interval': self.commit_config.commit_interval,
                'commit_on_end_of_file': self.commit_config.commit_on_end_of_file,
                'enable_transactions': self.commit_config.enable_transactions
            },
            
            'error_handling': {
                'recovery_strategy': self.error_config.recovery_strategy.value,
                'maximum_data_errors': self.error_config.maximum_data_errors,
                'stop_on_errors': self.error_config.stop_on_errors,
                'retry_on_deadlock': self.error_config.retry_on_deadlock
            },
            
            'data_processing': {
                'default_update_strategy': self.data_config.default_update_strategy.value,
                'default_date_format': self.data_config.default_date_format,
                'high_precision_enabled': self.data_config.high_precision_enabled
            },
            
            # Overrides
            'parameter_overrides': self.parameter_overrides,
            'connection_overrides': self.connection_overrides,
            'custom_properties': self.custom_properties
        }
        
    def validate_session_configuration(self) -> List[str]:
        """Validate complete session configuration"""
        errors = []
        
        # Basic validation
        if not self.mapping_ref:
            errors.append("Session must reference a mapping")
            
        # Validate configuration consistency
        if (self.commit_config.commit_type == CommitKind.USER_DEFINED and 
            self.commit_config.commit_interval <= 0):
            errors.append("User-defined commit requires positive commit interval")
            
        if (self.error_config.maximum_data_errors < 0):
            errors.append("Maximum data errors cannot be negative")
            
        if (self.performance_config.buffer_block_size <= 0):
            errors.append("Buffer block size must be positive")
            
        # Validate runtime characteristics
        runtime_errors = self.runtime_characteristic.validate_runtime_characteristic()
        errors.extend([f"Runtime: {err}" for err in runtime_errors])
        
        return errors
        
    def clone_with_overrides(self, overrides: Dict[str, Any]) -> 'XSDSession':
        """Create session copy with configuration overrides"""
        cloned_session = XSDSession(
            name=f"{self.name}_clone",
            mapping_ref=self.mapping_ref,
            id=f"{self.id}_clone" if self.id else None
        )
        
        # Copy all configuration
        cloned_session.performance_config = SessionPerformanceConfig(**vars(self.performance_config))
        cloned_session.commit_config = SessionCommitConfig(**vars(self.commit_config))
        cloned_session.error_config = SessionErrorConfig(**vars(self.error_config))
        cloned_session.data_config = SessionDataConfig(**vars(self.data_config))
        
        # Apply overrides
        for key, value in overrides.items():
            if key == 'name':
                cloned_session.name = value
            elif hasattr(cloned_session, key):
                setattr(cloned_session, key, value)
            elif '.' in key:
                # Handle nested property overrides (e.g., 'performance_config.buffer_block_size')
                obj_path, prop_name = key.rsplit('.', 1)
                if hasattr(cloned_session, obj_path):
                    config_obj = getattr(cloned_session, obj_path)
                    if hasattr(config_obj, prop_name):
                        setattr(config_obj, prop_name, value)
            else:
                # Add to custom properties
                cloned_session.custom_properties[key] = value
                        
        return cloned_session
        
    def get_session_summary(self) -> Dict[str, Any]:
        """Get session summary for analysis and reporting"""
        selected_env = self.runtime_characteristic.get_selected_environment()
        
        return {
            'name': self.name,
            'id': self.id,
            'mapping_ref': self.mapping_ref,
            'enabled': self.is_enabled,
            'execution_mode': self.execution_mode.value,
            'execution_count': self.execution_count,
            'last_execution_status': self.last_execution_status,
            
            # Configuration summary
            'commit_strategy': self.commit_config.commit_type.value,
            'recovery_strategy': self.error_config.recovery_strategy.value,
            'pushdown_optimization': self.performance_config.pushdown_strategy.value,
            'execution_environment': selected_env.environment_name if selected_env else None,
            'environment_type': type(selected_env).__name__ if selected_env else None,
            
            # Counts
            'parameter_overrides_count': len(self.parameter_overrides),
            'connection_overrides_count': len(self.connection_overrides),
            'custom_properties_count': len(self.custom_properties),
            'configured_environments': len(self.runtime_characteristic.environments)
        }

# Session collection and management classes
class SessionCollection(ElementCollection):
    """Collection for managing session objects with lookup capabilities"""
    
    def __init__(self):
        super().__init__()
        self._by_mapping_ref: Dict[str, List[XSDSession]] = {}
        
    def add(self, session: XSDSession):
        """Add session to collection"""
        # Ensure session has an ID for ElementCollection indexing
        if not session.id:
            session.id = session.name
            
        super().add(session)
        
        # Index by mapping reference
        if session.mapping_ref:
            if session.mapping_ref not in self._by_mapping_ref:
                self._by_mapping_ref[session.mapping_ref] = []
            self._by_mapping_ref[session.mapping_ref].append(session)
            
    def get_sessions_for_mapping(self, mapping_ref: str) -> List[XSDSession]:
        """Get all sessions that reference a specific mapping"""
        return self._by_mapping_ref.get(mapping_ref, [])
        
    def get_enabled_sessions(self) -> List[XSDSession]:
        """Get all enabled sessions"""
        return [session for session in self.list_by_type(XSDSession) if session.is_enabled]
        
    def validate_all_sessions(self) -> Dict[str, List[str]]:
        """Validate all sessions and return errors by session name"""
        validation_results = {}
        
        for session in self.list_by_type(XSDSession):
            errors = session.validate_session_configuration()
            if errors:
                validation_results[session.name] = errors
                
        return validation_results

# Global session registry
session_collection = SessionCollection()