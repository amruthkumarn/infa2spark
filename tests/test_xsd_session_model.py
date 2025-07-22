"""
Tests for XSD-compliant session model classes
"""
import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from src.core.xsd_session_model import (
    XSDSession, SessionCollection, ExecutionEnvironment,
    SparkExecutionEnvironment, NativeExecutionEnvironment, RuntimeCharacteristic,
    CommitKind, SessionRecoveryKind, PushdownOptimizationKind, SparkDeployMode,
    DefaultUpdateStrategyKind, UnicodeSortOrderKind, ExecutionMode,
    SessionPerformanceConfig, SessionCommitConfig, SessionErrorConfig, SessionDataConfig
)
from src.core.xsd_session_manager import (
    SessionConfigurationManager, SessionLifecycleManager, ValidationResult,
    SessionValidationSeverity, ConfigurationScope, ParameterDefinition,
    SessionExecutionContext
)
from src.core.reference_manager import ReferenceManager

class TestSessionConfiguration:
    """Test session configuration data classes"""
    
    def test_session_performance_config_defaults(self):
        """Test SessionPerformanceConfig default values"""
        config = SessionPerformanceConfig()
        
        assert config.buffer_block_size == 64000
        assert config.dtm_buffer_pool_size == 12000000
        assert config.dtm_buffer_size == 64000
        assert config.maximum_auto_mem_total_percent == 60
        assert config.constraint_based_load_ordering == False
        assert config.pushdown_strategy == PushdownOptimizationKind.NONE
        
    def test_session_commit_config_defaults(self):
        """Test SessionCommitConfig default values"""
        config = SessionCommitConfig()
        
        assert config.commit_type == CommitKind.TARGET
        assert config.commit_interval == 10000
        assert config.commit_on_end_of_file == True
        assert config.enable_transactions == True
        
    def test_session_error_config_defaults(self):
        """Test SessionErrorConfig default values"""
        config = SessionErrorConfig()
        
        assert config.recovery_strategy == SessionRecoveryKind.FAIL_TASK
        assert config.maximum_data_errors == 0
        assert config.stop_on_errors == True
        assert config.retry_on_deadlock == True
        assert config.retry_attempts == 3
        
    def test_session_data_config_defaults(self):
        """Test SessionDataConfig default values"""
        config = SessionDataConfig()
        
        assert config.default_update_strategy == DefaultUpdateStrategyKind.INSERT_ROW
        assert config.default_date_format == "MM/DD/YYYY HH24:MI:SS.US"
        assert config.high_precision_enabled == False
        assert config.sort_order == UnicodeSortOrderKind.BINARY

class TestExecutionEnvironments:
    """Test execution environment classes"""
    
    def test_native_execution_environment_creation(self):
        """Test NativeExecutionEnvironment creation and configuration"""
        env = NativeExecutionEnvironment("TestNative")
        
        assert env.environment_name == "TestNative"
        assert env.version == "1.0"
        assert env.partitioning_enabled == False
        assert env.max_partitions == 1
        assert env.load_balancing_enabled == True
        
    def test_native_environment_validation_success(self):
        """Test successful native environment validation"""
        env = NativeExecutionEnvironment()
        env.max_partitions = 4
        env.partitioning_enabled = True
        
        errors = env.validate_configuration()
        assert len(errors) == 0
        
    def test_native_environment_validation_errors(self):
        """Test native environment validation errors"""
        env = NativeExecutionEnvironment()
        env.max_partitions = 0  # Invalid
        
        errors = env.validate_configuration()
        assert len(errors) == 1
        assert "Max partitions must be at least 1" in errors[0]
        
        # Test partitioning inconsistency
        env.max_partitions = 1
        env.partitioning_enabled = True
        
        errors = env.validate_configuration()
        assert len(errors) == 1
        assert "Partitioning enabled but max partitions is 1" in errors[0]
        
    def test_spark_execution_environment_creation(self):
        """Test SparkExecutionEnvironment creation and defaults"""
        env = SparkExecutionEnvironment("TestSpark")
        
        assert env.environment_name == "TestSpark"
        assert env.deploy_mode == SparkDeployMode.CLIENT
        assert env.master == "local[*]"
        assert env.event_log_enabled == True
        assert env.dynamic_allocation_enabled == False
        
        # Check default Spark configurations
        assert 'spark.sql.adaptive.enabled' in env.spark_conf
        assert env.spark_conf['spark.sql.adaptive.enabled'] == 'true'
        assert 'spark.serializer' in env.spark_conf
        
    def test_spark_environment_config_management(self):
        """Test Spark configuration management"""
        env = SparkExecutionEnvironment()
        
        # Test adding configuration
        env.add_spark_config("spark.executor.memory", "2g")
        assert env.spark_conf["spark.executor.memory"] == "2g"
        
        # Test removing configuration
        env.remove_spark_config("spark.executor.memory")
        assert "spark.executor.memory" not in env.spark_conf
        
    def test_spark_environment_validation_success(self):
        """Test successful Spark environment validation"""
        env = SparkExecutionEnvironment()
        env.master = "spark://localhost:7077"
        env.event_log_dir = "/tmp/spark-events"
        
        errors = env.validate_configuration()
        assert len(errors) == 0
        
    def test_spark_environment_validation_errors(self):
        """Test Spark environment validation errors"""
        env = SparkExecutionEnvironment()
        
        # Test missing master
        env.master = ""
        errors = env.validate_configuration()
        assert len(errors) >= 1
        assert any("Spark master must be specified" in error for error in errors)
        
        # Test event log validation
        env.master = "local[*]"
        env.event_log_enabled = True
        env.event_log_dir = None
        errors = env.validate_configuration()
        assert any("Event log directory required" in error for error in errors)
        
        # Test deploy mode consistency
        env.deploy_mode = SparkDeployMode.CLUSTER
        env.master = "local[2]"
        errors = env.validate_configuration()
        assert any("Cluster deploy mode incompatible with local master" in error for error in errors)

class TestRuntimeCharacteristic:
    """Test RuntimeCharacteristic class"""
    
    def test_runtime_characteristic_creation(self):
        """Test RuntimeCharacteristic creation and defaults"""
        runtime = RuntimeCharacteristic()
        
        assert len(runtime.environments) == 0
        assert runtime.selected_execution_environment_name is None
        assert runtime.maximum_data_errors == 0
        assert runtime.state_store_enabled == False
        assert runtime.audit_enabled == False
        assert runtime.streaming_enabled == False
        
    def test_execution_environment_management(self):
        """Test execution environment management"""
        runtime = RuntimeCharacteristic()
        
        # Add environments
        native_env = NativeExecutionEnvironment("Native")
        spark_env = SparkExecutionEnvironment("Spark") 
        
        runtime.add_execution_environment(native_env)
        runtime.add_execution_environment(spark_env)
        
        assert len(runtime.environments) == 2
        assert runtime.selected_execution_environment_name == "Native"  # First added becomes selected
        
        # Test environment selection
        assert runtime.set_selected_environment("Spark") == True
        assert runtime.selected_execution_environment_name == "Spark"
        
        selected = runtime.get_selected_environment()
        assert selected is not None
        assert selected.environment_name == "Spark"
        
        # Test invalid environment selection
        assert runtime.set_selected_environment("NonExistent") == False
        
    def test_runtime_characteristic_validation_success(self):
        """Test successful runtime characteristic validation"""
        runtime = RuntimeCharacteristic()
        native_env = NativeExecutionEnvironment()
        runtime.add_execution_environment(native_env)
        
        errors = runtime.validate_runtime_characteristic()
        assert len(errors) == 0
        
    def test_runtime_characteristic_validation_errors(self):
        """Test runtime characteristic validation errors"""
        runtime = RuntimeCharacteristic()
        
        # Test no environments
        errors = runtime.validate_runtime_characteristic()
        assert len(errors) >= 1
        assert any("At least one execution environment" in error for error in errors)
        
        # Test missing selected environment
        runtime.selected_execution_environment_name = "NonExistent"
        errors = runtime.validate_runtime_characteristic()
        assert any("Selected environment 'NonExistent' not found" in error for error in errors)
        
        # Test streaming validation
        runtime.streaming_enabled = True
        runtime.streaming_checkpoint_dir = None
        errors = runtime.validate_runtime_characteristic()
        assert any("Streaming checkpoint directory required" in error for error in errors)

class TestXSDSession:
    """Test XSDSession class"""
    
    def test_session_creation(self):
        """Test basic session creation"""
        session = XSDSession("TestSession", "MAPPING_001")
        
        assert session.name == "TestSession"
        assert session.mapping_ref == "MAPPING_001"
        assert session.version == "1.0"
        assert session.is_enabled == True
        assert session.execution_mode == ExecutionMode.NORMAL
        assert session.execution_count == 0
        
        # Check configuration objects are created
        assert session.performance_config is not None
        assert session.commit_config is not None
        assert session.error_config is not None
        assert session.data_config is not None
        assert session.runtime_characteristic is not None
        
    def test_parameter_override_management(self):
        """Test parameter override functionality"""
        session = XSDSession("TestSession", "MAPPING_001")
        
        # Add parameter overrides
        session.add_parameter_override("BUFFER_SIZE", 128000)
        session.add_parameter_override("LOG_LEVEL", "verbose")
        
        assert session.parameter_overrides["BUFFER_SIZE"] == 128000
        assert session.parameter_overrides["LOG_LEVEL"] == "verbose"
        assert len(session.parameter_overrides) == 2
        
    def test_connection_override_management(self):
        """Test connection override functionality"""
        session = XSDSession("TestSession", "MAPPING_001")
        
        # Add connection overrides
        session.add_connection_override("PROD_DB", "DEV_DB")
        session.add_connection_override("WAREHOUSE", "TEST_WAREHOUSE")
        
        assert session.connection_overrides["PROD_DB"] == "DEV_DB"
        assert session.connection_overrides["WAREHOUSE"] == "TEST_WAREHOUSE"
        assert len(session.connection_overrides) == 2
        
    def test_effective_configuration(self):
        """Test effective configuration generation"""
        session = XSDSession("TestSession", "MAPPING_001")
        session.add_parameter_override("TEST_PARAM", "test_value")
        
        config = session.get_effective_configuration()
        
        assert config["session_name"] == "TestSession"
        assert config["mapping_ref"] == "MAPPING_001"
        assert "performance" in config
        assert "commit" in config
        assert "error_handling" in config
        assert "data_processing" in config
        assert config["parameter_overrides"]["TEST_PARAM"] == "test_value"
        
    def test_session_validation_success(self):
        """Test successful session validation"""
        session = XSDSession("TestSession", "MAPPING_001")
        
        # Add required environment
        env = NativeExecutionEnvironment()
        session.runtime_characteristic.add_execution_environment(env)
        
        errors = session.validate_session_configuration()
        assert len(errors) == 0
        
    def test_session_validation_errors(self):
        """Test session validation errors"""
        session = XSDSession("TestSession")  # No mapping_ref
        
        errors = session.validate_session_configuration()
        assert len(errors) >= 1
        assert any("Session must reference a mapping" in error for error in errors)
        
        # Test configuration validation errors
        session.mapping_ref = "MAPPING_001"
        session.performance_config.buffer_block_size = -1  # Invalid
        
        errors = session.validate_session_configuration()
        assert any("Buffer block size must be positive" in error for error in errors)
        
    def test_session_cloning_with_overrides(self):
        """Test session cloning with configuration overrides"""
        original_session = XSDSession("OriginalSession", "MAPPING_001")
        original_session.performance_config.buffer_block_size = 64000
        
        overrides = {
            "name": "ClonedSession",
            "performance_config.buffer_block_size": 128000,
            "execution_mode": ExecutionMode.DEBUG
        }
        
        cloned_session = original_session.clone_with_overrides(overrides)
        
        assert cloned_session.name == "ClonedSession"  # Override applied
        assert cloned_session.mapping_ref == "MAPPING_001"
        assert cloned_session.performance_config.buffer_block_size == 128000
        
        # Original should be unchanged
        assert original_session.performance_config.buffer_block_size == 64000
        
    def test_session_summary(self):
        """Test session summary generation"""
        session = XSDSession("TestSession", "MAPPING_001")
        env = SparkExecutionEnvironment("TestSpark")
        session.runtime_characteristic.add_execution_environment(env)
        session.add_parameter_override("TEST_PARAM", "value")
        session.execution_count = 5
        
        summary = session.get_session_summary()
        
        assert summary["name"] == "TestSession"
        assert summary["mapping_ref"] == "MAPPING_001"
        assert summary["execution_count"] == 5
        assert summary["execution_environment"] == "TestSpark"
        assert summary["environment_type"] == "SparkExecutionEnvironment"
        assert summary["parameter_overrides_count"] == 1
        assert summary["configured_environments"] == 1

class TestSessionCollection:
    """Test SessionCollection class"""
    
    def test_session_collection_basic_operations(self):
        """Test basic session collection operations"""
        collection = SessionCollection()
        
        session1 = XSDSession("Session1", "MAPPING_001", id="SESSION_1")
        session2 = XSDSession("Session2", "MAPPING_002", id="SESSION_2")
        
        collection.add(session1)
        collection.add(session2)
        
        assert collection.count() == 2
        assert collection.get_by_id("SESSION_1") == session1
        assert collection.get_by_name("Session2") == session2
        
    def test_session_collection_mapping_index(self):
        """Test session collection mapping reference indexing"""
        collection = SessionCollection()
        
        session1 = XSDSession("Session1", "MAPPING_001")
        session2 = XSDSession("Session2", "MAPPING_001")  # Same mapping
        session3 = XSDSession("Session3", "MAPPING_002")
        
        collection.add(session1)
        collection.add(session2)
        collection.add(session3)
        
        mapping_001_sessions = collection.get_sessions_for_mapping("MAPPING_001")
        assert len(mapping_001_sessions) == 2
        assert session1 in mapping_001_sessions
        assert session2 in mapping_001_sessions
        
        mapping_002_sessions = collection.get_sessions_for_mapping("MAPPING_002")
        assert len(mapping_002_sessions) == 1
        assert session3 in mapping_002_sessions
        
    def test_session_collection_enabled_sessions(self):
        """Test enabled sessions filtering"""
        collection = SessionCollection()
        
        session1 = XSDSession("Session1", "MAPPING_001")
        session2 = XSDSession("Session2", "MAPPING_002")
        session3 = XSDSession("Session3", "MAPPING_003")
        
        session2.is_enabled = False  # Disable one session
        
        collection.add(session1)
        collection.add(session2)
        collection.add(session3)
        
        enabled_sessions = collection.get_enabled_sessions()
        assert len(enabled_sessions) == 2
        assert session1 in enabled_sessions
        assert session3 in enabled_sessions
        assert session2 not in enabled_sessions
        
    def test_session_collection_validation(self):
        """Test collection-wide session validation"""
        collection = SessionCollection()
        
        # Create valid session
        valid_session = XSDSession("ValidSession", "MAPPING_001")
        env = NativeExecutionEnvironment()
        valid_session.runtime_characteristic.add_execution_environment(env)
        
        # Create invalid session
        invalid_session = XSDSession("InvalidSession")  # No mapping_ref
        
        collection.add(valid_session)
        collection.add(invalid_session)
        
        validation_results = collection.validate_all_sessions()
        
        assert "ValidSession" not in validation_results  # No errors
        assert "InvalidSession" in validation_results
        assert len(validation_results["InvalidSession"]) > 0

class TestSessionConfigurationManager:
    """Test SessionConfigurationManager class"""
    
    def test_configuration_manager_initialization(self):
        """Test configuration manager initialization"""
        manager = SessionConfigurationManager()
        
        # Check built-in parameters are loaded
        assert "DTM_BUFFER_POOL_SIZE" in manager.parameter_definitions
        assert "COMMIT_INTERVAL" in manager.parameter_definitions
        assert "PUSHDOWN_OPTIMIZATION" in manager.parameter_definitions
        
        # Check built-in templates are loaded
        assert "default" in manager.configuration_templates
        assert "high_performance" in manager.configuration_templates
        assert "production" in manager.configuration_templates
        
        # Check validation rules are loaded
        assert len(manager.validation_rules) > 0
        
    def test_custom_parameter_registration(self):
        """Test custom parameter definition registration"""
        manager = SessionConfigurationManager()
        
        custom_param = ParameterDefinition(
            name="CUSTOM_PARAM",
            data_type="string",
            default_value="default_value",
            description="Custom test parameter",
            allowed_values=["value1", "value2", "value3"]
        )
        
        manager.register_parameter_definition(custom_param)
        
        assert "CUSTOM_PARAM" in manager.parameter_definitions
        assert manager.parameter_definitions["CUSTOM_PARAM"].default_value == "default_value"
        
    def test_custom_template_registration(self):
        """Test custom configuration template registration"""
        manager = SessionConfigurationManager()
        
        custom_template = {
            "performance_config.buffer_block_size": 256000,
            "commit_config.commit_interval": 50000,
            "error_config.maximum_data_errors": 100
        }
        
        manager.register_configuration_template("custom_template", custom_template)
        
        assert "custom_template" in manager.configuration_templates
        assert manager.configuration_templates["custom_template"]["performance_config.buffer_block_size"] == 256000
        
    def test_session_creation_from_template(self):
        """Test session creation from configuration template"""
        manager = SessionConfigurationManager()
        
        session = manager.create_session_from_template(
            "TestSession", 
            "MAPPING_001", 
            "high_performance"
        )
        
        assert session.name == "TestSession"
        assert session.mapping_ref == "MAPPING_001"
        assert session.performance_config.buffer_block_size == 128000  # From high_performance template
        assert session.commit_config.commit_interval == 50000
        
    def test_session_creation_with_overrides(self):
        """Test session creation with configuration overrides"""
        manager = SessionConfigurationManager()
        
        overrides = {
            "performance_config.buffer_block_size": 512000,
            "execution_mode": "debug"
        }
        
        session = manager.create_session_from_template(
            "TestSession",
            "MAPPING_001", 
            "default",
            overrides
        )
        
        assert session.performance_config.buffer_block_size == 512000  # Override applied
        assert session.execution_mode.value == "debug"  # Direct property override
        
    def test_parameter_resolution(self):
        """Test session parameter resolution"""
        manager = SessionConfigurationManager()
        
        session = XSDSession("TestSession", "MAPPING_001")
        session.add_parameter_override("DTM_BUFFER_POOL_SIZE", 24000000)
        session.add_parameter_override("CUSTOM_PARAM", "custom_value")
        
        runtime_params = {"RUNTIME_PARAM": "runtime_value"}
        
        context = manager.resolve_session_parameters(session, runtime_params)
        
        assert context.session_id == "TestSession"
        assert context.parameters["DTM_BUFFER_POOL_SIZE"] == 24000000  # Session override
        assert context.parameters["BUFFER_BLOCK_SIZE"] == 64000  # Default value
        assert context.parameters["CUSTOM_PARAM"] == "custom_value"  # Session parameter
        assert context.parameters["RUNTIME_PARAM"] == "runtime_value"  # Runtime parameter
        
        # Check runtime variables
        assert context.runtime_variables["SESSION_NAME"] == "TestSession"
        assert context.runtime_variables["MAPPING_REF"] == "MAPPING_001"
        assert "EXECUTION_ID" in context.runtime_variables
        
    def test_configuration_summary(self):
        """Test configuration summary generation"""
        manager = SessionConfigurationManager()
        
        session = XSDSession("TestSession", "MAPPING_001")
        env = NativeExecutionEnvironment()
        session.runtime_characteristic.add_execution_environment(env)
        session.add_parameter_override("DTM_BUFFER_POOL_SIZE", 24000000)
        
        summary = manager.get_configuration_summary(session)
        
        assert summary["session_name"] == "TestSession"
        assert summary["configuration_valid"] == True  # Should be valid
        assert summary["parameter_definitions_used"] == 1  # DTM_BUFFER_POOL_SIZE
        assert summary["execution_environments"] == 1

class TestSessionLifecycleManager:
    """Test SessionLifecycleManager class"""
    
    def test_lifecycle_manager_initialization(self):
        """Test lifecycle manager initialization"""
        ref_manager = ReferenceManager()
        lifecycle_manager = SessionLifecycleManager(ref_manager)
        
        assert lifecycle_manager.reference_manager == ref_manager
        assert lifecycle_manager.config_manager is not None
        assert lifecycle_manager.session_collection is not None
        
    def test_session_creation_and_registration(self):
        """Test session creation and automatic registration"""
        ref_manager = ReferenceManager()
        lifecycle_manager = SessionLifecycleManager(ref_manager)
        
        session = lifecycle_manager.create_session(
            "TestSession",
            "MAPPING_001",
            template="default"
        )
        
        assert session.name == "TestSession"
        assert session.mapping_ref == "MAPPING_001"
        
        # Check session is registered
        retrieved_session = lifecycle_manager.get_session_by_name("TestSession")
        assert retrieved_session == session
        
        # Check default environment is added
        assert len(session.runtime_characteristic.environments) > 0
        
    def test_session_validation_integration(self):
        """Test session validation through lifecycle manager"""
        ref_manager = ReferenceManager()
        lifecycle_manager = SessionLifecycleManager(ref_manager)
        
        session = lifecycle_manager.create_session("TestSession", "MAPPING_001")
        
        validation_results = lifecycle_manager.validate_session(session)
        
        # Should be valid with default configuration
        errors = [r for r in validation_results if r.severity in [SessionValidationSeverity.ERROR, SessionValidationSeverity.CRITICAL]]
        assert len(errors) == 0
        
    def test_session_execution_preparation(self):
        """Test session preparation for execution"""
        ref_manager = ReferenceManager()
        lifecycle_manager = SessionLifecycleManager(ref_manager)
        
        session = lifecycle_manager.create_session("TestSession", "MAPPING_001")
        
        runtime_params = {"RUNTIME_PARAM": "test_value"}
        context = lifecycle_manager.prepare_session_for_execution(session, runtime_params)
        
        assert context.session_id == "TestSession"
        assert context.parameters["RUNTIME_PARAM"] == "test_value"
        assert context.execution_mode == "normal"
        
    def test_session_execution_preparation_validation_failure(self):
        """Test session execution preparation with validation failure"""
        ref_manager = ReferenceManager()
        lifecycle_manager = SessionLifecycleManager(ref_manager)
        
        # Create invalid session
        session = XSDSession("InvalidSession")  # No mapping_ref
        lifecycle_manager.session_collection.add(session)
        
        with pytest.raises(ValueError) as exc_info:
            lifecycle_manager.prepare_session_for_execution(session)
            
        assert "Session validation failed" in str(exc_info.value)
        
    def test_mapping_session_lookup(self):
        """Test looking up sessions by mapping reference"""
        ref_manager = ReferenceManager()
        lifecycle_manager = SessionLifecycleManager(ref_manager)
        
        session1 = lifecycle_manager.create_session("Session1", "MAPPING_001")
        session2 = lifecycle_manager.create_session("Session2", "MAPPING_001")
        session3 = lifecycle_manager.create_session("Session3", "MAPPING_002")
        
        mapping_001_sessions = lifecycle_manager.get_sessions_for_mapping("MAPPING_001")
        assert len(mapping_001_sessions) == 2
        assert session1 in mapping_001_sessions
        assert session2 in mapping_001_sessions
        
        mapping_002_sessions = lifecycle_manager.get_sessions_for_mapping("MAPPING_002")
        assert len(mapping_002_sessions) == 1
        assert session3 in mapping_002_sessions
        
    def test_bulk_session_validation(self):
        """Test bulk validation of all sessions"""
        ref_manager = ReferenceManager()
        lifecycle_manager = SessionLifecycleManager(ref_manager)
        
        # Create valid session
        valid_session = lifecycle_manager.create_session("ValidSession", "MAPPING_001")
        
        # Create invalid session manually
        invalid_session = XSDSession("InvalidSession")  # No mapping_ref
        lifecycle_manager.session_collection.add(invalid_session)
        
        validation_results = lifecycle_manager.validate_all_sessions()
        
        assert "ValidSession" not in validation_results  # Valid session not in results
        assert "InvalidSession" in validation_results    # Invalid session in results
        assert len(validation_results["InvalidSession"]) > 0

if __name__ == "__main__":
    pytest.main([__file__])