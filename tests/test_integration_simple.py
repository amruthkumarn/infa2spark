"""
Simplified integration tests for XSD framework
Tests end-to-end functionality across all 4 phases with current implementation
"""
import pytest
import tempfile
import os
from datetime import datetime
from unittest.mock import Mock, patch

# Phase 1 imports
from src.core.xsd_xml_parser import XSDXMLParser
from src.core.xsd_project_model import XSDProject
from src.core.reference_manager import ReferenceManager

# Phase 2 imports
from src.core.xsd_mapping_model import (
    XSDMapping, XSDInstance, XSDFieldMapLinkage,
    PortDirection
)
from src.core.xsd_execution_engine import XSDExecutionEngine, ExecutionContext

# Phase 3 imports
from src.core.xsd_transformation_model import (
    TransformationRegistry, XSDExpressionTransformation
)

# Phase 4 imports
from src.core.xsd_session_model import (
    XSDSession, SessionCollection, CommitKind, SessionRecoveryKind,
    SparkExecutionEnvironment, NativeExecutionEnvironment
)
from src.core.xsd_session_manager import (
    SessionConfigurationManager, SessionLifecycleManager, SessionExecutionContext
)
from src.core.xsd_session_runtime import SessionAwareExecutionEngine


class TestSimpleIntegration:
    """Test basic integration across framework phases"""
    
    def setup_method(self):
        """Setup for each test"""
        self.transformation_registry = TransformationRegistry()
        self.session_collection = SessionCollection()
        
    def test_basic_cross_phase_integration(self):
        """Test basic functionality across all phases"""
        
        # Phase 1: Create project and mapping
        project = XSDProject("IntegrationTestProject")
        mapping = XSDMapping("TestMapping")
        project.add_to_contents(mapping)
        
        # Phase 2: Add instances to mapping
        source_instance = XSDInstance("SOURCE_DATA", "Source")
        target_instance = XSDInstance("TARGET_DATA", "Target")
        
        mapping.add_instance(source_instance)
        mapping.add_instance(target_instance)
        
        # Create simple field map linkage
        linkage = XSDFieldMapLinkage()
        linkage.from_instance_ref = source_instance.id
        linkage.to_instance_ref = target_instance.id
        linkage.from_field = "ID"
        linkage.to_field = "ID"
        
        if not hasattr(mapping, 'field_map_spec') or mapping.field_map_spec is None:
            from src.core.xsd_mapping_model import XSDFieldMapSpec
            mapping.field_map_spec = XSDFieldMapSpec()
            
        mapping.field_map_spec.add_linkage(linkage)
        
        # Phase 3: Use transformation registry to create transformation
        expression_transform = self.transformation_registry.create_transformation("Expression", "EXPR_TRANSFORM")
        assert expression_transform is not None
        
        # Add transformation instance to mapping
        transform_instance = XSDInstance("TRANSFORM_1", "Expression")
        transform_instance.transformation_ref = expression_transform.id
        mapping.add_instance(transform_instance)
        
        # Phase 4: Create session
        session_manager = SessionConfigurationManager()
        lifecycle_manager = SessionLifecycleManager(session_manager)
        
        session = lifecycle_manager.create_session(
            "IntegrationTest_Session",
            mapping.id,
            template="default"
        )
        
        # Add execution environment
        native_env = NativeExecutionEnvironment()
        session.runtime_characteristic.add_execution_environment(native_env)
        
        # Validate cross-phase integration
        assert project.name == "IntegrationTestProject"
        # Check if mapping was added to contents
        all_mappings = [item for item in project.contents.list_all() if isinstance(item, XSDMapping)]
        assert len(all_mappings) == 1
        assert len(mapping.instances) == 3  # source, target, transform
        assert session.mapping_ref == mapping.id
        
        # Validate transformation registry integration
        retrieved_transform = self.transformation_registry.get_transformation(expression_transform.id)
        assert retrieved_transform is not None
        assert retrieved_transform.name == "EXPR_TRANSFORM"
        
        # Validate session configuration
        session_config = session.get_effective_configuration()
        assert session_config['mapping_ref'] == mapping.id
        assert session_config['session_name'] == "IntegrationTest_Session"
        
    def test_session_lifecycle_integration(self):
        """Test session lifecycle with mapping integration"""
        
        # Create mapping
        mapping = XSDMapping("LifecycleTestMapping")
        source = XSDInstance("SRC_TEST", "Source")
        target = XSDInstance("TGT_TEST", "Target")
        mapping.add_instance(source)
        mapping.add_instance(target)
        
        # Create session lifecycle components
        session_manager = SessionConfigurationManager()
        lifecycle_manager = SessionLifecycleManager(session_manager)
        
        # Create session
        session = lifecycle_manager.create_session(
            "Lifecycle_Session",
            mapping.id,
            template="production",
            overrides={
                "performance_config.buffer_block_size": 128000,
                "commit_config.commit_type": CommitKind.TARGET
            }
        )
        
        # Add to collection
        self.session_collection.add(session)
        
        # Test collection operations
        sessions_for_mapping = self.session_collection.get_sessions_for_mapping(mapping.id)
        assert len(sessions_for_mapping) == 1
        assert sessions_for_mapping[0] == session
        
        # Test session validation
        validation_errors = session.validate_session_configuration()
        assert len(validation_errors) == 0  # Should be valid
        
        # Test session preparation
        execution_context = lifecycle_manager.prepare_session_for_execution(
            session,
            runtime_parameters={"TEST_PARAM": "test_value"},
            connection_overrides={"TEST_CONN": "override_conn"}
        )
        
        assert execution_context.session_id == session.id
        assert execution_context.parameters["TEST_PARAM"] == "test_value"
        assert execution_context.connection_overrides["TEST_CONN"] == "override_conn"
        
    def test_execution_engine_integration(self):
        """Test execution engine with session integration"""
        
        # Create mapping with data flow
        mapping = XSDMapping("ExecutionTestMapping")
        
        source = XSDInstance("SRC_EXECUTION", "Source")
        target = XSDInstance("TGT_EXECUTION", "Target")
        
        mapping.add_instance(source)
        mapping.add_instance(target)
        
        # Create session for execution
        session = XSDSession("ExecutionTest_Session", mapping.id)
        session.commit_config.commit_type = CommitKind.USER_DEFINED
        session.commit_config.commit_interval = 1000
        
        # Add native environment
        native_env = NativeExecutionEnvironment()
        session.runtime_characteristic.add_execution_environment(native_env)
        
        # Test standard execution engine
        standard_engine = XSDExecutionEngine()
        
        execution_context = ExecutionContext(
            session_id=session.id,
            mapping_id=mapping.id,
            parameters={"BATCH_SIZE": 1000},
            variables={}
        )
        
        # Test session-aware execution engine
        session_engine = SessionAwareExecutionEngine()
        
        session_context = SessionExecutionContext(
            session_id=session.id,
            execution_id="EXEC_001",
            start_time=datetime.now(),
            parameters={"TEST_MODE": True},
            connection_overrides={},
            environment_config={},
            runtime_variables={}
        )
        
        # Mock execution for testing
        with patch.object(session_engine, '_execute_mapping_with_session_config') as mock_exec:
            from src.core.xsd_execution_engine import ExecutionResult, ExecutionState
            
            mock_exec.return_value = [
                ExecutionResult(
                    instance_id="SRC_EXECUTION",
                    instance_name="SRC_EXECUTION",
                    state=ExecutionState.COMPLETED,
                    rows_processed=1000
                ),
                ExecutionResult(
                    instance_id="TGT_EXECUTION", 
                    instance_name="TGT_EXECUTION",
                    state=ExecutionState.COMPLETED,
                    rows_processed=1000
                )
            ]
            
            with patch.object(session_engine, '_validate_session_for_execution'):
                result = session_engine.execute_session(session, mapping, session_context)
                
                # Validate execution integration
                assert result.is_successful()
                assert result.session_id == session.id
                assert result.execution_id == "EXEC_001"
                assert result.metrics.total_rows_processed == 2000
                assert len(result.execution_results) == 2
                
                # Validate commit strategy was applied
                assert result.metrics.commit_count > 0
                
    def test_optimization_integration(self):
        """Test pushdown optimization integration"""
        
        # Create mapping for optimization testing
        mapping = XSDMapping("OptimizationTestMapping")
        
        # Add multiple sources and targets for optimization scenarios
        for i in range(3):
            source = XSDInstance(f"SRC_TABLE_{i}", "Source")
            mapping.add_instance(source)
            
        target = XSDInstance("TGT_OPTIMIZED", "Target")
        mapping.add_instance(target)
        
        # Create session with optimization settings
        session = XSDSession("OptimizationTest_Session", mapping.id)
        session.performance_config.pushdown_strategy = "full"
        session.performance_config.constraint_based_load_ordering = True
        
        # Add Spark environment for optimization
        spark_env = SparkExecutionEnvironment("OptimizationSpark")
        spark_env.master = "local[4]"
        spark_env.add_spark_config("spark.sql.adaptive.enabled", "true")
        spark_env.add_spark_config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        
        session.runtime_characteristic.add_execution_environment(spark_env)
        
        # Test optimization integration
        session_engine = SessionAwareExecutionEngine()
        
        # Test pushdown optimizer
        optimized_mapping, optimization_report = session_engine.pushdown_optimizer.optimize_mapping_for_session(
            mapping, session
        )
        
        assert optimization_report['strategy'] == 'full'
        assert len(optimization_report['optimizations_applied']) > 0
        
        # Validate configuration is applied
        effective_config = session.get_effective_configuration()
        assert effective_config['performance']['pushdown_strategy'] == 'full'
        assert effective_config['performance']['constraint_based_load_ordering'] == True
        
    def test_error_handling_integration(self):
        """Test error handling across phases"""
        
        # Create mapping
        mapping = XSDMapping("ErrorTestMapping")
        source = XSDInstance("SRC_ERROR_TEST", "Source")
        target = XSDInstance("TGT_ERROR_TEST", "Target")
        mapping.add_instance(source)
        mapping.add_instance(target)
        
        # Create session with error handling configuration
        session = XSDSession("ErrorTest_Session", mapping.id)
        session.error_config.recovery_strategy = SessionRecoveryKind.RESTART_TASK
        session.error_config.maximum_data_errors = 10
        session.error_config.retry_attempts = 3
        session.error_config.stop_on_errors = False
        
        # Test error handling components
        session_engine = SessionAwareExecutionEngine()
        recovery_manager = session_engine.recovery_manager
        
        # Test error scenarios
        test_error = RuntimeError("Test execution error")
        
        session_context = SessionExecutionContext(
            session_id=session.id,
            execution_id="ERROR_TEST_001",
            start_time=datetime.now(),
            parameters={},
            connection_overrides={},
            environment_config={},
            runtime_variables={}
        )
        
        from src.core.xsd_session_runtime import SessionExecutionMetrics, RecoveryAction
        metrics = SessionExecutionMetrics(session.id, "ERROR_TEST_001", datetime.now())
        
        # Test recovery action generation
        action, recovery_info = recovery_manager.handle_session_failure(
            session, test_error, session_context, metrics
        )
        
        assert action == RecoveryAction.RESTART
        assert recovery_info['strategy'] == 'restartTask'
        assert 'attempt 1/3' in recovery_info['action']
        
        # Test error threshold checking
        assert not recovery_manager.should_stop_on_error(session, 5)   # Below threshold
        assert recovery_manager.should_stop_on_error(session, 15)      # Above threshold
        
    def test_performance_monitoring_integration(self):
        """Test performance monitoring across phases"""
        
        # Create performance test mapping
        mapping = XSDMapping("PerformanceTestMapping")
        source = XSDInstance("SRC_PERF", "Source")
        target = XSDInstance("TGT_PERF", "Target")
        mapping.add_instance(source)
        mapping.add_instance(target)
        
        # Create high-performance session
        session_manager = SessionConfigurationManager()
        lifecycle_manager = SessionLifecycleManager(session_manager)
        
        session = lifecycle_manager.create_session(
            "Performance_Session",
            mapping.id,
            template="high_performance",
            overrides={
                "performance_config.buffer_block_size": 512000,
                "performance_config.dtm_buffer_pool_size": 64000000
            }
        )
        
        # Test performance configuration
        effective_config = session.get_effective_configuration()
        assert effective_config['performance']['buffer_block_size'] == 512000
        
        # Test execution with performance monitoring
        session_engine = SessionAwareExecutionEngine()
        
        session_context = SessionExecutionContext(
            session_id=session.id,
            execution_id="PERF_001",
            start_time=datetime.now(),
            parameters={"ENABLE_MONITORING": True},
            connection_overrides={},
            environment_config={},
            runtime_variables={}
        )
        
        # Mock execution with performance data
        with patch.object(session_engine, '_execute_mapping_with_session_config') as mock_exec:
            from src.core.xsd_execution_engine import ExecutionResult, ExecutionState
            
            mock_exec.return_value = [
                ExecutionResult(
                    instance_id="SRC_PERF",
                    instance_name="SRC_PERF",
                    state=ExecutionState.COMPLETED,
                    rows_processed=100000,
                    execution_time_ms=5000
                ),
                ExecutionResult(
                    instance_id="TGT_PERF",
                    instance_name="TGT_PERF", 
                    state=ExecutionState.COMPLETED,
                    rows_processed=100000,
                    execution_time_ms=3000
                )
            ]
            
            with patch.object(session_engine, '_validate_session_for_execution'):
                result = session_engine.execute_session(session, mapping, session_context)
                
                # Validate performance metrics
                assert result.is_successful()
                assert result.metrics.total_rows_processed == 200000
                assert result.metrics.throughput_rows_per_second > 0
                
                # Test metrics calculation
                duration = result.metrics.calculate_duration()
                assert duration is not None
                
                summary = result.get_summary()
                assert summary['rows_processed'] == 200000
                assert summary['throughput_rps'] > 0


if __name__ == "__main__":
    pytest.main([__file__])