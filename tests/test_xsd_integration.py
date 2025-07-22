"""
Comprehensive integration tests for XSD framework
Tests end-to-end functionality across all 4 phases:
- Phase 1: XSD Base Classes and XML Parsing
- Phase 2: Instance-Port Mapping Model  
- Phase 3: Transformation Framework
- Phase 4: Session and Runtime Execution
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
    XSDMapping, XSDInstance, XSDFieldMapLinkage, XSDLoadOrderStrategy,
    PortDirection, LoadOrderType
)
from src.core.xsd_execution_engine import XSDExecutionEngine, ExecutionContext

# Phase 3 imports
from src.core.xsd_transformation_model import (
    TransformationRegistry, XSDExpressionTransformation, 
    XSDAggregatorTransformation, XSDAbstractTransformation
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


class TestCrossPhaseIntegration:
    """Test integration across all framework phases"""
    
    def setup_method(self):
        """Setup for each test"""
        self.reference_manager = ReferenceManager()
        self.transformation_registry = TransformationRegistry()
        self.session_collection = SessionCollection()
        
    def test_complete_xml_to_execution_pipeline(self):
        """Test complete pipeline from XML parsing to session execution"""
        
        # Phase 1: Create and parse XML project
        xml_content = self._create_test_xml_project()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write(xml_content)
            xml_file = f.name
            
        try:
            parser = XSDXMLParser()
            project = parser.parse_project_file(xml_file)
            
            # Validate Phase 1 parsing
            assert isinstance(project, XSDProject)
            assert project.name == "IntegrationTestProject"
            assert len(project.mappings) >= 1
            
            # Phase 2: Extract and validate mapping model
            mapping = project.mappings[0]
            assert isinstance(mapping, XSDMapping)
            assert mapping.name == "CustomerDimensionLoad"
            assert len(mapping.instances) >= 2
            
            # Validate instances and ports
            source_instance = None
            target_instance = None
            for instance in mapping.instances:
                if "SOURCE" in instance.name.upper():
                    source_instance = instance
                elif "TARGET" in instance.name.upper():
                    target_instance = instance
                    
            assert source_instance is not None
            assert target_instance is not None
            assert len(source_instance.output_ports) > 0
            assert len(target_instance.input_ports) > 0
            
            # Phase 3: Create and register transformations
            expression_transform = XSDExpressionTransformation("EXPR_CUSTOMER_CLEANUP")
            # Note: Actual expression methods would be called here if available
            
            # Create a generic transformation for filter (since FilterTransformation doesn't exist)
            filter_transform = XSDAbstractTransformation("FILTER_ACTIVE_CUSTOMERS", "Filter")
            
            self.transformation_registry.register_transformation(expression_transform)
            self.transformation_registry.register_transformation(filter_transform)
            
            # Add transformation instances to mapping
            expr_instance = XSDInstance("EXPR_TRANSFORM", "Expression")
            expr_instance.transformation_ref = expression_transform.id
            mapping.add_instance(expr_instance)
            
            filter_instance = XSDInstance("FILTER_TRANSFORM", "Filter") 
            filter_instance.transformation_ref = filter_transform.id
            mapping.add_instance(filter_instance)
            
            # Phase 4: Create session configuration
            session_manager = SessionConfigurationManager()
            lifecycle_manager = SessionLifecycleManager(session_manager)
            
            # Create session from production template
            session = lifecycle_manager.create_session(
                "CustomerDimension_Session",
                mapping.id,
                template="production",
                overrides={
                    "performance_config.buffer_block_size": 128000,
                    "commit_config.commit_type": CommitKind.TARGET,
                    "commit_config.commit_interval": 5000
                }
            )
            
            # Add execution environment
            spark_env = SparkExecutionEnvironment("ProductionSpark")
            spark_env.master = "spark://cluster:7077"
            spark_env.add_spark_config("spark.executor.memory", "4g")
            spark_env.add_spark_config("spark.executor.cores", "4")
            session.runtime_characteristic.add_execution_environment(spark_env)
            
            # Validate session configuration
            validation_errors = session.validate_session_configuration()
            assert len(validation_errors) == 0
            
            # Test complete execution pipeline
            self._test_end_to_end_execution(session, mapping, lifecycle_manager)
            
        finally:
            os.unlink(xml_file)
            
    def test_transformation_registry_integration(self):
        """Test transformation registry integration across phases"""
        
        # Phase 3: Create various transformations
        transformations = [
            ExpressionTransformation("EXPR_DATE_CALC"),
            AggregatorTransformation("AGG_SALES_SUMMARY"),
            FilterTransformation("FILTER_VALID_RECORDS")
        ]
        
        # Register all transformations
        for transform in transformations:
            self.transformation_registry.register_transformation(transform)
            
        # Phase 2: Create mapping with transformation instances
        mapping = XSDMapping("TransformationTestMapping")
        
        for i, transform in enumerate(transformations):
            instance = XSDInstance(f"INSTANCE_{i}", transform.transformation_type)
            instance.transformation_ref = transform.id
            mapping.add_instance(instance)
            
        # Validate transformation resolution
        for instance in mapping.instances:
            transform = self.transformation_registry.get_transformation(instance.transformation_ref)
            assert transform is not None
            assert transform.transformation_type == instance.instance_type
            
        # Phase 4: Create session for transformation testing
        session = XSDSession("TransformationTest_Session", mapping.id)
        session.performance_config.pushdown_strategy = "auto"
        
        # Test session can access all transformations
        effective_config = session.get_effective_configuration()
        assert effective_config['mapping_ref'] == mapping.id
        assert effective_config['performance']['pushdown_strategy'] == "auto"
        
    def test_reference_resolution_across_phases(self):
        """Test reference resolution and validation across all phases"""
        
        # Phase 1: Create project with references
        project = XSDProject("ReferenceTestProject")
        
        # Phase 2: Create mapping with instances
        mapping = XSDMapping("ReferenceTestMapping")
        source_instance = XSDInstance("SRC_CUSTOMERS", "Source")
        target_instance = XSDInstance("TGT_CUSTOMERS", "Target")
        
        mapping.add_instance(source_instance)
        mapping.add_instance(target_instance)
        project.add_mapping(mapping)
        
        # Create field map linkage
        linkage = XSDFieldMapLinkage()
        linkage.from_instance_ref = source_instance.id
        linkage.to_instance_ref = target_instance.id
        linkage.from_field = "CUSTOMER_ID"
        linkage.to_field = "CUSTOMER_ID"
        
        mapping.field_map_spec.add_linkage(linkage)
        
        # Phase 4: Create session collection with references
        session1 = XSDSession("Session1", mapping.id)
        session2 = XSDSession("Session2", mapping.id)
        
        self.session_collection.add(session1)
        self.session_collection.add(session2)
        
        # Test reference resolution
        sessions_for_mapping = self.session_collection.get_sessions_for_mapping(mapping.id)
        assert len(sessions_for_mapping) == 2
        assert session1 in sessions_for_mapping
        assert session2 in sessions_for_mapping
        
        # Test cross-phase reference validation
        all_references = []
        
        # Collect mapping references
        for instance in mapping.instances:
            if hasattr(instance, 'transformation_ref') and instance.transformation_ref:
                all_references.append(instance.transformation_ref)
                
        # Collect session references
        for session in self.session_collection.list_by_type(XSDSession):
            if session.mapping_ref:
                all_references.append(session.mapping_ref)
                
        # Validate all references can be resolved
        assert len(all_references) >= 2  # At least session mapping references
        
    def test_execution_engine_integration(self):
        """Test execution engine integration with all phases"""
        
        # Phase 2: Create comprehensive mapping
        mapping = XSDMapping("ExecutionTestMapping")
        
        # Add source and target instances
        source = XSDInstance("SRC_ORDERS", "Source")
        target = XSDInstance("TGT_ORDERS", "Target")
        mapping.add_instance(source)
        mapping.add_instance(target)
        
        # Phase 3: Add transformation instances
        expression = XSDInstance("EXPR_CALC", "Expression")
        aggregator = XSDInstance("AGG_SUMMARY", "Aggregator")
        mapping.add_instance(expression)
        mapping.add_instance(aggregator)
        
        # Create data flow linkages
        linkage1 = XSDFieldMapLinkage()
        linkage1.from_instance_ref = source.id
        linkage1.to_instance_ref = expression.id
        mapping.field_map_spec.add_linkage(linkage1)
        
        linkage2 = XSDFieldMapLinkage()
        linkage2.from_instance_ref = expression.id
        linkage2.to_instance_ref = aggregator.id
        mapping.field_map_spec.add_linkage(linkage2)
        
        linkage3 = XSDFieldMapLinkage()
        linkage3.from_instance_ref = aggregator.id
        linkage3.to_instance_ref = target.id
        mapping.field_map_spec.add_linkage(linkage3)
        
        # Phase 4: Create session with execution configuration
        session = XSDSession("ExecutionTest_Session", mapping.id)
        session.commit_config.commit_type = CommitKind.USER_DEFINED
        session.commit_config.commit_interval = 1000
        session.error_config.recovery_strategy = SessionRecoveryKind.RESTART_TASK
        session.error_config.retry_attempts = 3
        
        # Add native execution environment
        native_env = NativeExecutionEnvironment()
        native_env.partitioning_enabled = True
        native_env.max_partitions = 4
        session.runtime_characteristic.add_execution_environment(native_env)
        
        # Test execution with standard engine
        standard_engine = XSDExecutionEngine()
        execution_context = ExecutionContext(
            session_id=session.id,
            mapping_id=mapping.id,
            parameters={"BATCH_SIZE": 5000},
            variables={"RUN_DATE": "2024-01-01"}
        )
        
        # Validate mapping can be executed
        validation_errors = standard_engine.validate_mapping_for_execution(mapping)
        assert isinstance(validation_errors, list)  # Should return validation results
        
        # Test session-aware execution
        session_engine = SessionAwareExecutionEngine()
        
        session_context = SessionExecutionContext(
            session_id=session.id,
            execution_id="EXEC_001",
            start_time=datetime.now(),
            parameters={"RUNTIME_MODE": "PRODUCTION"},
            connection_overrides={},
            environment_config={},
            runtime_variables={}
        )
        
        # Mock the internal execution methods for testing
        with patch.object(session_engine, '_execute_mapping_with_session_config') as mock_exec:
            mock_exec.return_value = []  # Mock execution results
            
            with patch.object(session_engine, '_validate_session_for_execution') as mock_validate:
                mock_validate.return_value = None  # Mock successful validation
                
                result = session_engine.execute_session(session, mapping, session_context)
                
                # Validate session execution result
                assert result.session_id == session.id
                assert result.execution_id == "EXEC_001"
                assert result.is_successful()
                
    def test_performance_optimization_integration(self):
        """Test performance optimization features across phases"""
        
        # Phase 2: Create performance-critical mapping
        mapping = XSDMapping("PerformanceTestMapping")
        
        # Add multiple sources for join operations
        for i in range(3):
            source = XSDInstance(f"SRC_TABLE_{i}", "Source")
            mapping.add_instance(source)
            
        # Add transformation chain
        joiner = XSDInstance("JOIN_TABLES", "Joiner")
        expression = XSDInstance("EXPR_PERFORMANCE", "Expression")
        target = XSDInstance("TGT_RESULT", "Target")
        
        mapping.add_instance(joiner)
        mapping.add_instance(expression)
        mapping.add_instance(target)
        
        # Phase 4: Create high-performance session
        session = XSDSession("HighPerformance_Session", mapping.id)
        
        # Configure for maximum performance
        session.performance_config.buffer_block_size = 256000
        session.performance_config.dtm_buffer_pool_size = 64000000
        session.performance_config.pushdown_strategy = "full"
        session.performance_config.constraint_based_load_ordering = True
        
        session.commit_config.commit_type = CommitKind.SOURCE
        session.commit_config.commit_interval = 50000
        session.commit_config.enable_transactions = False  # For performance
        
        # Add Spark environment for distributed processing
        spark_env = SparkExecutionEnvironment("HighPerformanceSpark")
        spark_env.master = "spark://cluster:7077"
        spark_env.deploy_mode = "cluster"
        spark_env.dynamic_allocation_enabled = True
        
        # Performance-optimized Spark configuration
        spark_env.add_spark_config("spark.executor.memory", "8g")
        spark_env.add_spark_config("spark.executor.cores", "8")
        spark_env.add_spark_config("spark.sql.adaptive.enabled", "true")
        spark_env.add_spark_config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        spark_env.add_spark_config("spark.sql.adaptive.skewJoin.enabled", "true")
        
        session.runtime_characteristic.add_execution_environment(spark_env)
        
        # Test performance configuration
        effective_config = session.get_effective_configuration()
        
        assert effective_config['performance']['buffer_block_size'] == 256000
        assert effective_config['performance']['pushdown_strategy'] == "full"
        assert effective_config['commit']['commit_type'] == "source"
        assert effective_config['commit']['enable_transactions'] == False
        
        # Test session-aware execution with optimization
        session_engine = SessionAwareExecutionEngine()
        
        # Test pushdown optimization
        optimized_mapping, optimization_report = session_engine.pushdown_optimizer.optimize_mapping_for_session(
            mapping, session
        )
        
        assert optimization_report['strategy'] == 'full'
        assert len(optimization_report['optimizations_applied']) > 0
        assert 'full_query_pushdown' in optimization_report['optimizations_applied']
        
    def test_error_handling_and_recovery_integration(self):
        """Test error handling and recovery across all phases"""
        
        # Phase 2: Create mapping with potential failure points
        mapping = XSDMapping("ErrorTestMapping")
        
        source = XSDInstance("SRC_UNRELIABLE", "Source")
        expression = XSDInstance("EXPR_COMPLEX", "Expression")
        target = XSDInstance("TGT_STRICT", "Target")
        
        mapping.add_instance(source)
        mapping.add_instance(expression)
        mapping.add_instance(target)
        
        # Phase 4: Create session with comprehensive error handling
        session = XSDSession("ErrorRecovery_Session", mapping.id)
        
        # Configure error handling
        session.error_config.recovery_strategy = SessionRecoveryKind.RESUME_FROM_LAST_CHECKPOINT
        session.error_config.maximum_data_errors = 100
        session.error_config.stop_on_errors = False
        session.error_config.retry_on_deadlock = True
        session.error_config.retry_attempts = 5
        session.error_config.retry_delay_seconds = 60
        
        # Enable state store for checkpointing
        session.runtime_characteristic.state_store_enabled = True
        session.runtime_characteristic.streaming_checkpoint_dir = "/tmp/checkpoints"
        
        # Test error handling components
        session_engine = SessionAwareExecutionEngine()
        
        # Test recovery manager
        recovery_manager = session_engine.recovery_manager
        
        # Create test error scenarios
        test_error = RuntimeError("Simulated execution error")
        
        session_context = SessionExecutionContext(
            session_id=session.id,
            execution_id="ERROR_TEST_001",
            start_time=datetime.now(),
            parameters={},
            connection_overrides={},
            environment_config={},
            runtime_variables={}
        )
        
        from src.core.xsd_session_runtime import SessionExecutionMetrics
        metrics = SessionExecutionMetrics(session.id, "ERROR_TEST_001", datetime.now())
        
        # Test checkpoint recovery
        recovery_manager.create_checkpoint(session, session_context, {"test": "checkpoint_data"})
        
        action, recovery_info = recovery_manager.handle_session_failure(
            session, test_error, session_context, metrics
        )
        
        from src.core.xsd_session_runtime import RecoveryAction
        assert action == RecoveryAction.RESUME
        assert recovery_info['strategy'] == 'resumeFromLastCheckpoint'
        assert recovery_info['checkpoint_found'] == True
        
        # Test error threshold management
        assert not recovery_manager.should_stop_on_error(session, 50)  # Below threshold
        assert recovery_manager.should_stop_on_error(session, 150)     # Above threshold
        
    def test_session_lifecycle_integration(self):
        """Test complete session lifecycle across all phases"""
        
        # Phase 1: Project setup
        project = XSDProject("LifecycleTestProject")
        
        # Phase 2: Mapping creation
        mapping = XSDMapping("LifecycleTestMapping")
        source = XSDInstance("SRC_LIFECYCLE", "Source")
        target = XSDInstance("TGT_LIFECYCLE", "Target")
        mapping.add_instance(source)
        mapping.add_instance(target)
        project.add_mapping(mapping)
        
        # Phase 4: Complete session lifecycle
        session_manager = SessionConfigurationManager()
        lifecycle_manager = SessionLifecycleManager(session_manager)
        
        # 1. Session Creation
        session = lifecycle_manager.create_session(
            "Lifecycle_Session",
            mapping.id,
            template="default",
            overrides={"execution_mode": "debug"}
        )
        
        assert session.name == "Lifecycle_Session"
        assert session.mapping_ref == mapping.id
        assert session.execution_mode.value == "debug"
        
        # 2. Session Registration and Validation
        lifecycle_manager.register_session(session)
        
        validation_result = lifecycle_manager.validate_session(session)
        assert validation_result.is_valid
        
        # 3. Session Preparation for Execution
        execution_context = lifecycle_manager.prepare_session_for_execution(
            session,
            runtime_parameters={"DEBUG_MODE": True, "LOG_LEVEL": "verbose"},
            connection_overrides={"DEV_DB": "TEST_DB"}
        )
        
        assert execution_context.session_id == session.id
        assert execution_context.parameters["DEBUG_MODE"] == True
        assert execution_context.connection_overrides["DEV_DB"] == "TEST_DB"
        
        # 4. Session Execution (mocked)
        session_engine = SessionAwareExecutionEngine()
        
        with patch.object(session_engine, '_execute_mapping_with_session_config') as mock_exec:
            from src.core.xsd_execution_engine import ExecutionResult, ExecutionState
            mock_exec.return_value = [
                ExecutionResult(
                    instance_id="SRC_LIFECYCLE",
                    instance_name="SRC_LIFECYCLE",
                    state=ExecutionState.COMPLETED,
                    rows_processed=1000
                )
            ]
            
            with patch.object(session_engine, '_validate_session_for_execution'):
                result = session_engine.execute_session(session, mapping, execution_context)
                
                # 5. Session Result Analysis
                assert result.is_successful()
                assert result.metrics.total_rows_processed == 1000
                assert len(result.execution_results) == 1
                
                summary = result.get_summary()
                assert summary['session_id'] == session.id
                assert summary['successful'] == True
                assert summary['rows_processed'] == 1000
                
        # 6. Session Collection Management
        self.session_collection.add(session)
        
        # Test collection operations
        all_sessions = self.session_collection.list_by_type(XSDSession)
        assert session in all_sessions
        
        mapping_sessions = self.session_collection.get_sessions_for_mapping(mapping.id)
        assert len(mapping_sessions) == 1
        assert mapping_sessions[0] == session
        
        # Test bulk validation
        validation_results = self.session_collection.validate_all_sessions()
        assert len(validation_results) == 0  # No validation errors
        
    def _create_test_xml_project(self) -> str:
        """Create test XML project for integration testing"""
        return '''<?xml version="1.0" encoding="UTF-8"?>
<project name="IntegrationTestProject" version="1.0">
    <mappings>
        <mapping name="CustomerDimensionLoad" id="MAP_001">
            <instances>
                <instance name="SOURCE_CUSTOMERS" type="Source" id="SRC_001">
                    <ports>
                        <port name="CUSTOMER_ID" direction="output" datatype="integer"/>
                        <port name="CUSTOMER_NAME" direction="output" datatype="string"/>
                        <port name="ADDRESS1" direction="output" datatype="string"/>
                        <port name="ADDRESS2" direction="output" datatype="string"/>
                        <port name="STATUS" direction="output" datatype="string"/>
                    </ports>
                </instance>
                <instance name="TARGET_CUSTOMERS" type="Target" id="TGT_001">
                    <ports>
                        <port name="CUSTOMER_ID" direction="input" datatype="integer"/>
                        <port name="CUSTOMER_NAME" direction="input" datatype="string"/>
                        <port name="FULL_ADDRESS" direction="input" datatype="string"/>
                        <port name="STATUS" direction="input" datatype="string"/>
                    </ports>
                </instance>
            </instances>
            <fieldMapSpec>
                <fieldMapLinkages>
                    <linkage fromInstance="SRC_001" toInstance="TGT_001" 
                             fromField="CUSTOMER_ID" toField="CUSTOMER_ID"/>
                </fieldMapLinkages>
            </fieldMapSpec>
        </mapping>
    </mappings>
</project>'''
    
    def _test_end_to_end_execution(self, session, mapping, lifecycle_manager):
        """Test complete end-to-end execution pipeline"""
        
        # Prepare session for execution
        execution_context = lifecycle_manager.prepare_session_for_execution(
            session,
            runtime_parameters={
                "BATCH_SIZE": 10000,
                "EXECUTION_MODE": "PRODUCTION",
                "MAX_ERRORS": 100
            }
        )
        
        # Create session-aware execution engine
        session_engine = SessionAwareExecutionEngine()
        
        # Mock the execution for testing
        with patch.object(session_engine, '_execute_mapping_with_session_config') as mock_exec:
            from src.core.xsd_execution_engine import ExecutionResult, ExecutionState
            
            # Mock successful execution results
            mock_exec.return_value = [
                ExecutionResult(
                    instance_id="SOURCE_CUSTOMERS",
                    instance_name="SOURCE_CUSTOMERS", 
                    state=ExecutionState.COMPLETED,
                    rows_processed=5000
                ),
                ExecutionResult(
                    instance_id="TARGET_CUSTOMERS",
                    instance_name="TARGET_CUSTOMERS",
                    state=ExecutionState.COMPLETED,
                    rows_processed=5000
                )
            ]
            
            with patch.object(session_engine, '_validate_session_for_execution'):
                # Execute the session
                result = session_engine.execute_session(session, mapping, execution_context)
                
                # Validate execution results
                assert result.is_successful()
                assert result.session_id == session.id
                assert result.metrics.total_rows_processed == 10000
                assert len(result.execution_results) == 2
                assert len(result.commit_points) > 0  # Should have commit points
                
                # Test commit strategy was applied
                assert result.metrics.commit_count > 0
                assert result.metrics.total_rows_committed > 0
                
                # Validate session summary
                summary = result.get_summary()
                assert summary['successful'] == True
                assert summary['rows_processed'] == 10000
                assert summary['commit_count'] > 0


if __name__ == "__main__":
    pytest.main([__file__])