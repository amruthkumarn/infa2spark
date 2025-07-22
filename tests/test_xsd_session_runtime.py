"""
Tests for session-aware runtime execution engine
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

from src.core.xsd_session_runtime import (
    SessionAwareExecutionEngine, CommitStrategyManager, PushdownOptimizer, RecoveryManager,
    SessionExecutionStatus, CommitScope, RecoveryAction, CommitPoint,
    SessionExecutionMetrics, SessionExecutionResult
)
from src.core.xsd_session_model import (
    XSDSession, CommitKind, SessionRecoveryKind, PushdownOptimizationKind,
    SparkExecutionEnvironment, NativeExecutionEnvironment
)
from src.core.xsd_session_manager import SessionExecutionContext
from src.core.xsd_mapping_model import XSDMapping, XSDInstance
from src.core.xsd_execution_engine import ExecutionResult, ExecutionState

class TestSessionExecutionMetrics:
    """Test SessionExecutionMetrics class"""
    
    def test_metrics_creation(self):
        """Test metrics creation and initialization"""
        metrics = SessionExecutionMetrics(
            session_id="TEST_SESSION",
            execution_id="EXEC_001",
            start_time=datetime.now()
        )
        
        assert metrics.session_id == "TEST_SESSION"
        assert metrics.execution_id == "EXEC_001"
        assert metrics.total_rows_processed == 0
        assert metrics.total_errors == 0
        assert metrics.commit_count == 0
        assert metrics.recovery_count == 0
        
    def test_duration_calculation(self):
        """Test duration calculation"""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=30)
        
        metrics = SessionExecutionMetrics(
            session_id="TEST_SESSION",
            execution_id="EXEC_001",
            start_time=start_time,
            end_time=end_time
        )
        
        duration = metrics.calculate_duration()
        assert duration is not None
        assert duration.total_seconds() == 30
        
    def test_throughput_calculation(self):
        """Test throughput calculation"""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=10)
        
        metrics = SessionExecutionMetrics(
            session_id="TEST_SESSION",
            execution_id="EXEC_001",
            start_time=start_time,
            end_time=end_time,
            total_rows_processed=1000
        )
        
        metrics.calculate_throughput()
        assert metrics.throughput_rows_per_second == 100.0

class TestCommitStrategyManager:
    """Test CommitStrategyManager class"""
    
    def test_commit_manager_creation(self):
        """Test commit manager creation"""
        manager = CommitStrategyManager()
        
        assert manager.spark is None
        assert len(manager.active_transactions) == 0
        assert len(manager.commit_counters) == 0
        
    def test_source_based_commit_strategy(self):
        """Test source-based commit strategy"""
        manager = CommitStrategyManager()
        
        # Create test session with source commit strategy
        session = XSDSession("TestSession", "MAPPING_001")
        session.commit_config.commit_type = CommitKind.SOURCE
        session.commit_config.commit_interval = 1000
        
        # Create test execution results
        execution_results = [
            ExecutionResult(
                instance_id="SOURCE_1",
                instance_name="SOURCE_CUSTOMER",
                state=ExecutionState.COMPLETED,
                rows_processed=1500
            )
        ]
        
        metrics = SessionExecutionMetrics("TEST_SESSION", "EXEC_001", datetime.now())
        
        # Apply commit strategy
        commit_points = manager.apply_commit_strategy(session, execution_results, metrics)
        
        assert len(commit_points) == 1
        assert commit_points[0].scope == CommitScope.SOURCE_BASED
        assert commit_points[0].row_count == 1500
        assert metrics.commit_count == 1
        assert metrics.total_rows_committed == 1500
        
    def test_target_based_commit_strategy(self):
        """Test target-based commit strategy"""
        manager = CommitStrategyManager()
        
        # Create test session with target commit strategy
        session = XSDSession("TestSession", "MAPPING_001")
        session.commit_config.commit_type = CommitKind.TARGET
        session.commit_config.commit_interval = 500
        
        # Create test execution results
        execution_results = [
            ExecutionResult(
                instance_id="TARGET_1",
                instance_name="TARGET_CUSTOMER",
                state=ExecutionState.COMPLETED,
                rows_processed=800
            )
        ]
        
        metrics = SessionExecutionMetrics("TEST_SESSION", "EXEC_001", datetime.now())
        
        # Apply commit strategy
        commit_points = manager.apply_commit_strategy(session, execution_results, metrics)
        
        assert len(commit_points) == 1
        assert commit_points[0].scope == CommitScope.TARGET_BASED
        assert commit_points[0].row_count == 800
        
    def test_user_defined_commit_strategy(self):
        """Test user-defined commit strategy"""
        manager = CommitStrategyManager()
        
        # Create test session with user-defined commit strategy
        session = XSDSession("TestSession", "MAPPING_001")
        session.commit_config.commit_type = CommitKind.USER_DEFINED
        session.commit_config.commit_interval = 1000
        session.commit_config.commit_on_end_of_file = True
        
        # Create test execution results
        execution_results = [
            ExecutionResult(
                instance_id="TRANS_1",
                instance_name="EXPRESSION_TRANSFORM",
                state=ExecutionState.COMPLETED,
                rows_processed=2500
            )
        ]
        
        metrics = SessionExecutionMetrics("TEST_SESSION", "EXEC_001", datetime.now())
        
        # Apply commit strategy
        commit_points = manager.apply_commit_strategy(session, execution_results, metrics)
        
        # Should have 2 commits (2 cycles of 1000) + 1 final commit (500 remaining)
        assert len(commit_points) == 3
        assert commit_points[0].scope == CommitScope.USER_DEFINED
        assert commit_points[0].row_count == 1000
        assert commit_points[1].row_count == 1000
        assert commit_points[2].row_count == 500  # Final commit
        
    def test_commit_execution_with_transactions(self):
        """Test commit execution with transaction support"""
        mock_spark = Mock()
        manager = CommitStrategyManager(mock_spark)
        
        session = XSDSession("TestSession", "MAPPING_001")
        session.commit_config.enable_transactions = True
        
        commit_point = CommitPoint(
            commit_id="TEST_COMMIT",
            timestamp=datetime.now(),
            scope=CommitScope.USER_DEFINED,
            row_count=1000,
            transformation_state={}
        )
        
        # Execute commit (should not raise exception)
        manager._execute_commit(session, commit_point)
        
        # Verify Spark transaction calls
        mock_spark.sql.assert_any_call("BEGIN")
        mock_spark.sql.assert_any_call("COMMIT")
        
        # Verify checkpoint data was created
        assert commit_point.checkpoint_data is not None
        assert commit_point.checkpoint_data['commit_id'] == "TEST_COMMIT"

class TestPushdownOptimizer:
    """Test PushdownOptimizer class"""
    
    def test_optimizer_creation(self):
        """Test optimizer creation"""
        optimizer = PushdownOptimizer()
        
        assert optimizer.spark is None
        assert len(optimizer.optimization_rules) > 0
        
    def test_no_optimization_strategy(self):
        """Test no optimization strategy"""
        optimizer = PushdownOptimizer()
        
        # Create test mapping and session
        mapping = XSDMapping("TestMapping")
        session = XSDSession("TestSession", "MAPPING_001")
        session.performance_config.pushdown_strategy = PushdownOptimizationKind.NONE
        
        optimized_mapping, report = optimizer.optimize_mapping_for_session(mapping, session)
        
        assert optimized_mapping == mapping  # Should be unchanged
        assert report['strategy'] == 'none'
        assert len(report['optimizations_applied']) == 0
        
    def test_source_optimization_strategy(self):
        """Test source optimization strategy"""
        optimizer = PushdownOptimizer()
        
        # Create test mapping and session
        mapping = XSDMapping("TestMapping")
        session = XSDSession("TestSession", "MAPPING_001")
        session.performance_config.pushdown_strategy = PushdownOptimizationKind.TO_SOURCE
        
        optimized_mapping, report = optimizer.optimize_mapping_for_session(mapping, session)
        
        assert report['strategy'] == 'toSource'
        assert len(report['optimizations_applied']) > 0
        assert '_optimize_filter_pushdown' in report['optimizations_applied']
        
    def test_target_optimization_strategy(self):
        """Test target optimization strategy"""
        optimizer = PushdownOptimizer()
        
        # Create test mapping and session
        mapping = XSDMapping("TestMapping")
        session = XSDSession("TestSession", "MAPPING_001")
        session.performance_config.pushdown_strategy = PushdownOptimizationKind.TO_TARGET
        
        optimized_mapping, report = optimizer.optimize_mapping_for_session(mapping, session)
        
        assert report['strategy'] == 'toTarget'
        assert len(report['optimizations_applied']) > 0
        
    def test_full_optimization_strategy(self):
        """Test full optimization strategy"""
        optimizer = PushdownOptimizer()
        
        # Create test mapping and session
        mapping = XSDMapping("TestMapping")
        session = XSDSession("TestSession", "MAPPING_001")
        session.performance_config.pushdown_strategy = PushdownOptimizationKind.FULL
        
        optimized_mapping, report = optimizer.optimize_mapping_for_session(mapping, session)
        
        assert report['strategy'] == 'full'
        assert len(report['optimizations_applied']) > 0
        assert 'full_query_pushdown' in report['optimizations_applied']
        assert 'expected_speedup' in report['performance_improvements']
        
    def test_auto_optimization_strategy(self):
        """Test auto optimization strategy"""
        optimizer = PushdownOptimizer()
        
        # Create test mapping and session
        mapping = XSDMapping("TestMapping")
        # Add some instances to influence auto optimization
        mapping.add_instance(XSDInstance("SOURCE_1", "Source"))
        mapping.add_instance(XSDInstance("TARGET_1", "Target"))
        
        session = XSDSession("TestSession", "MAPPING_001")
        session.performance_config.pushdown_strategy = PushdownOptimizationKind.AUTO
        
        optimized_mapping, report = optimizer.optimize_mapping_for_session(mapping, session)
        
        assert report['strategy'] == 'auto'
        assert len(report['optimizations_applied']) > 0
        # Should have chosen either auto_to_source or auto_to_target
        assert any('auto_to_' in opt for opt in report['optimizations_applied'])

class TestRecoveryManager:
    """Test RecoveryManager class"""
    
    def test_recovery_manager_creation(self):
        """Test recovery manager creation"""
        manager = RecoveryManager()
        
        assert len(manager.checkpoint_storage) == 0
        assert len(manager.error_tracker) == 0
        
    def test_checkpoint_recovery_strategy(self):
        """Test resume from checkpoint recovery strategy"""
        manager = RecoveryManager()
        
        # Create test session and context
        session = XSDSession("TestSession", "MAPPING_001")
        session.error_config.recovery_strategy = SessionRecoveryKind.RESUME_FROM_LAST_CHECKPOINT
        
        context = SessionExecutionContext(
            session_id="TestSession",
            execution_id="EXEC_001",
            start_time=datetime.now(),
            parameters={},
            connection_overrides={},
            environment_config={},
            runtime_variables={}
        )
        
        metrics = SessionExecutionMetrics("TestSession", "EXEC_001", datetime.now())
        
        # Create a checkpoint first
        manager.create_checkpoint(session, context, {"test": "data"})
        
        # Simulate failure and recovery
        test_error = RuntimeError("Test error")
        action, recovery_info = manager.handle_session_failure(session, test_error, context, metrics)
        
        assert action == RecoveryAction.RESUME
        assert recovery_info['strategy'] == 'resumeFromLastCheckpoint'
        assert recovery_info['checkpoint_found'] == True
        assert metrics.recovery_count == 1
        
    def test_restart_task_recovery_strategy(self):
        """Test restart task recovery strategy"""
        manager = RecoveryManager()
        
        # Create test session and context
        session = XSDSession("TestSession", "MAPPING_001")
        session.error_config.recovery_strategy = SessionRecoveryKind.RESTART_TASK
        session.error_config.retry_attempts = 3
        
        context = SessionExecutionContext(
            session_id="TestSession",
            execution_id="EXEC_001",
            start_time=datetime.now(),
            parameters={},
            connection_overrides={},
            environment_config={},
            runtime_variables={}
        )
        
        metrics = SessionExecutionMetrics("TestSession", "EXEC_001", datetime.now())
        
        # First failure - should restart
        test_error = RuntimeError("Test error")
        action, recovery_info = manager.handle_session_failure(session, test_error, context, metrics)
        
        assert action == RecoveryAction.RESTART
        assert recovery_info['strategy'] == 'restartTask'
        assert 'attempt 1/3' in recovery_info['action']
        
    def test_fail_task_recovery_strategy(self):
        """Test fail task recovery strategy"""
        manager = RecoveryManager()
        
        # Create test session and context
        session = XSDSession("TestSession", "MAPPING_001")
        session.error_config.recovery_strategy = SessionRecoveryKind.FAIL_TASK
        
        context = SessionExecutionContext(
            session_id="TestSession",
            execution_id="EXEC_001",
            start_time=datetime.now(),
            parameters={},
            connection_overrides={},
            environment_config={},
            runtime_variables={}
        )
        
        metrics = SessionExecutionMetrics("TestSession", "EXEC_001", datetime.now())
        
        # Simulate failure
        test_error = RuntimeError("Test error")
        action, recovery_info = manager.handle_session_failure(session, test_error, context, metrics)
        
        assert action == RecoveryAction.ABORT
        assert recovery_info['strategy'] == 'failTask'
        
    def test_max_retries_exceeded(self):
        """Test behavior when max retries are exceeded"""
        manager = RecoveryManager()
        
        # Create test session with limited retries
        session = XSDSession("TestSession", "MAPPING_001")
        session.error_config.recovery_strategy = SessionRecoveryKind.RESTART_TASK
        session.error_config.retry_attempts = 2
        
        context = SessionExecutionContext(
            session_id="TestSession",
            execution_id="EXEC_001",
            start_time=datetime.now(),
            parameters={},
            connection_overrides={},
            environment_config={},
            runtime_variables={}
        )
        
        metrics = SessionExecutionMetrics("TestSession", "EXEC_001", datetime.now())
        test_error = RuntimeError("Test error")
        
        # First two failures should restart
        action1, _ = manager.handle_session_failure(session, test_error, context, metrics)
        assert action1 == RecoveryAction.RESTART
        
        action2, _ = manager.handle_session_failure(session, test_error, context, metrics)
        assert action2 == RecoveryAction.ABORT  # Max retries exceeded
        
    def test_error_threshold_checking(self):
        """Test error threshold checking"""
        manager = RecoveryManager()
        
        # Session that stops on any error
        session = XSDSession("TestSession", "MAPPING_001")
        session.error_config.stop_on_errors = True
        
        assert manager.should_stop_on_error(session, 1) == True
        assert manager.should_stop_on_error(session, 0) == False
        
        # Session with error threshold
        session.error_config.stop_on_errors = False
        session.error_config.maximum_data_errors = 5
        
        assert manager.should_stop_on_error(session, 3) == False
        assert manager.should_stop_on_error(session, 5) == True
        assert manager.should_stop_on_error(session, 10) == True

class TestSessionAwareExecutionEngine:
    """Test SessionAwareExecutionEngine class"""
    
    def test_engine_creation(self):
        """Test engine creation and initialization"""
        engine = SessionAwareExecutionEngine()
        
        assert engine.commit_manager is not None
        assert engine.pushdown_optimizer is not None
        assert engine.recovery_manager is not None
        assert len(engine.active_sessions) == 0
        assert len(engine.session_results) == 0
        
    def test_session_validation_for_execution(self):
        """Test session validation before execution"""
        engine = SessionAwareExecutionEngine()
        
        # Create invalid session (no mapping reference)
        invalid_session = XSDSession("InvalidSession")
        mapping = XSDMapping("TestMapping")
        
        with pytest.raises(ValueError) as exc_info:
            engine._validate_session_for_execution(invalid_session, mapping)
            
        assert "Session validation failed" in str(exc_info.value)
        
        # Create valid session
        valid_session = XSDSession("ValidSession", "MAPPING_001")
        env = NativeExecutionEnvironment()
        valid_session.runtime_characteristic.add_execution_environment(env)
        
        # Should not raise exception
        engine._validate_session_for_execution(valid_session, mapping)
        
    def test_execution_environment_configuration(self):
        """Test execution environment configuration"""
        engine = SessionAwareExecutionEngine()
        
        # Create session with Spark environment
        session = XSDSession("TestSession", "MAPPING_001")
        spark_env = SparkExecutionEnvironment("TestSpark")
        spark_env.add_spark_config("test.config", "test.value")
        session.runtime_characteristic.add_execution_environment(spark_env)
        
        context = SessionExecutionContext(
            session_id="TestSession",
            execution_id="EXEC_001",
            start_time=datetime.now(),
            parameters={"TEST_PARAM": "test_value"},
            connection_overrides={},
            environment_config={},
            runtime_variables={}
        )
        
        execution_context = engine._configure_execution_environment(session, context)
        
        assert 'session_config' in execution_context
        assert 'environment_config' in execution_context
        assert 'runtime_parameters' in execution_context
        assert 'buffer_config' in execution_context
        assert 'spark_config' in execution_context
        
        # Verify buffer configuration
        buffer_config = execution_context['buffer_config']
        assert buffer_config['buffer_block_size'] == session.performance_config.buffer_block_size
        
        # Verify Spark configuration
        spark_config = execution_context['spark_config']
        assert 'test.config' in spark_config
        assert spark_config['test.config'] == 'test.value'
        
    @patch('src.core.xsd_session_runtime.SessionAwareExecutionEngine._execute_mapping_with_session_config')
    @patch('src.core.xsd_session_runtime.SessionAwareExecutionEngine._validate_session_for_execution')
    def test_successful_session_execution(self, mock_validate, mock_execute_mapping):
        """Test successful session execution"""
        engine = SessionAwareExecutionEngine()
        
        # Setup mocks
        mock_validate.return_value = None
        mock_execute_mapping.return_value = [
            ExecutionResult(
                instance_id="TEST_INSTANCE",
                instance_name="TEST_EXPRESSION",
                state=ExecutionState.COMPLETED,
                rows_processed=1000
            )
        ]
        
        # Create test session and mapping
        session = XSDSession("TestSession", "MAPPING_001")
        session.performance_config.pushdown_strategy = PushdownOptimizationKind.NONE
        env = NativeExecutionEnvironment()
        session.runtime_characteristic.add_execution_environment(env)
        
        mapping = XSDMapping("TestMapping")
        
        context = SessionExecutionContext(
            session_id="TestSession",
            execution_id="EXEC_001",
            start_time=datetime.now(),
            parameters={},
            connection_overrides={},
            environment_config={},
            runtime_variables={}
        )
        
        # Execute session
        result = engine.execute_session(session, mapping, context)
        
        assert result.status == SessionExecutionStatus.COMPLETED
        assert result.session_id == "TestSession"
        assert result.execution_id == "EXEC_001"
        assert result.is_successful() == True
        assert len(result.execution_results) == 1
        assert result.metrics.total_rows_processed == 1000
        
        # Verify result is stored
        assert "EXEC_001" in engine.session_results
        
    def test_session_execution_with_error_handling(self):
        """Test session execution with error handling"""
        engine = SessionAwareExecutionEngine()
        
        # Create session that fails on errors
        session = XSDSession("TestSession", "MAPPING_001")
        session.error_config.recovery_strategy = SessionRecoveryKind.FAIL_TASK
        env = NativeExecutionEnvironment()
        session.runtime_characteristic.add_execution_environment(env)
        
        mapping = XSDMapping("TestMapping")
        
        context = SessionExecutionContext(
            session_id="TestSession",
            execution_id="EXEC_001",
            start_time=datetime.now(),
            parameters={},
            connection_overrides={},
            environment_config={},
            runtime_variables={}
        )
        
        # Mock validation to raise an error
        with patch.object(engine, '_validate_session_for_execution') as mock_validate:
            mock_validate.side_effect = RuntimeError("Test validation error")
            
            result = engine.execute_session(session, mapping, context)
            
            assert result.status == SessionExecutionStatus.FAILED
            assert len(result.errors) == 1
            assert "Test validation error" in result.errors[0]
            assert result.is_successful() == False
            
    def test_execution_summary_retrieval(self):
        """Test execution summary retrieval"""
        engine = SessionAwareExecutionEngine()
        
        # Create test result
        metrics = SessionExecutionMetrics("TEST_SESSION", "EXEC_001", datetime.now())
        metrics.total_rows_processed = 5000
        metrics.commit_count = 5
        
        result = SessionExecutionResult(
            session_id="TEST_SESSION",
            execution_id="EXEC_001",
            status=SessionExecutionStatus.COMPLETED,
            metrics=metrics,
            commit_points=[],
            execution_results=[],
            errors=[],
            warnings=[],
            recovery_actions=[]
        )
        
        engine.session_results["EXEC_001"] = result
        
        # Test summary retrieval
        summary = engine.get_session_execution_summary("EXEC_001")
        
        assert summary is not None
        assert summary['session_id'] == "TEST_SESSION"
        assert summary['execution_id'] == "EXEC_001"
        assert summary['status'] == 'completed'
        assert summary['successful'] == True
        assert summary['rows_processed'] == 5000
        assert summary['commit_count'] == 5
        
        # Test non-existent execution
        assert engine.get_session_execution_summary("NON_EXISTENT") is None
        
    def test_active_session_metrics(self):
        """Test active session metrics retrieval"""
        engine = SessionAwareExecutionEngine()
        
        # Add active session metrics
        metrics = SessionExecutionMetrics("ACTIVE_SESSION", "EXEC_001", datetime.now())
        engine.active_sessions["EXEC_001"] = metrics
        
        active_metrics = engine.get_active_session_metrics()
        
        assert len(active_metrics) == 1
        assert "EXEC_001" in active_metrics
        assert active_metrics["EXEC_001"].session_id == "ACTIVE_SESSION"
        
    def test_session_cleanup(self):
        """Test cleanup of completed sessions"""
        engine = SessionAwareExecutionEngine()
        
        # Create old completed session
        old_time = datetime.now() - timedelta(hours=25)  # Older than 24 hours
        old_metrics = SessionExecutionMetrics("OLD_SESSION", "OLD_EXEC", old_time)
        old_metrics.end_time = old_time + timedelta(minutes=30)
        
        old_result = SessionExecutionResult(
            session_id="OLD_SESSION",
            execution_id="OLD_EXEC",
            status=SessionExecutionStatus.COMPLETED,
            metrics=old_metrics,
            commit_points=[],
            execution_results=[],
            errors=[],
            warnings=[],
            recovery_actions=[]
        )
        
        # Create recent session
        recent_time = datetime.now() - timedelta(hours=1)
        recent_metrics = SessionExecutionMetrics("RECENT_SESSION", "RECENT_EXEC", recent_time)
        recent_metrics.end_time = recent_time + timedelta(minutes=30)
        
        recent_result = SessionExecutionResult(
            session_id="RECENT_SESSION",
            execution_id="RECENT_EXEC",
            status=SessionExecutionStatus.COMPLETED,
            metrics=recent_metrics,
            commit_points=[],
            execution_results=[],
            errors=[],
            warnings=[],
            recovery_actions=[]
        )
        
        engine.session_results["OLD_EXEC"] = old_result
        engine.session_results["RECENT_EXEC"] = recent_result
        engine.active_sessions["OLD_EXEC"] = old_metrics
        engine.active_sessions["RECENT_EXEC"] = recent_metrics
        
        # Cleanup with 24 hour retention
        engine.cleanup_completed_sessions(retention_hours=24)
        
        # Old session should be removed, recent should remain
        assert "OLD_EXEC" not in engine.session_results
        assert "RECENT_EXEC" in engine.session_results
        assert "OLD_EXEC" not in engine.active_sessions
        assert "RECENT_EXEC" in engine.active_sessions

class TestSessionExecutionResult:
    """Test SessionExecutionResult class"""
    
    def test_result_creation(self):
        """Test session execution result creation"""
        metrics = SessionExecutionMetrics("TEST_SESSION", "EXEC_001", datetime.now())
        
        result = SessionExecutionResult(
            session_id="TEST_SESSION",
            execution_id="EXEC_001",
            status=SessionExecutionStatus.COMPLETED,
            metrics=metrics,
            commit_points=[],
            execution_results=[],
            errors=[],
            warnings=[],
            recovery_actions=[]
        )
        
        assert result.session_id == "TEST_SESSION"
        assert result.execution_id == "EXEC_001"
        assert result.status == SessionExecutionStatus.COMPLETED
        assert result.is_successful() == True
        
    def test_failed_result(self):
        """Test failed session execution result"""
        metrics = SessionExecutionMetrics("TEST_SESSION", "EXEC_001", datetime.now())
        
        result = SessionExecutionResult(
            session_id="TEST_SESSION",
            execution_id="EXEC_001",
            status=SessionExecutionStatus.FAILED,
            metrics=metrics,
            commit_points=[],
            execution_results=[],
            errors=["Test error"],
            warnings=[],
            recovery_actions=[]
        )
        
        assert result.status == SessionExecutionStatus.FAILED
        assert result.is_successful() == False
        assert len(result.errors) == 1
        
    def test_recovered_result(self):
        """Test recovered session execution result"""
        metrics = SessionExecutionMetrics("TEST_SESSION", "EXEC_001", datetime.now())
        
        result = SessionExecutionResult(
            session_id="TEST_SESSION",
            execution_id="EXEC_001",
            status=SessionExecutionStatus.RECOVERED,
            metrics=metrics,
            commit_points=[],
            execution_results=[],
            errors=[],
            warnings=[],
            recovery_actions=[{"action": "resumed_from_checkpoint"}]
        )
        
        assert result.status == SessionExecutionStatus.RECOVERED
        assert result.is_successful() == True  # Recovered is considered successful
        assert len(result.recovery_actions) == 1
        
    def test_result_summary(self):
        """Test execution result summary generation"""
        start_time = datetime.now()
        end_time = start_time + timedelta(seconds=45)
        
        metrics = SessionExecutionMetrics(
            session_id="TEST_SESSION",
            execution_id="EXEC_001",
            start_time=start_time,
            end_time=end_time,
            total_rows_processed=2000,
            total_rows_committed=1800,
            commit_count=3,
            total_errors=1,
            recovery_count=1,
            throughput_rows_per_second=44.4
        )
        
        result = SessionExecutionResult(
            session_id="TEST_SESSION",
            execution_id="EXEC_001",
            status=SessionExecutionStatus.COMPLETED,
            metrics=metrics,
            commit_points=[],
            execution_results=[],
            errors=[],
            warnings=[],
            recovery_actions=[]
        )
        
        summary = result.get_summary()
        
        assert summary['session_id'] == "TEST_SESSION"
        assert summary['execution_id'] == "EXEC_001"
        assert summary['status'] == 'completed'
        assert summary['successful'] == True
        assert summary['duration_seconds'] == 45.0
        assert summary['rows_processed'] == 2000
        assert summary['rows_committed'] == 1800
        assert summary['commit_count'] == 3
        assert summary['error_count'] == 1
        assert summary['recovery_count'] == 1
        assert summary['throughput_rps'] == 44.4

if __name__ == "__main__":
    pytest.main([__file__])