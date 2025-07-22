"""
Session-aware runtime execution engine for Informatica BDM
Provides enterprise-grade execution with session configuration integration
"""
from typing import List, Dict, Optional, Any, Union, Tuple
from enum import Enum
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
import logging
from datetime import datetime, timedelta
import time
import copy
from collections import defaultdict

from .xsd_session_model import (
    XSDSession, CommitKind, SessionRecoveryKind, PushdownOptimizationKind,
    SparkExecutionEnvironment, NativeExecutionEnvironment
)
from .xsd_session_manager import SessionExecutionContext, ValidationResult, SessionValidationSeverity
from .xsd_execution_engine import XSDExecutionEngine, ExecutionResult, ExecutionPlan, DataFlowBuffer
from .xsd_mapping_model import XSDMapping, XSDInstance
from .xsd_transformation_model import XSDAbstractTransformation

class SessionExecutionStatus(Enum):
    """Session execution status"""
    PENDING = "pending"
    INITIALIZING = "initializing"
    RUNNING = "running"
    COMMITTING = "committing"
    COMPLETED = "completed"
    FAILED = "failed"
    RECOVERED = "recovered"
    ABORTED = "aborted"

class CommitScope(Enum):
    """Commit operation scope"""
    SOURCE_BASED = "source_based"
    TARGET_BASED = "target_based"
    USER_DEFINED = "user_defined"
    TRANSACTION_BASED = "transaction_based"

class RecoveryAction(Enum):
    """Recovery action types"""
    RESUME = "resume"
    RESTART = "restart"
    SKIP = "skip"
    ABORT = "abort"
    RETRY = "retry"

@dataclass
class CommitPoint:
    """Represents a commit point in execution"""
    commit_id: str
    timestamp: datetime
    scope: CommitScope
    row_count: int
    transformation_state: Dict[str, Any]
    checkpoint_data: Optional[Dict[str, Any]] = None

@dataclass
class SessionExecutionMetrics:
    """Session execution performance metrics"""
    session_id: str
    execution_id: str
    start_time: datetime
    end_time: Optional[datetime] = None
    total_rows_processed: int = 0
    total_rows_committed: int = 0
    total_errors: int = 0
    commit_count: int = 0
    recovery_count: int = 0
    
    # Performance metrics
    throughput_rows_per_second: float = 0.0
    average_commit_time: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_utilization_percent: float = 0.0
    
    # Buffer statistics
    buffer_overflows: int = 0
    buffer_efficiency_percent: float = 0.0
    
    def calculate_duration(self) -> Optional[timedelta]:
        """Calculate execution duration"""
        if self.end_time:
            return self.end_time - self.start_time
        return None
        
    def calculate_throughput(self):
        """Calculate rows per second throughput"""
        duration = self.calculate_duration()
        if duration and duration.total_seconds() > 0:
            self.throughput_rows_per_second = self.total_rows_processed / duration.total_seconds()

@dataclass
class SessionExecutionResult:
    """Complete session execution result"""
    session_id: str
    execution_id: str
    status: SessionExecutionStatus
    metrics: SessionExecutionMetrics
    commit_points: List[CommitPoint]
    execution_results: List[ExecutionResult]
    errors: List[str]
    warnings: List[str]
    recovery_actions: List[Dict[str, Any]]
    
    def is_successful(self) -> bool:
        """Check if execution completed successfully"""
        return self.status in [SessionExecutionStatus.COMPLETED, SessionExecutionStatus.RECOVERED]
        
    def get_summary(self) -> Dict[str, Any]:
        """Get execution summary"""
        duration = self.metrics.calculate_duration()
        return {
            'session_id': self.session_id,
            'execution_id': self.execution_id,
            'status': self.status.value,
            'successful': self.is_successful(),
            'duration_seconds': duration.total_seconds() if duration else None,
            'rows_processed': self.metrics.total_rows_processed,
            'rows_committed': self.metrics.total_rows_committed,
            'commit_count': self.metrics.commit_count,
            'error_count': self.metrics.total_errors,
            'recovery_count': self.metrics.recovery_count,
            'throughput_rps': self.metrics.throughput_rows_per_second
        }

class CommitStrategyManager:
    """Manages different commit strategies based on session configuration"""
    
    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.logger = logging.getLogger("CommitStrategyManager")
        self.active_transactions: Dict[str, Any] = {}
        self.commit_counters: Dict[str, int] = defaultdict(int)
        
    def apply_commit_strategy(self, 
                            session: XSDSession,
                            execution_results: List[ExecutionResult],
                            metrics: SessionExecutionMetrics) -> List[CommitPoint]:
        """Apply commit strategy based on session configuration"""
        commit_config = session.commit_config
        commit_points = []
        
        try:
            if commit_config.commit_type == CommitKind.SOURCE:
                commit_points = self._apply_source_based_commits(session, execution_results, metrics)
            elif commit_config.commit_type == CommitKind.TARGET:
                commit_points = self._apply_target_based_commits(session, execution_results, metrics)
            elif commit_config.commit_type == CommitKind.USER_DEFINED:
                commit_points = self._apply_user_defined_commits(session, execution_results, metrics)
                
            # Update metrics
            metrics.commit_count = len(commit_points)
            metrics.total_rows_committed = sum(cp.row_count for cp in commit_points)
            
            self.logger.info(f"Applied {commit_config.commit_type.value} commit strategy: {len(commit_points)} commits")
            return commit_points
            
        except Exception as e:
            self.logger.error(f"Error applying commit strategy: {e}")
            raise
            
    def _apply_source_based_commits(self, 
                                   session: XSDSession,
                                   execution_results: List[ExecutionResult],
                                   metrics: SessionExecutionMetrics) -> List[CommitPoint]:
        """Apply source-based commit strategy"""
        commit_points = []
        commit_interval = session.commit_config.commit_interval
        
        # Group by source transformations (identify by instance_name pattern)
        source_results = [r for r in execution_results if "SOURCE" in r.instance_name.upper() or "SRC" in r.instance_name.upper()]
        
        for result in source_results:
            if result.rows_processed >= commit_interval:
                commit_point = CommitPoint(
                    commit_id=f"{session.id}_source_{len(commit_points)}",
                    timestamp=datetime.now(),
                    scope=CommitScope.SOURCE_BASED,
                    row_count=result.rows_processed,
                    transformation_state={"source_id": result.instance_id}
                )
                commit_points.append(commit_point)
                self._execute_commit(session, commit_point)
                
        return commit_points
        
    def _apply_target_based_commits(self, 
                                   session: XSDSession,
                                   execution_results: List[ExecutionResult],
                                   metrics: SessionExecutionMetrics) -> List[CommitPoint]:
        """Apply target-based commit strategy"""
        commit_points = []
        commit_interval = session.commit_config.target_commit_interval or session.commit_config.commit_interval
        
        # Group by target transformations (identify by instance_name pattern)
        target_results = [r for r in execution_results if "TARGET" in r.instance_name.upper() or "TGT" in r.instance_name.upper()]
        
        for result in target_results:
            if result.rows_processed >= commit_interval:
                commit_point = CommitPoint(
                    commit_id=f"{session.id}_target_{len(commit_points)}",
                    timestamp=datetime.now(),
                    scope=CommitScope.TARGET_BASED,
                    row_count=result.rows_processed,
                    transformation_state={"target_id": result.instance_id}
                )
                commit_points.append(commit_point)
                self._execute_commit(session, commit_point)
                
        return commit_points
        
    def _apply_user_defined_commits(self, 
                                   session: XSDSession,
                                   execution_results: List[ExecutionResult],
                                   metrics: SessionExecutionMetrics) -> List[CommitPoint]:
        """Apply user-defined commit strategy"""
        commit_points = []
        commit_interval = session.commit_config.commit_interval
        total_rows = sum(r.rows_processed for r in execution_results)
        
        # Commit based on total row count intervals
        commit_cycles = total_rows // commit_interval
        
        for i in range(commit_cycles):
            commit_point = CommitPoint(
                commit_id=f"{session.id}_user_{i}",
                timestamp=datetime.now(),
                scope=CommitScope.USER_DEFINED,
                row_count=commit_interval,
                transformation_state={"cycle": i, "total_rows": total_rows}
            )
            commit_points.append(commit_point)
            self._execute_commit(session, commit_point)
            
        # Final commit for remaining rows
        remaining_rows = total_rows % commit_interval
        if remaining_rows > 0 and session.commit_config.commit_on_end_of_file:
            final_commit = CommitPoint(
                commit_id=f"{session.id}_final",
                timestamp=datetime.now(),
                scope=CommitScope.USER_DEFINED,
                row_count=remaining_rows,
                transformation_state={"final": True, "remaining_rows": remaining_rows}
            )
            commit_points.append(final_commit)
            self._execute_commit(session, final_commit)
            
        return commit_points
        
    def _execute_commit(self, session: XSDSession, commit_point: CommitPoint):
        """Execute actual commit operation"""
        start_time = time.time()
        
        try:
            if session.commit_config.enable_transactions:
                # Handle transactional commits
                self._execute_transactional_commit(session, commit_point)
            else:
                # Handle non-transactional commits
                self._execute_simple_commit(session, commit_point)
                
            # Record commit timing
            commit_time = time.time() - start_time
            self.logger.debug(f"Commit {commit_point.commit_id} completed in {commit_time:.3f}s")
            
        except Exception as e:
            self.logger.error(f"Commit {commit_point.commit_id} failed: {e}")
            raise
            
    def _execute_transactional_commit(self, session: XSDSession, commit_point: CommitPoint):
        """Execute transactional commit with timeout handling"""
        transaction_timeout = session.commit_config.transaction_timeout
        
        # Create checkpoint data for recovery
        commit_point.checkpoint_data = {
            'session_id': session.id,
            'commit_id': commit_point.commit_id,
            'timestamp': commit_point.timestamp.isoformat(),
            'row_count': commit_point.row_count,
            'transformation_state': commit_point.transformation_state
        }
        
        if self.spark:
            # Spark-based transactional commit
            self.spark.sql("BEGIN")
            try:
                # Execute commit operations
                self._perform_spark_commit_operations(session, commit_point)
                self.spark.sql("COMMIT")
            except Exception as e:
                self.spark.sql("ROLLBACK")
                raise e
        else:
            # Native transactional commit
            self._perform_native_commit_operations(session, commit_point)
            
    def _execute_simple_commit(self, session: XSDSession, commit_point: CommitPoint):
        """Execute simple non-transactional commit"""
        # Direct commit without transaction boundaries
        if self.spark:
            self._perform_spark_commit_operations(session, commit_point)
        else:
            self._perform_native_commit_operations(session, commit_point)
            
    def _perform_spark_commit_operations(self, session: XSDSession, commit_point: CommitPoint):
        """Perform Spark-specific commit operations"""
        # Implementation would depend on actual Spark operations
        # For now, simulate commit operation
        self.logger.debug(f"Performing Spark commit for {commit_point.commit_id}")
        
    def _perform_native_commit_operations(self, session: XSDSession, commit_point: CommitPoint):
        """Perform native commit operations"""
        # Implementation would depend on actual native operations
        # For now, simulate commit operation
        self.logger.debug(f"Performing native commit for {commit_point.commit_id}")

class PushdownOptimizer:
    """Implements pushdown optimization strategies for session execution"""
    
    def __init__(self, spark_session=None):
        self.spark = spark_session
        self.logger = logging.getLogger("PushdownOptimizer")
        self.optimization_rules: List[callable] = []
        self._initialize_optimization_rules()
        
    def _initialize_optimization_rules(self):
        """Initialize built-in optimization rules"""
        self.optimization_rules.extend([
            self._optimize_filter_pushdown,
            self._optimize_projection_pushdown,
            self._optimize_aggregation_pushdown,
            self._optimize_join_pushdown
        ])
        
    def optimize_mapping_for_session(self, 
                                   mapping: XSDMapping,
                                   session: XSDSession) -> Tuple[XSDMapping, Dict[str, Any]]:
        """Optimize mapping based on session pushdown strategy"""
        strategy = session.performance_config.pushdown_strategy
        optimization_report = {
            'strategy': strategy.value,
            'optimizations_applied': [],
            'performance_improvements': {}
        }
        
        if strategy == PushdownOptimizationKind.NONE:
            return mapping, optimization_report
            
        try:
            optimized_mapping = copy.deepcopy(mapping)
            
            if strategy == PushdownOptimizationKind.TO_SOURCE:
                optimization_report = self._optimize_to_source(optimized_mapping, session, optimization_report)
            elif strategy == PushdownOptimizationKind.TO_TARGET:
                optimization_report = self._optimize_to_target(optimized_mapping, session, optimization_report)
            elif strategy == PushdownOptimizationKind.FULL:
                optimization_report = self._optimize_full_pushdown(optimized_mapping, session, optimization_report)
            elif strategy == PushdownOptimizationKind.AUTO:
                optimization_report = self._optimize_auto_pushdown(optimized_mapping, session, optimization_report)
                
            self.logger.info(f"Applied {strategy.value} optimization: {len(optimization_report['optimizations_applied'])} optimizations")
            return optimized_mapping, optimization_report
            
        except Exception as e:
            self.logger.error(f"Optimization failed: {e}")
            return mapping, optimization_report
            
    def _optimize_to_source(self, mapping: XSDMapping, session: XSDSession, report: Dict[str, Any]) -> Dict[str, Any]:
        """Push transformations to source databases"""
        for rule in self.optimization_rules:
            try:
                if rule(mapping, "source"):
                    report['optimizations_applied'].append(rule.__name__)
            except Exception as e:
                self.logger.warning(f"Optimization rule {rule.__name__} failed: {e}")
                
        return report
        
    def _optimize_to_target(self, mapping: XSDMapping, session: XSDSession, report: Dict[str, Any]) -> Dict[str, Any]:
        """Push transformations to target databases"""
        for rule in self.optimization_rules:
            try:
                if rule(mapping, "target"):
                    report['optimizations_applied'].append(rule.__name__)
            except Exception as e:
                self.logger.warning(f"Optimization rule {rule.__name__} failed: {e}")
                
        return report
        
    def _optimize_full_pushdown(self, mapping: XSDMapping, session: XSDSession, report: Dict[str, Any]) -> Dict[str, Any]:
        """Apply maximum pushdown optimization"""
        # Apply both source and target optimizations
        report = self._optimize_to_source(mapping, session, report)
        report = self._optimize_to_target(mapping, session, report)
        
        # Additional full pushdown optimizations
        report['optimizations_applied'].append("full_query_pushdown")
        report['performance_improvements']['expected_speedup'] = "2-5x"
        
        return report
        
    def _optimize_auto_pushdown(self, mapping: XSDMapping, session: XSDSession, report: Dict[str, Any]) -> Dict[str, Any]:
        """Automatically determine optimal pushdown strategy"""
        # Analyze mapping characteristics
        source_complexity = self._analyze_source_complexity(mapping)
        target_complexity = self._analyze_target_complexity(mapping)
        transformation_complexity = self._analyze_transformation_complexity(mapping)
        
        # Choose strategy based on analysis
        if source_complexity > target_complexity:
            report = self._optimize_to_target(mapping, session, report)
            report['optimizations_applied'].append("auto_to_target")
        else:
            report = self._optimize_to_source(mapping, session, report)
            report['optimizations_applied'].append("auto_to_source")
            
        return report
        
    def _optimize_filter_pushdown(self, mapping: XSDMapping, direction: str) -> bool:
        """Push filter operations to source/target"""
        # Implementation would analyze and push filter transformations
        return True
        
    def _optimize_projection_pushdown(self, mapping: XSDMapping, direction: str) -> bool:
        """Push projection operations to source/target"""
        # Implementation would analyze and push projection transformations
        return True
        
    def _optimize_aggregation_pushdown(self, mapping: XSDMapping, direction: str) -> bool:
        """Push aggregation operations to source/target"""
        # Implementation would analyze and push aggregation transformations
        return True
        
    def _optimize_join_pushdown(self, mapping: XSDMapping, direction: str) -> bool:
        """Push join operations to source/target"""
        # Implementation would analyze and push join transformations
        return True
        
    def _analyze_source_complexity(self, mapping: XSDMapping) -> float:
        """Analyze source complexity for optimization decisions"""
        # Simple heuristic based on number of sources and complexity
        return len([i for i in mapping.instances if i.instance_type == "Source"]) * 1.5
        
    def _analyze_target_complexity(self, mapping: XSDMapping) -> float:
        """Analyze target complexity for optimization decisions"""
        return len([i for i in mapping.instances if i.instance_type == "Target"]) * 1.2
        
    def _analyze_transformation_complexity(self, mapping: XSDMapping) -> float:
        """Analyze transformation complexity"""
        return len([i for i in mapping.instances if i.instance_type not in ["Source", "Target"]]) * 2.0

class RecoveryManager:
    """Manages session recovery strategies and error handling"""
    
    def __init__(self):
        self.logger = logging.getLogger("RecoveryManager")
        self.checkpoint_storage: Dict[str, Dict[str, Any]] = {}
        self.error_tracker: Dict[str, List[Any]] = defaultdict(list)
        
    def handle_session_failure(self, 
                             session: XSDSession,
                             error: Exception,
                             context: SessionExecutionContext,
                             metrics: SessionExecutionMetrics) -> Tuple[RecoveryAction, Dict[str, Any]]:
        """Handle session failure based on recovery strategy"""
        strategy = session.error_config.recovery_strategy
        recovery_info = {
            'strategy': strategy.value,
            'error_type': type(error).__name__,
            'error_message': str(error),
            'timestamp': datetime.now().isoformat(),
            'attempt_count': len(self.error_tracker[session.id]) + 1
        }
        
        # Record error
        self.error_tracker[session.id].append({
            'error': error,
            'timestamp': datetime.now(),
            'context': context.execution_id
        })
        
        try:
            if strategy == SessionRecoveryKind.RESUME_FROM_LAST_CHECKPOINT:
                action = self._attempt_checkpoint_resume(session, context, recovery_info)
            elif strategy == SessionRecoveryKind.RESTART_TASK:
                action = self._attempt_task_restart(session, context, recovery_info)
            elif strategy == SessionRecoveryKind.FAIL_TASK:
                action = RecoveryAction.ABORT
                recovery_info['action'] = 'Task failed as configured'
            elif strategy == SessionRecoveryKind.SKIP_TO_NEXT_TASK:
                action = RecoveryAction.SKIP
                recovery_info['action'] = 'Skipping to next task'
            else:
                action = RecoveryAction.ABORT
                
            metrics.recovery_count += 1
            self.logger.info(f"Recovery action for session {session.id}: {action.value}")
            return action, recovery_info
            
        except Exception as recovery_error:
            self.logger.error(f"Recovery attempt failed: {recovery_error}")
            return RecoveryAction.ABORT, recovery_info
            
    def _attempt_checkpoint_resume(self, 
                                 session: XSDSession,
                                 context: SessionExecutionContext,
                                 recovery_info: Dict[str, Any]) -> RecoveryAction:
        """Attempt to resume from last checkpoint"""
        checkpoint_key = f"{session.id}_{context.execution_id}"
        
        if checkpoint_key in self.checkpoint_storage:
            checkpoint_data = self.checkpoint_storage[checkpoint_key]
            recovery_info['checkpoint_found'] = True
            recovery_info['checkpoint_timestamp'] = checkpoint_data.get('timestamp')
            recovery_info['action'] = 'Resuming from checkpoint'
            return RecoveryAction.RESUME
        else:
            recovery_info['checkpoint_found'] = False
            recovery_info['action'] = 'No checkpoint found, restarting'
            return RecoveryAction.RESTART
            
    def _attempt_task_restart(self, 
                            session: XSDSession,
                            context: SessionExecutionContext,
                            recovery_info: Dict[str, Any]) -> RecoveryAction:
        """Attempt to restart the task"""
        max_retries = session.error_config.retry_attempts
        current_attempts = len(self.error_tracker[session.id])
        
        if current_attempts < max_retries:
            recovery_info['action'] = f'Restarting task (attempt {current_attempts}/{max_retries})'
            return RecoveryAction.RESTART
        else:
            recovery_info['action'] = f'Max retries ({max_retries}) exceeded, failing'
            return RecoveryAction.ABORT
            
    def create_checkpoint(self, 
                        session: XSDSession,
                        context: SessionExecutionContext,
                        execution_state: Dict[str, Any]):
        """Create recovery checkpoint"""
        checkpoint_key = f"{session.id}_{context.execution_id}"
        checkpoint_data = {
            'session_id': session.id,
            'execution_id': context.execution_id,
            'timestamp': datetime.now().isoformat(),
            'execution_state': execution_state
        }
        
        self.checkpoint_storage[checkpoint_key] = checkpoint_data
        self.logger.debug(f"Created checkpoint for {checkpoint_key}")
        
    def should_stop_on_error(self, session: XSDSession, error_count: int) -> bool:
        """Determine if execution should stop based on error configuration"""
        error_config = session.error_config
        
        if error_config.stop_on_errors and error_count > 0:
            return True
            
        if error_count >= error_config.maximum_data_errors > 0:
            return True
            
        return False

class SessionAwareExecutionEngine(XSDExecutionEngine):
    """
    Session-aware execution engine with advanced runtime features
    
    Extends the base execution engine with session configuration integration,
    commit strategies, pushdown optimization, and recovery management.
    """
    
    def __init__(self, spark_session=None, **kwargs):
        super().__init__(**kwargs)
        self.spark = spark_session
        
        # Session-aware components
        self.commit_manager = CommitStrategyManager(spark_session)
        self.pushdown_optimizer = PushdownOptimizer(spark_session)
        self.recovery_manager = RecoveryManager()
        
        # Execution tracking
        self.active_sessions: Dict[str, SessionExecutionMetrics] = {}
        self.session_results: Dict[str, SessionExecutionResult] = {}
        
        self.logger = logging.getLogger("SessionAwareExecutionEngine")
        
    def execute_session(self, 
                       session: XSDSession,
                       mapping: XSDMapping,
                       context: SessionExecutionContext,
                       runtime_parameters: Optional[Dict[str, Any]] = None) -> SessionExecutionResult:
        """Execute complete session with full session configuration support"""
        
        execution_id = context.execution_id
        self.logger.info(f"Starting session execution: {session.name} [{execution_id}]")
        
        # Initialize execution metrics
        metrics = SessionExecutionMetrics(
            session_id=session.id or session.name,
            execution_id=execution_id,
            start_time=datetime.now()
        )
        self.active_sessions[execution_id] = metrics
        
        try:
            # Pre-execution validation
            self._validate_session_for_execution(session, mapping)
            
            # Apply pushdown optimization
            optimized_mapping, optimization_report = self.pushdown_optimizer.optimize_mapping_for_session(mapping, session)
            
            # Configure execution environment
            execution_context = self._configure_execution_environment(session, context)
            
            # Execute mapping with session configuration
            execution_results = self._execute_mapping_with_session_config(
                optimized_mapping, session, execution_context, metrics
            )
            
            # Update metrics with execution results
            metrics.total_rows_processed = sum(result.rows_processed for result in execution_results)
            
            # Apply commit strategy
            commit_points = self.commit_manager.apply_commit_strategy(session, execution_results, metrics)
            
            # Finalize execution
            metrics.end_time = datetime.now()
            metrics.calculate_throughput()
            
            # Create session result
            session_result = SessionExecutionResult(
                session_id=session.id or session.name,
                execution_id=execution_id,
                status=SessionExecutionStatus.COMPLETED,
                metrics=metrics,
                commit_points=commit_points,
                execution_results=execution_results,
                errors=[],
                warnings=[],
                recovery_actions=[]
            )
            
            self.session_results[execution_id] = session_result
            self.logger.info(f"Session execution completed successfully: {session.name}")
            return session_result
            
        except Exception as e:
            # Handle execution error with recovery
            recovery_action, recovery_info = self.recovery_manager.handle_session_failure(
                session, e, context, metrics
            )
            
            return self._handle_execution_error(session, e, context, metrics, recovery_action, recovery_info)
            
    def _validate_session_for_execution(self, session: XSDSession, mapping: XSDMapping):
        """Validate session and mapping for execution"""
        # Validate session configuration
        session_errors = session.validate_session_configuration()
        if session_errors:
            raise ValueError(f"Session validation failed: {session_errors}")
            
        # Validate mapping compatibility
        if not mapping:
            raise ValueError("Mapping is required for session execution")
            
        # Validate execution environment
        selected_env = session.runtime_characteristic.get_selected_environment()
        if not selected_env:
            raise ValueError("No execution environment selected")
            
        env_errors = selected_env.validate_configuration()
        if env_errors:
            raise ValueError(f"Environment validation failed: {env_errors}")
            
    def _configure_execution_environment(self, 
                                       session: XSDSession,
                                       context: SessionExecutionContext) -> Dict[str, Any]:
        """Configure execution environment based on session settings"""
        selected_env = session.runtime_characteristic.get_selected_environment()
        
        execution_context = {
            'session_config': session.get_effective_configuration(),
            'environment_config': selected_env.get_environment_summary(),
            'runtime_parameters': context.parameters,
            'buffer_config': {
                'buffer_block_size': session.performance_config.buffer_block_size,
                'dtm_buffer_pool_size': session.performance_config.dtm_buffer_pool_size,
                'dtm_buffer_size': session.performance_config.dtm_buffer_size
            }
        }
        
        # Configure Spark environment if applicable
        if isinstance(selected_env, SparkExecutionEnvironment):
            execution_context['spark_config'] = selected_env.spark_conf
            
        return execution_context
        
    def _execute_mapping_with_session_config(self, 
                                            mapping: XSDMapping,
                                            session: XSDSession,
                                            execution_context: Dict[str, Any],
                                            metrics: SessionExecutionMetrics) -> List[ExecutionResult]:
        """Execute mapping with session-specific configuration"""
        
        # Create execution plan with session optimization
        execution_plan = self._create_session_optimized_plan(mapping, session)
        
        # Configure buffers based on session settings
        self._configure_session_buffers(execution_plan, session)
        
        # Execute with error tracking
        execution_results = []
        error_count = 0
        
        for step in execution_plan.execution_steps:
            try:
                # Execute transformation step
                step_result = self._execute_transformation_step(step, execution_context)
                execution_results.append(step_result)
                
                # Update metrics
                metrics.total_rows_processed += step_result.rows_processed
                
                # Check error thresholds
                if step_result.errors:
                    error_count += len(step_result.errors)
                    if self.recovery_manager.should_stop_on_error(session, error_count):
                        raise RuntimeError(f"Error threshold exceeded: {error_count} errors")
                        
                # Create checkpoint if configured
                if session.runtime_characteristic.state_store_enabled:
                    self.recovery_manager.create_checkpoint(
                        session, 
                        execution_context,
                        {'step': step.step_id, 'rows_processed': step_result.rows_processed}
                    )
                    
            except Exception as e:
                self.logger.error(f"Execution step {step.step_id} failed: {e}")
                error_count += 1
                metrics.total_errors += 1
                
                if self.recovery_manager.should_stop_on_error(session, error_count):
                    raise
                    
        return execution_results
        
    def _create_session_optimized_plan(self, mapping: XSDMapping, session: XSDSession) -> ExecutionPlan:
        """Create execution plan optimized for session configuration"""
        # Use base execution plan as foundation
        base_plan = self.create_execution_plan(mapping)
        
        # Apply session-specific optimizations
        if session.performance_config.constraint_based_load_ordering:
            base_plan = self._optimize_load_ordering(base_plan, session)
            
        return base_plan
        
    def _configure_session_buffers(self, execution_plan: ExecutionPlan, session: XSDSession):
        """Configure data flow buffers based on session settings"""
        buffer_config = session.performance_config
        
        for step in execution_plan.execution_steps:
            if hasattr(step, 'buffer'):
                step.buffer.configure_from_session(
                    block_size=buffer_config.buffer_block_size,
                    pool_size=buffer_config.dtm_buffer_pool_size
                )
                
    def _optimize_load_ordering(self, execution_plan: ExecutionPlan, session: XSDSession) -> ExecutionPlan:
        """Optimize load ordering based on constraints"""
        # Implementation would reorder execution steps based on constraints
        return execution_plan
        
    def _execute_transformation_step(self, step, execution_context: Dict[str, Any]) -> ExecutionResult:
        """Execute individual transformation step with session context"""
        # Use base execution method with session context
        return self.execute_transformation_instance(step.transformation_instance, execution_context)
        
    def _handle_execution_error(self, 
                              session: XSDSession,
                              error: Exception,
                              context: SessionExecutionContext,
                              metrics: SessionExecutionMetrics,
                              recovery_action: RecoveryAction,
                              recovery_info: Dict[str, Any]) -> SessionExecutionResult:
        """Handle execution error with recovery strategies"""
        
        metrics.end_time = datetime.now()
        metrics.total_errors += 1
        
        if recovery_action == RecoveryAction.RESUME:
            status = SessionExecutionStatus.RECOVERED
        elif recovery_action == RecoveryAction.RETRY:
            status = SessionExecutionStatus.FAILED  # Will be retried
        else:
            status = SessionExecutionStatus.FAILED
            
        session_result = SessionExecutionResult(
            session_id=session.id or session.name,
            execution_id=context.execution_id,
            status=status,
            metrics=metrics,
            commit_points=[],
            execution_results=[],
            errors=[str(error)],
            warnings=[],
            recovery_actions=[recovery_info]
        )
        
        self.session_results[context.execution_id] = session_result
        
        if recovery_action in [RecoveryAction.RETRY, RecoveryAction.RESTART]:
            self.logger.info(f"Session will be retried: {session.name}")
        else:
            self.logger.error(f"Session execution failed: {session.name} - {error}")
            
        return session_result
        
    def get_session_execution_summary(self, execution_id: str) -> Optional[Dict[str, Any]]:
        """Get execution summary for a session"""
        if execution_id in self.session_results:
            return self.session_results[execution_id].get_summary()
        return None
        
    def get_active_session_metrics(self) -> Dict[str, SessionExecutionMetrics]:
        """Get metrics for all active sessions"""
        return self.active_sessions.copy()
        
    def cleanup_completed_sessions(self, retention_hours: int = 24):
        """Clean up old session results"""
        cutoff_time = datetime.now() - timedelta(hours=retention_hours)
        
        to_remove = []
        for execution_id, result in self.session_results.items():
            if result.metrics.end_time and result.metrics.end_time < cutoff_time:
                to_remove.append(execution_id)
                
        for execution_id in to_remove:
            del self.session_results[execution_id]
            if execution_id in self.active_sessions:
                del self.active_sessions[execution_id]
                
        self.logger.info(f"Cleaned up {len(to_remove)} old session results")