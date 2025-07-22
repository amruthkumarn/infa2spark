"""
XSD-compliant data flow execution engine
Executes mapping instances and handles data flow through ports
"""
from typing import Dict, List, Optional, Any, Set, Tuple
from enum import Enum
from dataclasses import dataclass
import logging
from abc import ABC, abstractmethod

from .xsd_mapping_model import (
    XSDMapping, XSDInstance, XSDPort, XSDFieldMapLinkage, XSDLoadOrderStrategy,
    PortDirection, LoadOrderType
)
from .xsd_base_classes import Element, PMDataType
from .reference_manager import ReferenceManager

class BufferOverflowError(Exception):
    """Exception raised when buffer exceeds capacity"""
    pass

class ExecutionState(Enum):
    """Execution state enumeration"""
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"

class ExecutionMode(Enum):
    """Execution mode enumeration"""
    SEQUENTIAL = "SEQUENTIAL"
    PARALLEL = "PARALLEL"
    OPTIMIZED = "OPTIMIZED"

@dataclass
class ExecutionContext:
    """Context for execution with parameters and state"""
    session_id: str
    mapping_id: str
    parameters: Dict[str, Any]
    variables: Dict[str, Any]
    debug_mode: bool = False
    max_errors: int = 10
    batch_size: int = 10000

@dataclass
class ExecutionResult:
    """Result of execution with statistics"""
    instance_id: str
    instance_name: str
    state: ExecutionState
    rows_processed: int = 0
    rows_rejected: int = 0
    execution_time_ms: int = 0
    error_message: Optional[str] = None
    output_data: Optional[Any] = None

class DataFlowBuffer:
    """Buffer for data flowing between instances"""
    
    def __init__(self, port_id: str, buffer_size: int = 10000):
        self.port_id = port_id
        self.buffer_size = buffer_size
        self.data: List[Dict[str, Any]] = []
        self.metadata: Dict[str, Any] = {}
        self.is_eof = False
        
    def add_row(self, row: Dict[str, Any]):
        """Add a row to the buffer"""
        if len(self.data) >= self.buffer_size:
            raise BufferOverflowError(f"Buffer overflow for port {self.port_id}")
        self.data.append(row)
        
    def get_rows(self, count: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get rows from buffer"""
        if count is None:
            rows = self.data[:]
            self.data.clear()
            return rows
        else:
            rows = self.data[:count]
            self.data = self.data[count:]
            return rows
            
    def has_data(self) -> bool:
        """Check if buffer has data"""
        return len(self.data) > 0
        
    def set_eof(self):
        """Mark end of data"""
        self.is_eof = True
        
    def is_complete(self) -> bool:
        """Check if buffer is complete (EOF and no data)"""
        return self.is_eof and not self.has_data()

class TransformationExecutor(ABC):
    """Abstract base class for transformation executors"""
    
    def __init__(self, instance: XSDInstance, context: ExecutionContext):
        self.instance = instance
        self.context = context
        self.logger = logging.getLogger(f"Executor.{instance.name}")
        
    @abstractmethod
    def execute(self, input_buffers: Dict[str, DataFlowBuffer]) -> Dict[str, DataFlowBuffer]:
        """Execute transformation with input buffers and return output buffers"""
        pass
        
    def validate_inputs(self, input_buffers: Dict[str, DataFlowBuffer]) -> List[str]:
        """Validate input buffers against required ports"""
        errors = []
        required_ports = [port.id for port in self.instance.input_ports if port.is_required]
        
        for port_id in required_ports:
            if port_id not in input_buffers:
                errors.append(f"Missing required input for port {port_id}")
            elif not input_buffers[port_id].has_data() and not input_buffers[port_id].is_complete():
                errors.append(f"No data available for required port {port_id}")
                
        return errors

class SourceExecutor(TransformationExecutor):
    """Executor for source instances"""
    
    def execute(self, input_buffers: Dict[str, DataFlowBuffer]) -> Dict[str, DataFlowBuffer]:
        """Execute source to produce output data"""
        output_buffers = {}
        
        # Create output buffers for each output port
        for port in self.instance.output_ports:
            buffer = DataFlowBuffer(port.id, self.context.batch_size)
            
            # Generate mock data based on port configuration
            data = self._generate_source_data(port)
            for row in data:
                buffer.add_row(row)
            buffer.set_eof()
            
            output_buffers[port.id] = buffer
            
        return output_buffers
        
    def _generate_source_data(self, port: XSDPort) -> List[Dict[str, Any]]:
        """Generate source data for a port"""
        # This is a simplified implementation - would connect to actual data sources
        sample_data = []
        for i in range(100):  # Generate 100 sample rows
            row = {
                'ID': i + 1,
                'NAME': f'Record_{i + 1}',
                'VALUE': (i + 1) * 10,
                'TIMESTAMP': '2024-01-01 00:00:00'
            }
            sample_data.append(row)
        return sample_data

class TargetExecutor(TransformationExecutor):
    """Executor for target instances"""
    
    def execute(self, input_buffers: Dict[str, DataFlowBuffer]) -> Dict[str, DataFlowBuffer]:
        """Execute target to consume input data"""
        total_rows = 0
        
        # Process all input buffers
        for port_id, buffer in input_buffers.items():
            while buffer.has_data():
                rows = buffer.get_rows(self.context.batch_size)
                self._write_target_data(rows)
                total_rows += len(rows)
                
        self.logger.info(f"Target {self.instance.name} processed {total_rows} rows")
        
        # Target instances don't produce output
        return {}
        
    def _write_target_data(self, rows: List[Dict[str, Any]]):
        """Write data to target"""
        # This is a simplified implementation - would write to actual targets
        self.logger.debug(f"Writing {len(rows)} rows to target {self.instance.name}")

class TransformationInstanceExecutor(TransformationExecutor):
    """Executor for transformation instances"""
    
    def execute(self, input_buffers: Dict[str, DataFlowBuffer]) -> Dict[str, DataFlowBuffer]:
        """Execute transformation logic"""
        output_buffers = {}
        
        # Create output buffers
        for port in self.instance.output_ports:
            output_buffers[port.id] = DataFlowBuffer(port.id, self.context.batch_size)
            
        # Process input data through transformation
        self._process_transformation(input_buffers, output_buffers)
        
        # Mark all output buffers as complete
        for buffer in output_buffers.values():
            buffer.set_eof()
            
        return output_buffers
        
    def _process_transformation(self, input_buffers: Dict[str, DataFlowBuffer], 
                              output_buffers: Dict[str, DataFlowBuffer]):
        """Process data through transformation logic"""
        # This is a simplified implementation - would execute actual transformation logic
        for port_id, input_buffer in input_buffers.items():
            while input_buffer.has_data():
                rows = input_buffer.get_rows(self.context.batch_size)
                
                # Apply transformation logic (example: add calculated field)
                transformed_rows = []
                for row in rows:
                    transformed_row = row.copy()
                    transformed_row['PROCESSED_FLAG'] = 'Y'
                    transformed_row['CALCULATED_VALUE'] = row.get('VALUE', 0) * 2
                    transformed_rows.append(transformed_row)
                
                # Route to appropriate output ports
                for output_port in self.instance.output_ports:
                    if output_port.id in output_buffers:
                        for row in transformed_rows:
                            output_buffers[output_port.id].add_row(row)

class ExecutionPlan:
    """Execution plan for a mapping"""
    
    def __init__(self, mapping: XSDMapping):
        self.mapping = mapping
        self.execution_order: List[str] = []
        self.dependencies: Dict[str, Set[str]] = {}
        self.parallel_groups: List[List[str]] = []
        
    def build_execution_plan(self):
        """Build execution plan based on data flow"""
        # Build dependency graph
        self._build_dependency_graph()
        
        # Calculate execution order using topological sort
        self._calculate_execution_order()
        
        # Identify parallel execution opportunities
        self._identify_parallel_groups()
        
    def _build_dependency_graph(self):
        """Build dependency graph from field map linkages"""
        # Initialize dependencies
        for instance in self.mapping.instances:
            self.dependencies[instance.id] = set()
            
        # Add dependencies from field map linkages
        if self.mapping.field_map_spec:
            for linkage in self.mapping.field_map_spec.field_map_linkages:
                if linkage.from_instance_ref and linkage.to_instance_ref:
                    # to_instance depends on from_instance
                    self.dependencies[linkage.to_instance_ref].add(linkage.from_instance_ref)
                    
        # Add dependencies from load order constraints
        if self.mapping.load_order_strategy:
            for constraint in self.mapping.load_order_strategy.constraints:
                if constraint.constraint_type == LoadOrderType.ONE_AFTER_ANOTHER:
                    # secondary depends on primary
                    if (constraint.primary_instance_ref and constraint.secondary_instance_ref and
                        constraint.secondary_instance_ref in self.dependencies):
                        self.dependencies[constraint.secondary_instance_ref].add(
                            constraint.primary_instance_ref)
                            
    def _calculate_execution_order(self):
        """Calculate execution order using topological sort"""
        # Kahn's algorithm for topological sorting
        in_degree = {instance_id: len(deps) for instance_id, deps in self.dependencies.items()}
        queue = [instance_id for instance_id, degree in in_degree.items() if degree == 0]
        
        while queue:
            current = queue.pop(0)
            self.execution_order.append(current)
            
            # Update in-degrees for dependent instances
            for instance_id, deps in self.dependencies.items():
                if current in deps:
                    in_degree[instance_id] -= 1
                    if in_degree[instance_id] == 0:
                        queue.append(instance_id)
                        
        # Check for cycles
        if len(self.execution_order) != len(self.dependencies):
            remaining = set(self.dependencies.keys()) - set(self.execution_order)
            raise ValueError(f"Circular dependency detected in instances: {remaining}")
            
    def _identify_parallel_groups(self):
        """Identify instances that can execute in parallel"""
        # Group instances by dependency level
        levels: Dict[int, List[str]] = {}
        instance_levels: Dict[str, int] = {}
        
        # Calculate dependency levels
        for instance_id in self.execution_order:
            max_dep_level = -1
            for dep_id in self.dependencies[instance_id]:
                if dep_id in instance_levels:
                    max_dep_level = max(max_dep_level, instance_levels[dep_id])
                    
            level = max_dep_level + 1
            instance_levels[instance_id] = level
            
            if level not in levels:
                levels[level] = []
            levels[level].append(instance_id)
            
        # Convert to parallel groups
        self.parallel_groups = [levels[level] for level in sorted(levels.keys())]

class XSDExecutionEngine:
    """Main execution engine for XSD mappings"""
    
    def __init__(self, execution_mode: ExecutionMode = ExecutionMode.SEQUENTIAL):
        self.execution_mode = execution_mode
        self.logger = logging.getLogger("XSDExecutionEngine")
        
    def execute_mapping(self, mapping: XSDMapping, context: ExecutionContext) -> List[ExecutionResult]:
        """Execute a complete mapping"""
        self.logger.info(f"Starting execution of mapping: {mapping.name}")
        
        # Build execution plan
        plan = ExecutionPlan(mapping)
        plan.build_execution_plan()
        
        # Execute based on mode
        if self.execution_mode == ExecutionMode.SEQUENTIAL:
            return self._execute_sequential(mapping, plan, context)
        elif self.execution_mode == ExecutionMode.PARALLEL:
            return self._execute_parallel(mapping, plan, context)
        else:  # OPTIMIZED
            return self._execute_optimized(mapping, plan, context)
            
    def _execute_sequential(self, mapping: XSDMapping, plan: ExecutionPlan, 
                          context: ExecutionContext) -> List[ExecutionResult]:
        """Execute mapping sequentially"""
        results = []
        instance_buffers: Dict[str, Dict[str, DataFlowBuffer]] = {}
        
        for instance_id in plan.execution_order:
            instance = mapping.get_instance(instance_id)
            if not instance:
                continue
                
            try:
                # Get input buffers for this instance
                input_buffers = self._get_input_buffers(instance, instance_buffers)
                
                # Create appropriate executor
                executor = self._create_executor(instance, context)
                
                # Execute transformation
                start_time = self._get_current_time_ms()
                output_buffers = executor.execute(input_buffers)
                end_time = self._get_current_time_ms()
                
                # Store output buffers
                instance_buffers[instance_id] = output_buffers
                
                # Calculate statistics
                rows_processed = sum(len(buf.data) for buf in output_buffers.values())
                
                result = ExecutionResult(
                    instance_id=instance_id,
                    instance_name=instance.name,
                    state=ExecutionState.COMPLETED,
                    rows_processed=rows_processed,
                    execution_time_ms=end_time - start_time
                )
                results.append(result)
                
                self.logger.info(f"Completed execution of instance: {instance.name}")
                
            except Exception as e:
                self.logger.error(f"Error executing instance {instance.name}: {e}")
                result = ExecutionResult(
                    instance_id=instance_id,
                    instance_name=instance.name,
                    state=ExecutionState.FAILED,
                    error_message=str(e)
                )
                results.append(result)
                
                # Stop execution on error in sequential mode
                break
                
        return results
        
    def _execute_parallel(self, mapping: XSDMapping, plan: ExecutionPlan, 
                         context: ExecutionContext) -> List[ExecutionResult]:
        """Execute mapping with parallel groups"""
        # This is a simplified implementation - would use actual threading/multiprocessing
        results = []
        instance_buffers: Dict[str, Dict[str, DataFlowBuffer]] = {}
        
        for group in plan.parallel_groups:
            group_results = []
            
            # Execute all instances in group (simulated parallel execution)
            for instance_id in group:
                instance = mapping.get_instance(instance_id)
                if not instance:
                    continue
                    
                try:
                    input_buffers = self._get_input_buffers(instance, instance_buffers)
                    executor = self._create_executor(instance, context)
                    
                    start_time = self._get_current_time_ms()
                    output_buffers = executor.execute(input_buffers)
                    end_time = self._get_current_time_ms()
                    
                    instance_buffers[instance_id] = output_buffers
                    
                    rows_processed = sum(len(buf.data) for buf in output_buffers.values())
                    
                    result = ExecutionResult(
                        instance_id=instance_id,
                        instance_name=instance.name,
                        state=ExecutionState.COMPLETED,
                        rows_processed=rows_processed,
                        execution_time_ms=end_time - start_time
                    )
                    group_results.append(result)
                    
                except Exception as e:
                    self.logger.error(f"Error executing instance {instance.name}: {e}")
                    result = ExecutionResult(
                        instance_id=instance_id,
                        instance_name=instance.name,
                        state=ExecutionState.FAILED,
                        error_message=str(e)
                    )
                    group_results.append(result)
                    
            results.extend(group_results)
            
            # Check for failures in group
            failed_count = len([r for r in group_results if r.state == ExecutionState.FAILED])
            if failed_count > 0:
                self.logger.error(f"Execution stopped due to {failed_count} failures in parallel group")
                break
                
        return results
        
    def _execute_optimized(self, mapping: XSDMapping, plan: ExecutionPlan, 
                          context: ExecutionContext) -> List[ExecutionResult]:
        """Execute mapping with optimizations"""
        # For now, use parallel execution with additional optimizations
        return self._execute_parallel(mapping, plan, context)
        
    def _get_input_buffers(self, instance: XSDInstance, 
                          instance_buffers: Dict[str, Dict[str, DataFlowBuffer]]) -> Dict[str, DataFlowBuffer]:
        """Get input buffers for an instance based on field map linkages"""
        input_buffers = {}
        
        # Find linkages that feed into this instance
        mapping = None  # Would need to pass mapping reference
        if hasattr(instance, '_parent_mapping') and instance._parent_mapping:
            mapping = instance._parent_mapping
            
        if mapping and mapping.field_map_spec:
            for linkage in mapping.field_map_spec.field_map_linkages:
                if linkage.to_instance_ref == instance.id:
                    # Find the source instance and its output buffer
                    from_instance_id = linkage.from_instance_ref
                    if from_instance_id in instance_buffers:
                        # Map output buffer to input port
                        for port in instance.input_ports:
                            # Simplified mapping - would need more sophisticated port resolution
                            if port.id not in input_buffers and instance_buffers[from_instance_id]:
                                # Take the first available output buffer
                                available_buffers = list(instance_buffers[from_instance_id].values())
                                if available_buffers:
                                    input_buffers[port.id] = available_buffers[0]
                                    break
                                    
        return input_buffers
        
    def _create_executor(self, instance: XSDInstance, context: ExecutionContext) -> TransformationExecutor:
        """Create appropriate executor for instance type"""
        if instance.is_source():
            return SourceExecutor(instance, context)
        elif instance.is_target():
            return TargetExecutor(instance, context)
        else:
            return TransformationInstanceExecutor(instance, context)
            
    def _get_current_time_ms(self) -> int:
        """Get current time in milliseconds"""
        import time
        return int(time.time() * 1000)
        
    def validate_mapping_for_execution(self, mapping: XSDMapping) -> List[str]:
        """Validate mapping is ready for execution"""
        errors = []
        
        # Check basic mapping structure
        if not mapping.instances:
            errors.append("Mapping has no instances")
            
        # Check for at least one source and one target
        sources = mapping.get_source_instances()
        targets = mapping.get_target_instances()
        
        if not sources:
            errors.append("Mapping has no source instances")
        if not targets:
            errors.append("Mapping has no target instances")
            
        # Validate data flow
        data_flow_errors = mapping.validate_data_flow()
        errors.extend(data_flow_errors)
        
        # Check execution plan can be built
        try:
            plan = ExecutionPlan(mapping)
            plan.build_execution_plan()
        except Exception as e:
            errors.append(f"Cannot build execution plan: {e}")
            
        return errors

# Utility functions for execution monitoring and debugging
class ExecutionMonitor:
    """Monitor and track execution progress"""
    
    def __init__(self):
        self.execution_history: List[ExecutionResult] = []
        self.active_executions: Dict[str, ExecutionContext] = {}
        
    def start_execution(self, context: ExecutionContext):
        """Start monitoring an execution"""
        self.active_executions[context.session_id] = context
        
    def complete_execution(self, session_id: str, results: List[ExecutionResult]):
        """Complete execution monitoring"""
        if session_id in self.active_executions:
            del self.active_executions[session_id]
        self.execution_history.extend(results)
        
    def get_execution_statistics(self) -> Dict[str, Any]:
        """Get execution statistics"""
        total_executions = len(self.execution_history)
        successful = len([r for r in self.execution_history if r.state == ExecutionState.COMPLETED])
        failed = len([r for r in self.execution_history if r.state == ExecutionState.FAILED])
        
        return {
            'total_executions': total_executions,
            'successful_executions': successful,
            'failed_executions': failed,
            'success_rate': successful / total_executions if total_executions > 0 else 0,
            'active_executions': len(self.active_executions)
        }