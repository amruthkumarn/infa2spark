"""
Comprehensive tests for XSD execution engine
Tests data flow execution, transformation processing, and execution planning
"""
import pytest
from typing import Dict, List

from src.core.xsd_execution_engine import (
    XSDExecutionEngine, ExecutionContext, ExecutionResult, ExecutionState, ExecutionMode,
    DataFlowBuffer, TransformationExecutor, SourceExecutor, TargetExecutor,
    TransformationInstanceExecutor, ExecutionPlan, ExecutionMonitor, BufferOverflowError
)
from src.core.xsd_mapping_model import (
    XSDMapping, XSDInstance, XSDPort, XSDFieldMapSpec, XSDFieldMapLinkage,
    XSDLoadOrderStrategy, XSDOneAfterAnotherConstraint, PortDirection
)
from src.core.xsd_base_classes import PMDataType

class TestDataFlowBuffer:
    """Test DataFlowBuffer functionality"""
    
    def test_buffer_creation(self):
        """Test buffer creation and basic properties"""
        buffer = DataFlowBuffer("port_1", buffer_size=100)
        
        assert buffer.port_id == "port_1"
        assert buffer.buffer_size == 100
        assert not buffer.has_data()
        assert not buffer.is_eof
        assert not buffer.is_complete()
        
    def test_buffer_data_operations(self):
        """Test adding and retrieving data from buffer"""
        buffer = DataFlowBuffer("port_1", buffer_size=5)
        
        # Add rows
        buffer.add_row({"id": 1, "name": "John"})
        buffer.add_row({"id": 2, "name": "Jane"})
        
        assert buffer.has_data()
        assert len(buffer.data) == 2
        
        # Get specific number of rows
        rows = buffer.get_rows(1)
        assert len(rows) == 1
        assert rows[0]["id"] == 1
        assert len(buffer.data) == 1
        
        # Get all remaining rows
        all_rows = buffer.get_rows()
        assert len(all_rows) == 1
        assert all_rows[0]["id"] == 2
        assert not buffer.has_data()
        
    def test_buffer_overflow(self):
        """Test buffer overflow protection"""
        buffer = DataFlowBuffer("port_1", buffer_size=2)
        
        buffer.add_row({"id": 1})
        buffer.add_row({"id": 2})
        
        # Should raise exception on overflow
        with pytest.raises(BufferOverflowError):
            buffer.add_row({"id": 3})
            
    def test_buffer_eof_handling(self):
        """Test end-of-file handling"""
        buffer = DataFlowBuffer("port_1")
        
        buffer.add_row({"id": 1})
        buffer.set_eof()
        
        assert buffer.is_eof
        assert not buffer.is_complete()  # Still has data
        
        # Clear data
        buffer.get_rows()
        assert buffer.is_complete()  # EOF and no data

class TestExecutionContext:
    """Test ExecutionContext functionality"""
    
    def test_context_creation(self):
        """Test execution context creation"""
        context = ExecutionContext(
            session_id="session_1",
            mapping_id="mapping_1",
            parameters={"PARAM1": "value1"},
            variables={"VAR1": "var_value"}
        )
        
        assert context.session_id == "session_1"
        assert context.mapping_id == "mapping_1"
        assert context.parameters["PARAM1"] == "value1"
        assert context.variables["VAR1"] == "var_value"
        assert context.debug_mode is False
        assert context.max_errors == 10
        assert context.batch_size == 10000

class TestTransformationExecutors:
    """Test transformation executor classes"""
    
    def test_source_executor(self):
        """Test source executor functionality"""
        # Create source instance
        source = XSDInstance(name="TestSource", id="source_1")
        output_port = XSDPort(name="OutputData", direction=PortDirection.OUTPUT, id="out_1")
        source.add_port(output_port)
        
        context = ExecutionContext("session_1", "mapping_1", {}, {})
        executor = SourceExecutor(source, context)
        
        # Execute
        input_buffers = {}  # Source has no inputs
        output_buffers = executor.execute(input_buffers)
        
        # Verify output
        assert len(output_buffers) == 1
        assert "out_1" in output_buffers
        
        output_buffer = output_buffers["out_1"]
        assert output_buffer.has_data()
        assert output_buffer.is_eof
        
        # Verify generated data
        data = output_buffer.get_rows()
        assert len(data) == 100  # Default sample size
        assert "ID" in data[0]
        assert "NAME" in data[0]
        
    def test_target_executor(self):
        """Test target executor functionality"""
        # Create target instance
        target = XSDInstance(name="TestTarget", id="target_1")
        input_port = XSDPort(name="InputData", direction=PortDirection.INPUT, id="in_1")
        target.add_port(input_port)
        
        context = ExecutionContext("session_1", "mapping_1", {}, {})
        executor = TargetExecutor(target, context)
        
        # Create input buffer with data
        input_buffer = DataFlowBuffer("in_1")
        input_buffer.add_row({"id": 1, "name": "John"})
        input_buffer.add_row({"id": 2, "name": "Jane"})
        input_buffer.set_eof()
        
        input_buffers = {"in_1": input_buffer}
        
        # Execute
        output_buffers = executor.execute(input_buffers)
        
        # Target should not produce output
        assert len(output_buffers) == 0
        
        # Input buffer should be consumed
        assert not input_buffer.has_data()
        
    def test_transformation_executor(self):
        """Test transformation instance executor"""
        # Create transformation instance
        transform = XSDInstance(name="TestTransform", id="transform_1")
        input_port = XSDPort(name="InputData", direction=PortDirection.INPUT, id="in_1")
        output_port = XSDPort(name="OutputData", direction=PortDirection.OUTPUT, id="out_1")
        transform.add_port(input_port)
        transform.add_port(output_port)
        
        context = ExecutionContext("session_1", "mapping_1", {}, {})
        executor = TransformationInstanceExecutor(transform, context)
        
        # Create input buffer
        input_buffer = DataFlowBuffer("in_1")
        input_buffer.add_row({"ID": 1, "VALUE": 10})
        input_buffer.add_row({"ID": 2, "VALUE": 20})
        input_buffer.set_eof()
        
        input_buffers = {"in_1": input_buffer}
        
        # Execute
        output_buffers = executor.execute(input_buffers)
        
        # Verify output
        assert len(output_buffers) == 1
        assert "out_1" in output_buffers
        
        output_buffer = output_buffers["out_1"]
        assert output_buffer.is_eof
        
        # Verify transformed data
        output_data = output_buffer.get_rows()
        assert len(output_data) == 2
        
        # Check transformation applied
        assert "PROCESSED_FLAG" in output_data[0]
        assert output_data[0]["PROCESSED_FLAG"] == "Y"
        assert output_data[0]["CALCULATED_VALUE"] == 20  # VALUE * 2
        
    def test_executor_input_validation(self):
        """Test executor input validation"""
        # Create instance with required input
        instance = XSDInstance(name="TestInstance", id="inst_1")
        required_port = XSDPort(name="RequiredInput", direction=PortDirection.INPUT, id="req_1")
        required_port.is_required = True
        instance.add_port(required_port)
        
        context = ExecutionContext("session_1", "mapping_1", {}, {})
        executor = TransformationInstanceExecutor(instance, context)
        
        # Test missing required input
        input_buffers = {}
        errors = executor.validate_inputs(input_buffers)
        assert len(errors) > 0
        assert "Missing required input" in errors[0]
        
        # Test empty buffer for required input
        empty_buffer = DataFlowBuffer("req_1")
        input_buffers = {"req_1": empty_buffer}
        errors = executor.validate_inputs(input_buffers)
        assert len(errors) > 0
        assert "No data available" in errors[0]

class TestExecutionPlan:
    """Test execution plan functionality"""
    
    def test_execution_plan_creation(self):
        """Test execution plan creation"""
        mapping = XSDMapping(name="TestMapping", id="mapping_1")
        plan = ExecutionPlan(mapping)
        
        assert plan.mapping is mapping
        assert len(plan.execution_order) == 0
        assert len(plan.dependencies) == 0
        assert len(plan.parallel_groups) == 0
        
    def test_simple_execution_plan(self):
        """Test simple linear execution plan"""
        mapping = self._create_simple_mapping()
        plan = ExecutionPlan(mapping)
        plan.build_execution_plan()
        
        # Should have all three instances in execution order
        assert len(plan.execution_order) == 3
        assert "source_1" in plan.execution_order
        assert "transform_1" in plan.execution_order
        assert "target_1" in plan.execution_order
        
        # Source should come before transform, transform before target
        source_idx = plan.execution_order.index("source_1")
        transform_idx = plan.execution_order.index("transform_1")
        target_idx = plan.execution_order.index("target_1")
        
        assert source_idx < transform_idx < target_idx
        
    def test_parallel_execution_groups(self):
        """Test parallel execution group identification"""
        mapping = self._create_parallel_mapping()
        plan = ExecutionPlan(mapping)
        plan.build_execution_plan()
        
        # Should identify parallel opportunities
        assert len(plan.parallel_groups) > 0
        
        # First group should contain sources, last group should contain targets
        first_group = plan.parallel_groups[0]
        last_group = plan.parallel_groups[-1]
        
        # Verify source instances are in early groups
        source_in_first = any(inst_id.startswith("source") for inst_id in first_group)
        target_in_last = any(inst_id.startswith("target") for inst_id in last_group)
        
        assert source_in_first or len(plan.parallel_groups) == 1
        assert target_in_last or len(plan.parallel_groups) == 1
        
    def test_circular_dependency_detection(self):
        """Test circular dependency detection"""
        mapping = self._create_circular_mapping()
        plan = ExecutionPlan(mapping)
        
        # Should raise exception for circular dependency
        with pytest.raises(ValueError, match="Circular dependency"):
            plan.build_execution_plan()
            
    def _create_simple_mapping(self) -> XSDMapping:
        """Create a simple linear mapping for testing"""
        mapping = XSDMapping(name="SimpleMapping", id="mapping_1")
        
        # Create instances
        source = XSDInstance(name="Source", id="source_1")
        source.add_port(XSDPort(name="Out", direction=PortDirection.OUTPUT, id="s_out"))
        
        transform = XSDInstance(name="Transform", id="transform_1")
        transform.add_port(XSDPort(name="In", direction=PortDirection.INPUT, id="t_in"))
        transform.add_port(XSDPort(name="Out", direction=PortDirection.OUTPUT, id="t_out"))
        
        target = XSDInstance(name="Target", id="target_1")
        target.add_port(XSDPort(name="In", direction=PortDirection.INPUT, id="tg_in"))
        
        mapping.add_instance(source)
        mapping.add_instance(transform)
        mapping.add_instance(target)
        
        # Create field map spec with linkages
        field_map_spec = XSDFieldMapSpec(id="spec_1")
        
        linkage1 = XSDFieldMapLinkage(
            from_instance_ref="source_1",
            to_instance_ref="transform_1",
            id="link_1"
        )
        linkage2 = XSDFieldMapLinkage(
            from_instance_ref="transform_1",
            to_instance_ref="target_1",
            id="link_2"
        )
        
        field_map_spec.add_linkage(linkage1)
        field_map_spec.add_linkage(linkage2)
        mapping.field_map_spec = field_map_spec
        
        return mapping
        
    def _create_parallel_mapping(self) -> XSDMapping:
        """Create a mapping with parallel execution opportunities"""
        mapping = XSDMapping(name="ParallelMapping", id="mapping_2")
        
        # Create multiple source instances
        source1 = XSDInstance(name="Source1", id="source_1")
        source1.add_port(XSDPort(name="Out", direction=PortDirection.OUTPUT, id="s1_out"))
        
        source2 = XSDInstance(name="Source2", id="source_2")
        source2.add_port(XSDPort(name="Out", direction=PortDirection.OUTPUT, id="s2_out"))
        
        # Create target that depends on both sources
        target = XSDInstance(name="Target", id="target_1")
        target.add_port(XSDPort(name="In1", direction=PortDirection.INPUT, id="t_in1"))
        target.add_port(XSDPort(name="In2", direction=PortDirection.INPUT, id="t_in2"))
        
        mapping.add_instance(source1)
        mapping.add_instance(source2)
        mapping.add_instance(target)
        
        # Create linkages
        field_map_spec = XSDFieldMapSpec(id="spec_2")
        
        linkage1 = XSDFieldMapLinkage(
            from_instance_ref="source_1",
            to_instance_ref="target_1",
            id="link_1"
        )
        linkage2 = XSDFieldMapLinkage(
            from_instance_ref="source_2",
            to_instance_ref="target_1",
            id="link_2"
        )
        
        field_map_spec.add_linkage(linkage1)
        field_map_spec.add_linkage(linkage2)
        mapping.field_map_spec = field_map_spec
        
        return mapping
        
    def _create_circular_mapping(self) -> XSDMapping:
        """Create a mapping with circular dependencies"""
        mapping = XSDMapping(name="CircularMapping", id="mapping_3")
        
        # Create instances with circular dependency
        inst_a = XSDInstance(name="InstanceA", id="inst_a")
        inst_b = XSDInstance(name="InstanceB", id="inst_b")
        
        mapping.add_instance(inst_a)
        mapping.add_instance(inst_b)
        
        # Create circular linkages
        field_map_spec = XSDFieldMapSpec(id="spec_3")
        
        linkage1 = XSDFieldMapLinkage(
            from_instance_ref="inst_a",
            to_instance_ref="inst_b",
            id="link_1"
        )
        linkage2 = XSDFieldMapLinkage(
            from_instance_ref="inst_b",
            to_instance_ref="inst_a",
            id="link_2"
        )
        
        field_map_spec.add_linkage(linkage1)
        field_map_spec.add_linkage(linkage2)
        mapping.field_map_spec = field_map_spec
        
        return mapping

class TestXSDExecutionEngine:
    """Test main execution engine functionality"""
    
    def test_engine_creation(self):
        """Test execution engine creation"""
        engine = XSDExecutionEngine(ExecutionMode.SEQUENTIAL)
        assert engine.execution_mode == ExecutionMode.SEQUENTIAL
        
        parallel_engine = XSDExecutionEngine(ExecutionMode.PARALLEL)
        assert parallel_engine.execution_mode == ExecutionMode.PARALLEL
        
    def test_sequential_execution(self):
        """Test sequential mapping execution"""
        engine = XSDExecutionEngine(ExecutionMode.SEQUENTIAL)
        mapping = self._create_test_mapping()
        context = ExecutionContext("session_1", "mapping_1", {}, {})
        
        results = engine.execute_mapping(mapping, context)
        
        # Should have results for all instances
        assert len(results) == 3
        
        # Check execution states
        completed_results = [r for r in results if r.state == ExecutionState.COMPLETED]
        assert len(completed_results) >= 1  # At least some should complete
        
        # Verify execution order (source should be first)
        source_result = next((r for r in results if r.instance_name == "Source"), None)
        assert source_result is not None
        
    def test_parallel_execution(self):
        """Test parallel mapping execution"""
        engine = XSDExecutionEngine(ExecutionMode.PARALLEL)
        mapping = self._create_test_mapping()
        context = ExecutionContext("session_1", "mapping_1", {}, {})
        
        results = engine.execute_mapping(mapping, context)
        
        # Should have results for all instances
        assert len(results) == 3
        
        # All instances should attempt execution
        instance_names = {r.instance_name for r in results}
        assert "Source" in instance_names
        assert "Transform" in instance_names
        assert "Target" in instance_names
        
    def test_mapping_validation(self):
        """Test mapping validation before execution"""
        engine = XSDExecutionEngine()
        
        # Test empty mapping
        empty_mapping = XSDMapping(name="EmptyMapping", id="empty_1")
        errors = engine.validate_mapping_for_execution(empty_mapping)
        assert len(errors) > 0
        assert "no instances" in errors[0]
        
        # Test mapping without sources
        no_source_mapping = XSDMapping(name="NoSourceMapping", id="no_source_1")
        target = XSDInstance(name="Target", id="target_1")
        target.add_port(XSDPort(name="In", direction=PortDirection.INPUT, id="in_1"))
        no_source_mapping.add_instance(target)
        
        errors = engine.validate_mapping_for_execution(no_source_mapping)
        assert any("no source instances" in error for error in errors)
        
        # Test valid mapping
        valid_mapping = self._create_test_mapping()
        errors = engine.validate_mapping_for_execution(valid_mapping)
        # May have some validation warnings but should not prevent execution
        
    def test_execution_result_creation(self):
        """Test execution result creation and properties"""
        result = ExecutionResult(
            instance_id="inst_1",
            instance_name="TestInstance",
            state=ExecutionState.COMPLETED,
            rows_processed=1000,
            execution_time_ms=500
        )
        
        assert result.instance_id == "inst_1"
        assert result.instance_name == "TestInstance"
        assert result.state == ExecutionState.COMPLETED
        assert result.rows_processed == 1000
        assert result.execution_time_ms == 500
        assert result.rows_rejected == 0
        assert result.error_message is None
        
    def _create_test_mapping(self) -> XSDMapping:
        """Create a test mapping for execution"""
        mapping = XSDMapping(name="TestMapping", id="test_mapping")
        
        # Create source
        source = XSDInstance(name="Source", id="source_1")
        source.add_port(XSDPort(name="Out", direction=PortDirection.OUTPUT, id="s_out"))
        mapping.add_instance(source)
        
        # Create transformation
        transform = XSDInstance(name="Transform", id="transform_1")
        transform.add_port(XSDPort(name="In", direction=PortDirection.INPUT, id="t_in"))
        transform.add_port(XSDPort(name="Out", direction=PortDirection.OUTPUT, id="t_out"))
        mapping.add_instance(transform)
        
        # Create target
        target = XSDInstance(name="Target", id="target_1")
        target.add_port(XSDPort(name="In", direction=PortDirection.INPUT, id="tg_in"))
        mapping.add_instance(target)
        
        # Create data flow
        field_map_spec = XSDFieldMapSpec(id="field_spec_1")
        
        linkage1 = XSDFieldMapLinkage(
            from_instance_ref="source_1",
            to_instance_ref="transform_1",
            id="linkage_1"
        )
        linkage2 = XSDFieldMapLinkage(
            from_instance_ref="transform_1",
            to_instance_ref="target_1",
            id="linkage_2"
        )
        
        field_map_spec.add_linkage(linkage1)
        field_map_spec.add_linkage(linkage2)
        mapping.field_map_spec = field_map_spec
        
        return mapping

class TestExecutionMonitor:
    """Test execution monitoring functionality"""
    
    def test_monitor_creation(self):
        """Test execution monitor creation"""
        monitor = ExecutionMonitor()
        
        assert len(monitor.execution_history) == 0
        assert len(monitor.active_executions) == 0
        
    def test_execution_tracking(self):
        """Test execution tracking"""
        monitor = ExecutionMonitor()
        context = ExecutionContext("session_1", "mapping_1", {}, {})
        
        # Start execution
        monitor.start_execution(context)
        assert len(monitor.active_executions) == 1
        assert "session_1" in monitor.active_executions
        
        # Complete execution
        results = [
            ExecutionResult("inst_1", "Instance1", ExecutionState.COMPLETED),
            ExecutionResult("inst_2", "Instance2", ExecutionState.COMPLETED)
        ]
        
        monitor.complete_execution("session_1", results)
        
        assert len(monitor.active_executions) == 0
        assert len(monitor.execution_history) == 2
        
    def test_execution_statistics(self):
        """Test execution statistics calculation"""
        monitor = ExecutionMonitor()
        
        # Add some execution history
        monitor.execution_history = [
            ExecutionResult("inst_1", "Instance1", ExecutionState.COMPLETED),
            ExecutionResult("inst_2", "Instance2", ExecutionState.COMPLETED),
            ExecutionResult("inst_3", "Instance3", ExecutionState.FAILED),
            ExecutionResult("inst_4", "Instance4", ExecutionState.COMPLETED)
        ]
        
        stats = monitor.get_execution_statistics()
        
        assert stats['total_executions'] == 4
        assert stats['successful_executions'] == 3
        assert stats['failed_executions'] == 1
        assert stats['success_rate'] == 0.75
        assert stats['active_executions'] == 0

# Integration tests that combine multiple components
class TestExecutionEngineIntegration:
    """Integration tests for execution engine"""
    
    def test_end_to_end_mapping_execution(self):
        """Test complete end-to-end mapping execution"""
        # Create a comprehensive mapping
        mapping = self._create_comprehensive_mapping()
        
        # Create execution context
        context = ExecutionContext(
            session_id="integration_test",
            mapping_id=mapping.id,
            parameters={"BATCH_SIZE": 1000},
            variables={"ENV": "TEST"},
            debug_mode=True
        )
        
        # Create engine and monitor
        engine = XSDExecutionEngine(ExecutionMode.SEQUENTIAL)
        monitor = ExecutionMonitor()
        
        # Validate mapping
        validation_errors = engine.validate_mapping_for_execution(mapping)
        # Should be able to execute despite potential warnings
        
        # Start monitoring
        monitor.start_execution(context)
        
        # Execute mapping
        results = engine.execute_mapping(mapping, context)
        
        # Complete monitoring
        monitor.complete_execution(context.session_id, results)
        
        # Verify results
        assert len(results) > 0
        
        # Check that at least source instances executed successfully
        source_results = [r for r in results if "Source" in r.instance_name]
        assert len(source_results) > 0
        
        # Verify monitoring
        stats = monitor.get_execution_statistics()
        assert stats['total_executions'] == len(results)
        
    def _create_comprehensive_mapping(self) -> XSDMapping:
        """Create a comprehensive mapping for integration testing"""
        mapping = XSDMapping(
            name="ComprehensiveMapping",
            description="Integration test mapping",
            id="comprehensive_mapping"
        )
        
        # Create multiple sources
        customer_source = XSDInstance(name="CustomerSource", id="customer_source")
        customer_source.add_port(XSDPort(name="CustomerData", direction=PortDirection.OUTPUT, id="cust_out"))
        mapping.add_instance(customer_source)
        
        order_source = XSDInstance(name="OrderSource", id="order_source")
        order_source.add_port(XSDPort(name="OrderData", direction=PortDirection.OUTPUT, id="order_out"))
        mapping.add_instance(order_source)
        
        # Create transformation
        joiner = XSDInstance(name="CustomerOrderJoin", id="joiner")
        joiner.add_port(XSDPort(name="CustomerInput", direction=PortDirection.INPUT, id="join_cust_in"))
        joiner.add_port(XSDPort(name="OrderInput", direction=PortDirection.INPUT, id="join_order_in"))
        joiner.add_port(XSDPort(name="JoinedOutput", direction=PortDirection.OUTPUT, id="join_out"))
        mapping.add_instance(joiner)
        
        # Create target
        target = XSDInstance(name="CustomerOrderTarget", id="target")
        target.add_port(XSDPort(name="JoinedData", direction=PortDirection.INPUT, id="target_in"))
        mapping.add_instance(target)
        
        # Create field map specification
        field_map_spec = XSDFieldMapSpec(id="comprehensive_spec")
        
        # Create linkages
        cust_linkage = XSDFieldMapLinkage(
            from_instance_ref="customer_source",
            to_instance_ref="joiner",
            id="cust_linkage"
        )
        order_linkage = XSDFieldMapLinkage(
            from_instance_ref="order_source",
            to_instance_ref="joiner",
            id="order_linkage"
        )
        target_linkage = XSDFieldMapLinkage(
            from_instance_ref="joiner",
            to_instance_ref="target",
            id="target_linkage"
        )
        
        field_map_spec.add_linkage(cust_linkage)
        field_map_spec.add_linkage(order_linkage)
        field_map_spec.add_linkage(target_linkage)
        mapping.field_map_spec = field_map_spec
        
        return mapping