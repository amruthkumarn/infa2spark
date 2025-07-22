"""
Comprehensive tests for XSD mapping model
Tests the Instance-Port mapping system and data flow functionality
"""
import pytest
from typing import List

from src.core.xsd_mapping_model import (
    XSDMapping, XSDInstance, XSDPort, XSDFieldMapSpec, XSDFieldMapLinkage,
    XSDLoadOrderStrategy, XSDLoadOrderConstraint, XSDOneAfterAnotherConstraint,
    XSDLinkageOrder, XSDUserDefinedParameter, XSDUserDefinedOutput,
    XSDInputBinding, XSDOutputBinding, XSDLinkPolicy, XSDFieldMapPort,
    PortDirection, LinkPolicyType, LoadOrderType, MappingCharacteristics
)
from src.core.xsd_base_classes import PMDataType
from src.core.reference_manager import ReferenceManager

class TestPortDirection:
    """Test PortDirection enumeration"""
    
    def test_port_direction_values(self):
        """Test port direction enum values"""
        assert PortDirection.INPUT.value == "INPUT"
        assert PortDirection.OUTPUT.value == "OUTPUT"
        assert PortDirection.VARIABLE.value == "VARIABLE"
        assert PortDirection.INPUT_OUTPUT.value == "INPUT_OUTPUT"

class TestMappingCharacteristics:
    """Test MappingCharacteristics dataclass"""
    
    def test_default_characteristics(self):
        """Test default characteristics values"""
        chars = MappingCharacteristics()
        assert chars.version == "1.0"
        assert chars.is_template is False
        assert chars.is_reusable is False
        assert chars.description == ""
        assert chars.created_by is None
        
    def test_custom_characteristics(self):
        """Test custom characteristics values"""
        chars = MappingCharacteristics(
            version="2.0",
            is_template=True,
            is_reusable=True,
            description="Custom mapping",
            created_by="testuser"
        )
        assert chars.version == "2.0"
        assert chars.is_template is True
        assert chars.is_reusable is True
        assert chars.description == "Custom mapping"
        assert chars.created_by == "testuser"

class TestXSDPort:
    """Test XSD Port functionality"""
    
    def test_port_creation(self):
        """Test basic port creation"""
        port = XSDPort(
            name="InputPort1",
            direction=PortDirection.INPUT,
            port_type="TransformationFieldPort",
            id="port_1"
        )
        
        assert port.name == "InputPort1"
        assert port.direction == PortDirection.INPUT
        assert port.port_type == "TransformationFieldPort"
        assert port.id == "port_1"
        assert port.is_required is True
        assert port.is_key is False
        
    def test_port_connections(self):
        """Test port connection functionality"""
        input_port = XSDPort(name="Input", direction=PortDirection.INPUT, id="input_1")
        output_port = XSDPort(name="Output", direction=PortDirection.OUTPUT, id="output_1")
        
        # Connect ports
        input_port.connect_to(output_port)
        
        # Verify connections
        assert output_port.id in input_port.to_port_refs
        assert output_port.from_port_ref == input_port.id
        assert input_port.is_connected()
        assert output_port.is_connected()
        assert input_port.get_connection_count() == 1
        
        # Disconnect ports
        input_port.disconnect_from(output_port)
        assert output_port.id not in input_port.to_port_refs
        assert output_port.from_port_ref is None
        assert not input_port.is_connected()
        assert not output_port.is_connected()
        
    def test_field_map_port(self):
        """Test specialized FieldMapPort"""
        field_port = XSDFieldMapPort(
            name="FieldMapPort1",
            direction=PortDirection.INPUT,
            id="field_port_1"
        )
        
        assert field_port.port_type == "FieldMapPort"
        assert len(field_port.mapping_rules) == 0
        
        # Add mapping rules
        field_port.mapping_rules.append("FIELD1 = INPUT.FIELD_A")
        field_port.mapping_rules.append("FIELD2 = UPPER(INPUT.FIELD_B)")
        
        assert len(field_port.mapping_rules) == 2

class TestXSDInstance:
    """Test XSD Instance functionality"""
    
    def test_instance_creation(self):
        """Test basic instance creation"""
        instance = XSDInstance(
            name="SourceInstance",
            transformation_ref="source_def_1",
            id="inst_1"
        )
        
        assert instance.name == "SourceInstance"
        assert instance.transformation_ref == "source_def_1"
        assert instance.id == "inst_1"
        assert instance.is_active is True
        assert len(instance.ports) == 0
        
    def test_port_management(self):
        """Test port addition and retrieval"""
        instance = XSDInstance(name="TestInstance", id="inst_1")
        
        # Add ports
        input_port = XSDPort(name="Input1", direction=PortDirection.INPUT, id="port_1")
        output_port = XSDPort(name="Output1", direction=PortDirection.OUTPUT, id="port_2")
        variable_port = XSDPort(name="Var1", direction=PortDirection.VARIABLE, id="port_3")
        
        instance.add_port(input_port)
        instance.add_port(output_port)
        instance.add_port(variable_port)
        
        # Verify collections
        assert len(instance.ports) == 3
        assert len(instance.input_ports) == 1
        assert len(instance.output_ports) == 1
        assert len(instance.variable_ports) == 1
        
        # Test retrieval
        assert instance.get_port("Input1") is input_port
        assert instance.get_port("port_2") is output_port
        
        # Test direction filtering
        inputs = instance.get_ports_by_direction(PortDirection.INPUT)
        assert len(inputs) == 1
        assert inputs[0] is input_port
        
    def test_instance_type_detection(self):
        """Test instance type detection logic"""
        # Source instance (no inputs, has outputs)
        source = XSDInstance(name="Source", id="source_1")
        source.add_port(XSDPort(name="Out", direction=PortDirection.OUTPUT, id="out_1"))
        
        assert source.is_source()
        assert not source.is_target()
        assert not source.is_transformation()
        
        # Target instance (has inputs, no outputs)
        target = XSDInstance(name="Target", id="target_1")
        target.add_port(XSDPort(name="In", direction=PortDirection.INPUT, id="in_1"))
        
        assert not target.is_source()
        assert target.is_target()
        assert not target.is_transformation()
        
        # Transformation instance (has both inputs and outputs)
        transform = XSDInstance(name="Transform", id="trans_1")
        transform.add_port(XSDPort(name="In", direction=PortDirection.INPUT, id="in_2"))
        transform.add_port(XSDPort(name="Out", direction=PortDirection.OUTPUT, id="out_2"))
        
        assert not transform.is_source()
        assert not transform.is_target()
        assert transform.is_transformation()
        
        # Explicit type setting
        explicit_source = XSDInstance(name="ExplicitSource", id="exp_1")
        explicit_source.instance_type = "SOURCE"
        assert explicit_source.is_source()
        
    def test_port_connection_validation(self):
        """Test port connection validation"""
        instance = XSDInstance(name="TestInstance", id="inst_1")
        
        # Add unconnected ports
        input_port = XSDPort(name="Input1", direction=PortDirection.INPUT, id="port_1")
        output_port = XSDPort(name="Output1", direction=PortDirection.OUTPUT, id="port_2")
        
        instance.add_port(input_port)
        instance.add_port(output_port)
        
        # Validate (should find unconnected ports)
        errors = instance.validate_port_connections()
        assert len(errors) == 2  # One for unconnected input, one for unconnected output
        assert "unconnected input ports" in errors[0]
        assert "unconnected output ports" in errors[1]

class TestXSDFieldMapLinkage:
    """Test XSD FieldMapLinkage functionality"""
    
    def test_linkage_creation(self):
        """Test field map linkage creation"""
        linkage = XSDFieldMapLinkage(
            from_data_interface_ref="interface_1",
            to_data_interface_ref="interface_2",
            to_instance_ref="instance_1",
            id="linkage_1"
        )
        
        assert linkage.from_data_interface_ref == "interface_1"
        assert linkage.to_data_interface_ref == "interface_2"
        assert linkage.to_instance_ref == "instance_1"
        assert linkage.link_policy_enabled is False
        assert linkage.parameter_enabled is False
        
    def test_linkage_validation(self):
        """Test linkage validation"""
        # Valid linkage
        valid_linkage = XSDFieldMapLinkage(
            from_data_interface_ref="interface_1",
            to_data_interface_ref="interface_2",
            id="linkage_1"
        )
        assert valid_linkage.is_valid()
        
        # Invalid linkage (missing to_data_interface_ref)
        invalid_linkage = XSDFieldMapLinkage(
            from_data_interface_ref="interface_1",
            id="linkage_2"
        )
        assert not invalid_linkage.is_valid()
        
    def test_link_policy(self):
        """Test link policy configuration"""
        policy = XSDLinkPolicy(
            policy_type=LinkPolicyType.STRICT,
            id="policy_1"
        )
        
        assert policy.policy_type == LinkPolicyType.STRICT
        assert policy.strict_matching is False
        assert policy.case_sensitive is True
        
        # Configure policy
        policy.strict_matching = True
        policy.case_sensitive = False
        
        linkage = XSDFieldMapLinkage(
            from_data_interface_ref="interface_1",
            to_data_interface_ref="interface_2",
            id="linkage_1"
        )
        linkage.link_policy_enabled = True
        linkage.link_policy = policy
        
        assert linkage.link_policy.policy_type == LinkPolicyType.STRICT

class TestXSDLoadOrderStrategy:
    """Test XSD LoadOrderStrategy functionality"""
    
    def test_load_order_strategy_creation(self):
        """Test load order strategy creation"""
        strategy = XSDLoadOrderStrategy(id="strategy_1")
        assert len(strategy.constraints) == 0
        
    def test_load_order_constraints(self):
        """Test load order constraint management"""
        strategy = XSDLoadOrderStrategy(id="strategy_1")
        
        # Add constraints
        constraint1 = XSDOneAfterAnotherConstraint(
            primary_instance_ref="instance_1",
            secondary_instance_ref="instance_2",
            id="constraint_1"
        )
        
        constraint2 = XSDLoadOrderConstraint(
            primary_instance_ref="instance_2",
            secondary_instance_ref="instance_3",
            constraint_type=LoadOrderType.PARALLEL,
            id="constraint_2"
        )
        
        strategy.add_constraint(constraint1)
        strategy.add_constraint(constraint2)
        
        assert len(strategy.constraints) == 2
        assert constraint1.constraint_type == LoadOrderType.ONE_AFTER_ANOTHER
        assert constraint2.constraint_type == LoadOrderType.PARALLEL
        
    def test_loading_order_calculation(self):
        """Test loading order calculation"""
        strategy = XSDLoadOrderStrategy(id="strategy_1")
        
        # Add constraints to define order
        constraint1 = XSDLoadOrderConstraint(
            primary_instance_ref="instance_1",
            secondary_instance_ref="instance_2",
            id="constraint_1"
        )
        constraint2 = XSDLoadOrderConstraint(
            primary_instance_ref="instance_2",
            secondary_instance_ref="instance_3",
            id="constraint_2"
        )
        
        strategy.add_constraint(constraint1)
        strategy.add_constraint(constraint2)
        
        # Calculate order
        order = strategy.get_loading_order()
        assert len(order) == 3
        assert "instance_1" in order
        assert "instance_2" in order
        assert "instance_3" in order

class TestXSDMapping:
    """Test XSD Mapping functionality"""
    
    def test_mapping_creation(self):
        """Test basic mapping creation"""
        mapping = XSDMapping(
            name="TestMapping",
            description="Test mapping description",
            id="mapping_1"
        )
        
        assert mapping.name == "TestMapping"
        assert mapping.description == "Test mapping description"
        assert mapping.id == "mapping_1"
        assert len(mapping.instances) == 0
        assert mapping.characteristics.version == "1.0"
        
    def test_instance_management(self):
        """Test instance addition and retrieval"""
        mapping = XSDMapping(name="TestMapping", id="mapping_1")
        
        # Add instances
        source = XSDInstance(name="Source1", id="source_1")
        transform = XSDInstance(name="Transform1", id="transform_1")
        target = XSDInstance(name="Target1", id="target_1")
        
        mapping.add_instance(source)
        mapping.add_instance(transform)
        mapping.add_instance(target)
        
        assert len(mapping.instances) == 3
        
        # Test retrieval
        assert mapping.get_instance("Source1") is source
        assert mapping.get_instance("transform_1") is transform
        
        # Test removal
        mapping.remove_instance(transform)
        assert len(mapping.instances) == 2
        assert mapping.get_instance("Transform1") is None
        
    def test_instance_filtering(self):
        """Test instance filtering by type"""
        mapping = XSDMapping(name="TestMapping", id="mapping_1")
        
        # Create instances with proper port configurations
        source = XSDInstance(name="Source1", id="source_1")
        source.add_port(XSDPort(name="Out", direction=PortDirection.OUTPUT, id="out_1"))
        
        transform = XSDInstance(name="Transform1", id="transform_1")
        transform.add_port(XSDPort(name="In", direction=PortDirection.INPUT, id="in_1"))
        transform.add_port(XSDPort(name="Out", direction=PortDirection.OUTPUT, id="out_2"))
        
        target = XSDInstance(name="Target1", id="target_1")
        target.add_port(XSDPort(name="In", direction=PortDirection.INPUT, id="in_2"))
        
        mapping.add_instance(source)
        mapping.add_instance(transform)
        mapping.add_instance(target)
        
        # Test filtering
        sources = mapping.get_source_instances()
        transforms = mapping.get_transformation_instances()
        targets = mapping.get_target_instances()
        
        assert len(sources) == 1
        assert sources[0] is source
        assert len(transforms) == 1
        assert transforms[0] is transform
        assert len(targets) == 1
        assert targets[0] is target
        
    def test_data_flow_validation(self):
        """Test data flow validation"""
        mapping = XSDMapping(name="TestMapping", id="mapping_1")
        
        # Add instances
        source = XSDInstance(name="Source1", id="source_1")
        target = XSDInstance(name="Target1", id="target_1")
        disconnected = XSDInstance(name="Disconnected", id="disconnected_1")
        
        mapping.add_instance(source)
        mapping.add_instance(target)
        mapping.add_instance(disconnected)
        
        # Create field map spec with linkage
        field_map_spec = XSDFieldMapSpec(id="spec_1")
        linkage = XSDFieldMapLinkage(
            from_data_interface_ref="source_interface",
            to_data_interface_ref="target_interface",
            from_instance_ref="source_1",
            to_instance_ref="target_1",
            id="linkage_1"
        )
        field_map_spec.add_linkage(linkage)
        mapping.field_map_spec = field_map_spec
        
        # Validate data flow
        errors = mapping.validate_data_flow()
        
        # Should find disconnected instance
        assert len(errors) > 0
        disconnected_errors = [e for e in errors if "not connected" in e]
        assert len(disconnected_errors) == 1
        
    def test_cycle_detection(self):
        """Test data flow cycle detection"""
        mapping = XSDMapping(name="TestMapping", id="mapping_1")
        
        # Create instances
        inst_a = XSDInstance(name="InstanceA", id="inst_a")
        inst_b = XSDInstance(name="InstanceB", id="inst_b")
        inst_c = XSDInstance(name="InstanceC", id="inst_c")
        
        mapping.add_instance(inst_a)
        mapping.add_instance(inst_b)
        mapping.add_instance(inst_c)
        
        # Create circular data flow: A -> B -> C -> A
        field_map_spec = XSDFieldMapSpec(id="spec_1")
        linkage1 = XSDFieldMapLinkage(
            from_data_interface_ref="interface_a", to_data_interface_ref="interface_b",
            from_instance_ref="inst_a", to_instance_ref="inst_b", id="link_1"
        )
        linkage2 = XSDFieldMapLinkage(
            from_data_interface_ref="interface_b", to_data_interface_ref="interface_c",
            from_instance_ref="inst_b", to_instance_ref="inst_c", id="link_2"
        )
        linkage3 = XSDFieldMapLinkage(
            from_data_interface_ref="interface_c", to_data_interface_ref="interface_a",
            from_instance_ref="inst_c", to_instance_ref="inst_a", id="link_3"
        )
        
        field_map_spec.add_linkage(linkage1)
        field_map_spec.add_linkage(linkage2)
        field_map_spec.add_linkage(linkage3)
        mapping.field_map_spec = field_map_spec
        
        # Detect cycles
        cycles = mapping._detect_data_flow_cycles()
        assert len(cycles) > 0  # Should detect the circular dependency
        
    def test_mapping_summary(self):
        """Test mapping summary export"""
        mapping = XSDMapping(name="SummaryTest", description="Test summary", id="mapping_1")
        
        # Add instances
        source = XSDInstance(name="Source1", id="source_1")
        source.add_port(XSDPort(name="Out", direction=PortDirection.OUTPUT, id="out_1"))
        
        target = XSDInstance(name="Target1", id="target_1")
        target.add_port(XSDPort(name="In", direction=PortDirection.INPUT, id="in_1"))
        
        mapping.add_instance(source)
        mapping.add_instance(target)
        
        # Add parameters and outputs
        param = XSDUserDefinedParameter(name="PARAM1", data_type=PMDataType.SQL_VARCHAR, id="param_1")
        output = XSDUserDefinedOutput(name="OUTPUT1", data_type=PMDataType.SQL_INTEGER, id="output_1")
        
        mapping.parameters["PARAM1"] = param
        mapping.outputs["OUTPUT1"] = output
        
        # Configure characteristics
        mapping.characteristics.is_template = True
        
        # Export summary
        summary = mapping.export_summary()
        
        assert summary['name'] == "SummaryTest"
        assert summary['instance_count'] == 2
        assert summary['source_instances'] == 1
        assert summary['target_instances'] == 1
        assert summary['transformation_instances'] == 0
        assert summary['parameters_count'] == 1
        assert summary['outputs_count'] == 1
        assert summary['characteristics']['is_template'] is True

class TestUserDefinedElements:
    """Test user-defined parameters and outputs"""
    
    def test_user_defined_parameter(self):
        """Test user-defined parameter creation"""
        param = XSDUserDefinedParameter(
            name="DATABASE_URL",
            data_type=PMDataType.SQL_VARCHAR,
            default_value="localhost:1521",
            id="param_1"
        )
        
        assert param.name == "DATABASE_URL"
        assert param.data_type == PMDataType.SQL_VARCHAR
        assert param.default_value == "localhost:1521"
        assert param.is_required is False  # Has default value
        
        # Parameter without default
        required_param = XSDUserDefinedParameter(
            name="REQUIRED_PARAM",
            data_type=PMDataType.SQL_INTEGER,
            id="param_2"
        )
        assert required_param.is_required is True
        
    def test_user_defined_output(self):
        """Test user-defined output creation"""
        output = XSDUserDefinedOutput(
            name="RECORD_COUNT",
            data_type=PMDataType.SQL_INTEGER,
            id="output_1"
        )
        
        assert output.name == "RECORD_COUNT"
        assert output.data_type == PMDataType.SQL_INTEGER
        assert output.expression is None
        
        # Output with expression
        calc_output = XSDUserDefinedOutput(
            name="CALCULATED_VALUE",
            data_type=PMDataType.SQL_DECIMAL,
            id="output_2"
        )
        calc_output.expression = "SUM(INPUT.AMOUNT)"
        
        assert calc_output.expression == "SUM(INPUT.AMOUNT)"
        
    def test_input_output_bindings(self):
        """Test input and output bindings"""
        # Input binding
        input_binding = XSDInputBinding(
            parameter_ref="param_1",
            value="test_value",
            id="binding_1"
        )
        
        assert input_binding.parameter_ref == "param_1"
        assert input_binding.value == "test_value"
        
        # Output binding
        output_binding = XSDOutputBinding(
            output_ref="output_1",
            id="binding_2"
        )
        
        assert output_binding.output_ref == "output_1"

class TestXSDLinkageOrder:
    """Test XSD LinkageOrder functionality"""
    
    def test_linkage_order_creation(self):
        """Test linkage order creation"""
        order = XSDLinkageOrder(
            to_data_interface_ref="interface_1",
            id="order_1"
        )
        
        assert order.to_data_interface_ref == "interface_1"
        assert len(order.field_map_linkage_refs) == 0
        
    def test_linkage_order_management(self):
        """Test linkage reference management"""
        order = XSDLinkageOrder(
            to_data_interface_ref="interface_1",
            id="order_1"
        )
        
        # Add linkage references
        order.add_linkage_ref("linkage_1")
        order.add_linkage_ref("linkage_2")
        order.add_linkage_ref("linkage_3")
        
        assert len(order.field_map_linkage_refs) == 3
        assert "linkage_1" in order.field_map_linkage_refs
        assert "linkage_2" in order.field_map_linkage_refs
        assert "linkage_3" in order.field_map_linkage_refs

# Integration test that combines multiple mapping model components
class TestMappingModelIntegration:
    """Integration tests for the complete mapping model"""
    
    def test_complete_mapping_workflow(self):
        """Test complete mapping creation and configuration workflow"""
        # Create mapping
        mapping = XSDMapping(
            name="IntegrationTestMapping",
            description="Complete integration test",
            id="integration_mapping"
        )
        
        # Configure characteristics
        mapping.characteristics.version = "2.0"
        mapping.characteristics.is_reusable = True
        
        # Create source instance
        source = XSDInstance(name="CustomerSource", transformation_ref="customer_def", id="source_1")
        source_port = XSDPort(name="CustomerData", direction=PortDirection.OUTPUT, id="source_port_1")
        source.add_port(source_port)
        mapping.add_instance(source)
        
        # Create transformation instance
        transform = XSDInstance(name="CustomerTransform", transformation_ref="expression_def", id="transform_1")
        transform_in = XSDPort(name="InputData", direction=PortDirection.INPUT, id="transform_in_1")
        transform_out = XSDPort(name="OutputData", direction=PortDirection.OUTPUT, id="transform_out_1")
        transform.add_port(transform_in)
        transform.add_port(transform_out)
        mapping.add_instance(transform)
        
        # Create target instance
        target = XSDInstance(name="CustomerTarget", transformation_ref="customer_target_def", id="target_1")
        target_port = XSDPort(name="CustomerData", direction=PortDirection.INPUT, id="target_port_1")
        target.add_port(target_port)
        mapping.add_instance(target)
        
        # Connect ports
        source_port.connect_to(transform_in)
        transform_out.connect_to(target_port)
        
        # Create field map specification
        field_map_spec = XSDFieldMapSpec(id="field_spec_1")
        
        # Add linkages for data flow
        linkage1 = XSDFieldMapLinkage(
            from_data_interface_ref="source_interface",
            to_data_interface_ref="transform_interface",
            from_instance_ref="source_1",
            to_instance_ref="transform_1",
            id="linkage_1"
        )
        linkage2 = XSDFieldMapLinkage(
            from_data_interface_ref="transform_interface",
            to_data_interface_ref="target_interface",
            from_instance_ref="transform_1",
            to_instance_ref="target_1",
            id="linkage_2"
        )
        
        field_map_spec.add_linkage(linkage1)
        field_map_spec.add_linkage(linkage2)
        mapping.field_map_spec = field_map_spec
        
        # Create load order strategy
        load_order = XSDLoadOrderStrategy(id="load_order_1")
        constraint = XSDOneAfterAnotherConstraint(
            primary_instance_ref="transform_1",
            secondary_instance_ref="target_1",
            id="constraint_1"
        )
        load_order.add_constraint(constraint)
        mapping.load_order_strategy = load_order
        
        # Add parameters and outputs
        param = XSDUserDefinedParameter(
            name="BATCH_SIZE",
            data_type=PMDataType.SQL_INTEGER,
            default_value=1000,
            id="param_1"
        )
        output = XSDUserDefinedOutput(
            name="RECORDS_PROCESSED",
            data_type=PMDataType.SQL_INTEGER,
            id="output_1"
        )
        
        mapping.parameters["BATCH_SIZE"] = param
        mapping.outputs["RECORDS_PROCESSED"] = output
        
        # Validate the complete mapping
        errors = mapping.validate_data_flow()
        assert len(errors) == 0  # Should have no validation errors
        
        # Verify instance types are detected correctly
        sources = mapping.get_source_instances()
        transforms = mapping.get_transformation_instances()
        targets = mapping.get_target_instances()
        
        assert len(sources) == 1
        assert len(transforms) == 1
        assert len(targets) == 1
        
        # Verify port connections
        assert source_port.is_connected()
        assert transform_in.is_connected()
        assert transform_out.is_connected()
        assert target_port.is_connected()
        
        # Test mapping summary
        summary = mapping.export_summary()
        assert summary['name'] == "IntegrationTestMapping"
        assert summary['instance_count'] == 3
        assert summary['source_instances'] == 1
        assert summary['transformation_instances'] == 1
        assert summary['target_instances'] == 1
        assert summary['has_field_map_spec'] is True
        assert summary['has_load_order_strategy'] is True
        assert summary['parameters_count'] == 1
        assert summary['outputs_count'] == 1
        assert summary['characteristics']['version'] == "2.0"
        assert summary['characteristics']['is_reusable'] is True
        
        # Test load order calculation
        loading_order = load_order.get_loading_order()
        assert len(loading_order) == 2
        assert "transform_1" in loading_order
        assert "target_1" in loading_order