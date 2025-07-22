"""
Comprehensive tests for XSD transformation model
Tests field-level metadata, data interfaces, and transformation hierarchy
"""
import pytest
from typing import List, Dict

from src.core.xsd_transformation_model import (
    XSDTransformationField, XSDFieldSelector, XSDDataInterface, XSDAbstractTransformation,
    XSDSourceTransformation, XSDTargetTransformation, XSDLookupTransformation,
    XSDExpressionTransformation, XSDJoinerTransformation, XSDAggregatorTransformation,
    TransformationConfiguration, TransformationRegistry, transformation_registry,
    FieldDirection, FieldConstraints, FieldConfiguration, LanguageKind,
    PartitioningKind, TransformationScope, OrderingKind, MatchResolutionKind, LoadScope
)
from src.core.xsd_base_classes import PMDataType

class TestFieldDirection:
    """Test FieldDirection enumeration"""
    
    def test_field_direction_values(self):
        """Test field direction enum values"""
        assert FieldDirection.INPUT.value == "INPUT"
        assert FieldDirection.OUTPUT.value == "OUTPUT"
        assert FieldDirection.BOTH.value == "BOTH"
        assert FieldDirection.RETURN.value == "RETURN"
        assert FieldDirection.LOOKUP.value == "LOOKUP"
        assert FieldDirection.GENERATED.value == "GENERATED"

class TestFieldConstraints:
    """Test FieldConstraints functionality"""
    
    def test_field_constraints_creation(self):
        """Test field constraints creation"""
        constraints = FieldConstraints(
            min_value=0,
            max_value=100,
            required=True,
            unique=True,
            pattern=r"^\d{3}-\d{2}-\d{4}$"
        )
        
        assert constraints.min_value == 0
        assert constraints.max_value == 100
        assert constraints.required is True
        assert constraints.unique is True
        assert constraints.pattern == r"^\d{3}-\d{2}-\d{4}$"

class TestFieldConfiguration:
    """Test FieldConfiguration functionality"""
    
    def test_field_configuration_creation(self):
        """Test field configuration creation"""
        config = FieldConfiguration(
            expression="UPPER(INPUT_FIELD)",
            default_value="DEFAULT",
            variable_field=True,
            parameterizable=True
        )
        
        assert config.expression == "UPPER(INPUT_FIELD)"
        assert config.default_value == "DEFAULT"
        assert config.variable_field is True
        assert config.parameterizable is True
        assert len(config.custom_properties) == 0

class TestXSDTransformationField:
    """Test XSD transformation field functionality"""
    
    def test_field_creation(self):
        """Test basic field creation"""
        field = XSDTransformationField(
            name="CustomerID",
            data_type=PMDataType.SQL_INTEGER,
            direction=FieldDirection.INPUT,
            id="field_1"
        )
        
        assert field.name == "CustomerID"
        assert field.data_type == PMDataType.SQL_INTEGER
        assert field.direction == FieldDirection.INPUT
        assert field.id == "field_1"
        assert field.input is True
        assert field.output is False
        
    def test_field_direction_logic(self):
        """Test field direction input/output logic"""
        # Input field
        input_field = XSDTransformationField(name="Input", direction=FieldDirection.INPUT)
        assert input_field.input is True
        assert input_field.output is False
        
        # Output field
        output_field = XSDTransformationField(name="Output", direction=FieldDirection.OUTPUT)
        assert output_field.input is False
        assert output_field.output is True
        
        # Both direction field
        both_field = XSDTransformationField(name="Both", direction=FieldDirection.BOTH)
        assert both_field.input is True
        assert both_field.output is True
        
        # Return field
        return_field = XSDTransformationField(name="Return", direction=FieldDirection.RETURN)
        assert return_field.input is False
        assert return_field.output is True
        
    def test_field_expression_handling(self):
        """Test field expression and derived field functionality"""
        field = XSDTransformationField(name="CalculatedField", direction=FieldDirection.OUTPUT)
        
        # Initially not derived
        assert field.is_derived is False
        assert field.expression is None
        
        # Set expression
        field.set_expression("PRICE * QUANTITY", "GENERAL")
        
        assert field.is_derived is True
        assert field.expression == "PRICE * QUANTITY"
        assert field.expression_type == "GENERAL"
        assert field.configuration.expression == "PRICE * QUANTITY"
        
    def test_source_field_tracking(self):
        """Test source field dependency tracking"""
        field = XSDTransformationField(name="DerivedField", direction=FieldDirection.OUTPUT)
        
        # Add source field references
        field.add_source_field("PRICE")
        field.add_source_field("QUANTITY")
        field.add_source_field("PRICE")  # Duplicate should be ignored
        
        assert len(field.source_fields) == 2
        assert "PRICE" in field.source_fields
        assert "QUANTITY" in field.source_fields
        
    def test_field_constraint_validation(self):
        """Test field constraint validation"""
        constraints = FieldConstraints(
            min_value=0,
            max_value=100,
            required=True
        )
        
        field = XSDTransformationField(name="TestField")
        field.configuration.constraints = constraints
        
        # Test required validation
        errors = field.validate_constraints(None)
        assert len(errors) > 0
        assert "required" in errors[0].lower()
        
        # Test min/max validation
        errors = field.validate_constraints(-5)
        assert len(errors) > 0
        assert "minimum" in errors[0].lower()
        
        errors = field.validate_constraints(150)
        assert len(errors) > 0
        assert "maximum" in errors[0].lower()
        
        # Test valid value
        errors = field.validate_constraints(50)
        assert len(errors) == 0
        
    def test_field_metadata_export(self):
        """Test field metadata export"""
        field = XSDTransformationField(
            name="TestField",
            data_type=PMDataType.SQL_VARCHAR,
            direction=FieldDirection.BOTH,
            length=255,
            precision=10,
            scale=2
        )
        field.set_expression("UPPER(INPUT_FIELD)")
        field.add_source_field("INPUT_FIELD")
        
        metadata = field.get_field_metadata()
        
        assert metadata['name'] == "TestField"
        assert metadata['data_type'] == "SQL_VARCHAR"
        assert metadata['direction'] == "BOTH"
        assert metadata['length'] == 255
        assert metadata['precision'] == 10
        assert metadata['scale'] == 2
        assert metadata['input'] is True
        assert metadata['output'] is True
        assert metadata['is_derived'] is True
        assert metadata['expression'] == "UPPER(INPUT_FIELD)"
        assert "INPUT_FIELD" in metadata['source_fields']

class TestXSDFieldSelector:
    """Test XSD field selector functionality"""
    
    def test_field_selector_creation(self):
        """Test field selector creation"""
        selector = XSDFieldSelector(name="TestSelector", id="selector_1")
        
        assert selector.name == "TestSelector"
        assert selector.id == "selector_1"
        assert selector.selection_type == "EXPLICIT"
        assert len(selector.field_names) == 0
        
    def test_explicit_field_selection(self):
        """Test explicit field name selection"""
        selector = XSDFieldSelector(name="ExplicitSelector")
        selector.selection_type = "EXPLICIT"
        
        selector.add_field_name("CUSTOMER_ID")
        selector.add_field_name("CUSTOMER_NAME")
        selector.add_field_name("CUSTOMER_ID")  # Duplicate should be ignored
        
        assert len(selector.field_names) == 2
        assert "CUSTOMER_ID" in selector.field_names
        assert "CUSTOMER_NAME" in selector.field_names
        
    def test_type_based_selection(self):
        """Test type-based field selection"""
        selector = XSDFieldSelector(name="TypeSelector")
        selector.selection_type = "TYPE_BASED"
        
        selector.add_data_type(PMDataType.SQL_INTEGER)
        selector.add_data_type(PMDataType.SQL_DECIMAL)
        
        assert len(selector.data_types) == 2
        assert PMDataType.SQL_INTEGER in selector.data_types
        assert PMDataType.SQL_DECIMAL in selector.data_types
        
    def test_pattern_based_selection(self):
        """Test pattern-based field selection"""
        selector = XSDFieldSelector(name="PatternSelector")
        selector.selection_type = "PATTERN_BASED"
        
        selector.add_name_pattern(r"^CUSTOMER_.*")
        selector.add_name_pattern(r"^ORDER_.*")
        selector.exclude_patterns.append(r".*_TEMP$")
        
        assert len(selector.name_patterns) == 2
        assert r"^CUSTOMER_.*" in selector.name_patterns
        assert r".*_TEMP$" in selector.exclude_patterns
        
    def test_field_matching(self):
        """Test field matching against selector criteria"""
        # Create test fields
        customer_field = XSDTransformationField(name="CUSTOMER_ID", data_type=PMDataType.SQL_INTEGER)
        name_field = XSDTransformationField(name="CUSTOMER_NAME", data_type=PMDataType.SQL_VARCHAR)
        temp_field = XSDTransformationField(name="CUSTOMER_TEMP", data_type=PMDataType.SQL_VARCHAR)
        
        # Test explicit selection
        explicit_selector = XSDFieldSelector(name="Explicit")
        explicit_selector.selection_type = "EXPLICIT"
        explicit_selector.add_field_name("CUSTOMER_ID")
        
        assert explicit_selector.matches_field(customer_field) is True
        assert explicit_selector.matches_field(name_field) is False
        
        # Test type-based selection
        type_selector = XSDFieldSelector(name="TypeBased")
        type_selector.selection_type = "TYPE_BASED"
        type_selector.add_data_type(PMDataType.SQL_INTEGER)
        
        assert type_selector.matches_field(customer_field) is True
        assert type_selector.matches_field(name_field) is False
        
        # Test pattern-based selection
        pattern_selector = XSDFieldSelector(name="PatternBased")
        pattern_selector.selection_type = "PATTERN_BASED"
        pattern_selector.add_name_pattern(r"^CUSTOMER_.*")
        pattern_selector.exclude_patterns.append(r".*_TEMP$")
        
        assert pattern_selector.matches_field(customer_field) is True
        assert pattern_selector.matches_field(name_field) is True
        assert pattern_selector.matches_field(temp_field) is False  # Excluded by pattern

class TestXSDDataInterface:
    """Test XSD data interface functionality"""
    
    def test_interface_creation(self):
        """Test data interface creation"""
        interface = XSDDataInterface(
            name="CustomerInterface",
            interface_type="SourceDataInterface",
            id="interface_1"
        )
        
        assert interface.name == "CustomerInterface"
        assert interface.interface_type == "SourceDataInterface"
        assert interface.id == "interface_1"
        assert interface.input is False
        assert interface.output is False
        assert len(interface.fields) == 0
        
    def test_field_management(self):
        """Test field addition and retrieval"""
        interface = XSDDataInterface(name="TestInterface")
        
        # Add fields
        field1 = XSDTransformationField(name="Field1", id="field_1")
        field2 = XSDTransformationField(name="Field2", id="field_2")
        
        interface.add_field(field1)
        interface.add_field(field2)
        
        assert len(interface.fields) == 2
        
        # Test retrieval by name
        retrieved = interface.get_field("Field1")
        assert retrieved is field1
        
        # Test retrieval by ID
        retrieved = interface.get_field("field_2")
        assert retrieved is field2
        
    def test_field_direction_filtering(self):
        """Test field filtering by direction"""
        interface = XSDDataInterface(name="TestInterface")
        
        # Add fields with different directions
        input_field = XSDTransformationField(name="Input", direction=FieldDirection.INPUT)
        output_field = XSDTransformationField(name="Output", direction=FieldDirection.OUTPUT)
        both_field = XSDTransformationField(name="Both", direction=FieldDirection.BOTH)
        derived_field = XSDTransformationField(name="Derived", direction=FieldDirection.OUTPUT)
        derived_field.set_expression("UPPER(Input)")
        
        interface.add_field(input_field)
        interface.add_field(output_field)
        interface.add_field(both_field)
        interface.add_field(derived_field)
        
        # Test filtering
        input_fields = interface.get_input_fields()
        output_fields = interface.get_output_fields()
        derived_fields = interface.get_derived_fields()
        
        assert len(input_fields) == 2  # input_field and both_field
        assert len(output_fields) == 3  # output_field, both_field, and derived_field
        assert len(derived_fields) == 1  # Only derived_field
        
    def test_field_groups(self):
        """Test field group management"""
        interface = XSDDataInterface(name="TestInterface")
        
        # Create fields
        customer_id = XSDTransformationField(name="CUSTOMER_ID")
        customer_name = XSDTransformationField(name="CUSTOMER_NAME")
        order_id = XSDTransformationField(name="ORDER_ID")
        
        # Add field groups
        customer_fields = [customer_id, customer_name]
        interface.add_field_group("customer_info", customer_fields)
        
        assert "customer_info" in interface.field_groups
        assert len(interface.field_groups["customer_info"]) == 2
        assert customer_id.group_ref == "customer_info"
        assert customer_name.group_ref == "customer_info"
        
    def test_field_selectors(self):
        """Test field selector integration"""
        interface = XSDDataInterface(name="TestInterface")
        
        # Add fields
        customer_id = XSDTransformationField(name="CUSTOMER_ID", data_type=PMDataType.SQL_INTEGER)
        customer_name = XSDTransformationField(name="CUSTOMER_NAME", data_type=PMDataType.SQL_VARCHAR)
        order_id = XSDTransformationField(name="ORDER_ID", data_type=PMDataType.SQL_INTEGER)
        
        interface.add_field(customer_id)
        interface.add_field(customer_name)
        interface.add_field(order_id)
        
        # Create and add selector
        selector = XSDFieldSelector(name="IntegerFields")
        selector.selection_type = "TYPE_BASED"
        selector.add_data_type(PMDataType.SQL_INTEGER)
        
        interface.add_field_selector(selector)
        
        # Test field selection
        selected_fields = interface.get_selected_fields("IntegerFields")
        assert len(selected_fields) == 2
        assert customer_id in selected_fields
        assert order_id in selected_fields
        assert customer_name not in selected_fields
        
    def test_interface_validation(self):
        """Test interface validation"""
        interface = XSDDataInterface(name="TestInterface")
        
        # Add fields with duplicate names
        field1 = XSDTransformationField(name="DuplicateName")
        field2 = XSDTransformationField(name="DuplicateName")
        
        interface.add_field(field1)
        interface.add_field(field2)
        
        errors = interface.validate_interface()
        assert len(errors) > 0
        assert "duplicate" in errors[0].lower()
        
    def test_interface_summary(self):
        """Test interface summary export"""
        interface = XSDDataInterface(name="SummaryInterface", interface_type="GenericDataInterface")
        interface.input = True
        
        # Add various fields
        input_field = XSDTransformationField(name="Input", direction=FieldDirection.INPUT)
        output_field = XSDTransformationField(name="Output", direction=FieldDirection.OUTPUT)
        derived_field = XSDTransformationField(name="Derived", direction=FieldDirection.OUTPUT)
        derived_field.set_expression("UPPER(Input)")
        
        interface.add_field(input_field)
        interface.add_field(output_field)
        interface.add_field(derived_field)
        
        # Add field group and selector
        interface.add_field_group("test_group", [input_field])
        selector = XSDFieldSelector(name="TestSelector")
        interface.add_field_selector(selector)
        
        summary = interface.get_interface_summary()
        
        assert summary['name'] == "SummaryInterface"
        assert summary['interface_type'] == "GenericDataInterface"
        assert summary['input'] is True
        assert summary['total_fields'] == 3
        assert summary['input_fields'] == 1
        assert summary['output_fields'] == 2
        assert summary['derived_fields'] == 1
        assert summary['field_groups'] == 1
        assert summary['field_selectors'] == 1

class TestTransformationConfiguration:
    """Test transformation configuration"""
    
    def test_configuration_creation(self):
        """Test transformation configuration creation"""
        config = TransformationConfiguration(
            active=True,
            class_name="CustomTransformation",
            language=LanguageKind.PYTHON,
            partitioning_type=PartitioningKind.GRID_PARTITIONABLE,
            scope=TransformationScope.TRANSACTION,
            output_deterministic=False
        )
        
        assert config.active is True
        assert config.class_name == "CustomTransformation"
        assert config.language == LanguageKind.PYTHON
        assert config.partitioning_type == PartitioningKind.GRID_PARTITIONABLE
        assert config.scope == TransformationScope.TRANSACTION
        assert config.output_deterministic is False

class TestXSDAbstractTransformation:
    """Test XSD abstract transformation functionality"""
    
    def test_transformation_creation(self):
        """Test basic transformation creation"""
        transformation = XSDAbstractTransformation(
            name="TestTransformation",
            transformation_type="Generic",
            id="trans_1"
        )
        
        assert transformation.name == "TestTransformation"
        assert transformation.transformation_type == "Generic"
        assert transformation.id == "trans_1"
        assert transformation.version == "1.0"
        assert len(transformation.data_interfaces) == 0
        
    def test_interface_management(self):
        """Test data interface management"""
        transformation = XSDAbstractTransformation(name="TestTrans")
        
        # Create interfaces
        input_interface = XSDDataInterface(name="Input", id="input_1")
        input_interface.input = True
        
        output_interface = XSDDataInterface(name="Output", id="output_1")
        output_interface.output = True
        
        # Add interfaces
        transformation.add_data_interface(input_interface)
        transformation.add_data_interface(output_interface)
        
        assert len(transformation.data_interfaces) == 2
        assert len(transformation.input_interfaces) == 1
        assert len(transformation.output_interfaces) == 1
        
        # Test retrieval
        retrieved = transformation.get_interface("Input")
        assert retrieved is input_interface
        
        retrieved = transformation.get_interface("output_1")
        assert retrieved is output_interface
        
    def test_field_access(self):
        """Test field access across interfaces"""
        transformation = XSDAbstractTransformation(name="TestTrans")
        
        # Create interface with fields
        interface = XSDDataInterface(name="TestInterface")
        field1 = XSDTransformationField(name="Field1")
        field2 = XSDTransformationField(name="Field2")
        
        interface.add_field(field1)
        interface.add_field(field2)
        transformation.add_data_interface(interface)
        
        # Test field access
        all_fields = transformation.get_all_fields()
        assert len(all_fields) == 2
        assert field1 in all_fields
        assert field2 in all_fields
        
        # Test field retrieval by name
        retrieved = transformation.get_field_by_name("Field1")
        assert retrieved is field1
        
    def test_transformation_validation(self):
        """Test transformation validation"""
        transformation = XSDAbstractTransformation(name="TestTrans", transformation_type="Generic")
        
        # Test validation without interfaces (should fail for non-source/target)
        errors = transformation.validate_transformation()
        assert len(errors) >= 2  # Should complain about missing input and output interfaces
        
        # Add interfaces
        input_interface = XSDDataInterface(name="Input")
        input_interface.input = True
        output_interface = XSDDataInterface(name="Output")
        output_interface.output = True
        
        transformation.add_data_interface(input_interface)
        transformation.add_data_interface(output_interface)
        
        # Should now pass basic validation
        errors = transformation.validate_transformation()
        # May still have interface-specific errors, but not missing interface errors
        missing_interface_errors = [e for e in errors if "no input interfaces" in e or "no output interfaces" in e]
        assert len(missing_interface_errors) == 0
        
    def test_transformation_metadata(self):
        """Test transformation metadata export"""
        transformation = XSDAbstractTransformation(
            name="MetadataTest",
            transformation_type="Expression",
            id="meta_trans"
        )
        transformation.version = "2.0"
        transformation.vendor = "TestVendor"
        transformation.category = "TestCategory"
        transformation.keywords = ["test", "metadata"]
        transformation.custom_properties = {"custom_prop": "custom_value"}
        
        # Add interface with fields
        interface = XSDDataInterface(name="TestInterface")
        field = XSDTransformationField(name="TestField")
        interface.add_field(field)
        transformation.add_data_interface(interface)
        
        metadata = transformation.get_transformation_metadata()
        
        assert metadata['name'] == "MetadataTest"
        assert metadata['transformation_type'] == "Expression"
        assert metadata['version'] == "2.0"
        assert metadata['vendor'] == "TestVendor"
        assert metadata['category'] == "TestCategory"
        assert metadata['keywords'] == ["test", "metadata"]
        assert metadata['configuration']['active'] is True
        assert metadata['configuration']['language'] == "python"
        assert len(metadata['interfaces']) == 1
        assert metadata['total_fields'] == 1
        assert metadata['custom_properties']['custom_prop'] == "custom_value"

class TestSpecializedTransformations:
    """Test specialized transformation classes"""
    
    def test_source_transformation(self):
        """Test source transformation"""
        source = XSDSourceTransformation(name="CustomerSource", id="source_1")
        
        assert source.transformation_type == "Source"
        assert source.connection_ref is None
        assert source.source_definition_ref is None
        
        # Should have output interface by default
        assert len(source.output_interfaces) == 1
        assert len(source.input_interfaces) == 0
        
        output_interface = source.output_interfaces[0]
        assert output_interface.name == "CustomerSource_OUTPUT"
        assert output_interface.interface_type == "SourceDataInterface"
        
    def test_target_transformation(self):
        """Test target transformation"""
        target = XSDTargetTransformation(name="CustomerTarget", id="target_1")
        
        assert target.transformation_type == "Target"
        assert target.load_scope == LoadScope.ALL
        assert target.truncate_target is False
        assert target.bulk_mode is True
        
        # Should have input interface by default
        assert len(target.input_interfaces) == 1
        assert len(target.output_interfaces) == 0
        
        input_interface = target.input_interfaces[0]
        assert input_interface.name == "CustomerTarget_INPUT"
        assert input_interface.interface_type == "TargetDataInterface"
        
    def test_lookup_transformation(self):
        """Test lookup transformation"""
        lookup = XSDLookupTransformation(name="CustomerLookup", id="lookup_1")
        
        assert lookup.transformation_type == "Lookup"
        assert lookup.dynamic_cache is False
        assert lookup.multiple_match == MatchResolutionKind.RETURN_FIRST
        
        # Should have input, lookup, and output interfaces
        assert len(lookup.data_interfaces) == 3
        
        interface_names = [interface.name for interface in lookup.data_interfaces]
        assert "CustomerLookup_INPUT" in interface_names
        assert "CustomerLookup_LOOKUP" in interface_names
        assert "CustomerLookup_OUTPUT" in interface_names
        
    def test_expression_transformation(self):
        """Test expression transformation with field addition"""
        expression = XSDExpressionTransformation(name="CustomerCalc", id="expr_1")
        
        assert expression.transformation_type == "Expression"
        assert len(expression.expressions) == 0
        assert len(expression.filter_conditions) == 0
        
        # Should have input and output interfaces
        assert len(expression.input_interfaces) == 1
        assert len(expression.output_interfaces) == 1
        
        # Add expression
        expression.add_expression("FULL_NAME", "FIRST_NAME || ' ' || LAST_NAME")
        
        assert "FULL_NAME" in expression.expressions
        assert expression.expressions["FULL_NAME"] == "FIRST_NAME || ' ' || LAST_NAME"
        
        # Check that derived field was added to output interface
        output_interface = expression.output_interfaces[0]
        derived_field = output_interface.get_field("FULL_NAME")
        
        assert derived_field is not None
        assert derived_field.is_derived is True
        assert derived_field.expression == "FIRST_NAME || ' ' || LAST_NAME"
        
        # Add filter condition
        expression.add_filter_condition("CUSTOMER_STATUS = 'ACTIVE'")
        assert "CUSTOMER_STATUS = 'ACTIVE'" in expression.filter_conditions
        
    def test_joiner_transformation(self):
        """Test joiner transformation"""
        joiner = XSDJoinerTransformation(name="CustomerOrderJoin", id="joiner_1")
        
        assert joiner.transformation_type == "Joiner"
        assert joiner.join_type == "INNER"
        assert len(joiner.join_conditions) == 0
        
        # Should have master, detail, and output interfaces
        assert len(joiner.data_interfaces) == 3
        
        interface_names = [interface.name for interface in joiner.data_interfaces]
        assert "CustomerOrderJoin_MASTER" in interface_names
        assert "CustomerOrderJoin_DETAIL" in interface_names
        assert "CustomerOrderJoin_OUTPUT" in interface_names
        
    def test_aggregator_transformation(self):
        """Test aggregator transformation with aggregation addition"""
        aggregator = XSDAggregatorTransformation(name="SalesAgg", id="agg_1")
        
        assert aggregator.transformation_type == "Aggregator"
        assert len(aggregator.group_by_fields) == 0
        assert len(aggregator.aggregation_expressions) == 0
        assert aggregator.sorted_input is False
        
        # Should have input and output interfaces
        assert len(aggregator.input_interfaces) == 1
        assert len(aggregator.output_interfaces) == 1
        
        # Add group by field
        aggregator.add_group_by_field("CUSTOMER_ID")
        aggregator.add_group_by_field("PRODUCT_CATEGORY")
        aggregator.add_group_by_field("CUSTOMER_ID")  # Duplicate should be ignored
        
        assert len(aggregator.group_by_fields) == 2
        assert "CUSTOMER_ID" in aggregator.group_by_fields
        assert "PRODUCT_CATEGORY" in aggregator.group_by_fields
        
        # Add aggregation
        aggregator.add_aggregation("TOTAL_SALES", "SUM(SALES_AMOUNT)")
        
        assert "TOTAL_SALES" in aggregator.aggregation_expressions
        assert aggregator.aggregation_expressions["TOTAL_SALES"] == "SUM(SALES_AMOUNT)"
        
        # Check that aggregated field was added to output interface
        output_interface = aggregator.output_interfaces[0]
        agg_field = output_interface.get_field("TOTAL_SALES")
        
        assert agg_field is not None
        assert agg_field.is_derived is True
        assert agg_field.expression == "SUM(SALES_AMOUNT)"
        assert agg_field.expression_type == "AGGREGATION"

class TestTransformationRegistry:
    """Test transformation registry functionality"""
    
    def test_registry_creation(self):
        """Test registry creation and built-in types"""
        registry = TransformationRegistry()
        
        supported_types = registry.get_supported_types()
        
        assert "Source" in supported_types
        assert "Target" in supported_types
        assert "Lookup" in supported_types
        assert "Expression" in supported_types
        assert "Joiner" in supported_types
        assert "Aggregator" in supported_types
        assert "Generic" in supported_types
        
    def test_transformation_creation(self):
        """Test transformation creation via registry"""
        registry = TransformationRegistry()
        
        # Create various transformation types
        source = registry.create_transformation("Source", "TestSource")
        assert isinstance(source, XSDSourceTransformation)
        assert source.name == "TestSource"
        
        target = registry.create_transformation("Target", "TestTarget")
        assert isinstance(target, XSDTargetTransformation)
        assert target.name == "TestTarget"
        
        lookup = registry.create_transformation("Lookup", "TestLookup")
        assert isinstance(lookup, XSDLookupTransformation)
        assert lookup.name == "TestLookup"
        
        # Test invalid type
        invalid = registry.create_transformation("InvalidType", "TestInvalid")
        assert invalid is None
        
    def test_custom_transformation_registration(self):
        """Test custom transformation type registration"""
        registry = TransformationRegistry()
        
        # Define custom transformation class
        class CustomTransformation(XSDAbstractTransformation):
            def __init__(self, name: str, **kwargs):
                super().__init__(name=name, transformation_type="Custom", **kwargs)
                self.custom_property = "custom_value"
        
        # Register custom type
        registry.register_transformation_type("Custom", CustomTransformation)
        
        # Test creation
        custom = registry.create_transformation("Custom", "TestCustom")
        assert isinstance(custom, CustomTransformation)
        assert custom.name == "TestCustom"
        assert custom.transformation_type == "Custom"
        assert custom.custom_property == "custom_value"
        
        # Verify it's in supported types
        supported_types = registry.get_supported_types()
        assert "Custom" in supported_types
        
    def test_global_registry(self):
        """Test global transformation registry instance"""
        from src.core.xsd_transformation_model import transformation_registry
        
        # Test that global registry works
        source = transformation_registry.create_transformation("Source", "GlobalTestSource")
        assert isinstance(source, XSDSourceTransformation)
        assert source.name == "GlobalTestSource"

# Integration tests that combine multiple components
class TestTransformationModelIntegration:
    """Integration tests for the complete transformation model"""
    
    def test_complete_transformation_workflow(self):
        """Test complete transformation creation and configuration workflow"""
        # Create expression transformation
        expression = XSDExpressionTransformation(name="CustomerProcessing", id="expr_proc")
        
        # Configure transformation
        expression.configuration.language = LanguageKind.PYTHON
        expression.configuration.partitioning_type = PartitioningKind.GRID_PARTITIONABLE
        expression.configuration.scope = TransformationScope.ROW
        
        # Add custom properties
        expression.custom_properties["processing_mode"] = "batch"
        expression.custom_properties["batch_size"] = 10000
        
        # Get input and output interfaces
        input_interface = expression.get_interface("CustomerProcessing_INPUT")
        output_interface = expression.get_interface("CustomerProcessing_OUTPUT")
        
        assert input_interface is not None
        assert output_interface is not None
        
        # Add input fields
        customer_id = XSDTransformationField(
            name="CUSTOMER_ID",
            data_type=PMDataType.SQL_INTEGER,
            direction=FieldDirection.INPUT,
            id="cust_id_field"
        )
        first_name = XSDTransformationField(
            name="FIRST_NAME",
            data_type=PMDataType.SQL_VARCHAR,
            direction=FieldDirection.INPUT,
            length=50
        )
        last_name = XSDTransformationField(
            name="LAST_NAME",
            data_type=PMDataType.SQL_VARCHAR,
            direction=FieldDirection.INPUT,
            length=50
        )
        
        input_interface.add_field(customer_id)
        input_interface.add_field(first_name)
        input_interface.add_field(last_name)
        
        # Add expressions for derived fields
        expression.add_expression("FULL_NAME", "FIRST_NAME || ' ' || LAST_NAME")
        expression.add_expression("NAME_LENGTH", "LENGTH(FULL_NAME)")
        expression.add_expression("CUSTOMER_KEY", "HASH(CUSTOMER_ID)")
        
        # Add filter conditions
        expression.add_filter_condition("FIRST_NAME IS NOT NULL")
        expression.add_filter_condition("LAST_NAME IS NOT NULL")
        
        # Add field selectors
        name_selector = XSDFieldSelector(name="NameFields")
        name_selector.selection_type = "PATTERN_BASED"
        name_selector.add_name_pattern(r".*_NAME$")
        
        input_interface.add_field_selector(name_selector)
        
        # Add field groups
        identity_fields = [customer_id]
        name_fields = [first_name, last_name]
        
        input_interface.add_field_group("identity", identity_fields)
        input_interface.add_field_group("names", name_fields)
        
        # Validate transformation
        errors = expression.validate_transformation()
        # Should have minimal validation errors for well-formed transformation
        critical_errors = [e for e in errors if "no input interfaces" in e or "no output interfaces" in e]
        assert len(critical_errors) == 0
        
        # Test field selection
        selected_fields = input_interface.get_selected_fields("NameFields")
        assert len(selected_fields) == 2
        assert first_name in selected_fields
        assert last_name in selected_fields
        
        # Verify derived fields in output interface
        derived_fields = output_interface.get_derived_fields()
        assert len(derived_fields) == 3
        
        field_names = [f.name for f in derived_fields]
        assert "FULL_NAME" in field_names
        assert "NAME_LENGTH" in field_names
        assert "CUSTOMER_KEY" in field_names
        
        # Test metadata export
        metadata = expression.get_transformation_metadata()
        
        assert metadata['name'] == "CustomerProcessing"
        assert metadata['transformation_type'] == "Expression"
        assert metadata['configuration']['language'] == "python"
        assert metadata['configuration']['partitioning_type'] == "gridPartitionable"
        assert metadata['total_fields'] == 6  # 3 input + 3 derived output
        assert len(metadata['interfaces']) == 2
        assert metadata['custom_properties']['processing_mode'] == "batch"
        
        # Test interface summaries
        input_summary = input_interface.get_interface_summary()
        output_summary = output_interface.get_interface_summary()
        
        assert input_summary['total_fields'] == 3
        assert input_summary['input_fields'] == 3
        assert input_summary['field_groups'] == 2
        assert input_summary['field_selectors'] == 1
        
        assert output_summary['total_fields'] == 3
        assert output_summary['output_fields'] == 3
        assert output_summary['derived_fields'] == 3
        
    def test_multi_transformation_scenario(self):
        """Test scenario with multiple connected transformations"""
        # Create transformations using registry
        registry = TransformationRegistry()
        
        source = registry.create_transformation("Source", "CustomerSource")
        lookup = registry.create_transformation("Lookup", "AddressLookup")
        expression = registry.create_transformation("Expression", "CustomerEnrichment")
        target = registry.create_transformation("Target", "EnrichedCustomerTarget")
        
        # Configure source
        source_output = source.get_interface("CustomerSource_OUTPUT")
        source_output.add_field(XSDTransformationField("CUSTOMER_ID", PMDataType.SQL_INTEGER))
        source_output.add_field(XSDTransformationField("CUSTOMER_NAME", PMDataType.SQL_VARCHAR, length=100))
        source_output.add_field(XSDTransformationField("ADDRESS_ID", PMDataType.SQL_INTEGER))
        
        # Configure lookup
        lookup_input = lookup.get_interface("AddressLookup_INPUT")
        lookup_lookup = lookup.get_interface("AddressLookup_LOOKUP")
        lookup_output = lookup.get_interface("AddressLookup_OUTPUT")
        
        # Lookup input fields (from source)
        lookup_input.add_field(XSDTransformationField("CUSTOMER_ID", PMDataType.SQL_INTEGER))
        lookup_input.add_field(XSDTransformationField("CUSTOMER_NAME", PMDataType.SQL_VARCHAR, length=100))
        lookup_input.add_field(XSDTransformationField("ADDRESS_ID", PMDataType.SQL_INTEGER))
        
        # Lookup table fields
        lookup_lookup.add_field(XSDTransformationField("ADDRESS_ID", PMDataType.SQL_INTEGER))
        lookup_lookup.add_field(XSDTransformationField("STREET_ADDRESS", PMDataType.SQL_VARCHAR, length=200))
        lookup_lookup.add_field(XSDTransformationField("CITY", PMDataType.SQL_VARCHAR, length=50))
        lookup_lookup.add_field(XSDTransformationField("STATE", PMDataType.SQL_VARCHAR, length=20))
        
        # Lookup output fields (combined)
        lookup_output.add_field(XSDTransformationField("CUSTOMER_ID", PMDataType.SQL_INTEGER))
        lookup_output.add_field(XSDTransformationField("CUSTOMER_NAME", PMDataType.SQL_VARCHAR, length=100))
        lookup_output.add_field(XSDTransformationField("STREET_ADDRESS", PMDataType.SQL_VARCHAR, length=200))
        lookup_output.add_field(XSDTransformationField("CITY", PMDataType.SQL_VARCHAR, length=50))
        lookup_output.add_field(XSDTransformationField("STATE", PMDataType.SQL_VARCHAR, length=20))
        
        # Configure expression transformation with derived fields
        expression.add_expression("FULL_ADDRESS", "STREET_ADDRESS || ', ' || CITY || ', ' || STATE")
        expression.add_expression("CUSTOMER_DISPLAY_NAME", "UPPER(CUSTOMER_NAME)")
        
        # Configure target
        target_input = target.get_interface("EnrichedCustomerTarget_INPUT")
        target_input.add_field(XSDTransformationField("CUSTOMER_ID", PMDataType.SQL_INTEGER))
        target_input.add_field(XSDTransformationField("CUSTOMER_DISPLAY_NAME", PMDataType.SQL_VARCHAR, length=100))
        target_input.add_field(XSDTransformationField("FULL_ADDRESS", PMDataType.SQL_VARCHAR, length=500))
        
        # Validate all transformations
        all_transformations = [source, lookup, expression, target]
        
        for transformation in all_transformations:
            errors = transformation.validate_transformation()
            # Should have minimal validation errors for this scenario
            critical_errors = [e for e in errors if "missing" in e.lower() or "no input" in e or "no output" in e]
            # Only source should potentially complain about missing input, target about missing output
            
        # Verify transformation metadata
        for transformation in all_transformations:
            metadata = transformation.get_transformation_metadata()
            assert 'name' in metadata
            assert 'transformation_type' in metadata
            assert 'interfaces' in metadata
            assert len(metadata['interfaces']) > 0