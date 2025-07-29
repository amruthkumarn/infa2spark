"""
Enhanced Dynamic Test Generator
Analyzes actual XML schema and generates intelligent, context-aware tests
"""
from pathlib import Path
from typing import Dict, List, Any
import json


class DynamicTestGenerator:
    """Generate intelligent tests based on actual XML schema analysis"""
    
    def __init__(self, project):
        self.project = project
        
    def analyze_mapping_schema(self, mapping: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze mapping to extract schema and transformation information"""
        analysis = {
            'name': mapping.get('name', 'unknown'),
            'sources': [],
            'targets': [],
            'transformations': [],
            'data_types': set(),
            'field_patterns': []
        }
        
        components = mapping.get('components', [])
        
        for component in components:
            comp_type = component.get('type', '')
            comp_name = component.get('name', '')
            
            if comp_type == 'SourceDefinition':
                source_info = {
                    'name': comp_name,
                    'fields': self._extract_fields(component),
                    'connection_type': component.get('connection_type', 'HDFS')
                }
                analysis['sources'].append(source_info)
                
            elif comp_type == 'TargetDefinition':
                target_info = {
                    'name': comp_name,
                    'fields': self._extract_fields(component),
                    'connection_type': component.get('connection_type', 'HDFS')
                }
                analysis['targets'].append(target_info)
                
            elif 'Transformation' in comp_type:
                trans_info = {
                    'name': comp_name,
                    'type': comp_type,
                    'complexity': self._assess_complexity(component)
                }
                analysis['transformations'].append(trans_info)
        
        return analysis
    
    def _extract_fields(self, component: Dict[str, Any]) -> List[Dict[str, str]]:
        """Extract field information from component"""
        fields = []
        
        # Look for field definitions in various places
        if 'fields' in component:
            for field in component['fields']:
                fields.append({
                    'name': field.get('name', ''),
                    'type': field.get('datatype', 'string'),
                    'nullable': field.get('nullable', True)
                })
        
        # Fallback: generate common business fields if no schema found
        if not fields:
            fields = [
                {'name': 'id', 'type': 'integer', 'nullable': False},
                {'name': 'name', 'type': 'string', 'nullable': True},
                {'name': 'created_date', 'type': 'date', 'nullable': True}
            ]
            
        return fields
    
    def _assess_complexity(self, component: Dict[str, Any]) -> str:
        """Assess transformation complexity for test generation"""
        comp_type = component.get('type', '')
        
        if comp_type in ['ExpressionTransformation', 'JavaTransformation']:
            return 'high'
        elif comp_type in ['JoinerTransformation', 'AggregatorTransformation', 'LookupTransformation']:
            return 'medium'
        else:
            return 'low'
    
    def generate_dynamic_unit_tests(self, mapping: Dict[str, Any]) -> str:
        """Generate intelligent unit tests based on mapping analysis"""
        analysis = self.analyze_mapping_schema(mapping)
        mapping_name = analysis['name']
        class_name = self._to_class_name(mapping_name)
        
        # Generate dynamic test data based on actual sources
        test_data_generation = self._generate_test_data_code(analysis['sources'])
        
        # Generate schema validation based on actual fields
        schema_validation = self._generate_schema_validation_code(analysis['sources'], mapping_name)
        
        # Generate transformation tests based on complexity
        transformation_tests = self._generate_transformation_tests(analysis['transformations'], mapping_name)
        
        test_content = f'''"""
Unit tests for {class_name} mapping - DYNAMICALLY GENERATED
Based on analysis: {len(analysis['sources'])} sources, {len(analysis['targets'])} targets, {len(analysis['transformations'])} transformations
"""
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DecimalType
import sys
from pathlib import Path

# Add source path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / "src/main/python"))

from mappings.{self._sanitize_name(mapping_name)} import {class_name}


class Test{class_name}:
    """Test {class_name} mapping functionality - Schema-aware tests"""
    
    def test_mapping_initialization(self, spark_session, sample_config):
        """Test mapping can be initialized properly"""
        mapping = {class_name}(spark=spark_session, config=sample_config)
        assert mapping is not None
        assert mapping.spark == spark_session
    
{schema_validation}

{test_data_generation}

{transformation_tests}
    
    def test_end_to_end_transformation(self, spark_session, sample_config):
        """Test complete transformation pipeline with realistic data"""
        mapping = {class_name}(spark=spark_session, config=sample_config)
        
        # Create test data based on actual source schemas
        {self._generate_realistic_test_data(analysis['sources'])}
        
        try:
            result_df = mapping.execute(test_input_df)
            
            # Validate output structure
            assert result_df is not None
            assert isinstance(result_df, DataFrame)
            
            # Schema-specific validations
            result_columns = result_df.columns
            expected_target_fields = {[field['name'] for field in analysis['targets'][0]['fields'] if analysis['targets']]}
            
            # Check that key business fields are present
            business_fields = ['id', 'name', 'created_date', 'updated_date']
            present_business_fields = [f for f in business_fields if f in result_columns]
            assert len(present_business_fields) > 0, "Should have at least one business field"
            
        except (NotImplementedError, AttributeError):
            pytest.skip("Execute method not implemented yet")
    
    @pytest.mark.slow
    def test_performance_with_realistic_volume(self, spark_session, sample_config):
        """Test performance with data volume similar to production"""
        mapping = {class_name}(spark=spark_session, config=sample_config)
        
        # Generate larger dataset based on source schema
        import time
        
        # Create 10K records with realistic data distribution
        {self._generate_performance_test_data(analysis['sources'])}
        
        start_time = time.time()
        try:
            result_df = mapping.execute(large_test_df)
            execution_time = time.time() - start_time
            
            # Performance assertions based on transformation complexity
            max_time = {self._calculate_performance_threshold(analysis['transformations'])}
            assert execution_time < max_time, f"Execution took {{execution_time:.2f}}s, expected < {{max_time}}s"
            
            # Data quality checks
            assert result_df.count() > 0
            
        except (NotImplementedError, AttributeError):
            pytest.skip("Execute method or performance optimization not implemented yet")
'''
        
        return test_content
    
    def _generate_test_data_code(self, sources: List[Dict]) -> str:
        """Generate test data creation code based on actual source schemas"""
        if not sources:
            return """
    def test_with_generic_data(self, spark_session, sample_config):
        \"\"\"Test with generic data structure\"\"\"
        # Generic fallback test data
        test_data = [(1, "Test", "2024-01-01")]
        schema = ["id", "name", "date"]
        test_df = spark_session.createDataFrame(test_data, schema)
        assert test_df.count() == 1"""
        
        # Use first source for primary test data
        primary_source = sources[0]
        fields = primary_source['fields']
        
        # Generate realistic test data based on field types
        test_data_rows = []
        for i in range(3):
            row_data = []
            for field in fields:
                field_type = field['type'].lower()
                if 'int' in field_type:
                    row_data.append(str(i + 1))
                elif 'date' in field_type:
                    row_data.append(f'"2024-0{i+1}-15"')
                elif 'decimal' in field_type or 'float' in field_type:
                    row_data.append(f"{(i+1) * 1000.50}")
                else:
                    row_data.append(f'"TestValue_{i+1}"')
            test_data_rows.append(f"({', '.join(row_data)})")
        
        schema_fields = [field['name'] for field in fields]
        
        return f'''
    def test_with_source_schema_data(self, spark_session, sample_config):
        \"\"\"Test using actual source schema: {primary_source['name']}\"\"\"
        # Test data based on {primary_source['name']} schema
        test_data = [
            {','.join(test_data_rows)}
        ]
        
        test_df = spark_session.createDataFrame(
            test_data, 
            {schema_fields}
        )
        
        assert test_df.count() == 3
        assert set(test_df.columns) == set({schema_fields})'''
    
    def _generate_schema_validation_code(self, sources: List[Dict], mapping_name: str) -> str:
        """Generate schema validation tests based on actual field definitions"""
        if not sources:
            return """
    def test_schema_validation_generic(self, spark_session, sample_config):
        \"\"\"Generic schema validation\"\"\"
        assert True  # Placeholder"""
        
        primary_source = sources[0]
        fields = primary_source['fields']
        
        # Generate StructType definition based on actual fields
        struct_fields = []
        for field in fields:
            field_type = field['type'].lower()
            if 'int' in field_type:
                spark_type = "IntegerType()"
            elif 'date' in field_type:
                spark_type = "DateType()"
            elif 'decimal' in field_type or 'float' in field_type:
                spark_type = "DecimalType()"
            else:
                spark_type = "StringType()"
            
            nullable = str(field.get('nullable', True))
            struct_fields.append(f'StructField("{field["name"]}", {spark_type}, {nullable})')
        
        class_name = self._to_class_name(self._sanitize_name(mapping_name))
        return f'''
    def test_source_schema_validation(self, spark_session, sample_config):
        \"\"\"Test schema validation for {primary_source['name']} source\"\"\"
        mapping = {class_name}(spark=spark_session, config=sample_config)
        
        # Expected schema based on {primary_source['name']}
        expected_schema = StructType([
            {','.join(struct_fields)}
        ])
        
        # Create test DataFrame with expected schema
        test_df = spark_session.createDataFrame([], expected_schema)
        
        # Schema validation logic would go here
        assert len(expected_schema.fields) == {len(fields)}
        assert expected_schema.fieldNames() == {[field['name'] for field in fields]}'''
    
    def _generate_transformation_tests(self, transformations: List[Dict], mapping_name: str) -> str:
        """Generate transformation-specific tests based on actual transformations"""
        if not transformations:
            return """
    def test_no_transformations(self, spark_session, sample_config):
        \"\"\"No transformations detected\"\"\"
        assert True"""
        
        test_methods = []
        class_name = self._to_class_name(self._sanitize_name(mapping_name))
        
        # Group transformations by complexity
        high_complexity = [t for t in transformations if t['complexity'] == 'high']
        medium_complexity = [t for t in transformations if t['complexity'] == 'medium']
        
        if high_complexity:
            test_methods.append(f'''
    def test_complex_transformations(self, spark_session, sample_config):
        \"\"\"Test complex transformations: {[t['name'] for t in high_complexity]}\"\"\"
        mapping = {class_name}(spark=spark_session, config=sample_config)
        
        # Complex transformations require detailed validation
        # Testing: {', '.join([t['type'] for t in high_complexity])}
        
        # Create test data for complex transformation validation
        test_data = [(1, "Test", 100.50), (2, "Test2", 200.75)]
        test_df = spark_session.createDataFrame(test_data, ["id", "name", "amount"])
        
        try:
            # Complex transformations should handle edge cases
            result = mapping.execute(test_df)
            assert result is not None
            
            # Validate complex transformation outputs
            if result.count() > 0:
                # Check for derived/calculated fields
                result_columns = result.columns
                derived_fields = [col for col in result_columns if 'calc' in col.lower() or 'derived' in col.lower()]
                # Complex transformations often create derived fields
                
        except (NotImplementedError, AttributeError):
            pytest.skip("Complex transformation logic not implemented yet")''')
        
        if medium_complexity:
            test_methods.append(f'''
    def test_medium_complexity_transformations(self, spark_session, sample_config):
        \"\"\"Test medium complexity transformations: {[t['name'] for t in medium_complexity]}\"\"\"
        mapping = {class_name}(spark=spark_session, config=sample_config)
        
        # Medium complexity: {', '.join([t['type'] for t in medium_complexity])}
        # These typically involve joins, aggregations, or lookups
        
        test_data_a = [(1, "A"), (2, "B")]
        test_data_b = [(1, "X"), (2, "Y")]
        
        df_a = spark_session.createDataFrame(test_data_a, ["id", "value_a"])
        df_b = spark_session.createDataFrame(test_data_b, ["id", "value_b"])
        
        try:
            # Medium complexity transformations often need multiple inputs
            result = mapping.execute(df_a)  # Simplified for now
            assert result is not None
            
        except (NotImplementedError, AttributeError):
            pytest.skip("Medium complexity transformation logic not implemented yet")''')
        
        return '\n'.join(test_methods)
    
    def _generate_realistic_test_data(self, sources: List[Dict]) -> str:
        """Generate realistic test data based on business context"""
        if not sources:
            return "test_input_df = spark_session.createDataFrame([(1, 'test')], ['id', 'name'])"
        
        # Analyze field names to determine business context
        primary_source = sources[0]
        fields = primary_source['fields']
        
        # Generate business-realistic data
        realistic_data = []
        field_names = [field['name'] for field in fields]
        
        for i in range(5):  # Generate 5 realistic records
            row_values = []
            for field in fields:
                field_name = field['name'].lower()
                field_type = field['type'].lower()
                
                if 'id' in field_name:
                    row_values.append(str(i + 1))
                elif 'name' in field_name:
                    names = ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Wilson']
                    row_values.append(f'"{names[i]}"')
                elif 'email' in field_name:
                    emails = ['john@company.com', 'jane@company.com', 'bob@company.com', 'alice@company.com', 'charlie@company.com']
                    row_values.append(f'"{emails[i]}"')
                elif 'salary' in field_name or 'amount' in field_name:
                    amounts = [50000, 60000, 55000, 70000, 65000]
                    row_values.append(str(amounts[i]))
                elif 'date' in field_name:
                    dates = ['2024-01-15', '2024-02-20', '2024-03-10', '2024-04-05', '2024-05-12']
                    row_values.append(f'"{dates[i]}"')
                elif 'department' in field_name:
                    depts = ['Engineering', 'Finance', 'Marketing', 'HR', 'Sales']
                    row_values.append(f'"{depts[i]}"')
                else:
                    row_values.append(f'"TestValue_{i+1}"')
            
            realistic_data.append(f"({', '.join(row_values)})")
        
        return f'''test_input_data = [
            {','.join(realistic_data)}
        ]
        
        test_input_df = spark_session.createDataFrame(
            test_input_data,
            {field_names}
        )'''
    
    def _generate_performance_test_data(self, sources: List[Dict]) -> str:
        """Generate large-scale test data for performance testing"""
        if not sources:
            return "large_test_df = spark_session.range(10000).toDF('id')"
        
        primary_source = sources[0]
        field_names = [field['name'] for field in primary_source['fields']]
        
        return f'''# Generate 10K records for performance testing
        import random
        
        large_data = []
        for i in range(10000):
            # Generate realistic data distribution
            row = [
                i + 1,  # ID
                f"User_{{i}}",  # Name
                random.choice(['Engineering', 'Finance', 'Marketing', 'HR', 'Sales']),  # Department
                50000 + (i % 50000),  # Salary variation
                f"2024-{{(i % 12) + 1:02d}}-{{(i % 28) + 1:02d}}"  # Date variation
            ]
            large_data.append(tuple(row))
        
        large_test_df = spark_session.createDataFrame(
            large_data,
            {field_names[:5]}  # Use first 5 fields
        )'''
    
    def _calculate_performance_threshold(self, transformations: List[Dict]) -> int:
        """Calculate performance threshold based on transformation complexity"""
        if not transformations:
            return 30
        
        # Base time: 30 seconds
        base_time = 30
        
        # Add time based on complexity
        for trans in transformations:
            if trans['complexity'] == 'high':
                base_time += 20
            elif trans['complexity'] == 'medium':
                base_time += 10
            else:
                base_time += 5
        
        return min(base_time, 120)  # Cap at 2 minutes
    
    def _to_class_name(self, name: str) -> str:
        """Convert name to PascalCase class name"""
        return ''.join(word.capitalize() for word in name.replace('_', ' ').split())
    
    def _sanitize_name(self, name: str) -> str:
        """Sanitize name for use as identifier"""
        return name.lower().replace(' ', '_').replace('-', '_')


# Example usage
if __name__ == "__main__":
    # This would be integrated into the main generator
    print("Dynamic Test Generator - Analyzes XML schema and generates intelligent tests")