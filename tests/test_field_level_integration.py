"""
Test Field-Level Integration Functionality
This test specifically validates the TransformationFieldPort and ExpressionField integration
"""

import unittest
import tempfile
import os
from src.core.xml_parser import InformaticaXMLParser
from src.core.spark_code_generator import SparkCodeGenerator


class TestFieldLevelIntegration(unittest.TestCase):
    """Test field-level integration functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.parser = InformaticaXMLParser()
        self.generator = SparkCodeGenerator()
        
    def test_xml_parser_field_extraction(self):
        """Test that XML parser correctly extracts field-level information"""
        # Use the actual complex production project file
        project = self.parser.parse_project('input/complex_production_project.xml')
        
        # Verify project was parsed
        self.assertIsNotNone(project)
        self.assertEqual(project.name, "Complex_Production_Project")
        
        # Verify mappings exist
        self.assertIn('Mappings', project.folders)
        self.assertGreater(len(project.folders['Mappings']), 0)
        
        # Get the mapping
        mapping = project.folders['Mappings'][0]
        self.assertEqual(mapping['name'], 'm_Process_Customer_Data')
        
        # Verify components with field-level information
        components = mapping['components']
        self.assertGreater(len(components), 0)
        
        # Find the Expression transformation
        expression_component = None
        for comp in components:
            if comp['name'] == 'EXP_Standardize_Names':
                expression_component = comp
                break
                
        self.assertIsNotNone(expression_component)
        
        # Verify ports are extracted
        self.assertIn('ports', expression_component)
        ports = expression_component['ports']
        self.assertGreater(len(ports), 0)
        
        # Verify specific port details
        input_ports = [p for p in ports if p['direction'] == 'INPUT']
        output_ports = [p for p in ports if p['direction'] == 'OUTPUT']
        
        self.assertGreater(len(input_ports), 0)
        self.assertGreater(len(output_ports), 0)
        
        # Verify expressions are extracted
        self.assertIn('expressions', expression_component)
        expressions = expression_component['expressions']
        self.assertGreater(len(expressions), 0)
        
        # Verify specific expression
        fullname_expr = None
        for expr in expressions:
            if expr['name'] == 'FullName_OUT':
                fullname_expr = expr
                break
                
        self.assertIsNotNone(fullname_expr)
        self.assertEqual(fullname_expr['expression'], "FirstName_IN || ' ' || LastName_IN")
        
    def test_code_generation_field_integration(self):
        """Test that generated code includes field-level logic"""
        # Generate code
        input_file = 'input/complex_production_project.xml'
        output_dir = 'generated_spark_app/generated_spark_apps/FieldLevelTest'
        
        self.generator.generate_spark_application(input_file, output_dir)
        
        # Check if mapping file was created
        mapping_file = f'{output_dir}/src/main/python/mappings/m_process_customer_data.py'
        
        # Handle nested directory structure
        if not os.path.exists(mapping_file):
            mapping_file = f'generated_spark_app/{output_dir}/src/main/python/mappings/m_process_customer_data.py'
        
        self.assertTrue(os.path.exists(mapping_file))
        
        # Read and verify generated content
        with open(mapping_file, 'r') as f:
            content = f.read()
            
        # Verify field-level integration elements
        self.assertIn('withColumn', content)
        self.assertIn('expr(', content)
        self.assertIn('cast(', content)
        self.assertIn('_apply_exp_standardize_names', content)
        
        # Verify specific field validation
        self.assertIn('CustomerID_IN', content)
        self.assertIn('FirstName_IN', content)
        self.assertIn('LastName_IN', content)
        self.assertIn('FullName_OUT', content)
        
        # Verify expression conversion
        self.assertIn('concat(FirstName_IN', content)
        
        # Verify input validation logic
        self.assertIn('input_fields', content)
        self.assertIn('missing_fields', content)
        
    def test_expression_conversion(self):
        """Test that Informatica expressions are converted to Spark expressions"""
        # Test the conversion functions directly
        generator = SparkCodeGenerator()
        
        # Test concatenation conversion
        informatica_expr = "FirstName_IN || ' ' || LastName_IN"
        expected_spark = "concat(FirstName_IN, ' ', LastName_IN)"
        
        actual_spark = generator._convert_informatica_expression_to_spark(informatica_expr)
        self.assertEqual(actual_spark, expected_spark)
        
        # Test type conversion
        informatica_type = "string"
        expected_spark_type = "string"
        
        actual_spark_type = generator._convert_informatica_type_to_spark(informatica_type)
        self.assertEqual(actual_spark_type, expected_spark_type)
        
    def test_end_to_end_field_integration(self):
        """Test complete end-to-end field-level integration"""
        # Parse XML
        project = self.parser.parse_project('input/complex_production_project.xml')
        
        # Generate code 
        output_dir = 'EndToEndTest'
        app_path = self.generator.generate_spark_application('input/complex_production_project.xml', output_dir)
        
        # Verify generated structure - use the returned path
        mapping_file = f'{app_path}/src/main/python/mappings/m_process_customer_data.py'
        
        self.assertTrue(os.path.exists(mapping_file))
        
        with open(mapping_file, 'r') as f:
            content = f.read()
            
        # Verify the complete field-level workflow
        lines = content.split('\n')
        
        # Find the transformation method
        in_transform_method = False
        has_input_validation = False
        has_expression_application = False
        has_type_casting = False
        
        # Join lines to handle multi-line expressions
        method_content = ""
        in_method = False
        
        for line in lines:
            if '_apply_exp_standardize_names' in line and 'def' in line:
                in_method = True
                method_content += line + " "
            elif in_method:
                method_content += line + " "
                if 'return result_df' in line:
                    break
                    
        # Check for patterns in the method content
        has_input_validation = 'input_fields = [' in method_content
        has_expression_application = 'withColumn(' in method_content and 'expr(' in method_content
        has_type_casting = 'withColumn(' in method_content and 'cast(' in method_content
                    
        self.assertTrue(has_input_validation, "Input field validation not found")
        self.assertTrue(has_expression_application, "Expression application not found")
        self.assertTrue(has_type_casting, "Type casting not found")


if __name__ == '__main__':
    unittest.main() 