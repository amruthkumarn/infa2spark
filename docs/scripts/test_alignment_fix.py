#!/usr/bin/env python3
"""
Quick test script to validate Jinja template alignment fixes
"""
import sys
import os
sys.path.insert(0, 'src')

from core.spark_code_generator import SparkCodeGenerator
from core.xml_parser import InformaticaXMLParser

def main():
    # Parse a simple XML file
    parser = InformaticaXMLParser()
    project = parser.parse_project("sample_imx_project.xml")
    
    # Generate spark application
    generator = SparkCodeGenerator()
    generator.generate_spark_application(project, "generated_spark_app/ALIGNMENT_TEST")
    
    # Read and check the generated mapping file
    mapping_file = "generated_spark_app/ALIGNMENT_TEST/src/main/python/mappings/m_process_customer_data.py"
    if os.path.exists(mapping_file):
        print("Generated mapping file found. Checking alignment...")
        with open(mapping_file, 'r') as f:
            content = f.read()
            
        # Check for specific alignment issues
        lines = content.split('\n')
        issues_found = 0
        
        for i, line in enumerate(lines, 1):
            # Look for lines that should be separate but are cramped together
            if 'self.logger.info' in line and len(line) > 100:
                print(f"Line {i}: Potential cramped line: {line[:100]}...")
                issues_found += 1
            
            # Look for missing proper indentation
            if line.strip().startswith('current_df =') and not line.startswith('            current_df'):
                print(f"Line {i}: Indentation issue: {line}")
                issues_found += 1
                
        if issues_found == 0:
            print("✅ No alignment issues detected! Template fix appears successful.")
        else:
            print(f"❌ Found {issues_found} potential alignment issues.")
            
        # Show first 30 lines for visual inspection
        print("\n📄 First 30 lines of generated file:")
        for i, line in enumerate(lines[:30], 1):
            print(f"{i:3d}→{line}")
    else:
        print(f"❌ Mapping file not found at {mapping_file}")

if __name__ == "__main__":
    main()