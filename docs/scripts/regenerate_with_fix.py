#!/usr/bin/env python3
"""
Regenerate Spark code with fixed Jinja2 template
"""
import sys
import os
from pathlib import Path
sys.path.insert(0, 'src')

# Import the necessary components
from core.xml_parser import InformaticaXMLParser
from core.spark_code_generator import SparkCodeGenerator

def main():
    print("🔧 Regenerating Spark Application with Fixed Template")
    print("=" * 55)
    
    # Parse the XML file
    print("📄 Parsing XML file: input/complex_production_project.xml")
    parser = InformaticaXMLParser()
    
    try:
        project = parser.parse_project("input/complex_production_project.xml")
        print(f"✅ Successfully parsed project: {project.name}")
    except Exception as e:
        print(f"❌ Error parsing XML: {e}")
        return
    
    # Generate the Spark application
    output_dir = "generated_spark_app/ALIGNMENT_FIXED_TEST"
    print(f"🚀 Generating Spark application to: {output_dir}")
    
    try:
        generator = SparkCodeGenerator()
        generator.generate_spark_application(project, output_dir)
        print("✅ Spark application generated successfully!")
        
        # Check the generated mapping file
        mapping_file = Path(output_dir) / "src/main/python/mappings/m_process_customer_data.py"
        if mapping_file.exists():
            print(f"\n📋 Generated mapping file: {mapping_file}")
            print("📄 First 35 lines of generated code:")
            
            with open(mapping_file, 'r') as f:
                lines = f.readlines()
                
            for i, line in enumerate(lines[:35], 1):
                print(f"{i:3d}→{line.rstrip()}")
                
            # Quick alignment check
            problematic_lines = []
            for i, line in enumerate(lines, 1):
                # Check for the specific issue we fixed
                if line.strip().startswith('self.logger.info("Starting') and len(line.strip()) > 100:
                    problematic_lines.append(i)
                elif ('_df = self._read_' in line and 'current_df =' in line):
                    problematic_lines.append(i)
                    
            if problematic_lines:
                print(f"\n❌ Still found alignment issues on lines: {problematic_lines}")
            else:
                print(f"\n✅ No obvious alignment issues detected!")
                
        else:
            print(f"❌ Generated mapping file not found at: {mapping_file}")
            
    except Exception as e:
        print(f"❌ Error generating Spark application: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()