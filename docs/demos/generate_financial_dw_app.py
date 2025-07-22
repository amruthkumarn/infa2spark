#!/usr/bin/env python3
"""
Financial Data Warehouse Spark Application Generator
====================================================

This script demonstrates the enhanced code generation capabilities
for the Financial DW project with advanced transformations.
"""

import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, 'src')

from core.enhanced_spark_generator import EnhancedSparkCodeGenerator
from core.xml_parser import XMLParser

def generate_financial_dw_app():
    """Generate enhanced Spark application for Financial DW project"""
    
    print("🏦 Financial Data Warehouse - Enhanced Spark Code Generation")
    print("=" * 65)
    
    # Initialize components
    xml_file = "input/financial_dw_project.xml"
    output_base = "generated_spark_apps"
    
    if not os.path.exists(xml_file):
        print(f"❌ ERROR: XML file not found: {xml_file}")
        return False
    
    try:
        # Parse XML
        print(f"📊 Parsing Financial DW project: {xml_file}")
        parser = XMLParser()
        project_data = parser.parse_project_xml(xml_file)
        
        print(f"✅ Successfully parsed project: {project_data.get('project_name', 'Unknown')}")
        print(f"   📋 Mappings found: {len(project_data.get('mappings', []))}")
        print(f"   🔄 Workflows found: {len(project_data.get('workflows', []))}")
        print(f"   🔗 Connections: {len(project_data.get('connections', []))}")
        
        # Show mapping details
        mappings = project_data.get('mappings', [])
        for i, mapping in enumerate(mappings, 1):
            print(f"\n   📈 Mapping {i}: {mapping.get('name', 'Unknown')}")
            transformations = mapping.get('transformations', [])
            print(f"      🔧 Transformations: {len(transformations)}")
            for txn in transformations:
                print(f"         - {txn.get('name', 'Unknown')} ({txn.get('type', 'Unknown')})")
        
        # Generate enhanced Spark application
        print(f"\n🚀 Generating Enhanced Spark Application...")
        generator = EnhancedSparkCodeGenerator()
        
        app_name = project_data.get('project_name', 'FinancialDW').replace(' ', '_').replace('-', '_')
        app_path = generator.generate_spark_application(project_data, output_base, app_name)
        
        print(f"✅ Enhanced Spark application generated successfully!")
        print(f"📁 Application path: {app_path}")
        
        # Show generated structure
        print(f"\n📂 Generated Application Structure:")
        show_directory_structure(app_path, max_depth=3)
        
        # Show key generated files content
        print(f"\n🔍 Key Generated Files Preview:")
        show_key_files_preview(app_path)
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR during generation: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def show_directory_structure(path, max_depth=3, current_depth=0):
    """Display directory structure"""
    if current_depth >= max_depth:
        return
        
    try:
        path_obj = Path(path)
        if not path_obj.exists():
            return
            
        items = sorted(path_obj.iterdir())
        
        for item in items:
            if item.name.startswith('.') or item.name == '__pycache__':
                continue
                
            indent = "  " * current_depth
            if item.is_dir():
                print(f"{indent}📁 {item.name}/")
                show_directory_structure(item, max_depth, current_depth + 1)
            else:
                size = item.stat().st_size
                print(f"{indent}📄 {item.name} ({format_file_size(size)})")
                
    except Exception as e:
        print(f"   Error reading {path}: {e}")

def format_file_size(size_bytes):
    """Format file size in human readable format"""
    if size_bytes < 1024:
        return f"{size_bytes}B"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.1f}KB"
    else:
        return f"{size_bytes/(1024*1024):.1f}MB"

def show_key_files_preview(app_path):
    """Show preview of key generated files"""
    key_files = [
        ("src/main/python/main.py", "Main Application Entry Point"),
        ("src/main/python/mappings/customer_dimension_load.py", "SCD Type 2 Customer Dimension"),
        ("src/main/python/mappings/transaction_fact_load.py", "Multi-Source Transaction Processing"),
        ("src/main/python/mappings/risk_analytics_aggregation.py", "Advanced Risk Analytics"),
        ("src/main/python/workflows/financial_dw_etl_process.py", "Workflow Orchestration"),
        ("config/application.yaml", "Application Configuration"),
    ]
    
    for relative_path, description in key_files:
        file_path = Path(app_path) / relative_path
        print(f"\n📋 {description}")
        print(f"   📁 {relative_path}")
        
        if file_path.exists():
            try:
                with open(file_path, 'r') as f:
                    content = f.read()
                    lines = content.split('\n')
                    
                print(f"   📊 Lines: {len(lines)}, Size: {format_file_size(len(content))}")
                
                # Show first few lines
                print("   🔍 Preview (first 15 lines):")
                for i, line in enumerate(lines[:15], 1):
                    if line.strip():
                        print(f"   {i:2d}: {line}")
                
                if len(lines) > 15:
                    print("   ... (truncated)")
                    
            except Exception as e:
                print(f"   ❌ Error reading file: {e}")
        else:
            print("   ❌ File not found")

def demonstrate_transformation_analysis():
    """Demonstrate transformation pattern detection and analysis"""
    print(f"\n🔬 Advanced Transformation Analysis")
    print("=" * 40)
    
    xml_file = "input/financial_dw_project.xml"
    parser = XMLParser()
    project_data = parser.parse_project_xml(xml_file)
    
    generator = EnhancedSparkCodeGenerator()
    
    for mapping in project_data.get('mappings', []):
        print(f"\n📊 Analyzing Mapping: {mapping.get('name')}")
        transformation_analysis = generator._analyze_mapping_transformations(mapping)
        
        print(f"   🔧 Total Transformations: {len(transformation_analysis.transformation_details)}")
        
        for detail in transformation_analysis.transformation_details:
            enhanced_type = detail.get('enhanced_type', 'standard')
            print(f"   • {detail.get('name')} ({detail.get('type')}) → {enhanced_type.upper()}")
            
            if enhanced_type != 'standard':
                config = detail.get('config', {})
                print(f"     📋 Enhanced Config: {list(config.keys())}")

if __name__ == "__main__":
    print("🎯 Starting Financial DW Enhanced Code Generation")
    print("=" * 55)
    
    # Generate the application
    success = generate_financial_dw_app()
    
    if success:
        # Show transformation analysis
        demonstrate_transformation_analysis()
        
        print(f"\n🎉 Financial DW Code Generation Complete!")
        print("   ✅ Enhanced Spark application ready for deployment")
        print("   🚀 Includes SCD Type 2, Advanced Aggregations, Multi-source Integration")
        print("   📊 Production-ready with monitoring, data quality, and performance optimization")
    else:
        print(f"\n❌ Code generation failed. Check the logs above.")
        sys.exit(1) 