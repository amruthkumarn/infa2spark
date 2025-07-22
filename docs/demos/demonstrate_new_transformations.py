#!/usr/bin/env python3
"""
Advanced Transformation Demonstration
Showcases the new production-ready transformation generators
"""
import sys
import logging
from pathlib import Path

# Add src directory to Python path
sys.path.insert(0, 'src')

try:
    from core.advanced_spark_transformations import (
        AdvancedTransformationEngine, 
        TransformationContext
    )
    from core.enhanced_spark_generator import EnhancedSparkCodeGenerator
except ImportError as e:
    print(f"Import error: {e}")
    print("Make sure you're running from the project root directory")
    sys.exit(1)

def setup_logging():
    """Setup demo logging"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

def demonstrate_transformation_engine():
    """Demonstrate the Advanced Transformation Engine capabilities"""
    
    logger = logging.getLogger("TransformationDemo")
    logger.info("🚀 Advanced Transformation Engine Demonstration")
    
    # Initialize the transformation engine
    engine = AdvancedTransformationEngine()
    
    print("="*80)
    print("🔧 SUPPORTED TRANSFORMATIONS")
    print("="*80)
    
    # Get transformation information
    transformation_info = engine.get_transformation_info()
    
    for i, (trans_type, info) in enumerate(transformation_info.items(), 1):
        print(f"{i:2}. {trans_type.upper().replace('_', ' ')}")
        print(f"    Class: {info['class']}")
        print(f"    Description: {info['description']}")
        print()
    
    print(f"✅ Total: {len(transformation_info)} advanced transformations available")
    print()
    
    # Demonstrate each transformation
    demonstrate_individual_transformations(engine)
    
    # Demonstrate pattern detection
    demonstrate_pattern_detection()

def demonstrate_individual_transformations(engine):
    """Demonstrate code generation for each transformation type"""
    
    print("="*80)
    print("🔬 TRANSFORMATION CODE GENERATION SAMPLES")
    print("="*80)
    
    # Sample transformation contexts
    transformation_samples = {
        'scd_type2': {
            'name': 'Customer SCD Type 2',
            'config': {
                'business_keys': ['customer_id'],
                'tracked_columns': ['name', 'email', 'phone', 'address', 'status'],
                'effective_date_column': 'effective_date',
                'expiry_date_column': 'expiry_date',
                'current_flag_column': 'is_current'
            }
        },
        'scd_type1': {
            'name': 'Product SCD Type 1',
            'config': {
                'business_keys': ['product_id'],
                'update_columns': ['name', 'category', 'price', 'status'],
                'handle_deletes': False
            }
        },
        'router': {
            'name': 'Transaction Router',
            'config': {
                'routes': [
                    {'name': 'high_value', 'condition': 'amount > 10000', 'priority': 1},
                    {'name': 'medium_value', 'condition': 'amount > 1000 AND amount <= 10000', 'priority': 2},
                    {'name': 'low_value', 'condition': 'amount <= 1000', 'priority': 3},
                    {'name': 'suspicious', 'condition': 'is_flagged = true', 'priority': 0}
                ],
                'default_route': 'unprocessed'
            }
        },
        'union': {
            'name': 'Multi-Source Union',
            'config': {
                'union_all': True,
                'remove_duplicates': True,
                'duplicate_columns': ['customer_id', 'transaction_date'],
                'add_source_columns': True
            }
        },
        'rank': {
            'name': 'Sales Ranking',
            'config': {
                'partition_by': ['region', 'product_category'],
                'order_by': [
                    {'column': 'sales_amount', 'direction': 'desc'},
                    {'column': 'transaction_date', 'direction': 'desc'}
                ],
                'rank_methods': ['rank', 'dense_rank', 'row_number', 'percent_rank'],
                'top_n': 10,
                'filter_rank_column': 'rank_value',
                'tie_breaking': 'first'
            }
        },
        'data_masking': {
            'name': 'PII Data Masking',
            'config': {
                'columns': [
                    {'name': 'customer_name', 'method': 'partial_mask', 'params': {'show_first': 2, 'show_last': 1, 'char': '*'}},
                    {'name': 'email', 'method': 'email_mask', 'params': {}},
                    {'name': 'phone', 'method': 'phone_mask', 'params': {}},
                    {'name': 'ssn', 'method': 'hash', 'params': {'salt': 'secure_salt_2024'}},
                    {'name': 'credit_card', 'method': 'tokenize', 'params': {'prefix': 'CC_TOKEN_'}},
                    {'name': 'address', 'method': 'redact', 'params': {'char': 'X', 'preserve_length': True}}
                ],
                'add_audit_columns': True,
                'version': '2.0'
            }
        },
        'complex_lookup': {
            'name': 'Customer Enrichment Lookup',
            'config': {
                'lookup_type': 'connected',
                'join_strategy': 'broadcast',
                'multiple_match': 'first',
                'cache_lookup': True,
                'lookup_keys': [
                    {'input_column': 'customer_id', 'lookup_column': 'cust_id'},
                    {'input_column': 'region', 'lookup_column': 'region_code'}
                ],
                'return_columns': ['customer_name', 'customer_tier', 'credit_limit', 'preferred_contact'],
                'default_values': {
                    'customer_tier': 'STANDARD',
                    'credit_limit': 0.0,
                    'preferred_contact': 'EMAIL'
                }
            }
        },
        'advanced_aggregation': {
            'name': 'Sales Analytics Aggregation',
            'config': {
                'group_by_columns': ['region', 'product_category', 'sales_month'],
                'aggregations': [
                    {'type': 'sum', 'column': 'sales_amount', 'alias': 'total_sales'},
                    {'type': 'count', 'column': 'transaction_id', 'alias': 'transaction_count'},
                    {'type': 'avg', 'column': 'sales_amount', 'alias': 'avg_transaction'},
                    {'type': 'percentile', 'column': 'sales_amount', 'percentile': 0.95, 'alias': 'sales_p95'},
                    {'type': 'stddev', 'column': 'sales_amount', 'alias': 'sales_volatility'},
                    {'type': 'count_distinct', 'column': 'customer_id', 'alias': 'unique_customers'}
                ],
                'window_functions': [
                    {
                        'type': 'row_number', 
                        'column': 'total_sales', 
                        'partition_by': ['region'], 
                        'order_by': [{'column': 'total_sales', 'direction': 'desc'}], 
                        'alias': 'region_rank'
                    },
                    {
                        'type': 'lag', 
                        'column': 'total_sales', 
                        'partition_by': ['region', 'product_category'], 
                        'order_by': [{'column': 'sales_month', 'direction': 'asc'}], 
                        'offset': 1,
                        'alias': 'previous_month_sales'
                    }
                ],
                'incremental_mode': False,
                'partition_by': ['region'],
                'pre_filters': ['sales_amount > 0', 'region IS NOT NULL'],
                'post_filters': ['total_sales > 1000']
            }
        }
    }
    
    for trans_type, sample in transformation_samples.items():
        print(f"📊 {sample['name']} ({trans_type.upper()})")
        print("-" * 60)
        
        try:
            context = TransformationContext(
                mapping_name=sample['name'],
                project_name="DemoProject",
                source_tables=[],
                target_tables=[],
                transformation_config=sample['config']
            )
            
            # Generate transformation code
            generated_code = engine.generate_transformation_code(trans_type, context)
            
            # Show first few lines of generated code
            lines = generated_code.split('\n')
            preview_lines = lines[:15]  # Show first 15 lines
            
            for line in preview_lines:
                print(f"  {line}")
            
            if len(lines) > 15:
                print(f"  ... ({len(lines) - 15} more lines)")
            
            print(f"✅ Generated {len(lines)} lines of production PySpark code")
            print()
            
        except Exception as e:
            print(f"❌ Error generating {trans_type}: {str(e)}")
            print()

def demonstrate_pattern_detection():
    """Demonstrate automatic pattern detection from transformation names"""
    
    print("="*80)
    print("🔍 AUTOMATIC PATTERN DETECTION")
    print("="*80)
    
    enhanced_generator = EnhancedSparkCodeGenerator()
    
    # Sample transformation patterns to test
    test_transformations = [
        {'name': 'Customer_SCD_Load', 'type': 'Java', 'description': 'Should detect SCD Type 2'},
        {'name': 'Product_Dimension_Update', 'type': 'Java', 'description': 'Should detect SCD Type 2'},
        {'name': 'Transaction_Router', 'type': 'Expression', 'description': 'Should detect Router'},
        {'name': 'Split_High_Value_Customers', 'type': 'Expression', 'description': 'Should detect Router'},
        {'name': 'Union_All_Sources', 'type': 'Expression', 'description': 'Should detect Union'},
        {'name': 'Combine_Regional_Data', 'type': 'Expression', 'description': 'Should detect Union'},
        {'name': 'Sales_Ranking_Transform', 'type': 'Expression', 'description': 'Should detect Rank'},
        {'name': 'Top_10_Products', 'type': 'Aggregator', 'description': 'Should detect Rank'},
        {'name': 'PII_Masking_Transform', 'type': 'Expression', 'description': 'Should detect Data Masking'},
        {'name': 'Customer_Privacy_Protection', 'type': 'Java', 'description': 'Should detect Data Masking'},
        {'name': 'Customer_Lookup_Enrichment', 'type': 'Lookup', 'description': 'Should detect Complex Lookup'},
        {'name': 'Sales_Aggregation_Summary', 'type': 'Aggregator', 'description': 'Should detect Advanced Aggregation'}
    ]
    
    # Create mock mapping for testing
    mock_mapping = {
        'name': 'Pattern_Detection_Test',
        'components': [
            {
                'name': transform['name'],
                'type': transform['type'],
                'component_type': 'transformation'
            } for transform in test_transformations
        ]
    }
    
    # Analyze transformations
    analysis = enhanced_generator._analyze_mapping_transformations(mock_mapping)
    
    print("Pattern Detection Results:")
    print()
    
    detected_patterns = []
    for key, value in analysis.items():
        if key.startswith('has_') and value:
            pattern_name = key.replace('has_', '').replace('_', ' ').title()
            detected_patterns.append(pattern_name)
    
    print(f"✅ Detected Patterns: {', '.join(detected_patterns)}")
    print()
    
    print("Transformation Analysis Details:")
    for detail in analysis['transformation_details']:
        enhanced_type = detail.get('enhanced_type', 'None')
        print(f"  • {detail['name']} ({detail['type']}) → {enhanced_type}")
    
    print()
    print(f"🎯 Pattern Detection Success: {len(detected_patterns)}/{len(enhanced_generator.transformation_patterns)} patterns detected")

def demonstrate_code_generation_integration():
    """Demonstrate integration with the enhanced mapping generator"""
    
    print("="*80)
    print("🏗️ ENHANCED MAPPING CODE GENERATION")
    print("="*80)
    
    enhanced_generator = EnhancedSparkCodeGenerator()
    
    # Create a comprehensive sample mapping
    sample_mapping = {
        'name': 'Comprehensive_ETL_Process',
        'description': 'End-to-end ETL with advanced transformations',
        'components': [
            {
                'name': 'Customer_Source',
                'type': 'HDFS',
                'component_type': 'source',
                'format': 'parquet'
            },
            {
                'name': 'Transaction_Source', 
                'type': 'Oracle',
                'component_type': 'source'
            },
            {
                'name': 'Customer_PII_Masking',
                'type': 'Java',
                'component_type': 'transformation'
            },
            {
                'name': 'Customer_SCD_Type2',
                'type': 'Java', 
                'component_type': 'transformation'
            },
            {
                'name': 'Transaction_Router_Split',
                'type': 'Expression',
                'component_type': 'transformation'
            },
            {
                'name': 'Sales_Analytics_Aggregation',
                'type': 'Aggregator',
                'component_type': 'transformation'
            },
            {
                'name': 'Customer_Enrichment_Lookup',
                'type': 'Lookup',
                'component_type': 'transformation'
            },
            {
                'name': 'Final_Output_Target',
                'type': 'HIVE',
                'component_type': 'target'
            }
        ]
    }
    
    sample_project = {
        'name': 'Advanced_ETL_Demo_Project',
        'version': '1.0'
    }
    
    try:
        # Generate enhanced mapping code
        generated_mapping_code = enhanced_generator.generate_enhanced_mapping_code(
            sample_mapping, 
            sample_project
        )
        
        # Show code statistics
        lines = generated_mapping_code.split('\n')
        methods = [line for line in lines if line.strip().startswith('def ')]
        classes = [line for line in lines if line.strip().startswith('class ')]
        imports = [line for line in lines if line.strip().startswith('from ') or line.strip().startswith('import ')]
        
        print("📊 Generated Mapping Code Statistics:")
        print(f"  • Total Lines: {len(lines):,}")
        print(f"  • Classes: {len(classes)}")
        print(f"  • Methods: {len(methods)}")
        print(f"  • Import Statements: {len(imports)}")
        print()
        
        print("🔍 Generated Methods (sample):")
        for method in methods[:10]:  # Show first 10 methods
            method_name = method.strip().split('(')[0].replace('def ', '')
            print(f"  • {method_name}")
        if len(methods) > 10:
            print(f"  ... and {len(methods) - 10} more methods")
        
        print()
        print("✅ Enhanced mapping code generated successfully!")
        
        # Optionally save to file for inspection
        output_path = Path("demo_generated_mapping.py")
        output_path.write_text(generated_mapping_code)
        print(f"💾 Full code saved to: {output_path}")
        
    except Exception as e:
        print(f"❌ Error generating enhanced mapping code: {str(e)}")
        import traceback
        traceback.print_exc()

def main():
    """Main demonstration function"""
    setup_logging()
    
    print("🚀 ADVANCED TRANSFORMATION FRAMEWORK DEMONSTRATION")
    print("=" * 80)
    print()
    print("This demonstration showcases the enhanced Informatica-to-Spark")
    print("transformation framework with production-ready capabilities.")
    print()
    
    try:
        # Demonstrate transformation engine
        demonstrate_transformation_engine()
        
        # Demonstrate code generation integration  
        demonstrate_code_generation_integration()
        
        print("="*80)
        print("🎉 DEMONSTRATION COMPLETE!")
        print("="*80)
        print()
        print("Key Achievements:")
        print("✅ 8 Production-ready transformation generators")
        print("✅ Automatic pattern detection from XML")
        print("✅ Enterprise-grade code generation")
        print("✅ Comprehensive error handling and logging")
        print("✅ Performance optimization features")
        print("✅ Data quality validation")
        print("✅ Flexible configuration system")
        print()
        print("The framework is now ready for enterprise Informatica-to-Spark migration!")
        
    except Exception as e:
        print(f"❌ Demonstration failed: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main() 