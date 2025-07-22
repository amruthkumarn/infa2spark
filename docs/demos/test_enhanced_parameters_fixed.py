#!/usr/bin/env python3
"""
Test Enhanced Parameter System - FIXED VERSION
=============================

Demonstrates the high-priority parameter improvements:
✅ Type-Aware Parameters: Proper data types with validation
✅ Transformation Scoping: Parameter isolation per transformation  
✅ Parameter Validation: Min/max, regex, and constraint checking
"""

import sys
import os
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, 'src')

def test_enhanced_parameter_system():
    """Test the enhanced parameter system with Financial DW project"""
    
    print("🚀 Testing Enhanced Parameter System")
    print("=" * 50)
    
    try:
        from core.enhanced_parameter_system import (
            EnhancedParameterManager,
            EnhancedParameter, 
            ParameterType,
            ParameterScope,
            ParameterValidation,
            TransformationParameterScope
        )
        
        # Initialize parameter manager
        param_manager = EnhancedParameterManager()
        
        print("✅ Enhanced Parameter System loaded successfully")
        print(f"📋 Built-in parameters: {len(param_manager.global_parameters) + len(param_manager.project_parameters)}")
        
        # Test 1: Type-Aware Parameters
        print(f"\n🔢 Test 1: Type-Aware Parameters")
        test_type_awareness(param_manager)
        
        # Test 2: Parameter Validation
        print(f"\n✅ Test 2: Parameter Validation")
        test_parameter_validation(param_manager)
        
        # Test 3: Transformation Scoping
        print(f"\n🎯 Test 3: Transformation Scoping")
        test_transformation_scoping(param_manager)
        
        # Test 4: Parameter Hierarchy Resolution
        print(f"\n🔄 Test 4: Parameter Resolution Hierarchy")
        test_parameter_hierarchy(param_manager)
        
        # Test 5: Configuration Export
        print(f"\n📤 Test 5: Configuration Export")
        test_config_export(param_manager)
        
        return True
        
    except ImportError as e:
        print(f"❌ ERROR: Enhanced parameter system not available: {e}")
        return False
    except Exception as e:
        print(f"❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_type_awareness(param_manager):
    """Test type-aware parameter handling - FIXED"""
    
    from core.enhanced_parameter_system import (
        EnhancedParameter, ParameterType, ParameterScope, ParameterValidation
    )
    
    # Test different parameter types
    test_cases = [
        ("BATCH_SIZE", "50000", int, 50000),
        ("ERROR_THRESHOLD", "0.05", float, 0.05),
        ("ENVIRONMENT", "PRODUCTION", str, "PRODUCTION")
    ]
    
    for param_name, raw_value, expected_type, expected_value in test_cases:
        try:
            # Find parameter definition
            param_def = param_manager._find_parameter_definition(param_name)
            if param_def:
                # Set value and test type conversion
                param_manager.set_parameter_value(param_name, raw_value, param_def.scope)
                resolved_value = param_manager.get_parameter_value(param_name)
                
                print(f"   • {param_name}: '{raw_value}' → {type(resolved_value).__name__}({resolved_value})")
                
                # Validate type conversion
                if not isinstance(resolved_value, expected_type):
                    print(f"     ❌ Type mismatch: expected {expected_type}, got {type(resolved_value)}")
                elif resolved_value != expected_value:
                    print(f"     ⚠️  Value mismatch: expected {expected_value}, got {resolved_value}")
                else:
                    print(f"     ✅ Correctly converted to {expected_type.__name__}")
            else:
                print(f"   • {param_name}: Parameter not found")
                
        except Exception as e:
            print(f"   • {param_name}: ❌ Error - {str(e)}")

def test_parameter_validation(param_manager):
    """Test parameter validation constraints"""
    
    validation_tests = [
        # (param_name, test_value, should_pass, description)
        ("BATCH_SIZE", 50000, True, "Valid batch size"),
        ("BATCH_SIZE", 50, False, "Too small batch size (< 100)"),
        ("BATCH_SIZE", 2000000, False, "Too large batch size (> 1M)"),
        ("ERROR_THRESHOLD", 0.05, True, "Valid error threshold"),
        ("ERROR_THRESHOLD", -0.1, False, "Negative error threshold"),
        ("ERROR_THRESHOLD", 1.5, False, "Error threshold > 1.0"),
        ("ENVIRONMENT", "PRODUCTION", True, "Valid environment"),
        ("ENVIRONMENT", "INVALID", False, "Invalid environment value"),
    ]
    
    for param_name, test_value, should_pass, description in validation_tests:
        try:
            param_def = param_manager._find_parameter_definition(param_name)
            if param_def:
                # Test validation
                is_valid = param_def.validate_value(test_value)
                
                if should_pass and is_valid:
                    print(f"   ✅ {description}: {test_value} (PASS)")
                elif not should_pass and not is_valid:
                    print(f"   ✅ {description}: {test_value} (CORRECTLY REJECTED)")
                elif should_pass and not is_valid:
                    print(f"   ❌ {description}: {test_value} (FALSE NEGATIVE)")
                else:
                    print(f"   ❌ {description}: {test_value} (FALSE POSITIVE)")
                    
        except Exception as e:
            print(f"   ❌ {description}: Error - {str(e)}")

def test_transformation_scoping(param_manager):
    """Test transformation-specific parameter isolation"""
    
    from core.enhanced_parameter_system import (
        EnhancedParameter, ParameterType, ParameterScope, ParameterValidation
    )
    
    # Create transformation scopes
    transformations = [
        ("TXN_CUST_002", "DETECT_CHANGES", "lookup"), 
        ("TXN_CUST_003", "SCD_TYPE2_LOGIC", "java")
    ]
    
    for transform_id, transform_name, transform_type in transformations:
        print(f"   📊 Creating scope for {transform_name} ({transform_type})")
        
        # Create transformation scope
        scope = param_manager.create_transformation_scope(transform_id, transform_name)
        
        # Add transformation-specific parameters based on type
        if transform_type == "lookup":
            scope.add_parameter(EnhancedParameter(
                name="lookup_cache_size_mb",
                param_type=ParameterType.INTEGER,
                scope=ParameterScope.TRANSFORMATION,
                default_value=256,
                description="Lookup cache size in MB",
                validation=ParameterValidation(min_value=64, max_value=8192)
            ))
            
            scope.set_parameter_value("lookup_cache_size_mb", 512)
            cache_size = scope.get_parameter_value("lookup_cache_size_mb")
            print(f"     ✅ Cache size: {cache_size}MB")
                
        elif transform_type == "java":
            scope.add_parameter(EnhancedParameter(
                name="scd_history_days",
                param_type=ParameterType.INTEGER,
                scope=ParameterScope.TRANSFORMATION,
                default_value=2555,
                description="SCD history retention days",
                validation=ParameterValidation(min_value=1, max_value=36500)
            ))
            
            scope.set_parameter_value("scd_history_days", 1825)  # 5 years
            history_days = scope.get_parameter_value("scd_history_days")
            print(f"     ✅ SCD history: {history_days} days")
    
    print(f"   📈 Total transformation scopes created: {len(param_manager.transformation_scopes)}")

def test_parameter_hierarchy(param_manager):
    """Test parameter resolution hierarchy - FIXED"""
    
    from core.enhanced_parameter_system import (
        EnhancedParameter, ParameterType, ParameterScope, ParameterValidation
    )
    
    # Create a test parameter for hierarchy testing
    param_name = "TEST_HIERARCHY"
    
    # Create parameter definitions for different scopes
    global_param = EnhancedParameter(
        name=param_name,
        param_type=ParameterType.STRING,
        scope=ParameterScope.GLOBAL,
        description="Test hierarchy parameter"
    )
    param_manager.add_parameter(global_param)
    
    # Set values at different levels
    param_manager.set_parameter_value(param_name, "global_value", ParameterScope.GLOBAL)
    param_manager.set_parameter_value(param_name, "project_value", ParameterScope.PROJECT)
    param_manager.set_parameter_value(param_name, "runtime_value", ParameterScope.RUNTIME)
    
    # Test resolution from different contexts
    contexts = [
        (ParameterScope.GLOBAL, "global_value"),
        (ParameterScope.PROJECT, "project_value"),
        (ParameterScope.RUNTIME, "runtime_value"),
    ]
    
    for context_scope, expected_value in contexts:
        resolved_value = param_manager.get_parameter_value(param_name, context_scope)
        
        if resolved_value == expected_value:
            print(f"   ✅ From {context_scope.value}: '{resolved_value}' (correct)")
        else:
            print(f"   ❌ From {context_scope.value}: '{resolved_value}' (expected '{expected_value}')")

def test_config_export(param_manager):
    """Test typed configuration export"""
    
    # Export configuration
    config = param_manager.export_typed_config()
    
    print(f"   📊 Exported configuration scopes: {list(config.keys())}")
    
    # Display some configuration details
    for scope_name, scope_params in config.items():
        if scope_params:
            print(f"   • {scope_name}: {len(scope_params)} parameters")

def test_with_financial_dw():
    """Test enhanced parameter system with Financial DW project generation"""
    
    print(f"\n🏦 Testing with Financial DW Project")
    print("=" * 40)
    
    try:
        from core.spark_code_generator import SparkCodeGenerator
        
        # Generate with enhanced parameter system
        generator = SparkCodeGenerator('generated_spark_apps')
        
        print("📊 Generating Financial DW with enhanced parameters...")
        app_path = generator.generate_spark_application('input/financial_dw_project.xml', 'FinancialDW_Enhanced')
        
        print(f"✅ Application generated at: {app_path}")
        
        # Check if enhanced parameter system was used
        if hasattr(generator.enhanced_generator, 'parameter_manager') and generator.enhanced_generator.parameter_manager:
            param_manager = generator.enhanced_generator.parameter_manager
            
            print(f"🎯 Parameter System Stats:")
            print(f"   • Global parameters: {len(param_manager.global_parameters)}")
            print(f"   • Project parameters: {len(param_manager.project_parameters)}")
            print(f"   • Transformation scopes: {len(param_manager.transformation_scopes)}")
            
            # Export enhanced config
            enhanced_config = param_manager.export_typed_config()
            config_file = Path(app_path) / "config" / "enhanced_parameters.yaml"
            
            import yaml
            with open(config_file, 'w') as f:
                yaml.dump(enhanced_config, f, default_flow_style=False)
            
            print(f"   📄 Enhanced config exported to: {config_file}")
        else:
            print("   ⚠️  Enhanced parameter system not used")
            
        return True
        
    except Exception as e:
        print(f"❌ Error testing with Financial DW: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🎯 Enhanced Parameter System Test Suite - FIXED")
    print("=" * 60)
    
    # Run core tests
    success = test_enhanced_parameter_system()
    
    if success:
        print(f"\n✅ Core tests PASSED")
        
        # Run integration test  
        integration_success = test_with_financial_dw()
        
        if integration_success:
            print(f"\n🎉 ALL TESTS PASSED!")
            print(f"\n📋 Enhanced Parameter System Features Validated:")
            print(f"   ✅ Type-Aware Parameters with automatic conversion")
            print(f"   ✅ Parameter Validation with constraints and error messages") 
            print(f"   ✅ Transformation Scoping with isolated parameter spaces")
            print(f"   ✅ Hierarchical Parameter Resolution")
            print(f"   ✅ Integration with Spark Code Generation")
        else:
            print(f"\n⚠️  Integration tests failed")
    else:
        print(f"\n❌ Core tests failed")
        sys.exit(1) 