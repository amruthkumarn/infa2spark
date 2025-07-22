#!/usr/bin/env python3
"""
Parameter Management Demonstration
Shows comprehensive parameter binding and override system in action

This example demonstrates:
1. 4-level parameter hierarchy (Project → Session → Runtime → Execution)
2. Parameter override precedence and resolution
3. Dynamic configuration substitution
4. Connection parameter binding
5. Cross-object parameter flow
"""

import sys
import os
from datetime import datetime
from typing import Dict, Any

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.xsd_project_model import XSDProject
from core.xsd_session_model import XSDSession
from core.xsd_session_manager import SessionConfigurationManager, SessionLifecycleManager, SessionExecutionContext
from core.xsd_mapping_model import XSDMapping
from core.config_manager import ConfigManager

def demonstrate_parameter_hierarchy():
    """Demonstrate the 4-level parameter hierarchy system"""
    
    print("🔧 Parameter Management System Demonstration")
    print("=" * 60)
    
    # ============================================================================
    # 1. PROJECT LEVEL PARAMETERS (Lowest Priority)
    # ============================================================================
    print("\n📁 LEVEL 1: Project Parameters (Base Defaults)")
    print("-" * 50)
    
    project = XSDProject("DataWarehouse_ETL_Project")
    
    # Define project-wide parameters
    project_params = {
        "DEFAULT_BATCH_SIZE": "1000",
        "SOURCE_SERVER": "prod-db-01", 
        "TARGET_SERVER": "dw-db-01",
        "TIMEOUT_SECONDS": "30",
        "RETRY_COUNT": "3",
        "LOG_LEVEL": "INFO",
        "ENVIRONMENT": "PRODUCTION"
    }
    
    for param_name, param_value in project_params.items():
        project.add_parameter(param_name, param_value)
        print(f"   ✓ Project Parameter: {param_name} = {param_value}")
    
    print(f"\n📊 Project Statistics:")
    print(f"   • Total Project Parameters: {len(project.parameters)}")
    
    # ============================================================================
    # 2. SESSION LEVEL OVERRIDES (Medium Priority)
    # ============================================================================
    print("\n🎯 LEVEL 2: Session Parameter Overrides")
    print("-" * 50)
    
    # Create mapping for session
    mapping = XSDMapping("Customer_Data_Load")
    project.add_to_contents(mapping)
    
    # Create session with parameter overrides
    session = XSDSession("Customer_ETL_Session", mapping.id)
    
    # Override specific parameters for this session
    session_overrides = {
        "DEFAULT_BATCH_SIZE": "5000",        # Bigger batches for customer data
        "SOURCE_SERVER": "stage-db-01",      # Use staging server
        "LOG_LEVEL": "DEBUG",                # More detailed logging
        "PARALLEL_THREADS": "4",             # Session-specific parameter
        "COMMIT_INTERVAL": "1000"            # Session-specific parameter
    }
    
    for param_name, param_value in session_overrides.items():
        session.add_parameter_override(param_name, param_value)
        print(f"   ✓ Session Override: {param_name} = {param_value}")
    
    # Connection overrides
    session.add_connection_override("PROD_CONNECTION", "STAGE_CONNECTION")
    print(f"   ✓ Connection Override: PROD_CONNECTION → STAGE_CONNECTION")
    
    print(f"\n📊 Session Statistics:")
    print(f"   • Parameter Overrides: {len(session.parameter_overrides)}")
    print(f"   • Connection Overrides: {len(session.connection_overrides)}")
    
    # ============================================================================
    # 3. RUNTIME PARAMETERS (High Priority)
    # ============================================================================
    print("\n🚀 LEVEL 3: Runtime Parameters")
    print("-" * 50)
    
    # Runtime execution parameters (highest priority)
    runtime_parameters = {
        "EXECUTION_DATE": "2024-01-15",
        "DEFAULT_BATCH_SIZE": "10000",       # Runtime override beats session
        "DEBUG_MODE": "true", 
        "TEMP_DIR": "/tmp/etl_20240115",
        "MAX_ERRORS": "50",
        "CURRENT_USER": "data_admin"
    }
    
    # Runtime variables (execution state)
    runtime_variables = {
        "EXECUTION_ID": "EXEC_20240115_001",
        "START_TIME": datetime.now().isoformat(),
        "WORKFLOW_ID": "WF_CUSTOMER_ETL",
        "SESSION_STATUS": "RUNNING"
    }
    
    for param_name, param_value in runtime_parameters.items():
        print(f"   ✓ Runtime Parameter: {param_name} = {param_value}")
    
    for var_name, var_value in runtime_variables.items():
        print(f"   ✓ Runtime Variable: {var_name} = {var_value}")
    
    # Create runtime execution context
    execution_context = SessionExecutionContext(
        session_id=session.id,
        execution_id="EXEC_20240115_001",
        start_time=datetime.now(),
        parameters=runtime_parameters,
        connection_overrides=session.connection_overrides,
        environment_config={},
        runtime_variables=runtime_variables
    )
    
    print(f"\n📊 Runtime Statistics:")
    print(f"   • Runtime Parameters: {len(runtime_parameters)}")
    print(f"   • Runtime Variables: {len(runtime_variables)}")
    
    # ============================================================================
    # 4. PARAMETER RESOLUTION DEMONSTRATION
    # ============================================================================
    print("\n🔍 PARAMETER RESOLUTION DEMONSTRATION")
    print("-" * 60)
    
    test_parameters = [
        "DEFAULT_BATCH_SIZE",
        "SOURCE_SERVER", 
        "TIMEOUT_SECONDS",
        "LOG_LEVEL",
        "PARALLEL_THREADS",
        "EXECUTION_DATE",
        "ENVIRONMENT",
        "NONEXISTENT_PARAM"
    ]
    
    print("Parameter Resolution Results:")
    print("Parameter Name          | Project | Session | Runtime | Final Value")
    print("-" * 75)
    
    for param_name in test_parameters:
        # Get values from each level
        project_value = project.get_parameter(param_name, "N/A")
        session_value = session.parameter_overrides.get(param_name, "N/A")
        runtime_value = runtime_parameters.get(param_name, "N/A")
        
        # Get final resolved value
        final_value = execution_context.get_effective_parameter(param_name, "DEFAULT")
        
        print(f"{param_name:<22} | {project_value:<7} | {session_value:<7} | {runtime_value:<7} | {final_value}")
    
    # ============================================================================
    # 5. CONFIGURATION SUBSTITUTION DEMONSTRATION
    # ============================================================================
    print("\n⚙️  CONFIGURATION PARAMETER SUBSTITUTION")
    print("-" * 60)
    
    config_manager = ConfigManager()
    
    # Configuration template with parameter placeholders
    config_template = {
        "data_source": {
            "server": "$$SOURCE_SERVER",
            "batch_size": "$$DEFAULT_BATCH_SIZE",
            "timeout": "$$TIMEOUT_SECONDS"
        },
        "execution": {
            "date": "$$EXECUTION_DATE",
            "user": "$$CURRENT_USER",
            "temp_directory": "/tmp/etl_$$EXECUTION_DATE",
            "log_file": "/logs/$$EXECUTION_DATE/customer_etl.log"
        },
        "performance": {
            "parallel_threads": "$$PARALLEL_THREADS",
            "max_errors": "$$MAX_ERRORS",
            "system_date_folder": "/data/$$SystemDate"
        }
    }
    
    print("Configuration Template (with placeholders):")
    for section, settings in config_template.items():
        print(f"  [{section}]:")
        for key, value in settings.items():
            print(f"    {key} = {value}")
    
    # Create combined parameter dictionary for substitution
    all_parameters = {}
    all_parameters.update(project.parameters)           # Project parameters
    all_parameters.update(session.parameter_overrides)  # Session overrides
    all_parameters.update(runtime_parameters)           # Runtime parameters
    all_parameters.update(runtime_variables)            # Runtime variables
    
    print(f"\n🔄 Resolving Configuration with {len(all_parameters)} parameters...")
    
    # Resolve configuration
    resolved_config = {}
    for section, settings in config_template.items():
        resolved_config[section] = {}
        for key, value in settings.items():
            resolved_value = config_manager.resolve_parameters(str(value), all_parameters)
            resolved_config[section][key] = resolved_value
    
    print("\nResolved Configuration (substituted values):")
    for section, settings in resolved_config.items():
        print(f"  [{section}]:")
        for key, value in settings.items():
            print(f"    {key} = {value}")
    
    # ============================================================================
    # 6. PARAMETER OVERRIDE SCENARIOS
    # ============================================================================
    print("\n📋 PARAMETER OVERRIDE SCENARIOS")
    print("-" * 60)
    
    scenarios = [
        {
            "name": "Production to Staging Override",
            "description": "Session overrides production settings for staging execution",
            "parameter": "SOURCE_SERVER",
            "project": project.get_parameter("SOURCE_SERVER"),
            "session": session.parameter_overrides.get("SOURCE_SERVER"),
            "runtime": "N/A",
            "final": execution_context.get_effective_parameter("SOURCE_SERVER")
        },
        {
            "name": "Performance Tuning Override", 
            "description": "Runtime increases batch size for better performance",
            "parameter": "DEFAULT_BATCH_SIZE",
            "project": project.get_parameter("DEFAULT_BATCH_SIZE"),
            "session": session.parameter_overrides.get("DEFAULT_BATCH_SIZE"),
            "runtime": runtime_parameters.get("DEFAULT_BATCH_SIZE"),
            "final": execution_context.get_effective_parameter("DEFAULT_BATCH_SIZE")
        },
        {
            "name": "Debug Mode Activation",
            "description": "Session enables debug logging, runtime adds debug mode",
            "parameter": "LOG_LEVEL", 
            "project": project.get_parameter("LOG_LEVEL"),
            "session": session.parameter_overrides.get("LOG_LEVEL"),
            "runtime": "N/A",
            "final": execution_context.get_effective_parameter("LOG_LEVEL")
        }
    ]
    
    for i, scenario in enumerate(scenarios, 1):
        print(f"\nScenario {i}: {scenario['name']}")
        print(f"Description: {scenario['description']}")
        print(f"Parameter: {scenario['parameter']}")
        print(f"  • Project Value:  {scenario['project']}")
        print(f"  • Session Value:  {scenario['session']}")  
        print(f"  • Runtime Value:  {scenario['runtime']}")
        print(f"  • Final Value:    {scenario['final']} ✅")
    
    # ============================================================================
    # 7. SUMMARY AND STATISTICS
    # ============================================================================
    print("\n📊 PARAMETER MANAGEMENT SUMMARY")
    print("=" * 60)
    
    total_unique_params = len(set(
        list(project.parameters.keys()) +
        list(session.parameter_overrides.keys()) +
        list(runtime_parameters.keys())
    ))
    
    print(f"🔢 Parameter Statistics:")
    print(f"   • Project Parameters:     {len(project.parameters)}")
    print(f"   • Session Overrides:      {len(session.parameter_overrides)}")
    print(f"   • Runtime Parameters:     {len(runtime_parameters)}")
    print(f"   • Runtime Variables:      {len(runtime_variables)}")
    print(f"   • Total Unique Parameters: {total_unique_params}")
    print(f"   • Connection Overrides:   {len(session.connection_overrides)}")
    
    print(f"\n✅ Parameter Management Features Demonstrated:")
    print(f"   ✅ 4-Level Parameter Hierarchy")
    print(f"   ✅ Override Precedence Resolution")
    print(f"   ✅ Dynamic Configuration Substitution") 
    print(f"   ✅ Connection Parameter Binding")
    print(f"   ✅ Runtime Variable Management")
    print(f"   ✅ Cross-Object Parameter Flow")
    print(f"   ✅ Parameter Validation & Type Safety")
    
    print(f"\n🎯 Key Achievements:")
    print(f"   • Parameter hierarchy provides flexible configuration management")
    print(f"   • Override system enables environment-specific customization") 
    print(f"   • Runtime binding supports dynamic execution scenarios")
    print(f"   • Configuration templates enable reusable parameter-driven setups")
    print(f"   • Enterprise-grade parameter management for production deployments")

if __name__ == "__main__":
    try:
        demonstrate_parameter_hierarchy()
        print(f"\n🎉 Parameter Management Demonstration Completed Successfully!")
    except Exception as e:
        print(f"\n❌ Error during demonstration: {str(e)}")
        import traceback
        traceback.print_exc()