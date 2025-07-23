#!/usr/bin/env python3
"""
Enhanced Workflow Task Demonstration
===================================

This script demonstrates the new workflow task capabilities in our 
Informatica-to-Spark converter framework. It showcases how various
Informatica workflow task types are converted to executable Python code.

Task Types Demonstrated:
- Command Tasks (shell script execution)
- Decision Tasks (conditional branching)
- Assignment Tasks (parameter management)
- Start Workflow Tasks (sub-workflow execution)
- Timer Tasks (delay execution)
- Email Tasks (notifications)
- Session/Mapping Tasks (data processing)
"""

import sys
import os
sys.path.insert(0, 'src')

from core.spark_code_generator import SparkCodeGenerator
from pathlib import Path
import tempfile
import xml.etree.ElementTree as ET


def create_enhanced_workflow_xml():
    """Create a comprehensive XML with various workflow task types"""
    
    xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
<imx:Document xmlns:imx="http://www.informatica.com/imx" xmlns:project="http://www.informatica.com/project" 
              xmlns:folder="http://www.informatica.com/folder" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    
    <project:Project name="Enhanced_Workflow_Demo" version="1.0">
        <folder:Folder name="Advanced_ETL">
            
            <!-- Advanced Workflow with Multiple Task Types -->
            <lObject xsi:type="Workflow" name="WF_Advanced_ETL_Process" id="advanced_wf_001">
                <description>Demonstrates all supported workflow task types</description>
                
                <!-- Task 1: Assignment Task - Set initial parameters -->
                <lObject xsi:type="Task" name="T1_Initialize_Parameters" type="Assignment">
                    <properties>
                        <property name="Assignments" value="[
                            {&apos;parameter_name&apos;: &apos;PROCESS_DATE&apos;, &apos;expression&apos;: &apos;datetime.now().strftime(&quot;%Y-%m-%d&quot;)&apos;, &apos;data_type&apos;: &apos;string&apos;},
                            {&apos;parameter_name&apos;: &apos;BATCH_SIZE&apos;, &apos;expression&apos;: &apos;10000&apos;, &apos;data_type&apos;: &apos;integer&apos;},
                            {&apos;parameter_name&apos;: &apos;DEBUG_MODE&apos;, &apos;expression&apos;: &apos;True&apos;, &apos;data_type&apos;: &apos;boolean&apos;}
                        ]"/>
                    </properties>
                </lObject>
                
                <!-- Task 2: Command Task - Prepare environment -->
                <lObject xsi:type="Task" name="T2_Prepare_Environment" type="Command">
                    <properties>
                        <property name="Command" value="mkdir -p /tmp/etl_staging && echo 'Environment prepared'"/>
                        <property name="WorkingDirectory" value="/tmp"/>
                        <property name="TimeoutSeconds" value="120"/>
                    </properties>
                </lObject>
                
                <!-- Task 3: Decision Task - Check data availability -->
                <lObject xsi:type="Task" name="T3_Check_Data_Availability" type="Decision">
                    <properties>
                        <property name="Conditions" value="[
                            {&apos;name&apos;: &apos;data_available&apos;, &apos;expression&apos;: &apos;current_hour &gt;= 6&apos;, &apos;target_path&apos;: &apos;process_data&apos;, &apos;priority&apos;: 1},
                            {&apos;name&apos;: &apos;weekend_check&apos;, &apos;expression&apos;: &apos;datetime.now().weekday() &gt;= 5&apos;, &apos;target_path&apos;: &apos;skip_processing&apos;, &apos;priority&apos;: 2}
                        ]"/>
                        <property name="DefaultPath" value="wait_for_data"/>
                    </properties>
                </lObject>
                
                <!-- Task 4: Timer Task - Wait for system resources -->
                <lObject xsi:type="Task" name="T4_Wait_For_Resources" type="Timer">
                    <properties>
                        <property name="DelayAmount" value="5"/>
                        <property name="DelayUnit" value="minutes"/>
                    </properties>
                </lObject>
                
                <!-- Task 5: Session/Mapping Task - Process customer data -->
                <lObject xsi:type="Task" name="T5_Process_Customer_Data" type="Mapping">
                    <mappingTaskConfig mapping="m_process_customer_data"/>
                </lObject>
                
                <!-- Task 6: Start Workflow Task - Launch child workflow -->
                <lObject xsi:type="Task" name="T6_Start_Archive_Workflow" type="StartWorkflow">
                    <properties>
                        <property name="TargetWorkflow" value="wf_archive_process"/>
                        <property name="ExecutionMode" value="asynchronous"/>
                        <property name="ParameterMapping" value="{
                            &apos;ARCHIVE_DATE&apos;: &apos;$PROCESS_DATE&apos;,
                            &apos;SOURCE_BATCH_SIZE&apos;: &apos;$BATCH_SIZE&apos;
                        }"/>
                    </properties>
                </lObject>
                
                <!-- Task 7: Assignment Task - Update completion status -->
                <lObject xsi:type="Task" name="T7_Update_Status" type="Assignment">
                    <properties>
                        <property name="Assignments" value="[
                            {&apos;parameter_name&apos;: &apos;COMPLETION_TIME&apos;, &apos;expression&apos;: &apos;datetime.now().strftime(&quot;%Y-%m-%d %H:%M:%S&quot;)&apos;, &apos;data_type&apos;: &apos;string&apos;},
                            {&apos;parameter_name&apos;: &apos;STATUS&apos;, &apos;expression&apos;: &apos;&quot;COMPLETED&quot;&apos;, &apos;data_type&apos;: &apos;string&apos;}
                        ]"/>
                    </properties>
                </lObject>
                
                <!-- Task 8: Email Task - Send completion notification -->
                <lObject xsi:type="Task" name="T8_Send_Completion_Email" type="Email">
                    <properties>
                        <property name="Recipients" value="[&apos;admin@company.com&apos;, &apos;etl-team@company.com&apos;]"/>
                        <property name="Subject" value="ETL Process Completed Successfully"/>
                    </properties>
                </lObject>
                
                <!-- Task 9: Command Task - Cleanup temporary files -->
                <lObject xsi:type="Task" name="T9_Cleanup" type="Command">
                    <properties>
                        <property name="Command" value="rm -rf /tmp/etl_staging && echo 'Cleanup completed'"/>
                        <property name="WorkingDirectory" value="/tmp"/>
                        <property name="TimeoutSeconds" value="60"/>
                    </properties>
                </lObject>
                
                <!-- Workflow Links (Task Dependencies) -->
                <outgoingSequenceFlows>
                    <lObject from="T1_Initialize_Parameters" to="T2_Prepare_Environment"/>
                    <lObject from="T2_Prepare_Environment" to="T3_Check_Data_Availability"/>
                    <lObject from="T3_Check_Data_Availability" to="T4_Wait_For_Resources"/>
                    <lObject from="T4_Wait_For_Resources" to="T5_Process_Customer_Data"/>
                    <lObject from="T5_Process_Customer_Data" to="T6_Start_Archive_Workflow"/>
                    <lObject from="T6_Start_Archive_Workflow" to="T7_Update_Status"/>
                    <lObject from="T7_Update_Status" to="T8_Send_Completion_Email"/>
                    <lObject from="T8_Send_Completion_Email" to="T9_Cleanup"/>
                </outgoingSequenceFlows>
            </lObject>
            
            <!-- Sample Mapping for the Session Task -->
            <lObject xsi:type="Mapping" name="m_process_customer_data" id="mapping_001">
                <description>Process customer data with transformations</description>
                <components>
                    <lObject component_type="source" name="SRC_Customer_Data" type="Source">
                        <format>CSV</format>
                    </lObject>
                    <lObject component_type="transformation" name="EXPR_Add_Audit_Columns" type="Expression">
                    </lObject>
                    <lObject component_type="transformation" name="FLTR_Active_Customers" type="Filter">
                    </lObject>
                    <lObject component_type="target" name="TGT_Customer_Processed" type="Target">
                        <format>PARQUET</format>
                    </lObject>
                </components>
            </lObject>
            
        </folder:Folder>
    </project:Project>
</imx:Document>'''
    
    return xml_content


def create_child_workflow_xml():
    """Create a simple child workflow for demonstration"""
    
    xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
<imx:Document xmlns:imx="http://www.informatica.com/imx" xmlns:project="http://www.informatica.com/project" 
              xmlns:folder="http://www.informatica.com/folder" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    
    <project:Project name="Archive_Process" version="1.0">
        <folder:Folder name="Archive">
            
            <lObject xsi:type="Workflow" name="wf_archive_process" id="archive_wf_001">
                <description>Child workflow for archiving processed data</description>
                
                <!-- Archive Task -->
                <lObject xsi:type="Task" name="T1_Archive_Data" type="Command">
                    <properties>
                        <property name="Command" value="echo 'Archiving data for date: $ARCHIVE_DATE'"/>
                        <property name="TimeoutSeconds" value="300"/>
                    </properties>
                </lObject>
                
                <!-- Notification Task -->
                <lObject xsi:type="Task" name="T2_Archive_Complete" type="Email">
                    <properties>
                        <property name="Recipients" value="[&apos;archive-team@company.com&apos;]"/>
                        <property name="Subject" value="Archive Process Completed"/>
                    </properties>
                </lObject>
                
                <outgoingSequenceFlows>
                    <lObject from="T1_Archive_Data" to="T2_Archive_Complete"/>
                </outgoingSequenceFlows>
            </lObject>
            
        </folder:Folder>
    </project:Project>
</imx:Document>'''
    
    return xml_content


def main():
    """Demonstrate enhanced workflow task capabilities"""
    
    print("🚀 Enhanced Workflow Task Demonstration")
    print("=" * 60)
    
    try:
        # Create temporary XML files
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as main_xml:
            main_xml.write(create_enhanced_workflow_xml())
            main_xml_path = main_xml.name
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as child_xml:
            child_xml.write(create_child_workflow_xml())
            child_xml_path = child_xml.name
        
        print(f"📁 Created temporary XML files:")
        print(f"   Main workflow: {main_xml_path}")
        print(f"   Child workflow: {child_xml_path}")
        
        # Generate Spark applications
        generator = SparkCodeGenerator('generated_spark_apps')
        
        print("\n📊 Generating Enhanced Workflow Application...")
        main_app_path = generator.generate_spark_application(main_xml_path, 'Enhanced_Workflow_Demo')
        
        print("\n📊 Generating Child Workflow Application...")  
        child_app_path = generator.generate_spark_application(child_xml_path, 'Archive_Process')
        
        print("\n✅ Generation Complete!")
        print(f"   Main app: {main_app_path}")
        print(f"   Child app: {child_app_path}")
        
        # Analyze generated workflow code
        main_workflow_file = Path(main_app_path) / "src/main/python/workflows/wf_advanced_etl_process.py"
        if main_workflow_file.exists():
            print("\n🔍 Analyzing Generated Workflow Code:")
            print("-" * 40)
            
            with open(main_workflow_file, 'r') as f:
                content = f.read()
                
            # Count task implementations
            task_implementations = {
                'Command Tasks': content.count('_execute_command_task'),
                'Decision Tasks': content.count('_execute_decision_task'),
                'Assignment Tasks': content.count('_execute_assignment_task'),
                'StartWorkflow Tasks': content.count('_execute_start_workflow_task'),
                'Timer Tasks': content.count('_execute_timer_task'),
                'Email Tasks': content.count('_send_notification'),
                'Mapping Tasks': content.count('mapping_classes')
            }
            
            print("\n📈 Task Implementation Summary:")
            for task_type, count in task_implementations.items():
                if count > 0:
                    print(f"   ✅ {task_type}: {count} implementations")
                else:
                    print(f"   ⚪ {task_type}: Not used in this workflow")
        
        # Show sample generated code
        print("\n📄 Sample Generated Task Code:")
        print("-" * 40)
        
        if main_workflow_file.exists():
            with open(main_workflow_file, 'r') as f:
                lines = f.readlines()
            
            # Find and show Command Task implementation
            for i, line in enumerate(lines):
                if '_execute_command_task' in line and 'def ' in line:
                    print("Command Task Implementation:")
                    for j in range(i, min(i+15, len(lines))):
                        print(f"   {lines[j].rstrip()}")
                    break
        
        print("\n🎯 Task Coverage Analysis:")
        print("-" * 40)
        print("✅ IMPLEMENTED:")
        print("   • Session/Mapping Tasks - Full data processing")
        print("   • Command Tasks - Shell script execution")  
        print("   • Decision Tasks - Conditional branching")
        print("   • Assignment Tasks - Parameter management")
        print("   • StartWorkflow Tasks - Sub-workflow execution")
        print("   • Timer Tasks - Delay execution")
        print("   • Email Tasks - Notification sending")
        
        print("\n⚠️  NOT YET IMPLEMENTED:")
        print("   • Event Wait/Raise Tasks - File monitoring, external events")
        print("   • Stop/Abort Workflow Tasks - Workflow termination")
        print("   • Control Workflow Tasks - Advanced orchestration")
        
        print("\n🏆 Implementation Status:")
        total_informatica_tasks = 11  # From PowerCenter XSD analysis
        implemented_tasks = 7
        coverage_percent = (implemented_tasks / total_informatica_tasks) * 100
        
        print(f"   📊 Task Coverage: {implemented_tasks}/{total_informatica_tasks} ({coverage_percent:.1f}%)")
        print(f"   🎯 Production Ready: {'YES' if coverage_percent >= 70 else 'NEEDS MORE WORK'}")
        
        print("\n🚀 Next Steps:")
        print("   1. Test generated workflow applications")
        print("   2. Add Event Task implementations")
        print("   3. Enhance error handling and recovery")
        print("   4. Add workflow monitoring and metrics")
        
        # Cleanup
        os.unlink(main_xml_path)
        os.unlink(child_xml_path)
        
    except Exception as e:
        print(f"❌ Error during demonstration: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main() 