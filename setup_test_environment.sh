#!/bin/bash

echo "Setting up Informatica to PySpark PoC testing environment..."

# Create all necessary directories
mkdir -p input
mkdir -p output
mkdir -p sample_data
mkdir -p logs
mkdir -p tests/data

# Copy the sample project XML if available
if [ -f "/Users/ninad/Downloads/informatica_xsd_xml/sample_project/sample_project.xml" ]; then
    cp "/Users/ninad/Downloads/informatica_xsd_xml/sample_project/sample_project.xml" input/
    echo "✓ Copied sample project XML"
else
    echo "⚠ Sample project XML not found, will create a test version"
    # Create a test XML file
    cat > input/sample_project.xml << 'EOF'
<project name="MyBDMProject" version="1.0" xmlns="http://www.informatica.com/BDM/Project/10.2">
  <description>Sample Informatica BDM Project</description>
  
  <folders>
    <folder name="Mappings">
      <mapping name="Sales_Staging">
        <description>Extracts and transforms sales data</description>
        <components>
          <source name="SALES_SOURCE" type="HDFS" format="PARQUET"/>
          <transformation name="FILTER_SALES" type="Expression"/>
          <transformation name="AGG_SALES" type="Aggregator"/>
          <target name="STG_SALES" type="HIVE"/>
        </components>
      </mapping>
      
      <mapping name="Customer_Dim_Load">
        <description>Customer dimension SCD Type 2 load</description>
        <components>
          <source name="CUSTOMER_SOURCE" type="DB2"/>
          <transformation name="LOOKUP_CUSTOMER" type="Lookup"/>
          <transformation name="SCD_LOGIC" type="Java"/>
          <target name="DIM_CUSTOMER" type="HIVE"/>
        </components>
      </mapping>
    </folder>
    
    <folder name="Workflows">
      <workflow name="Daily_ETL_Process">
        <description>Main daily ETL workflow</description>
        <tasks>
          <task name="T1_Sales_Staging" type="Mapping" mapping="Sales_Staging"/>
          <task name="T2_Customer_Dim" type="Mapping" mapping="Customer_Dim_Load"/>
          <task name="T5_Send_Notification" type="Email">
            <properties>
              <property name="Recipient" value="etl-team@company.com"/>
              <property name="Subject" value="ETL Process Completed"/>
            </properties>
          </task>
        </tasks>
        <links>
          <link from="T1_Sales_Staging" to="T2_Customer_Dim" condition="SUCCESS"/>
          <link from="T2_Customer_Dim" to="T5_Send_Notification" condition="SUCCESS"/>
        </links>
      </workflow>
    </folder>
  </folders>
  
  <connections>
    <connection name="HDFS_CONN" type="HDFS" host="namenode.company.com" port="8020"/>
    <connection name="HIVE_CONN" type="HIVE" host="hiveserver.company.com" port="10000"/>
    <connection name="DB2_CONN" type="DB2" host="db2.company.com" port="50000" database="SRC_DB"/>
  </connections>
  
  <parameters>
    <parameter name="PROJECT_VERSION" value="1.0"/>
    <parameter name="ERROR_DIR" value="/bdm/errors"/>
  </parameters>
</project>
EOF
    echo "✓ Created test sample project XML"
fi

# Set up Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Check Python and dependencies
echo "Checking Python environment..."
python --version

echo "Checking required packages..."
python -c "import pyspark; print(f'PySpark version: {pyspark.__version__}')" 2>/dev/null || echo "⚠ PySpark not found"
python -c "import yaml; print('✓ PyYAML available')" 2>/dev/null || echo "⚠ PyYAML not found"
python -c "import pytest; print('✓ pytest available')" 2>/dev/null || echo "⚠ pytest not found"

echo ""
echo "Test environment setup completed!"
echo "Run: python -m pytest tests/ -v  to execute tests"
echo "Run: ./run_poc.sh  to execute the full PoC"