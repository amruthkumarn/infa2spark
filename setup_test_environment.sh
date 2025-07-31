#!/bin/bash

echo "🔧 Setting up Informatica to PySpark Framework testing environment..."
echo "===================================================================="

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Create all necessary directories
print_info "Creating directory structure..."
mkdir -p input
mkdir -p output
mkdir -p sample_data
mkdir -p logs
mkdir -p tests/data
mkdir -p generated_spark_apps

print_success "✓ Directories created"

# Check if enterprise XML already exists (from our working example)
if [ -f "input/enterprise_complete_transformations.xml" ]; then
    print_success "✓ Enterprise transformation XML already available"
else
    print_info "Enterprise XML not found, checking for other existing XML files..."
    
    # Check for any existing XML files in informatica_xsd_xml directory (if available)
    SAMPLE_XML_LOCATIONS=(
        "informatica_xsd_xml/sample_project/sample_project.xml"
        "/Users/ninad/Downloads/informatica_xsd_xml/sample_project/sample_project.xml"
        "sample_data/sample_project.xml"
    )
    
    FOUND_SAMPLE=false
    for location in "${SAMPLE_XML_LOCATIONS[@]}"; do
        if [ -f "$location" ]; then
            cp "$location" input/sample_project.xml
            print_success "✓ Copied sample project XML from: $location"
            FOUND_SAMPLE=true
            break
        fi
    done
    
    if [ "$FOUND_SAMPLE" = false ]; then
        print_warning "⚠ No existing sample XML found, creating a comprehensive test version"
        # Create a comprehensive test XML file
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
        print_success "✓ Created comprehensive test sample project XML"
    fi
fi

# Create additional sample XML files for testing different scenarios
print_info "Creating additional test XML files..."

# Create a simple transformation showcase XML
cat > input/transformation_showcase.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<imx:IMX xmlns:imx="http://com.informatica.imx"
         xmlns:project="http://com.informatica.metadata.common.project/2"
         xmlns:folder="http://com.informatica.metadata.common.folder/2"
         xmlns:mapping="http://com.informatica.metadata.common.mapping/1"
         imx:version="2.1.1">
    
    <project:Project imx:id="PROJECT_001" name="Transformation_Showcase" version="1.0">
        <contents>
            <folder:Folder imx:id="FOLDER_001" name="Sample_Mappings">
                <contents>
                    <mapping:Mapping imx:id="MAPPING_001" name="m_Sample_Transformations">
                        <description>Sample mapping with multiple transformation types</description>
                        <components>
                            <source name="SRC_SALES" type="HDFS" format="PARQUET"/>
                            <transformation name="EXP_CLEAN_DATA" type="Expression"/>
                            <transformation name="AGG_SALES_SUMMARY" type="Aggregator"/>
                            <transformation name="LKP_CUSTOMER" type="Lookup"/>
                            <transformation name="JNR_SALES_CUSTOMER" type="Joiner"/>
                            <target name="TGT_SALES_MART" type="HIVE"/>
                        </components>
                    </mapping:Mapping>
                </contents>
            </folder:Folder>
        </contents>
    </project:Project>
</imx:IMX>
EOF

print_success "✓ Created transformation showcase XML"

# Set up Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Check Python and dependencies
print_info "Checking Python environment..."
python --version

print_info "Checking required packages..."
python -c "import pyspark; print(f'✓ PySpark version: {pyspark.__version__}')" 2>/dev/null || print_warning "⚠ PySpark not found - install with: pip install pyspark"
python -c "import yaml; print('✓ PyYAML available')" 2>/dev/null || print_warning "⚠ PyYAML not found - install with: pip install PyYAML"
python -c "import pytest; print('✓ pytest available')" 2>/dev/null || print_warning "⚠ pytest not found - install with: pip install pytest"
python -c "import jinja2; print('✓ Jinja2 available')" 2>/dev/null || print_warning "⚠ Jinja2 not found - install with: pip install jinja2"

print_info "Virtual environment check..."
if [[ "$VIRTUAL_ENV" != "" ]]; then
    print_success "✓ Virtual environment active: $(basename $VIRTUAL_ENV)"
else
    print_warning "⚠ No virtual environment detected"
    print_info "Consider running: source informatica_poc_env/bin/activate"
fi

echo ""
print_success "🎯 Test environment setup completed!"
echo ""
print_info "📝 Available XML files for testing:"
if [ -d "input" ] && [ "$(ls -A input/*.xml 2>/dev/null)" ]; then
    ls -la input/*.xml
else
    print_warning "No XML files found in input/ directory"
fi

echo ""
print_info "🚀 Next steps:"
echo "   1. Run tests:           python -m pytest tests/ -v"
echo "   2. Run full framework:  ./run_poc.sh"
echo "   3. Generate custom app: python src/main.py --generate-spark-app --xml-file input/your_file.xml --app-name YourApp"

echo ""
print_info "📚 For detailed instructions, see: docs/FRAMEWORK_RUNNING_GUIDE.md"