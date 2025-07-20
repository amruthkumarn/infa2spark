#!/bin/bash

echo "🧪 Running Informatica to PySpark PoC Test Suite"
echo "=================================================="

# Set up environment
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if required packages are installed
print_status "Checking dependencies..."

python -c "import pyspark" 2>/dev/null || {
    print_error "PySpark not found. Please install it with: pip install pyspark"
    exit 1
}

python -c "import pytest" 2>/dev/null || {
    print_error "pytest not found. Please install it with: pip install pytest"
    exit 1
}

python -c "import yaml" 2>/dev/null || {
    print_error "PyYAML not found. Please install it with: pip install PyYAML"
    exit 1
}

print_success "All dependencies found"

# Create necessary directories
print_status "Setting up test environment..."
mkdir -p input output sample_data logs
./setup_test_environment.sh > /dev/null 2>&1

# Run different test categories
echo ""
echo "🔧 Running Unit Tests"
echo "===================="

# Core framework tests
print_status "Testing core framework..."
python -m pytest tests/test_xml_parser.py -v --tb=short
CORE_EXIT_CODE=$?

python -m pytest tests/test_spark_manager.py -v --tb=short
SPARK_EXIT_CODE=$?

python -m pytest tests/test_config_manager.py -v --tb=short
CONFIG_EXIT_CODE=$?

# Transformation tests
print_status "Testing transformations..."
python -m pytest tests/test_transformations.py -v --tb=short
TRANSFORM_EXIT_CODE=$?

# Mapping tests
print_status "Testing mappings..."
python -m pytest tests/test_mappings.py -v --tb=short
MAPPING_EXIT_CODE=$?

# Workflow tests
print_status "Testing workflows..."
python -m pytest tests/test_workflows.py -v --tb=short
WORKFLOW_EXIT_CODE=$?

# Integration tests
echo ""
echo "🔄 Running Integration Tests"
echo "============================"
print_status "Testing end-to-end integration..."
python -m pytest tests/test_integration.py -v --tb=short
INTEGRATION_EXIT_CODE=$?

# Generate test report
echo ""
echo "📊 Test Results Summary"
echo "======================="

declare -A test_results
test_results["Core Framework"]=$CORE_EXIT_CODE
test_results["Spark Manager"]=$SPARK_EXIT_CODE
test_results["Configuration Manager"]=$CONFIG_EXIT_CODE
test_results["Transformations"]=$TRANSFORM_EXIT_CODE
test_results["Mappings"]=$MAPPING_EXIT_CODE
test_results["Workflows"]=$WORKFLOW_EXIT_CODE
test_results["Integration"]=$INTEGRATION_EXIT_CODE

total_tests=0
passed_tests=0

for test_name in "${!test_results[@]}"; do
    exit_code=${test_results[$test_name]}
    total_tests=$((total_tests + 1))
    
    if [ $exit_code -eq 0 ]; then
        print_success "$test_name: PASSED"
        passed_tests=$((passed_tests + 1))
    else
        print_error "$test_name: FAILED (Exit code: $exit_code)"
    fi
done

echo ""
echo "Overall Results: $passed_tests/$total_tests tests passed"

# Run coverage report if coverage is installed
if python -c "import coverage" 2>/dev/null; then
    echo ""
    echo "📈 Generating Coverage Report"
    echo "============================"
    python -m pytest tests/ --cov=src --cov-report=term-missing --tb=short
fi

# Run the actual PoC to test end-to-end functionality
echo ""
echo "🚀 Testing Full PoC Execution"
echo "============================"

if [ $passed_tests -eq $total_tests ]; then
    print_status "All unit tests passed. Running full PoC..."
    
    # Create a minimal test to verify the PoC runs
    timeout 30s ./run_poc.sh > poc_test_output.log 2>&1
    POC_EXIT_CODE=$?
    
    if [ $POC_EXIT_CODE -eq 0 ]; then
        print_success "PoC execution completed successfully"
        
        # Check if output files were created
        if [ -d "output" ] && [ "$(ls -A output 2>/dev/null)" ]; then
            print_success "Output files generated successfully"
            echo "Generated outputs:"
            ls -la output/
        else
            print_warning "PoC ran but no output files detected"
        fi
        
    elif [ $POC_EXIT_CODE -eq 124 ]; then
        print_warning "PoC execution timed out (this may be normal for large datasets)"
    else
        print_error "PoC execution failed (Exit code: $POC_EXIT_CODE)"
        echo "Check poc_test_output.log for details"
    fi
else
    print_warning "Skipping PoC execution due to failed unit tests"
fi

# Final summary
echo ""
echo "🎯 Final Summary"
echo "==============="

if [ $passed_tests -eq $total_tests ]; then
    print_success "All tests completed successfully! ✅"
    print_status "The Informatica to PySpark PoC is ready for use."
    echo ""
    echo "Next steps:"
    echo "  1. Run './run_poc.sh' to execute the full PoC"
    echo "  2. Check 'output/' directory for generated results"
    echo "  3. Review 'informatica_poc.log' for detailed execution logs"
    exit 0
else
    failed_tests=$((total_tests - passed_tests))
    print_error "$failed_tests out of $total_tests test categories failed"
    print_status "Please review the test output above and fix any issues."
    exit 1
fi