#!/bin/bash

# Test Execution Script for Informatica Framework
# Runs both framework tests and generated code tests

set -e  # Exit on any error

echo "🧪 Informatica Framework Testing Suite"
echo "======================================"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Check if virtual environment is activated
if [[ -z "${VIRTUAL_ENV}" ]]; then
    echo -e "${YELLOW}Warning: No virtual environment detected. Activating...${NC}"
    if [[ -f "../informatica_poc_env/bin/activate" ]]; then
        source ../informatica_poc_env/bin/activate
        echo -e "${GREEN}Virtual environment activated${NC}"
    else
        echo -e "${RED}Error: Virtual environment not found${NC}"
        exit 1
    fi
fi

# Install test requirements
echo -e "${BLUE}Installing test dependencies...${NC}"
pip install -r test_requirements.txt

# Create test results directory
mkdir -p test_results

echo ""
echo "🏗️ PHASE 1: Framework Unit Tests"
echo "================================="

# Run framework unit tests
pytest tests/framework/unit/ \
    -v \
    --tb=short \
    --junitxml=test_results/framework_unit_results.xml \
    --cov=src/informatica_spark \
    --cov-report=html:test_results/framework_coverage \
    --markers unit

echo ""
echo "🔗 PHASE 2: Framework Integration Tests"  
echo "======================================="

# Run framework integration tests
pytest tests/framework/integration/ \
    -v \
    --tb=short \
    --junitxml=test_results/framework_integration_results.xml \
    --markers integration

echo ""
echo "🚀 PHASE 3: Generated Code Tests"
echo "================================"

# Run generated code unit tests
pytest tests/generated_code/unit/ \
    -v \
    --tb=short \
    --junitxml=test_results/generated_code_results.xml \
    --markers unit

echo ""
echo "🌐 PHASE 4: End-to-End Pipeline Tests"
echo "====================================="

# Run end-to-end tests
pytest tests/e2e/ \
    -v \
    --tb=short \
    --junitxml=test_results/e2e_results.xml \
    --markers e2e \
    -s  # Show print statements for E2E tests

echo ""
echo "⚡ PHASE 5: Performance Tests (Optional)"
echo "======================================="

read -p "Run performance tests? This may take several minutes (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    pytest tests/ \
        -v \
        --tb=short \
        --junitxml=test_results/performance_results.xml \
        --markers performance \
        -s
else
    echo -e "${YELLOW}Skipping performance tests${NC}"
fi

echo ""
echo "📊 Test Results Summary"
echo "======================"

# Count test results
UNIT_TESTS=$(grep -c "test" test_results/framework_unit_results.xml 2>/dev/null || echo "0")
INTEGRATION_TESTS=$(grep -c "test" test_results/framework_integration_results.xml 2>/dev/null || echo "0")
GENERATED_TESTS=$(grep -c "test" test_results/generated_code_results.xml 2>/dev/null || echo "0")
E2E_TESTS=$(grep -c "test" test_results/e2e_results.xml 2>/dev/null || echo "0")

echo "Framework Unit Tests: $UNIT_TESTS"
echo "Framework Integration Tests: $INTEGRATION_TESTS"
echo "Generated Code Tests: $GENERATED_TESTS"
echo "End-to-End Tests: $E2E_TESTS"

# Check for failures
if grep -q "failures=" test_results/*.xml 2>/dev/null; then
    echo -e "${RED}❌ Some tests failed. Check test_results/ for details.${NC}"
    exit 1
else
    echo -e "${GREEN}✅ All tests passed!${NC}"
fi

echo ""
echo "📁 Test artifacts saved to test_results/"
echo "   - XML reports: test_results/*_results.xml"
echo "   - Coverage report: test_results/framework_coverage/index.html"

echo ""
echo -e "${GREEN}🎉 Testing complete!${NC}"