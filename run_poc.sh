#!/bin/bash

# Run the Informatica to PySpark PoC - Modern Application Generation Mode
echo "🚀 Starting Informatica to PySpark Framework..."
echo "================================================"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
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

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Set up environment
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Create necessary directories
print_info "Setting up directories..."
mkdir -p generated_spark_apps
mkdir -p logs

# Setup test environment if needed
if [ ! -f "input/enterprise_complete_transformations.xml" ]; then
    print_info "Setting up test environment..."
    ./setup_test_environment.sh
fi

# Check for available XML files
print_info "Available XML files for processing:"
if [ -d "input" ] && [ "$(ls -A input/*.xml 2>/dev/null)" ]; then
    ls -la input/*.xml
    
    # Use the first available XML file or default to enterprise example
    if [ -f "input/enterprise_complete_transformations.xml" ]; then
        XML_FILE="input/enterprise_complete_transformations.xml"
        APP_NAME="EnterpriseTransformationsDemo"
        print_info "Using enterprise example: $XML_FILE"
    elif [ -f "input/sample_project.xml" ]; then
        XML_FILE="input/sample_project.xml"
        APP_NAME="SampleProjectDemo"
        print_info "Using sample project: $XML_FILE"
    else
        # Use the first XML file found
        XML_FILE=$(ls input/*.xml | head -n 1)
        APP_NAME="GeneratedDemo"
        print_info "Using first available XML: $XML_FILE"
    fi
else
    print_error "No XML files found in input/ directory"
    print_info "Please run: ./setup_test_environment.sh to create sample data"
    exit 1
fi

print_info "Generating Spark application from: $XML_FILE"
print_info "Application name: $APP_NAME"
print_info "Output directory: generated_spark_apps/"

echo ""
echo "🔄 Running Framework in Application Generation Mode"
echo "=================================================="

# Run the modern framework in application generation mode
python src/main.py --generate-spark-app \
    --xml-file "$XML_FILE" \
    --app-name "$APP_NAME" \
    --output-dir generated_spark_apps

GENERATION_EXIT_CODE=$?

echo ""
if [ $GENERATION_EXIT_CODE -eq 0 ]; then
    print_success "✅ Spark application generated successfully!"
    
    # Check if application was created
    if [ -d "generated_spark_apps/$APP_NAME" ]; then
        print_info "Generated application structure:"
        ls -la "generated_spark_apps/$APP_NAME/"
        
        echo ""
        print_info "📖 To run the generated application:"
        echo "    cd generated_spark_apps/$APP_NAME"
        echo "    ./run.sh"
        
        echo ""
        print_info "📊 Generated application contains:"
        echo "    - Complete PySpark implementation"
        echo "    - Configuration files"
        echo "    - Deployment scripts (Docker, run.sh)"
        echo "    - Documentation (README.md)"
        
        # Optional: Offer to run the generated application
        echo ""
        read -p "🤔 Would you like to run the generated application now? (y/n): " -n 1 -r
        echo
        if [[ $REPLY =~ ^[Yy]$ ]]; then
            print_info "Running generated application..."
            cd "generated_spark_apps/$APP_NAME"
            chmod +x run.sh
            ./run.sh
            cd - > /dev/null
        else
            print_info "You can run it later with the commands shown above."
        fi
        
    else
        print_warning "Application directory not found, but generation reported success"
    fi
    
    print_success "🎯 PoC completed successfully!"
    print_info "Check logs/informatica_poc.log for detailed execution logs"
    
else
    print_error "❌ Application generation failed (Exit code: $GENERATION_EXIT_CODE)"
    print_info "Check logs/informatica_poc.log for error details"
    exit 1
fi