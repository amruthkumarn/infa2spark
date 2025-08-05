# 01_PROJECT_STRUCTURE.md

## 🏗️ Complete Project Structure Setup

Create the exact directory structure and initial files for the Informatica BDM to PySpark Converter Framework.

## 📁 Project Directory Structure

Create the following directory structure:

```bash
# Create main project directory
mkdir informatica_to_pyspark_framework
cd informatica_to_pyspark_framework

# Core source code structure
mkdir -p src/core/templates
mkdir -p src/transformations
mkdir -p src/mappings
mkdir -p src/workflows
mkdir -p src/data_sources
mkdir -p src/utils
mkdir -p src/config

# Configuration and data directories
mkdir -p config
mkdir -p input
mkdir -p output
mkdir -p logs

# Testing structure
mkdir -p tests/data
mkdir -p tests

# Documentation structure
mkdir -p docs/analysis
mkdir -p docs/demos
mkdir -p docs/implementation
mkdir -p docs/scripts
mkdir -p docs/summaries
mkdir -p docs/testing

# Generated applications directory (will contain complete standalone applications)
mkdir -p generated_spark_apps

# Examples and samples
mkdir -p examples

# Informatica XSD schemas directory (comprehensive XSD schema collection)
mkdir -p informatica_xsd_xml/sample_project

# Virtual environment (created by setup script)
# Note: informatica_poc_env/ will be created by setup_test_environment.sh
```

## 📄 Essential Root Files

### 1. Main README.md

```markdown
# Informatica BDM to PySpark Converter Framework

This production-ready framework converts Informatica BDM (Business Data Model) and PowerCenter XML projects into fully executable, professionally formatted PySpark applications with proper code formatting and industry-standard structure.

## 🚀 Quick Start

```bash
# 1. Setup environment
cd informatica_to_pyspark_framework
python -m venv informatica_poc_env
source informatica_poc_env/bin/activate  # On Windows: informatica_poc_env\Scripts\activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Generate Spark application
python src/main.py --generate-spark-app \
    --xml-file input/sample_project.xml \
    --app-name SampleApp \
    --output-dir generated_spark_apps

# 4. Run generated application
cd generated_spark_apps/SampleApp
./run.sh
```

## Overview

The framework converts Informatica XML projects into complete, deployable Spark applications that replicate ETL logic, including:

- **Production-Ready Code Generation**: Generates professionally formatted PySpark code using Black formatter
- **Complete Application Structure**: Full project structure with configuration, deployment, and documentation
- **Advanced Transformations**: Expression, Aggregator, Lookup, Joiner, Sequence, Sorter, Router, Union, and more
- **Field-Level Integration**: Complete TransformationFieldPort and ExpressionField support with data lineage
- **Multiple Project Support**: Works with various Informatica project formats (IMX, XML)
- **Parameterized Generation**: Command-line interface for flexible code generation

## Features Implemented

### 🏗️ XSD-Compliant Architecture (Enterprise-Grade)

Our framework is built on **XSD-compliant Python models** that directly mirror Informatica's official XML Schema Definition (XSD) files. This ensures 100% compatibility with Informatica metadata standards.

### 🔄 XSD-Compliant Transformations (Complete Implementation)

Our XSD framework supports **all major Informatica transformation types** with full schema compliance:
- **Source/Target**: DataFrame read/write operations
- **Expression**: withColumn, filter operations  
- **Lookup**: DataFrame join operations
- **Joiner**: Multi-source joins
- **Aggregator**: groupBy, agg operations
- **Sequence**: Row number generation
- **Sorter**: orderBy operations
- **Router**: Multi-condition filtering
- **Union**: DataFrame union operations

## Setup and Installation

### Prerequisites
- Python 3.8+
- Apache Spark 3.4+ (if using spark-submit)
- Java 8 or 11

### Installation Steps
1. Clone/Download the framework code
2. Install Python dependencies: `pip install -r requirements.txt`
3. Set up PySpark: `pip install pyspark[sql]==3.4.0`
4. Prepare input XML files in the `input/` directory

## Configuration

### Connections (`config/connections.yaml`)
```yaml
HDFS_CONN:
  type: HDFS
  host: namenode.company.com
  port: 8020
  local_path: sample_data/  # For PoC simulation
```

### Spark Settings (`config/spark_config.yaml`)
```yaml
spark:
  spark.master: "local[*]"
  spark.sql.adaptive.enabled: "true"
  spark.sql.warehouse.dir: "./spark-warehouse"
```

## Testing

Run the test suite:
```bash
pytest tests/ -v
```

## Documentation

- **Framework Running Guide**: `docs/FRAMEWORK_RUNNING_GUIDE.md`
- **XSD Architecture Guide**: `docs/XSD_ARCHITECTURE_GUIDE.md`
- **Generated Output Analysis**: `docs/GENERATED_OUTPUT_ANALYSIS.md`

## Support and Development

This framework demonstrates **enterprise-grade conversion** of Informatica BDM projects to PySpark using **XSD-compliant architecture**. The modular, XSD-based design allows for easy extension and customization based on specific requirements.
```

### 2. requirements.txt

```text
# PySpark and dependencies
pyspark>=3.4.0
py4j>=0.10.9.7

# Configuration and data handling
PyYAML>=6.0
pandas>=1.5.0

# Template processing
Jinja2>=3.1.0

# Testing
pytest>=7.0.0
pytest-spark>=0.6.0

# Development tools
black>=22.0.0
flake8>=5.0.0

# Optional: For enhanced logging
colorlog>=6.0.0
```

### 3. .gitignore

```text
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
pip-wheel-metadata/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# Virtual Environment
informatica_poc_env/
venv/
env/
ENV/

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# Logs
*.log
logs/

# Generated Applications
generated_spark_apps/*/

# Spark
spark-warehouse/
derby.log
metastore_db/

# Output files
output/

# Jupyter
.ipynb_checkpoints/
*.ipynb
```

### 4. run_poc.sh

```bash
#!/bin/bash
# Main execution script for Informatica to PySpark PoC

set -e

echo "🚀 Starting Informatica to PySpark Framework"

# Check if virtual environment exists
if [ ! -d "informatica_poc_env" ]; then
    echo "❌ Virtual environment not found. Please run setup_test_environment.sh first"
    exit 1
fi

# Activate virtual environment
source informatica_poc_env/bin/activate

# Set Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Check for sample XML files
if [ ! -f "input/sample_project.xml" ]; then
    echo "⚠️  No sample XML files found. Creating basic sample..."
    mkdir -p input
    # Create basic sample XML (will be replaced with proper sample in later step)
    cat > input/sample_project.xml << 'EOF'
<?xml version="1.0" encoding="UTF-8"?>
<Project name="SampleProject" version="1.0">
    <Folder name="SampleFolder">
        <Mapping name="SampleMapping">
            <Instance name="SRC_Customer" type="Source"/>
            <Instance name="EXP_Transform" type="Expression"/>
            <Instance name="TGT_Output" type="Target"/>
        </Mapping>
    </Folder>
</Project>
EOF
fi

# Run the main application
echo "🔄 Running framework..."
python src/main.py "$@"

echo "✅ Framework execution completed"
```

### 5. run_tests.sh

```bash
#!/bin/bash
# Test execution script

set -e

echo "🧪 Running Informatica to PySpark Framework Tests"

# Activate virtual environment
source informatica_poc_env/bin/activate

# Set Python path
export PYTHONPATH="${PYTHONPATH}:$(pwd)/src"

# Run tests with coverage
echo "Running test suite..."
pytest tests/ -v --tb=short

echo "✅ All tests completed"
```

### 6. setup_test_environment.sh

```bash
#!/bin/bash
# Environment setup script

set -e

echo "🔧 Setting up Informatica to PySpark Framework Environment"

# Create virtual environment
echo "Creating virtual environment..."
python3 -m venv informatica_poc_env

# Activate virtual environment  
source informatica_poc_env/bin/activate

# Upgrade pip
echo "Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "Installing dependencies..."
pip install -r requirements.txt

# Create necessary directories
echo "Creating directories..."
mkdir -p input output logs generated_spark_apps

# Make scripts executable
chmod +x run_poc.sh
chmod +x run_tests.sh

echo "✅ Environment setup completed"
echo ""
echo "To activate the environment, run:"
echo "source informatica_poc_env/bin/activate"
echo ""
echo "To run the framework, use:"
echo "./run_poc.sh --generate-spark-app --xml-file input/sample_project.xml --app-name SampleApp"
```

## 📦 Initial Source Structure

### Create Core __init__.py Files

```bash
# Create all necessary __init__.py files
touch src/__init__.py
touch src/core/__init__.py
touch src/transformations/__init__.py
touch src/mappings/__init__.py
touch src/workflows/__init__.py
touch src/data_sources/__init__.py
touch src/utils/__init__.py
touch src/config/__init__.py
touch tests/__init__.py
```

### Create Initial Core Module Stubs

Create these placeholder files in `src/core/`:

**`src/core/base_classes.py`**:
```python
"""
Base classes for Informatica metadata objects
Placeholder - will be implemented in Phase 2
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any

class BaseInformaticaObject(ABC):
    """Base class for all Informatica objects"""
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.properties: Dict[str, Any] = {}
```

**`src/core/xml_parser.py`**:
```python
"""
XML parser for Informatica projects
Placeholder - will be implemented in Phase 2
"""
import xml.etree.ElementTree as ET
import logging

class InformaticaXMLParser:
    """Parses Informatica XML projects"""
    def __init__(self):
        self.logger = logging.getLogger("XMLParser")
    
    def parse_project(self, xml_file_path: str):
        """Parse XML project file"""
        pass  # Will be implemented
```

**`src/main.py`**:
```python
"""
Main application entry point for Informatica to PySpark Framework
"""
import sys
import os
import logging
import argparse
from pathlib import Path

# Add src directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

def setup_logging():
    """Setup logging configuration"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('informatica_poc.log')
        ]
    )

def main():
    """Main application function"""
    setup_logging()
    logger = logging.getLogger("Main")
    
    # Set up argument parser
    parser = argparse.ArgumentParser(description='Informatica to PySpark Framework')
    parser.add_argument('--generate-spark-app', action='store_true', 
                       help='Generate standalone Spark application')
    parser.add_argument('--xml-file', type=str, 
                       help='Path to Informatica XML file')
    parser.add_argument('--app-name', type=str, 
                       help='Name for the generated Spark application')
    parser.add_argument('--output-dir', type=str, default='generated_spark_apps',
                       help='Output directory for generated application')
    
    args = parser.parse_args()
    
    logger.info("🚀 Informatica to PySpark Framework Started")
    logger.info("⚠️  Framework is under construction - core components will be implemented in phases")
    
    if args.generate_spark_app:
        logger.info("🔧 Spark application generation requested")
        logger.info("📋 This feature will be implemented in Phase 3")
    
    logger.info("✅ Framework initialization completed")

if __name__ == "__main__":
    main()
```

## 🎯 Verification Steps

After creating the structure, verify everything is set up correctly:

```bash
# 1. Check directory structure
find . -type d | head -20

# 2. Set up environment
chmod +x setup_test_environment.sh
./setup_test_environment.sh

# 3. Test basic execution
source informatica_poc_env/bin/activate
python src/main.py --help

# 4. Verify file structure
ls -la src/core/
ls -la tests/
ls -la docs/
```

## ✅ Expected Output

You should see:
- Complete directory structure created
- Virtual environment set up with dependencies
- All scripts executable
- Basic framework responds to commands
- Log file created: `informatica_poc.log`

## 🔗 Next Steps

After completing this structure setup, proceed to **`02_REQUIREMENTS_AND_DEPENDENCIES.md`** to implement the complete dependency management and configuration system.

The project structure is now ready for the core framework implementation!