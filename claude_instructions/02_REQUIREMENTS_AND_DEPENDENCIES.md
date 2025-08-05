# 02_REQUIREMENTS_AND_DEPENDENCIES.md

## 📦 Complete Dependencies and Environment Setup

Set up all required dependencies, development tools, and environment configuration for the enterprise-grade framework.

## 🔧 Enhanced requirements.txt

Replace the basic requirements.txt with the complete enterprise dependency list:

```text
# Core PySpark and Big Data
pyspark>=3.4.0,<4.0.0
py4j>=0.10.9.7

# Configuration Management
PyYAML>=6.0.1
python-dotenv>=1.0.0

# Data Processing
pandas>=1.5.0,<3.0.0
numpy>=1.21.0

# Template Engine and Code Generation
Jinja2>=3.1.2
MarkupSafe>=2.1.0

# XML Processing and Validation
lxml>=4.9.0
xmlschema>=2.3.0

# Logging and Monitoring
colorlog>=6.7.0
structlog>=23.1.0

# Testing Framework
pytest>=7.4.0
pytest-cov>=4.1.0
pytest-mock>=3.11.0
pytest-xdist>=3.3.0
pytest-timeout>=2.1.0

# Code Quality and Formatting  
black>=23.7.0
isort>=5.12.0
flake8>=6.0.0
mypy>=1.5.0
pre-commit>=3.3.0

# Development Tools
ipython>=8.12.0
jupyter>=1.0.0

# Performance and Profiling
memory-profiler>=0.61.0
line-profiler>=4.1.0

# Documentation Generation
sphinx>=7.1.0
sphinx-rtd-theme>=1.3.0
myst-parser>=2.0.0

# Enterprise Features
requests>=2.31.0
urllib3>=2.0.0
cryptography>=41.0.0

# Optional: Database Connectivity (if needed)
# psycopg2-binary>=2.9.0  # PostgreSQL
# pymongo>=4.4.0          # MongoDB
# sqlalchemy>=2.0.0       # SQL ORM

# Optional: Cloud Storage (if needed)
# boto3>=1.28.0           # AWS S3
# azure-storage-blob>=12.17.0  # Azure Blob
# google-cloud-storage>=2.10.0  # Google Cloud
```

## 🐳 Docker Configuration

Create comprehensive Docker support for the framework:

### Dockerfile

```dockerfile
# Multi-stage build for production-ready container
FROM python:3.11-slim as base

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app/src

# Install system dependencies
RUN apt-get update && apt-get install -y \
    openjdk-11-jre-headless \
    curl \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

WORKDIR /app

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Development stage
FROM base as development

# Install development dependencies
COPY requirements-dev.txt .
RUN pip install --no-cache-dir -r requirements-dev.txt

# Copy source code
COPY . .

# Make scripts executable
RUN chmod +x run_poc.sh run_tests.sh setup_test_environment.sh

# Production stage
FROM base as production

# Copy only necessary files
COPY src/ src/
COPY config/ config/
COPY input/ input/
COPY run_poc.sh .

RUN chmod +x run_poc.sh

# Create non-root user
RUN useradd --create-home --shell /bin/bash app
USER app

# Default command
CMD ["python", "src/main.py", "--help"]
```

### requirements-dev.txt

```text
# Development-only dependencies
pytest-html>=3.2.0
pytest-benchmark>=4.0.0
coverage>=7.2.0
bandit>=1.7.5
safety>=2.3.0
pipdeptree>=2.9.0

# IDE Support
python-lsp-server>=1.7.0
pylsp-mypy>=0.6.0
rope>=1.9.0

# Debugging
pdb++>=0.10.3
icecream>=2.1.0

# Jupyter Extensions
jupyterlab>=4.0.0
nbformat>=5.9.0
```

### docker-compose.yml

```yaml
version: '3.8'

services:
  informatica-framework:
    build:
      context: .
      target: development
    container_name: informatica_framework_dev
    volumes:
      - .:/app
      - informatica_logs:/app/logs
      - informatica_output:/app/output
      - informatica_generated:/app/generated_spark_apps
    environment:
      - PYTHONPATH=/app/src
      - SPARK_MASTER=local[*]
    ports:
      - "8888:8888"  # Jupyter
      - "4040:4040"  # Spark UI
    command: tail -f /dev/null

  # Production service
  informatica-framework-prod:
    build:
      context: .
      target: production
    container_name: informatica_framework_prod
    volumes:
      - informatica_logs_prod:/app/logs
      - informatica_output_prod:/app/output
      - informatica_generated_prod:/app/generated_spark_apps
    environment:
      - PYTHONPATH=/app/src
      - SPARK_MASTER=local[*]
    profiles:
      - production

volumes:
  informatica_logs:
  informatica_output:
  informatica_generated:
  informatica_logs_prod:
  informatica_output_prod:
  informatica_generated_prod:
```

## 🔧 Development Environment Configuration

### .env Template

Create `.env.template` for environment configuration:

```bash
# Environment Configuration Template
# Copy to .env and customize for your environment

# Application Settings
APP_NAME=informatica_to_pyspark_framework
APP_VERSION=1.0.0
ENVIRONMENT=development

# Logging Configuration
LOG_LEVEL=INFO
LOG_FORMAT=detailed

# Spark Configuration
SPARK_MASTER=local[*]
SPARK_EXECUTOR_MEMORY=2g
SPARK_DRIVER_MEMORY=1g
SPARK_SQL_ADAPTIVE_ENABLED=true

# Framework Settings
ENABLE_PROFILING=false
ENABLE_MONITORING=false
MAX_MEMORY_USAGE_MB=4096

# File Paths
INPUT_DIR=input
OUTPUT_DIR=output
GENERATED_APPS_DIR=generated_spark_apps
LOGS_DIR=logs

# Testing Settings
TEST_PARALLEL_WORKERS=auto
TEST_TIMEOUT=300

# Database Settings (if needed)
# DB_HOST=localhost
# DB_PORT=5432
# DB_NAME=informatica_metadata
# DB_USER=informatica_user
# DB_PASSWORD=your_password

# Cloud Storage (if needed)
# AWS_ACCESS_KEY_ID=your_key
# AWS_SECRET_ACCESS_KEY=your_secret
# AWS_REGION=us-east-1
# S3_BUCKET=informatica-projects
```

### Pre-commit Configuration

`.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/pre-commit/pre-commit-hooks
    rev: v4.4.0
    hooks:
      - id: trailing-whitespace
      - id: end-of-file-fixer
      - id: check-yaml
      - id: check-added-large-files
      - id: check-json
      - id: check-toml
      - id: check-xml
      - id: debug-statements
      - id: check-docstring-first
      - id: check-merge-conflict

  - repo: https://github.com/psf/black
    rev: 23.7.0
    hooks:
      - id: black
        language_version: python3
        args: [--line-length=88]

  - repo: https://github.com/pycqa/isort
    rev: 5.12.0
    hooks:
      - id: isort
        args: [--profile=black]

  - repo: https://github.com/pycqa/flake8
    rev: 6.0.0
    hooks:
      - id: flake8
        args: [--max-line-length=88, --extend-ignore=E203,W503]

  - repo: https://github.com/pre-commit/mirrors-mypy
    rev: v1.5.0
    hooks:
      - id: mypy
        additional_dependencies: [types-PyYAML, types-requests]
        args: [--ignore-missing-imports]

  - repo: https://github.com/PyCQA/bandit
    rev: 1.7.5
    hooks:
      - id: bandit
        args: [-r, src/, -f, json, -o, bandit-report.json]
        always_run: false
```

### Black Configuration

`pyproject.toml`:

```toml
[tool.black]
line-length = 88
target-version = ['py38', 'py39', 'py310', 'py311']
include = '\.pyi?$'
extend-exclude = '''
/(
  # directories
  \.eggs
  | \.git
  | \.hg
  | \.mypy_cache
  | \.tox
  | \.venv
  | informatica_poc_env
  | generated_spark_apps
)/
'''

[tool.isort]
profile = "black"
multi_line_output = 3
line_length = 88
known_first_party = ["src"]

[tool.mypy]
python_version = "3.8"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = true
ignore_missing_imports = true

[tool.pytest.ini_options]
testpaths = ["tests"]
python_files = ["test_*.py"]
python_classes = ["Test*"]
python_functions = ["test_*"]
addopts = "-v --tb=short --strict-markers"
markers = [
    "integration: marks tests as integration tests",
    "unit: marks tests as unit tests",
    "slow: marks tests as slow running",
]
timeout = 300

[tool.coverage.run]
source = ["src"]
omit = [
    "*/tests/*",
    "*/test_*.py",
    "informatica_poc_env/*",
    "generated_spark_apps/*"
]

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "raise AssertionError",
    "raise NotImplementedError",
]
```

## 🚀 Enhanced Setup Scripts

### Enhanced setup_test_environment.sh

```bash
#!/bin/bash
# Comprehensive environment setup script

set -e

PYTHON_VERSION="3.11"
VENV_NAME="informatica_poc_env"

echo "🔧 Setting up Informatica to PySpark Framework Environment"
echo "========================================================"

# Check Python version
python_check() {
    if command -v python3 &> /dev/null; then
        PYTHON_CMD="python3"
    elif command -v python &> /dev/null; then
        PYTHON_CMD="python"
    else
        echo "❌ Python not found. Please install Python 3.8+ first."
        exit 1
    fi

    PYTHON_VER=$($PYTHON_CMD --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
    echo "🐍 Found Python $PYTHON_VER"
    
    # Check if version is sufficient
    if [[ $(echo "$PYTHON_VER 3.8" | awk '{print ($1 >= $2)}') -eq 0 ]]; then
        echo "❌ Python 3.8+ required. Found $PYTHON_VER"
        exit 1
    fi
}

# Create virtual environment
create_venv() {
    echo "📦 Creating virtual environment: $VENV_NAME"
    if [ -d "$VENV_NAME" ]; then
        echo "⚠️  Virtual environment already exists. Removing..."
        rm -rf "$VENV_NAME"
    fi
    
    $PYTHON_CMD -m venv "$VENV_NAME"
    echo "✅ Virtual environment created"
}

# Activate and setup environment
setup_environment() {
    echo "🔄 Setting up Python environment..."
    
    # Activate virtual environment
    source "$VENV_NAME/bin/activate"
    
    # Upgrade pip and setuptools
    echo "📦 Upgrading pip and build tools..."
    pip install --upgrade pip setuptools wheel
    
    # Install main dependencies
    echo "📦 Installing main dependencies..."
    pip install -r requirements.txt
    
    # Install development dependencies if they exist
    if [ -f "requirements-dev.txt" ]; then
        echo "📦 Installing development dependencies..."
        pip install -r requirements-dev.txt
    fi
    
    # Install pre-commit hooks
    if command -v pre-commit &> /dev/null; then
        echo "🔧 Installing pre-commit hooks..."
        pre-commit install
    fi
}

# Create necessary directories and files
setup_directories() {
    echo "📁 Creating project directories..."
    
    # Create all necessary directories
    mkdir -p input output logs generated_spark_apps
    mkdir -p config/templates
    mkdir -p tests/data
    mkdir -p docs/{analysis,demos,implementation,scripts,summaries,testing}
    mkdir -p informatica_xsd_xml/sample_project
    
    # Create .env file if it doesn't exist
    if [ ! -f ".env" ]; then
        if [ -f ".env.template" ]; then
            echo "📄 Creating .env from template..."
            cp .env.template .env
        else
            echo "📄 Creating basic .env file..."
            cat > .env << 'EOF'
APP_NAME=informatica_to_pyspark_framework
ENVIRONMENT=development
LOG_LEVEL=INFO
SPARK_MASTER=local[*]
EOF
        fi
    fi
    
    # Make scripts executable
    chmod +x run_poc.sh run_tests.sh
    
    echo "✅ Directories and files created"
}

# Verify installation
verify_installation() {
    echo "🔍 Verifying installation..."
    
    source "$VENV_NAME/bin/activate"
    
    # Check critical packages
    python -c "import pyspark; print(f'✅ PySpark {pyspark.__version__} installed')"
    python -c "import yaml; print('✅ PyYAML installed')"
    python -c "import jinja2; print('✅ Jinja2 installed')"
    python -c "import pytest; print('✅ pytest installed')"
    python -c "import black; print('✅ Black installed')"
    
    echo "🔍 Testing framework startup..."
    python src/main.py --help > /dev/null && echo "✅ Framework startup test passed"
}

# Main execution
main() {
    python_check
    create_venv
    setup_environment
    setup_directories
    verify_installation
    
    echo ""
    echo "🎉 Environment setup completed successfully!"
    echo "========================================"
    echo ""
    echo "🚀 To get started:"
    echo "1. Activate environment: source $VENV_NAME/bin/activate"
    echo "2. Run framework: python src/main.py --help"
    echo "3. Run tests: ./run_tests.sh"
    echo "4. Generate app: ./run_poc.sh --generate-spark-app --xml-file input/sample.xml --app-name TestApp"
    echo ""
    echo "📚 Next steps:"
    echo "- Review and customize .env file"
    echo "- Add your Informatica XML files to input/ directory"
    echo "- Check docs/ for detailed guides"
}

# Run main function
main "$@"
```

### Enhanced Makefile

```makefile
# Informatica to PySpark Framework Makefile

.PHONY: help setup install install-dev test lint format clean docker-build docker-run docs

# Default target
help:
	@echo "Informatica to PySpark Framework - Available Commands"
	@echo "===================================================="
	@echo "setup          - Set up development environment"
	@echo "install        - Install production dependencies"
	@echo "install-dev    - Install development dependencies"
	@echo "test           - Run all tests"
	@echo "test-unit      - Run unit tests only"
	@echo "test-integration - Run integration tests only"
	@echo "lint           - Run code linting"
	@echo "format         - Format code with Black and isort"
	@echo "type-check     - Run type checking with mypy"
	@echo "security       - Run security checks"
	@echo "clean          - Clean generated files"
	@echo "docker-build   - Build Docker image"
	@echo "docker-run     - Run in Docker container"
	@echo "docs           - Generate documentation"
	@echo "generate-app   - Generate sample Spark application"

# Environment setup
setup:
	@echo "🔧 Setting up development environment..."
	@chmod +x setup_test_environment.sh
	@./setup_test_environment.sh

# Installation targets
install:
	@echo "📦 Installing production dependencies..."
	@pip install -r requirements.txt

install-dev: install
	@echo "📦 Installing development dependencies..."
	@pip install -r requirements-dev.txt
	@pre-commit install

# Testing targets
test:
	@echo "🧪 Running all tests..."
	@pytest tests/ -v --cov=src --cov-report=html --cov-report=term

test-unit:
	@echo "🧪 Running unit tests..."
	@pytest tests/ -v -m "not integration"

test-integration:
	@echo "🧪 Running integration tests..."
	@pytest tests/ -v -m integration

# Code quality targets
lint:
	@echo "🔍 Running linting..."
	@flake8 src/ tests/
	@mypy src/

format:
	@echo "🎨 Formatting code..."
	@black src/ tests/
	@isort src/ tests/

type-check:
	@echo "🔍 Running type checks..."
	@mypy src/ --show-error-codes

security:
	@echo "🔒 Running security checks..."
	@bandit -r src/ -f json -o bandit-report.json || true
	@safety check

# Cleanup
clean:
	@echo "🧹 Cleaning generated files..."
	@find . -type f -name "*.pyc" -delete
	@find . -type d -name "__pycache__" -delete
	@rm -rf .pytest_cache/
	@rm -rf htmlcov/
	@rm -rf .coverage
	@rm -rf build/
	@rm -rf dist/
	@rm -rf *.egg-info/
	@rm -f bandit-report.json

# Docker targets
docker-build:
	@echo "🐳 Building Docker image..."
	@docker build -t informatica-framework:latest .

docker-run:
	@echo "🐳 Running Docker container..."
	@docker-compose up -d

# Documentation
docs:
	@echo "📚 Generating documentation..."
	@sphinx-build -b html docs/ docs/_build/html

# Application generation
generate-app:
	@echo "🚀 Generating sample Spark application..."
	@python src/main.py --generate-spark-app \
		--xml-file input/sample_project.xml \
		--app-name SampleApp \
		--output-dir generated_spark_apps

# Development workflow
dev: install-dev format lint test
	@echo "✅ Development workflow completed"

# CI/CD workflow
ci: install lint type-check security test
	@echo "✅ CI/CD pipeline completed"
```

## ✅ Verification Commands

After setting up the dependencies, run these commands to verify everything works:

```bash
# 1. Set up the environment
make setup
# or
./setup_test_environment.sh

# 2. Activate environment
source informatica_poc_env/bin/activate

# 3. Verify all packages
python -c "import pyspark, yaml, jinja2, pytest, black; print('✅ All packages imported successfully')"

# 4. Check development tools
make lint
make format
make test

# 5. Test Docker setup
make docker-build

# 6. Generate documentation
make docs
```

## 🔗 Next Steps

After completing the dependencies setup:

1. **Verify all tools work**: Run the verification commands above
2. **Customize configuration**: Edit `.env` file for your environment
3. **Proceed to Phase 2**: Move to **`03_CONFIGURATION_FILES.md`**

The development environment is now fully configured with enterprise-grade tooling, dependencies, and automation scripts!