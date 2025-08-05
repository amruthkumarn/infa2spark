# 10_TEMPLATES_AND_FORMATTING.md

## 🎨 Professional Template System Implementation

Implement the enterprise-grade template system for generating professional, well-formatted PySpark code with proper styling, documentation, and deployment files.

## 📁 Template System Structure

**ASSUMPTION**: The `/informatica_xsd_xml/` directory with complete XSD schemas is provided and available.

Create the template system architecture:

```bash
templates/
├── spark/                     # PySpark application templates
│   ├── main_app.jinja2
│   ├── transformation.jinja2
│   ├── data_reader.jinja2
│   └── data_writer.jinja2
├── deployment/               # Deployment templates
│   ├── docker_compose.jinja2
│   ├── kubernetes.jinja2
│   └── run_script.jinja2
├── documentation/            # Documentation templates
│   ├── readme.jinja2
│   └── api_docs.jinja2
└── config/                   # Configuration templates
    ├── logging.jinja2
    └── spark_config.jinja2
```

## 🔧 Core Template System Components

### 1. Template Manager Implementation

Create `src/templates/template_manager.py`:

**Key Features to Implement:**
- Jinja2 environment setup with custom filters
- Template loading and caching mechanism
- Context variable management
- Professional code formatting integration (Black)
- Template inheritance support
- Custom template functions for Spark code generation

**Essential Methods:**
- `load_template(template_name)` - Load and cache templates
- `render_template(template_name, context)` - Render with context
- `register_custom_filters()` - Add Spark-specific filters
- `format_generated_code()` - Apply Black formatting
- `validate_template_syntax()` - Template validation

### 2. Code Formatting Engine

Create `src/templates/code_formatter.py`:

**Integration Requirements:**
- Black code formatter integration
- Import statement optimization with isort
- PEP 8 compliance enforcement
- Docstring formatting (Google/NumPy style)
- Line length and complexity management

**Key Functionality:**
- Format generated PySpark applications
- Optimize import statements
- Add professional docstrings
- Ensure consistent indentation
- Remove unused imports

### 3. Template Context Builder

Create `src/templates/context_builder.py`:

**Context Management:**
- Mapping metadata extraction
- Session configuration preparation
- Connection parameter resolution
- Transformation logic compilation
- Variable and parameter substitution

## 📋 Template Development Guidelines

### Spark Application Templates

**Main Application Template (`spark/main_app.jinja2`):**
- Application entry point structure
- Spark session initialization
- Configuration loading
- Error handling framework
- Logging setup
- Clean shutdown procedures

**Transformation Templates (`spark/transformation.jinja2`):**
- Transformation function templates
- DataFrame operations
- SQL query generation
- Data validation logic
- Error handling patterns

**Data I/O Templates:**
- Reader templates for different sources (HDFS, Hive, databases)
- Writer templates with various output formats
- Connection parameter handling
- Schema validation
- Performance optimization hints

### Deployment Templates

**Docker Templates:**
- Multi-stage Dockerfile generation
- Docker Compose configurations
- Environment variable management
- Volume mounting specifications

**Kubernetes Templates:**
- Job and Deployment manifests
- ConfigMap and Secret management
- Resource allocation specifications
- Monitoring integration

**Run Script Templates:**
- Shell script generation for execution
- Parameter passing mechanisms
- Environment setup
- Error handling and logging

## 🎯 Template Design Patterns

### 1. Modular Template Structure
- Base templates for common patterns
- Specialized templates for specific transformations
- Template inheritance for code reuse
- Macro definitions for repeated code blocks

### 2. Configuration-Driven Generation
- Template selection based on mapping complexity
- Dynamic content based on transformation types
- Environment-specific template variations
- Parameter-driven customization

### 3. Professional Code Standards
- Consistent naming conventions
- Comprehensive error handling
- Performance optimization patterns
- Security best practices
- Documentation standards

## 🔍 Template Quality Assurance

### Template Validation System
Create validation mechanisms to ensure:
- Template syntax correctness
- Generated code compilation
- Black formatting compliance
- Import statement validity
- Docstring completeness

### Testing Framework
Implement comprehensive testing for:
- Template rendering accuracy
- Context variable handling
- Code formatting consistency
- Generated application functionality
- Edge case handling

## 📝 Custom Template Filters

Implement Spark-specific Jinja2 filters:

**Data Type Filters:**
- `|spark_type` - Convert Informatica types to Spark types
- `|sql_type` - Convert to SQL data types
- `|python_type` - Convert to Python types

**Code Generation Filters:**
- `|snake_case` - Convert to snake_case naming
- `|camel_case` - Convert to camelCase naming
- `|spark_column` - Generate Spark column references
- `|sql_escape` - Escape SQL identifiers

**Formatting Filters:**
- `|indent` - Proper code indentation
- `|comment_block` - Generate comment blocks
- `|docstring` - Generate docstrings

## 🚀 Implementation Strategy

### Phase 1: Core Template Engine
1. Set up Jinja2 environment with custom configuration
2. Implement template loading and caching
3. Create basic template validation
4. Integrate Black formatter

### Phase 2: Spark Templates
1. Develop main application template
2. Create transformation templates
3. Build data I/O templates
4. Implement error handling templates

### Phase 3: Deployment Integration
1. Create Docker templates
2. Develop Kubernetes manifests
3. Build run script templates
4. Implement configuration templates

### Phase 4: Advanced Features
1. Add template inheritance system
2. Implement custom filters
3. Create template optimization
4. Add performance profiling

## ✅ Verification Requirements

### Template System Tests
- Unit tests for template manager
- Integration tests for code generation
- Template syntax validation
- Generated code compilation tests
- Formatting compliance verification

### Quality Metrics
- Code formatting consistency (100% Black compliance)
- Template rendering performance
- Generated application functionality
- Documentation completeness
- Error handling coverage

## 🔗 Integration Points

### With Existing Components
- **XML Parser**: Receives mapping metadata for template context
- **Code Generator**: Uses templates for Spark application generation
- **Configuration Manager**: Provides environment-specific template variables
- **Workflow Orchestration**: Generates workflow execution scripts

### External Dependencies
- Jinja2 template engine
- Black code formatter
- isort import optimizer
- PyYAML for configuration templates

## 📖 Usage Examples

### Basic Template Usage
```python
# Template manager initialization
template_manager = TemplateManager()

# Context preparation
context = {
    'app_name': 'CustomerETL',
    'mappings': mapping_list,
    'connections': connection_config,
    'spark_config': spark_settings
}

# Template rendering
spark_app = template_manager.render_template('spark/main_app.jinja2', context)
```

### Advanced Template Customization
- Environment-specific template selection
- Dynamic template composition
- Template performance optimization
- Custom filter implementation

## 🔗 Next Steps

After implementing the template system, proceed to **`11_SESSION_MANAGEMENT.md`** to implement comprehensive session configuration and management.

The template system provides the foundation for generating professional, maintainable, and properly formatted PySpark applications from Informatica mappings.