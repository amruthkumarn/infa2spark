# Claude Instructions: Informatica BDM to PySpark Converter Framework

## 🎯 Project Overview

Create a **production-ready, enterprise-grade framework** that converts Informatica Business Data Model (BDM) and PowerCenter XML projects into fully executable PySpark applications.

## 📋 Master Implementation Guide

This directory contains comprehensive instructions to recreate the entire project from scratch. Follow the files in numerical order:

### **Phase 1: Foundation Setup**
1. **`01_PROJECT_STRUCTURE.md`** - Complete directory structure and initial setup
2. **`02_REQUIREMENTS_AND_DEPENDENCIES.md`** - Dependencies, virtual environment, and tools
3. **`03_CONFIGURATION_FILES.md`** - Configuration system implementation

### **Phase 2: Core Framework**
4. **`04_XSD_BASE_ARCHITECTURE.md`** - XSD-compliant base classes and foundation
5. **`05_XML_PARSER_IMPLEMENTATION.md`** - Advanced XML parsing with namespace support
6. **`06_REFERENCE_MANAGEMENT.md`** - ID/IDREF resolution system
7. **`07_TRANSFORMATION_MODELS.md`** - Complete transformation hierarchy

### **Phase 3: Code Generation Engine**
8. **`08_SPARK_CODE_GENERATOR.md`** - PySpark application generation
9. **`09_WORKFLOW_ORCHESTRATION.md`** - Workflow and DAG processing
10. **`10_TEMPLATES_AND_FORMATTING.md`** - Jinja2 templates and code formatting

### **Phase 4: Advanced Features**
11. **`11_SESSION_MANAGEMENT.md`** - Session configuration and runtime
12. **`12_EXECUTION_ENGINE.md`** - Data flow execution engine
13. **`13_PARAMETER_SYSTEM.md`** - Enhanced parameter management
14. **`14_MONITORING_INTEGRATION.md`** - Enterprise monitoring and logging

### **Phase 5: Testing & Validation**
15. **`15_COMPREHENSIVE_TESTING.md`** - Complete test suite implementation
16. **`16_SAMPLE_DATA_GENERATION.md`** - Sample XML projects and test data
17. **`17_VALIDATION_FRAMEWORK.md`** - Code validation and quality checks

### **Phase 6: Documentation & Deployment**
18. **`18_DOCUMENTATION_SYSTEM.md`** - Comprehensive documentation generation
19. **`19_DEPLOYMENT_CONFIGURATION.md`** - Docker, scripts, and deployment
20. **`20_INTEGRATION_TESTING.md`** - End-to-end integration and validation

## 🏗️ Key Architecture Principles

### **XSD Compliance**
- All models directly mirror Informatica's official XML Schema Definition
- Complete namespace support and validation
- ID/IDREF reference resolution throughout

### **Enterprise-Grade Quality**
- Production-ready code generation with professional formatting
- Comprehensive error handling and logging
- Memory optimization and performance tuning
- Complete configuration externalization

### **Extensibility**
- Registry patterns for transformation types
- Plugin architecture for custom elements
- Template-based code generation
- Modular, testable design

## 📊 Success Metrics

### **Completion Targets**
- **Parse 95%** of real Informatica exports without errors
- **Support all major** XSD object types and relationships
- **Execute complex mappings** with proper data lineage
- **Generate production-ready** PySpark applications
- **Handle enterprise-scale** mappings (100+ transformations)

### **Quality Gates**
- **100% test coverage** for core components
- **Professional code formatting** with Black
- **Complete documentation** with examples
- **Real data validation** with actual Informatica files
- **Performance benchmarks** for large projects

## 🚀 Implementation Strategy

### **Development Approach**
1. **Phase-by-phase implementation** following the numbered instruction files
2. **Incremental testing** after each component
3. **Real data validation** throughout development
4. **Documentation-driven development** with comprehensive guides

### **Code Quality Standards**
- **Type hints** throughout all Python code
- **Comprehensive error handling** with proper logging
- **Professional documentation** with docstrings
- **Test-driven development** with pytest
- **Code formatting** with Black and linting with flake8

### **Architecture Validation**
- **XSD compliance testing** against official schemas
- **Memory profiling** for large project handling
- **Performance benchmarking** with enterprise data
- **Integration testing** with real Informatica exports

## 📁 Expected Output Structure

```
informatica_to_pyspark_framework/
├── src/                          # Core framework source
│   ├── core/                     # XSD-compliant core engine
│   ├── transformations/          # Transformation implementations
│   ├── mappings/                 # Sample mapping implementations
│   ├── workflows/                # Workflow orchestration
│   └── main.py                   # Main application entry
├── tests/                        # Comprehensive test suite
├── docs/                         # Complete documentation
├── config/                       # Configuration files
├── input/                        # Sample XML projects
├── generated_spark_apps/         # Generated applications
├── informatica_xsd_xml/          # Informatica XSD schemas
└── requirements.txt              # Dependencies
```

## 🎯 Key Implementation Notes

### **Critical Success Factors**
1. **Follow the exact sequence** of instruction files
2. **Implement comprehensive testing** at each phase
3. **Validate with real data** throughout development
4. **Maintain XSD compliance** in all object models
5. **Generate production-ready output** with proper formatting

### **Common Pitfalls to Avoid**
- Don't skip the XSD foundation - it's critical for compatibility
- Don't implement transformations without proper field-level metadata
- Don't generate code without professional formatting
- Don't create applications without complete configuration externalization
- Don't skip comprehensive testing - it ensures production readiness

### **Extension Points**
- Custom transformation types via registry pattern
- Additional output formats via template system
- Enhanced monitoring via plugin architecture
- Custom validation rules via configuration

## 🔧 Development Environment

### **Required Tools**
- Python 3.8+ with virtual environment
- Apache Spark 3.4+ for testing
- Black for code formatting
- pytest for testing
- Jinja2 for template processing
- PyYAML for configuration
- Docker for containerization

### **Recommended IDE Setup**
- VS Code with Python extension
- Type checking enabled
- Auto-formatting with Black
- Integrated testing with pytest
- Git integration for version control

## 📚 Reference Materials

### **Informatica Documentation**
- BDM XML Schema specifications
- PowerCenter XML export formats
- Transformation type documentation
- Session configuration options

### **PySpark Documentation**
- DataFrame API reference
- SQL functions documentation
- Performance optimization guides
- Deployment best practices

---

**🚀 Ready to build an enterprise-grade Informatica to PySpark conversion framework!**

Start with `01_PROJECT_STRUCTURE.md` and follow the sequence. Each file contains detailed, step-by-step instructions with complete code examples.