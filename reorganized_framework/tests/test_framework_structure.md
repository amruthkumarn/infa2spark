# Framework Testing Structure

## Directory Layout
```
tests/
├── framework/                     # Tests for the framework itself
│   ├── unit/
│   │   ├── test_xml_parser.py     # XML parsing unit tests
│   │   ├── test_spark_generator.py # Code generator unit tests
│   │   ├── test_templates.py      # Template rendering tests
│   │   ├── test_config_manager.py # Configuration management tests
│   │   └── test_base_classes.py   # Runtime base classes tests
│   ├── integration/
│   │   ├── test_xml_to_code.py    # XML → Code generation integration
│   │   ├── test_template_engine.py # Template system integration
│   │   └── test_config_externalization.py # Config system integration
│   └── performance/
│       ├── test_large_xml_parsing.py # Large XML performance
│       ├── test_code_generation_speed.py # Generation speed tests
│       └── test_memory_usage.py   # Framework memory usage
│
├── generated_code/               # Tests for generated applications
│   ├── unit/
│   │   ├── test_generated_mappings.py # Generated mapping classes
│   │   ├── test_generated_workflows.py # Generated workflow classes
│   │   ├── test_generated_transformations.py # Generated transformations
│   │   └── test_generated_config.py # Generated configuration handling
│   ├── integration/
│   │   ├── test_data_pipeline.py  # Complete data processing pipeline
│   │   ├── test_spark_execution.py # Spark job execution
│   │   └── test_error_handling.py # Generated error handling
│   └── performance/
│       ├── test_spark_performance.py # Generated Spark job performance
│       ├── test_large_dataset.py  # Large dataset processing
│       └── test_concurrent_execution.py # Concurrent job execution
│
└── e2e/                          # End-to-end tests (Framework + Generated)
    ├── test_complete_workflow.py # Full XML → Generated App → Data processing
    ├── test_multi_mapping.py     # Multiple mapping scenarios
    ├── test_enterprise_features.py # Enterprise feature testing
    └── test_real_world_scenarios.py # Real-world use cases
```

## Test Categories

### Framework Unit Tests
- **XML Parser**: Test parsing of various Informatica XML formats
- **Code Generator**: Test template rendering and code generation logic
- **Configuration**: Test config loading, validation, and externalization
- **Base Classes**: Test runtime framework components

### Generated Code Unit Tests  
- **Mappings**: Test individual generated mapping classes
- **Workflows**: Test generated workflow orchestration
- **Transformations**: Test generated transformation logic
- **Data Sources**: Test generated data source connections

### Integration Tests
- **Framework Integration**: Test framework component interactions
- **Generated Code Integration**: Test generated component interactions
- **Data Pipeline**: Test complete data processing workflows

### Performance Tests
- **Framework Performance**: Code generation speed and memory usage
- **Generated Code Performance**: Spark execution performance and scaling