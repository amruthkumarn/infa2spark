# Informatica Spark Framework

A comprehensive framework for converting Informatica BDM projects into production-ready PySpark applications.

## Features

- **XML/XSD Parsing**: Full support for Informatica project files
- **Code Generation**: Automated PySpark application generation
- **Enterprise Features**: Monitoring, validation, and configuration management
- **Production Ready**: Docker support, logging, and error handling

## Quick Start

```bash
# Install framework
pip install -e .

# Generate Spark application
from informatica_spark import SparkGenerator
generator = SparkGenerator()
generator.generate_from_xml("project.xml", "output_dir")
```

## Testing

The framework includes comprehensive testing at multiple levels:

### Test Structure
- `tests/framework/unit/` - Framework unit tests
- `tests/framework/integration/` - Framework integration tests  
- `tests/generated_code/unit/` - Generated code validation tests
- `tests/e2e/` - End-to-end pipeline tests
- `tests/data/` - Test data and fixtures

### Running Tests

```bash
# Run all tests with the comprehensive test runner
./run_tests.sh

# Or run specific test suites
pytest tests/framework/unit/ -v        # Framework unit tests
pytest tests/e2e/ -v                   # End-to-end tests
pytest tests/ -m performance -v        # Performance tests
```

### Test Data
- Sample XML projects in `tests/data/xml/`
- Test datasets in `tests/data/input/`
- Expected outputs in `tests/data/expected/`
- Configuration fixtures in `tests/data/fixtures/`

### Testing Strategy
1. **Framework Testing**: Validates core parsing, generation and runtime components
2. **Generated Code Testing**: Ensures generated applications function correctly
3. **Integration Testing**: Tests complete XML-to-Spark pipeline
4. **Performance Testing**: Validates framework performance with realistic datasets

## Documentation

- [API Reference](docs/api/)
- [Tutorials](docs/tutorials/)
- [Examples](examples/)

## Structure

- `src/informatica_spark/` - Main framework code
- `examples/` - Usage examples and generated applications
- `tests/` - Comprehensive test suite
- `docs/` - Documentation
- `config/` - Default configuration templates