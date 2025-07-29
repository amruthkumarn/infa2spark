# Enterprise_Complete_Transformations Spark Application

Clean, production-ready Spark application generated from Informatica BDM project.

## Structure

```
├── src/app/           # Application code
│   ├── main.py        # Entry point
│   ├── mappings/      # Business logic
│   └── workflows/     # Orchestration
├── config/            # Configuration files
├── tests/             # Test suite
├── data/              # Data directories
└── scripts/           # Utility scripts
```

## Quick Start

```bash
# Install dependencies
pip install -r requirements.txt

# Run application
python src/app/main.py

# Run tests
python -m pytest tests/
```

## Configuration

- `config/application.yaml` - Main application configuration
- `config/connections/` - Connection-specific configurations  
- `config/mappings/` - Mapping-specific configurations

## Generated Components

- **Connections**: 2 real connections configured
- **Mappings**: 1 mapping(s) implemented
- **Sources**: Auto-detected from XML
- **Targets**: Auto-detected from XML
