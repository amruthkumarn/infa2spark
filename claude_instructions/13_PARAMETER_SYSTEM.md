# 13_PARAMETER_SYSTEM.md

## 🔧 Advanced Parameter Management System

Implement the comprehensive parameter management system that handles Informatica parameters, system variables, environment-specific configurations, and dynamic parameter resolution throughout the framework.

## 📁 Parameter System Architecture

**ASSUMPTION**: The `/informatica_xsd_xml/` directory with complete XSD schemas is provided and available.

Create the parameter management system:

```bash
src/parameters/
├── __init__.py
├── parameter_manager.py      # Core parameter management
├── parameter_resolver.py     # Parameter resolution engine
├── parameter_validator.py    # Parameter validation framework
├── parameter_types.py        # Parameter type definitions
├── environment_manager.py    # Environment-specific parameter handling
└── parameter_persistence.py  # Parameter storage and retrieval
```

## 🎯 Parameter System Components

### 1. Parameter Type Definitions

Create `src/parameters/parameter_types.py`:

**Parameter Categories:**
- **Session Parameters**: User-defined session-level variables
- **Mapping Parameters**: Mapping-specific parameters
- **Workflow Parameters**: Workflow-level parameters
- **System Parameters**: Built-in Informatica system parameters
- **Environment Parameters**: Environment-specific configurations
- **Runtime Parameters**: Dynamic runtime values

**Parameter Attributes:**
- Name and description
- Data type and validation rules
- Default values and constraints
- Scope and visibility
- Environment specificity
- Security classification

### 2. Parameter Manager Core

Create `src/parameters/parameter_manager.py`:

**Core Functionality:**
- Parameter registration and management
- Hierarchical parameter resolution
- Parameter validation and type checking
- Environment-specific parameter handling
- Parameter persistence and caching
- Parameter change tracking and auditing

**Parameter Hierarchy (highest to lowest precedence):**
1. Command-line parameters
2. Runtime parameters
3. Environment-specific parameters
4. Session parameters
5. Mapping parameters
6. Workflow parameters
7. System default parameters

### 3. Parameter Resolution Engine

Create `src/parameters/parameter_resolver.py`:

**Resolution Features:**
- Dynamic parameter evaluation
- Circular dependency detection
- Parameter expression parsing
- System parameter substitution
- Environment variable integration
- Conditional parameter resolution

**System Parameter Support:**
- `$$PMSessionName` - Current session name
- `$$PMWorkflowName` - Current workflow name
- `$$PMSessionStartTime` - Session start timestamp
- `$$PMSystemDate` - Current system date
- `$$PMSourceSystemName` - Source system identifier
- `$$PMTargetSystemName` - Target system identifier

### 4. Parameter Validation Framework

Create `src/parameters/parameter_validator.py`:

**Validation Types:**
- **Type Validation**: Data type compliance
- **Range Validation**: Numeric range constraints
- **Format Validation**: String format patterns
- **Dependency Validation**: Parameter dependency checks
- **Business Rule Validation**: Custom business logic
- **Security Validation**: Sensitive data handling

**Validation Rules:**
- Required parameter presence
- Parameter value constraints
- Cross-parameter validation
- Environment-specific validation
- Security compliance checks

## 🔍 Parameter Resolution Process

### Resolution Algorithm

**Step 1: Parameter Discovery**
- Scan XML files for parameter references
- Identify parameter dependencies
- Build parameter dependency graph
- Detect circular dependencies

**Step 2: Environment Context**
- Load environment-specific configurations
- Apply environment parameter overrides
- Resolve environment variables
- Set environment-specific defaults

**Step 3: Parameter Substitution**
- Resolve system parameters
- Substitute environment variables
- Apply user-defined parameters
- Evaluate parameter expressions

**Step 4: Validation and Verification**
- Validate parameter types and constraints
- Check required parameters
- Verify parameter dependencies
- Validate business rules

### Dynamic Parameter Evaluation

**Expression Processing:**
- Mathematical expressions: `$$Param1 + $$Param2`
- String concatenation: `$$Prefix + "_" + $$Suffix`
- Conditional expressions: `$$Environment == "PROD" ? "production_db" : "dev_db"`
- Date/time expressions: `$$SystemDate + 7` (days)
- Function calls: `UPPER($$DatabaseName)`

**Built-in Functions:**
- String functions: `UPPER()`, `LOWER()`, `TRIM()`, `SUBSTRING()`
- Date functions: `SYSDATE()`, `ADD_DAYS()`, `FORMAT_DATE()`
- Math functions: `ABS()`, `ROUND()`, `MAX()`, `MIN()`
- Conditional functions: `IIF()`, `DECODE()`, `NVL()`

## 🌍 Environment Management

### Environment-Specific Parameters

Create `src/parameters/environment_manager.py`:

**Environment Types:**
- **Development**: Local development settings
- **Testing**: Test environment configurations
- **Staging**: Pre-production settings
- **Production**: Production environment settings
- **Custom**: User-defined environments

**Environment Configuration:**
```yaml
environments:
  development:
    database_host: "dev-db.company.com"
    batch_size: 1000
    debug_mode: true
    
  production:
    database_host: "prod-db.company.com"
    batch_size: 50000
    debug_mode: false
    monitoring_enabled: true
```

### Environment Parameter Resolution

**Resolution Strategy:**
- Base configuration loading
- Environment-specific override application
- User-specific customization
- Runtime parameter injection

**Precedence Rules:**
1. Runtime overrides
2. User-specific settings
3. Environment-specific settings
4. Base configuration defaults

## 🔐 Security and Sensitive Parameters

### Sensitive Parameter Handling

**Security Features:**
- Parameter encryption for sensitive values
- Secure parameter storage mechanisms
- Access control and authorization
- Audit logging for parameter access
- Credential rotation support

**Sensitive Parameter Types:**
- Database passwords and connection strings
- API keys and authentication tokens
- Encryption keys and certificates
- Personal identifiable information (PII)
- Business-critical configuration values

### Security Implementation

**Encryption Strategy:**
- AES-256 encryption for sensitive values
- Key derivation from master key
- Environment-specific encryption keys
- Secure key management integration

**Access Control:**
- Role-based parameter access
- Parameter visibility restrictions
- Audit trail for parameter changes
- Secure parameter distribution

## 📊 Parameter Monitoring and Auditing

### Parameter Usage Tracking

**Tracking Features:**
- Parameter access logging
- Parameter modification history
- Performance impact monitoring
- Usage pattern analysis
- Compliance reporting

**Audit Information:**
- Parameter access timestamps
- User/system accessing parameters
- Parameter values (excluding sensitive)
- Environment context
- Operation context

### Performance Monitoring

**Performance Metrics:**
- Parameter resolution time
- Cache hit/miss ratios
- Memory usage for parameter storage
- Network overhead for parameter retrieval
- Impact on overall framework performance

## 🚀 Implementation Strategy

### Phase 1: Core Parameter System
1. Implement basic parameter types and structures
2. Create parameter manager with basic resolution
3. Build validation framework
4. Integrate with existing configuration system

### Phase 2: Advanced Resolution
1. Implement expression parsing and evaluation
2. Add system parameter support
3. Create dependency resolution system
4. Build circular dependency detection

### Phase 3: Environment Integration
1. Implement environment-specific parameter handling
2. Create environment manager
3. Add environment switching capabilities
4. Build environment validation

### Phase 4: Security and Monitoring
1. Implement sensitive parameter encryption
2. Add access control and auditing
3. Create monitoring and reporting
4. Build compliance features

## ✅ Testing Framework

### Parameter System Testing

**Test Categories:**
- Parameter resolution accuracy
- Environment-specific behavior
- Security and encryption functionality
- Performance and scalability
- Error handling and recovery

**Test Scenarios:**
- Simple parameter resolution
- Complex expression evaluation
- Environment switching
- Circular dependency handling
- Sensitive parameter security
- Large-scale parameter sets

### Integration Testing

**Integration Points:**
- Configuration manager integration
- XML parser parameter extraction
- Template system parameter injection
- Session manager parameter usage
- Workflow orchestration parameter passing

## 📋 Usage Examples

### Basic Parameter Definition
```yaml
parameters:
  session:
    BATCH_SIZE: 10000
    ERROR_THRESHOLD: 100
    CONNECTION_NAME: "PRIMARY_DB"
    
  mapping:
    SOURCE_TABLE: "CUSTOMERS"
    TARGET_TABLE: "DIM_CUSTOMER"
    LOAD_TYPE: "INCREMENTAL"
```

### Advanced Parameter Expressions
```yaml
parameters:
  calculated:
    LOAD_DATE: "$$SystemDate"
    BATCH_ID: "$$PMSessionName + '_' + FORMAT_DATE($$SystemDate, 'YYYYMMDD')"
    PARTITION_COUNT: "$$Environment == 'PROD' ? 10 : 2"
    
  conditional:
    DATABASE_URL: "IIF($$Environment == 'PROD', $$PROD_DB_URL, $$DEV_DB_URL)"
    LOG_LEVEL: "DECODE($$Environment, 'DEV', 'DEBUG', 'TEST', 'INFO', 'WARN')"
```

### Environment-Specific Parameters
```yaml
environments:
  development:
    BATCH_SIZE: 1000
    PARALLEL_DEGREE: 2
    DEBUG_ENABLED: true
    
  production:
    BATCH_SIZE: 50000
    PARALLEL_DEGREE: 8
    DEBUG_ENABLED: false
    MONITORING_ENABLED: true
```

## 🔗 Integration Architecture

### Framework Integration

**Component Integration:**
- **XML Parser**: Extracts parameter definitions from XML
- **Session Manager**: Uses parameters for session configuration
- **Code Generator**: Injects parameters into generated code
- **Template System**: Provides parameter substitution in templates
- **Workflow Orchestrator**: Manages workflow-level parameters

**Data Flow:**
1. Parameter definitions extracted from XML
2. Environment-specific parameters loaded
3. Parameter resolution and validation performed
4. Resolved parameters provided to downstream components
5. Parameter usage tracked and audited

### External System Integration

**External Dependencies:**
- Configuration management systems
- Secret management systems (HashiCorp Vault, AWS Secrets Manager)
- Environment management systems
- Monitoring and alerting systems

## 📈 Advanced Features

### Parameter Templates

**Template Features:**
- Parameter definition templates
- Environment-specific parameter templates
- Parameter validation rule templates
- Parameter documentation templates

### Parameter Migration

**Migration Capabilities:**
- Parameter definition migration between environments
- Parameter value migration and transformation
- Parameter schema evolution support
- Backward compatibility maintenance

### Parameter Analytics

**Analytics Features:**
- Parameter usage analysis
- Performance impact analysis
- Parameter dependency analysis
- Cost optimization recommendations

## 🔗 Next Steps

After implementing the parameter system, proceed to **`14_MONITORING_INTEGRATION.md`** to implement comprehensive monitoring and observability features.

The parameter system provides:
- Comprehensive parameter management
- Advanced resolution and validation
- Environment-specific handling
- Security and compliance features
- Performance monitoring and optimization
- Enterprise-grade parameter governance