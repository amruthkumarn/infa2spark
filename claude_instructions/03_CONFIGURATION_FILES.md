# 03_CONFIGURATION_FILES.md

## ⚙️ Complete Configuration System Implementation

Implement the comprehensive configuration management system that supports enterprise-grade settings, external configuration, and environment-specific deployments.

## 📁 Configuration Structure

**ASSUMPTION**: The `/informatica_xsd_xml/` directory with complete XSD schemas is provided and available.

Create the configuration system with these files:

```bash
config/
├── connections.yaml          # Database and data source connections
├── spark_config.yaml         # Spark-specific settings
├── sample_project_config.yaml # Project parameters and settings
└── templates/                # Configuration templates
    ├── production.yaml
    ├── development.yaml
    └── testing.yaml
```

## 🔧 Core Configuration Files

### 1. config/connections.yaml

```yaml
# Connection configurations for various data sources
# Used by the framework to generate connection code in Spark applications

# HDFS Connections
HDFS_CONN:
  type: HDFS
  description: "Primary HDFS cluster connection"
  host: namenode.company.com
  port: 8020
  user: informatica_user
  # For local development/testing
  local_path: sample_data/
  properties:
    dfs.client.use.datanode.hostname: "true"
    dfs.client.use.datanode.hostname.for.datanode: "true"

ENTERPRISE_HDFS_CONN:
  type: HDFS
  description: "Enterprise HDFS connection for production workloads"
  host: enterprise-namenode.company.com
  port: 8020
  user: enterprise_etl_user
  local_path: enterprise_data/
  properties:
    dfs.replication: "3"
    dfs.block.size: "134217728"

# Hive Connections
HIVE_CONN:
  type: HIVE
  description: "Hive metastore connection"
  host: hive-metastore.company.com
  port: 9083
  database: default
  user: hive_user
  url: "jdbc:hive2://hive-server.company.com:10000/default"
  properties:
    hive.exec.dynamic.partition: "true"
    hive.exec.dynamic.partition.mode: "nonstrict"

ENTERPRISE_HIVE_CONN:
  type: HIVE
  description: "Enterprise Hive connection with advanced settings"
  host: enterprise-hive.company.com
  port: 10000
  database: enterprise_dw
  user: enterprise_hive_user
  url: "jdbc:hive2://enterprise-hive.company.com:10000/enterprise_dw"
  properties:
    hive.vectorized.execution.enabled: "true"
    hive.optimize.ppd: "true"
    hive.optimize.index.filter: "true"

# Database Connections
ORACLE_CONN:
  type: ORACLE
  description: "Oracle database connection"
  host: oracle-db.company.com
  port: 1521
  service_name: ORCL
  user: oracle_user
  # Password would be provided via environment variables or secrets management
  url: "jdbc:oracle:thin:@oracle-db.company.com:1521:ORCL"
  driver: "oracle.jdbc.driver.OracleDriver"
  properties:
    oracle.jdbc.ReadTimeout: "60000"
    oracle.net.CONNECT_TIMEOUT: "10000"

POSTGRES_CONN:
  type: POSTGRESQL
  description: "PostgreSQL database connection"
  host: postgres-db.company.com
  port: 5432
  database: enterprise_db
  user: postgres_user
  url: "jdbc:postgresql://postgres-db.company.com:5432/enterprise_db"
  driver: "org.postgresql.Driver"
  properties:
    ssl: "true"
    sslmode: "require"

SQLSERVER_CONN:
  type: SQLSERVER
  description: "SQL Server database connection"
  host: sqlserver-db.company.com
  port: 1433
  database: EnterpriseDB
  user: sqlserver_user
  url: "jdbc:sqlserver://sqlserver-db.company.com:1433;databaseName=EnterpriseDB"
  driver: "com.microsoft.sqlserver.jdbc.SQLServerDriver"
  properties:
    encrypt: "true"
    trustServerCertificate: "true"

# File System Connections
LOCAL_FILE_CONN:
  type: FILE
  description: "Local file system connection for development"
  base_path: ./sample_data
  format: DELIMITED
  properties:
    delimiter: ","
    header: "true"
    encoding: "UTF-8"

S3_CONN:
  type: S3
  description: "AWS S3 connection"
  bucket: enterprise-data-lake
  region: us-east-1
  access_key_id: "${AWS_ACCESS_KEY_ID}"  # From environment
  secret_access_key: "${AWS_SECRET_ACCESS_KEY}"  # From environment
  properties:
    server_side_encryption: "AES256"
    storage_class: "STANDARD"

# Kafka Connections (for streaming data)
KAFKA_CONN:
  type: KAFKA
  description: "Kafka cluster connection"
  bootstrap_servers: "kafka1.company.com:9092,kafka2.company.com:9092,kafka3.company.com:9092"
  security_protocol: "SASL_SSL"
  sasl_mechanism: "PLAIN"
  properties:
    acks: "all"
    retries: "3"
    batch.size: "16384"
    linger.ms: "5"
```

### 2. config/spark_config.yaml

```yaml
# Spark configuration for different environments and use cases

# Default Spark Configuration
spark:
  # Application Settings
  spark.app.name: "Informatica_to_PySpark_Framework"
  spark.master: "local[*]"
  
  # Driver Settings
  spark.driver.memory: "2g"
  spark.driver.maxResultSize: "1g"
  spark.driver.cores: "2"
  
  # Executor Settings  
  spark.executor.memory: "2g"
  spark.executor.cores: "2"
  spark.executor.instances: "2"
  
  # SQL and DataFrame Settings
  spark.sql.adaptive.enabled: "true"
  spark.sql.adaptive.coalescePartitions.enabled: "true"
  spark.sql.adaptive.coalescePartitions.minPartitionNum: "1"
  spark.sql.adaptive.advisoryPartitionSizeInBytes: "64MB"
  spark.sql.adaptive.skewJoin.enabled: "true"
  
  # Serialization
  spark.serializer: "org.apache.spark.serializer.KryoSerializer"
  spark.sql.execution.arrow.pyspark.enabled: "true"
  
  # Storage
  spark.sql.warehouse.dir: "./spark-warehouse"
  spark.sql.catalogImplementation: "hive"
  
  # Networking
  spark.network.timeout: "300s"
  spark.sql.broadcastTimeout: "300"
  
  # Optimization
  spark.sql.optimizer.excludedRules: ""
  spark.sql.cbo.enabled: "true"
  spark.sql.statistics.histogram.enabled: "true"

# Development Environment Configuration
development:
  spark:
    spark.app.name: "Informatica_Framework_DEV"
    spark.master: "local[*]"
    spark.driver.memory: "1g"
    spark.executor.memory: "1g"
    spark.sql.shuffle.partitions: "4"
    spark.sql.adaptive.enabled: "true"
    spark.log.level: "INFO"
    # Enable debugging features
    spark.sql.execution.debug.maxToStringFields: "100"
    spark.sql.ui.retainedExecutions: "10"

# Testing Environment Configuration  
testing:
  spark:
    spark.app.name: "Informatica_Framework_TEST"
    spark.master: "local[2]"
    spark.driver.memory: "512m"
    spark.executor.memory: "512m"
    spark.sql.shuffle.partitions: "2"
    spark.log.level: "WARN"
    # Minimize resource usage for tests
    spark.sql.adaptive.enabled: "false"
    spark.ui.enabled: "false"

# Production Environment Configuration
production:
  spark:
    spark.app.name: "Informatica_Framework_PROD"
    spark.master: "yarn"
    spark.submit.deployMode: "cluster"
    
    # Production Resource Allocation
    spark.driver.memory: "4g"
    spark.driver.cores: "4"
    spark.executor.memory: "8g"
    spark.executor.cores: "4"
    spark.executor.instances: "10"
    
    # Production Optimizations
    spark.sql.shuffle.partitions: "200"
    spark.sql.adaptive.enabled: "true"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    spark.sql.adaptive.skewJoin.enabled: "true"
    spark.sql.adaptive.localShuffleReader.enabled: "true"
    
    # Production Stability
    spark.task.maxAttempts: "3"
    spark.stage.maxConsecutiveAttempts: "8"
    spark.kubernetes.executor.deleteOnTermination: "true"
    
    # Performance Monitoring
    spark.eventLog.enabled: "true"
    spark.eventLog.dir: "hdfs://namenode:8020/spark-events"
    spark.history.fs.logDirectory: "hdfs://namenode:8020/spark-events"
    
    # Security (for enterprise environments)
    spark.authenticate: "true"
    spark.network.crypto.enabled: "true"
    spark.io.encryption.enabled: "true"

# Cluster-specific configurations
cluster_configs:
  # AWS EMR Configuration
  emr:
    spark.master: "yarn"
    spark.submit.deployMode: "cluster"
    spark.driver.memory: "4g"
    spark.executor.memory: "8g"
    spark.executor.instances: "5"
    spark.sql.catalogImplementation: "hive"
    spark.hadoop.fs.s3a.impl: "org.apache.hadoop.fs.s3a.S3AFileSystem"
    spark.hadoop.fs.s3a.aws.credentials.provider: "org.apache.hadoop.fs.s3a.InstanceProfileCredentialsProvider"
    
  # Google Dataproc Configuration
  dataproc:
    spark.master: "yarn"
    spark.submit.deployMode: "cluster"  
    spark.driver.memory: "3g"
    spark.executor.memory: "6g"
    spark.executor.instances: "4"
    spark.hadoop.fs.gs.impl: "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
    spark.hadoop.fs.AbstractFileSystem.gs.impl: "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
    
  # Azure HDInsight Configuration
  hdinsight:
    spark.master: "yarn"
    spark.submit.deployMode: "cluster"
    spark.driver.memory: "3g"
    spark.executor.memory: "7g"
    spark.executor.instances: "6"
    spark.hadoop.fs.azure.impl: "org.apache.hadoop.fs.azure.NativeAzureFileSystem"
    spark.hadoop.fs.AbstractFileSystem.wasbs.impl: "org.apache.hadoop.fs.azure.Wasbs"

# Performance Tuning Profiles
performance_profiles:
  # Memory-intensive workloads
  memory_intensive:
    spark.executor.memory: "12g"
    spark.executor.memoryFraction: "0.8"
    spark.storage.memoryFraction: "0.5"
    spark.sql.adaptive.coalescePartitions.enabled: "true"
    
  # CPU-intensive workloads  
  cpu_intensive:
    spark.executor.cores: "8"
    spark.task.cpus: "1"
    spark.sql.adaptive.skewJoin.enabled: "true"
    
  # IO-intensive workloads
  io_intensive:
    spark.sql.adaptive.advisoryPartitionSizeInBytes: "128MB"
    spark.sql.files.maxPartitionBytes: "134217728"  # 128MB
    spark.hadoop.mapreduce.input.fileinputformat.split.maxsize: "134217728"
```

### 3. config/sample_project_config.yaml

```yaml
# Project-specific configuration and parameters
# This file demonstrates parameter management and project settings

project:
  name: "Sample_Informatica_Project"
  version: "1.0.0"
  description: "Sample project configuration for Informatica to PySpark conversion"
  created_by: "Informatica Framework"
  created_date: "2025-01-01"

# Global Parameters (equivalent to Informatica project parameters)
parameters:
  # System Parameters
  LOAD_DATE: "$$SystemDate"
  LOAD_TIMESTAMP: "$$SystemTimestamp"
  PROJECT_VERSION: "1.0"
  ENVIRONMENT: "DEV"
  
  # Business Parameters
  FISCAL_YEAR: "2025"
  REGION: "US"
  BATCH_SIZE: "10000"
  
  # Technical Parameters
  ERROR_THRESHOLD: "100"
  WARNING_THRESHOLD: "50"
  COMMIT_INTERVAL: "10000"
  
  # File Path Parameters
  INPUT_BASE_PATH: "/data/input"
  OUTPUT_BASE_PATH: "/data/output"
  ARCHIVE_PATH: "/data/archive"
  ERROR_PATH: "/data/error"
  
  # Database Parameters
  SOURCE_DATABASE: "SOURCE_DB"
  TARGET_DATABASE: "TARGET_DW"
  STAGING_DATABASE: "STAGING_DB"
  
  # Connection Parameters
  PRIMARY_CONNECTION: "ENTERPRISE_HIVE_CONN"
  BACKUP_CONNECTION: "HIVE_CONN"
  FILE_CONNECTION: "HDFS_CONN"

# Session Configuration Templates
session_templates:
  default:
    commit_type: "target"
    commit_interval: 10000
    rollback_transaction: "on_errors"
    recovery_strategy: "fail_task_and_continue_workflow"
    pushdown_optimization: "none"
    
  high_performance:
    commit_type: "source"
    commit_interval: 50000
    rollback_transaction: "on_errors"
    recovery_strategy: "fail_task_and_continue_workflow" 
    pushdown_optimization: "full"
    buffer_block_size: 128000
    dtm_buffer_pool_size: 512000000
    
  data_quality:
    commit_type: "target"
    commit_interval: 1000
    rollback_transaction: "on_errors"
    recovery_strategy: "fail_task_and_continue_workflow"
    error_handling: "continue_on_error"
    save_session_log: "by_session_runs"

# Mapping Configuration
mapping_config:
  # Default transformation settings
  transformations:
    expression:
      data_movement_mode: "ascii"
      tracing_level: "normal"
      
    aggregator:
      data_movement_mode: "unicode"
      tracing_level: "normal"
      sorted_input: false
      
    lookup:
      cache_policy: "static"
      lookup_policy: "use_first_value"
      multiple_return_rows: false
      
    joiner:
      join_type: "normal"
      cache_directory: "$PMCacheDir"
      
    sorter:
      case_sensitive: true
      distinct_output_rows: false
      
    sequence:
      start_value: 1
      increment_by: 1
      end_value: 999999999
      cycle: false
      
# Workflow Configuration
workflow_config:
  # Default workflow settings
  concurrent_workflows: 1
  workflow_recovery: "fail_task_and_continue_workflow"
  
  # Task configuration
  tasks:
    session_task:
      fail_parent_if_instance_fails: true
      fail_parent_if_instance_does_not_run: true
      treat_input_links_as: "and"
      
    command_task:
      command_type: "shell"
      success_exit_codes: [0]
      
    decision_task:
      return_value: "integer"
      
    timer_task:
      absolute_time: false
      
    email_task:
      email_format: "text"

# Data Quality Configuration
data_quality:
  # Validation rules
  validation_rules:
    null_check:
      enabled: true
      action: "reject"
      
    range_check:
      enabled: true  
      action: "flag"
      
    format_check:
      enabled: true
      action: "correct"
      
  # Profiling settings
  profiling:
    sample_size: 10000
    profile_all_columns: false
    generate_statistics: true

# Monitoring and Logging
monitoring:
  # Logging configuration
  logging:
    level: "INFO"
    format: "detailed"
    max_file_size: "100MB"
    retention_days: 30
    
  # Metrics collection
  metrics:
    enabled: true
    collection_interval: "5s"
    export_format: "json"
    
  # Alerting
  alerts:
    enabled: true
    error_threshold: 10
    warning_threshold: 5
    notification_email: "admin@company.com"

# Environment-specific overrides
environments:
  development:
    parameters:
      ENVIRONMENT: "DEV"
      BATCH_SIZE: "1000"
      ERROR_THRESHOLD: "10"
    monitoring:
      logging:
        level: "DEBUG"
    
  testing:
    parameters:
      ENVIRONMENT: "TEST"  
      BATCH_SIZE: "5000"
      ERROR_THRESHOLD: "50"
    monitoring:
      logging:
        level: "INFO"
        
  production:
    parameters:
      ENVIRONMENT: "PROD"
      BATCH_SIZE: "50000"
      ERROR_THRESHOLD: "1000"
    monitoring:
      logging:
        level: "WARN"
      alerts:
        enabled: true
```

### 4. Configuration Templates

Create `config/templates/` with environment-specific templates:

**config/templates/production.yaml**:
```yaml
# Production environment configuration template
extends: "../sample_project_config.yaml"

project:
  environment: "PRODUCTION"

parameters:
  ENVIRONMENT: "PROD"
  ERROR_THRESHOLD: "1000"
  COMMIT_INTERVAL: "50000"
  INPUT_BASE_PATH: "/prod/data/input"
  OUTPUT_BASE_PATH: "/prod/data/output"

spark:
  spark.master: "yarn"
  spark.submit.deployMode: "cluster"
  spark.driver.memory: "4g"
  spark.executor.memory: "8g"
  spark.executor.instances: "10"

monitoring:
  logging:
    level: "WARN"
  alerts:
    enabled: true
    notification_email: "prod-alerts@company.com"
```

**config/templates/development.yaml**:
```yaml
# Development environment configuration template
extends: "../sample_project_config.yaml"

project:
  environment: "DEVELOPMENT"

parameters:
  ENVIRONMENT: "DEV"
  ERROR_THRESHOLD: "10"
  COMMIT_INTERVAL: "1000"
  INPUT_BASE_PATH: "./sample_data/input"
  OUTPUT_BASE_PATH: "./sample_data/output"

spark:
  spark.master: "local[*]"
  spark.driver.memory: "1g"
  spark.executor.memory: "1g"

monitoring:
  logging:
    level: "DEBUG"
  alerts:
    enabled: false
```

## 🐍 Configuration Management Implementation

Create `src/core/config_manager.py`:

```python
"""
Configuration Management System
Handles loading, merging, and validation of configuration files
"""
import os
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass
import re

@dataclass
class ConfigurationError(Exception):
    """Configuration-related errors"""
    message: str
    config_file: Optional[str] = None
    
class ConfigManager:
    """
    Enterprise configuration manager with support for:
    - Environment-specific configurations
    - Parameter substitution
    - Configuration validation
    - Template inheritance
    """
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.logger = logging.getLogger("ConfigManager")
        self.environment = os.getenv("ENVIRONMENT", "development").lower()
        self.loaded_configs: Dict[str, Any] = {}
        
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration for current environment"""
        return self._load_config("spark_config.yaml", "spark")
        
    def get_connections_config(self) -> Dict[str, Any]:
        """Get connections configuration"""
        return self._load_config("connections.yaml")
        
    def get_sample_project_config(self) -> Dict[str, Any]:
        """Get project configuration"""
        return self._load_config("sample_project_config.yaml")
        
    def get_environment_config(self, environment: str = None) -> Dict[str, Any]:
        """Get environment-specific configuration"""
        env = environment or self.environment
        template_file = self.config_dir / "templates" / f"{env}.yaml"
        
        if template_file.exists():
            return self._load_config(str(template_file.relative_to(self.config_dir)))
        else:
            self.logger.warning(f"No environment template found for {env}")
            return {}
    
    def _load_config(self, config_file: str, section: str = None) -> Dict[str, Any]:
        """Load and process configuration file"""
        cache_key = f"{config_file}:{section or 'all'}"
        
        if cache_key in self.loaded_configs:
            return self.loaded_configs[cache_key]
            
        config_path = self.config_dir / config_file
        
        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")
            
        try:
            with open(config_path, 'r') as f:
                config_data = yaml.safe_load(f)
                
            # Handle template inheritance
            if 'extends' in config_data:
                parent_config = self._load_config(config_data['extends'])
                config_data = self._merge_configs(parent_config, config_data)
                
            # Apply environment-specific overrides
            if self.environment in config_data.get('environments', {}):
                env_overrides = config_data['environments'][self.environment]
                config_data = self._merge_configs(config_data, env_overrides)
                
            # Substitute parameters
            config_data = self._substitute_parameters(config_data)
            
            # Extract specific section if requested
            if section and section in config_data:
                config_data = config_data[section]
                
            self.loaded_configs[cache_key] = config_data
            return config_data
            
        except Exception as e:
            raise ConfigurationError(f"Error loading {config_file}: {str(e)}", config_file)
    
    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge configuration dictionaries"""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._merge_configs(result[key], value)
            else:
                result[key] = value
                
        return result
    
    def _substitute_parameters(self, config: Any) -> Any:
        """Substitute environment variables and parameters in configuration"""
        if isinstance(config, dict):
            return {k: self._substitute_parameters(v) for k, v in config.items()}
        elif isinstance(config, list):
            return [self._substitute_parameters(item) for item in config]
        elif isinstance(config, str):
            return self._substitute_string_parameters(config)
        else:
            return config
    
    def _substitute_string_parameters(self, value: str) -> str:
        """Substitute parameters in string values"""
        # Environment variable substitution: ${VAR_NAME}
        env_pattern = r'\$\{([^}]+)\}'
        value = re.sub(env_pattern, lambda m: os.getenv(m.group(1), m.group(0)), value)
        
        # Informatica system parameters: $$SystemDate, $$SystemTimestamp
        import datetime
        now = datetime.datetime.now()
        value = value.replace('$$SystemDate', now.strftime('%Y-%m-%d'))
        value = value.replace('$$SystemTimestamp', now.strftime('%Y-%m-%d %H:%M:%S'))
        
        return value
    
    def merge_configs(self, *configs: Dict[str, Any]) -> Dict[str, Any]:
        """Merge multiple configuration dictionaries"""
        result = {}
        for config in configs:
            result = self._merge_configs(result, config)
        return result
    
    def validate_configuration(self, config: Dict[str, Any]) -> List[str]:
        """Validate configuration and return list of errors"""
        errors = []
        
        # Add validation rules as needed
        if 'spark' in config:
            spark_config = config['spark']
            if 'spark.master' not in spark_config:
                errors.append("spark.master is required in Spark configuration")
                
        return errors
    
    def get_connection_config(self, connection_name: str) -> Dict[str, Any]:
        """Get specific connection configuration"""
        connections = self.get_connections_config()
        
        if connection_name not in connections:
            raise ConfigurationError(f"Connection '{connection_name}' not found in configuration")
            
        return connections[connection_name]
    
    def list_connections(self) -> List[str]:
        """List all available connection names"""
        connections = self.get_connections_config()
        return list(connections.keys())
    
    def export_config(self, output_file: str, config_data: Dict[str, Any] = None):
        """Export current configuration to file"""
        if config_data is None:
            config_data = {
                'spark': self.get_spark_config(),
                'connections': self.get_connections_config(),
                'project': self.get_sample_project_config()
            }
            
        with open(output_file, 'w') as f:
            yaml.dump(config_data, f, default_flow_style=False, indent=2)
            
        self.logger.info(f"Configuration exported to {output_file}")
```

## ✅ Configuration System Testing

Create `tests/test_config_manager.py`:

```python
"""
Tests for configuration management system
"""
import pytest
import tempfile
import yaml
from pathlib import Path
from src.core.config_manager import ConfigManager, ConfigurationError

class TestConfigManager:
    
    def test_load_spark_config(self):
        """Test loading Spark configuration"""
        config_manager = ConfigManager("config")
        spark_config = config_manager.get_spark_config()
        
        assert 'spark.master' in spark_config
        assert 'spark.driver.memory' in spark_config
        
    def test_load_connections_config(self):
        """Test loading connections configuration"""
        config_manager = ConfigManager("config")
        connections = config_manager.get_connections_config()
        
        assert 'HDFS_CONN' in connections
        assert 'HIVE_CONN' in connections
        
    def test_parameter_substitution(self):
        """Test parameter substitution"""
        config_manager = ConfigManager("config") 
        
        # Test environment variable substitution
        import os
        os.environ['TEST_VAR'] = 'test_value'
        
        result = config_manager._substitute_string_parameters('${TEST_VAR}')
        assert result == 'test_value'
        
    def test_config_merging(self):
        """Test configuration merging"""
        config_manager = ConfigManager("config")
        
        base = {'a': 1, 'b': {'x': 1, 'y': 2}}
        override = {'b': {'y': 3, 'z': 4}, 'c': 5}
        
        merged = config_manager._merge_configs(base, override)
        
        assert merged['a'] == 1
        assert merged['b']['x'] == 1  
        assert merged['b']['y'] == 3  # overridden
        assert merged['b']['z'] == 4  # added
        assert merged['c'] == 5       # added
        
    def test_get_connection_config(self):
        """Test getting specific connection configuration"""
        config_manager = ConfigManager("config")
        
        hdfs_config = config_manager.get_connection_config('HDFS_CONN')
        assert hdfs_config['type'] == 'HDFS'
        
        with pytest.raises(ConfigurationError):
            config_manager.get_connection_config('NONEXISTENT_CONN')
```

## 🚀 Usage Examples

Create example usage in `examples/config_usage_demo.py`:

```python
"""
Configuration system usage examples
"""
from src.core.config_manager import ConfigManager
import logging

def demonstrate_config_usage():
    """Demonstrate configuration system features"""
    logging.basicConfig(level=logging.INFO)
    
    # Initialize configuration manager
    config_manager = ConfigManager("config")
    
    # Load different configuration types
    spark_config = config_manager.get_spark_config()
    connections = config_manager.get_connections_config()
    project_config = config_manager.get_sample_project_config()
    
    print("🔧 Spark Configuration Sample:")
    print(f"  Master: {spark_config.get('spark.master')}")
    print(f"  Driver Memory: {spark_config.get('spark.driver.memory')}")
    
    print("\n📊 Available Connections:")
    for conn_name in config_manager.list_connections():
        conn_config = config_manager.get_connection_config(conn_name)
        print(f"  {conn_name}: {conn_config.get('type')} ({conn_config.get('description', 'No description')})")
    
    print("\n📋 Project Parameters:")
    parameters = project_config.get('parameters', {})
    for param_name, param_value in parameters.items():
        print(f"  {param_name}: {param_value}")
    
    # Demonstrate configuration merging
    merged_config = config_manager.merge_configs(
        spark_config,
        {"connections": connections},
        project_config
    )
    
    print(f"\n✅ Configuration system loaded successfully!")
    print(f"   Total configuration keys: {len(merged_config)}")

if __name__ == "__main__":
    demonstrate_config_usage()
```

## ✅ Verification Steps

After implementing the configuration system:

```bash
# 1. Test configuration loading
python -c "from src.core.config_manager import ConfigManager; cm = ConfigManager(); print('✅ Config loading works')"

# 2. Run configuration tests
pytest tests/test_config_manager.py -v

# 3. Run configuration demo
python examples/config_usage_demo.py

# 4. Validate YAML syntax
python -c "import yaml; [yaml.safe_load(open(f)) for f in ['config/connections.yaml', 'config/spark_config.yaml', 'config/sample_project_config.yaml']]; print('✅ All YAML files valid')"
```

## 🔗 Next Steps

With the configuration system implemented, proceed to **`04_XSD_BASE_ARCHITECTURE.md`** to implement the XSD-compliant foundation classes that will use these configurations.

The configuration system now provides:
- ✅ Enterprise-grade configuration management
- ✅ Environment-specific settings
- ✅ Parameter substitution
- ✅ Configuration validation
- ✅ Template inheritance
- ✅ Connection management
- ✅ Comprehensive testing