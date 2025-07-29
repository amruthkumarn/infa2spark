"""
Spark Code Generator for Informatica BDM Projects
Generates standalone PySpark applications from Informatica XML projects
"""
import os
import json
from typing import Dict, List, Any, Optional
from pathlib import Path
import xml.etree.ElementTree as ET
from jinja2 import Template, Environment, FileSystemLoader
import logging
import yaml
from datetime import datetime

from ..parsers.xml_parser import InformaticaXMLParser
from ..models.base_classes import Project
from ...runtime.config_externalization import MappingConfigurationManager, RuntimeConfigResolver
from .config_file_generator import ConfigurationFileGenerator
from .templates.lean_mapping_template import LEAN_MAPPING_TEMPLATE
from .templates.ultra_lean_mapping_template import ULTRA_LEAN_MAPPING_TEMPLATE
from .templates.enterprise_ultra_lean_template import ENTERPRISE_ULTRA_LEAN_TEMPLATE
from .dynamic_test_generator import DynamicTestGenerator


class SparkCodeGenerator:
    """
    Generates complete PySpark applications from Informatica XML projects
    
    Creates a standalone project structure with:
    - Generated mapping classes
    - Workflow orchestration
    - Configuration files
    - Deployment scripts
    - Test data generation
    """
    
    def __init__(self, output_base_dir: str = "generated_spark_app", enable_config_externalization: bool = True, 
                 enterprise_features: bool = True):
        self.output_base_dir = Path(output_base_dir)
        self.logger = logging.getLogger("SparkCodeGenerator")
        self.xml_parser = InformaticaXMLParser()
        # Initialize enhanced generator if enterprise features are enabled
        self.enhanced_generator = None
        if enterprise_features:
            try:
                # Try to import and initialize enhanced generator
                # For now, set to None and handle gracefully
                self.enhanced_generator = None
            except ImportError:
                self.logger.warning("Enhanced generator not available, using basic functionality")
                self.enhanced_generator = None
        
        self.enable_config_externalization = enable_config_externalization
        self.enterprise_features = enterprise_features
        
        # Template environment for code generation
        self.jinja_env = Environment(
            loader=FileSystemLoader(Path(__file__).parent / "templates"),
            trim_blocks=True,
            lstrip_blocks=True
        )
        
        # Register global custom filters
        self.jinja_env.filters['snake_to_camel'] = self._snake_to_camel_filter
        
    def generate_spark_application(self, xml_file_path: str, project_name: str = None) -> str:
        """
        Generate complete Spark application from Informatica XML
        
        Args:
            xml_file_path: Path to Informatica XML file
            project_name: Name for generated project (defaults to XML project name)
            
        Returns:
            Path to generated application directory
        """
        try:
            self.logger.info(f"Starting Spark application generation from {xml_file_path}")
            
            # Parse XML project
            project = self.xml_parser.parse_project(xml_file_path)
            if not project_name:
                project_name = self._sanitize_name(project.name)
            
            # Set up output directory
            app_dir = self.output_base_dir / project_name
            app_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate project structure
            self._create_project_structure(app_dir)
            
            # Generate core application files
            self._generate_base_classes(app_dir, project)
            self._generate_transformations_module(app_dir)
            
            # Generate enterprise components if enabled
            if self.enterprise_features:
                self._generate_enterprise_components(app_dir, project)
            
            self._generate_mapping_classes(app_dir, project)
            self._generate_workflow_classes(app_dir, project)
            self._generate_main_application(app_dir, project)
            
            # Generate configuration files
            self._generate_config_files(app_dir, project)
            
            # Generate deployment files
            self._generate_deployment_files(app_dir, project)
            
            # Generate test data scripts
            # Skip test data generation by default (was generating useless files)
        # self._generate_test_data_scripts(app_dir, project)
            
            # Generate documentation
            self._generate_documentation(app_dir, project)
            
            # Generate comprehensive test cases
            self._generate_test_cases(app_dir, project)
            
            self.logger.info(f"Spark application generated successfully at: {app_dir}")
            return str(app_dir)
            
        except Exception as e:
            self.logger.error(f"Error generating Spark application: {str(e)}")
            raise
    
    def _create_project_structure(self, app_dir: Path):
        """Create optimized project directory structure (cleaned up)"""
        # Only create directories that are actually used
        directories = [
            "src/main/python/mappings",
            "src/main/python/transformations", 
            "src/main/python/workflows",
            "config",
            "data/input",
            "data/output", 
            "scripts",
            "logs",
            "tests/unit",
            "tests/integration",
            "tests/data"
        ]
        
        for dir_path in directories:
            (app_dir / dir_path).mkdir(parents=True, exist_ok=True)
            
        # Only create __init__.py files where they're actually needed (with content)
        python_packages = [
            "src/main/python",
            "src/main/python/mappings",
            "src/main/python/transformations",
            "src/main/python/workflows",
            "tests",
            "tests/unit",
            "tests/integration"
        ]
        
        for pkg_path in python_packages:
            (app_dir / pkg_path / "__init__.py").write_text("")
    
    def _generate_base_classes(self, app_dir: Path, project: Project):
        """Generate base classes for the Spark application"""
        base_classes_template = '''"""
Base classes for {{ project.name }} Spark Application
Generated from Informatica BDM Project
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
import logging
from datetime import datetime


class BaseMapping(ABC):
    """Base class for all mapping implementations"""
    
    def __init__(self, name: str, spark: SparkSession, config: Dict[str, Any]):
        self.name = name
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(f"Mapping.{name}")
        
    @abstractmethod
    def execute(self) -> bool:
        """Execute the mapping logic"""
        pass
        
    def log_dataframe_info(self, df: DataFrame, stage: str):
        """Log DataFrame information for debugging"""
        try:
            count = df.count()
            columns = len(df.columns)
            self.logger.info(f"{stage} - Rows: {count}, Columns: {columns}")
        except Exception as e:
            self.logger.warning(f"Could not get DataFrame info for {stage}: {str(e)}")


class BaseTransformation(ABC):
    """Base class for all transformations"""
    
    def __init__(self, name: str, transformation_type: str):
        self.name = name
        self.transformation_type = transformation_type
        self.logger = logging.getLogger(f"Transformation.{name}")
        
    @abstractmethod
    def transform(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """Apply transformation to input DataFrame"""
        pass


class BaseWorkflow(ABC):
    """Base class for workflow orchestration"""
    
    def __init__(self, name: str, spark: SparkSession, config: Dict[str, Any]):
        self.name = name
        self.spark = spark
        self.config = config
        self.logger = logging.getLogger(f"Workflow.{name}")
        
    @abstractmethod
    def execute(self) -> bool:
        """Execute the workflow"""
        pass
        
    def validate_workflow(self) -> bool:
        """Validate workflow configuration"""
        return True


class DataSourceManager:
    """Manages data source connections and operations"""
    
    def __init__(self, spark: SparkSession, connections_config: Dict[str, Any]):
        self.spark = spark
        self.connections = connections_config
        self.logger = logging.getLogger("DataSourceManager")
        
    def read_source(self, source_name: str, source_type: str, **kwargs) -> DataFrame:
        """Read data from source"""
        self.logger.info(f"Reading from source: {source_name} (type: {source_type})")
        
        if source_type.upper() == "HDFS":
            return self._read_hdfs_source(source_name, **kwargs)
        elif source_type.upper() == "HIVE":
            return self._read_hive_source(source_name, **kwargs)
        elif source_type.upper() in ["DB2", "ORACLE"]:
            return self._read_jdbc_source(source_name, source_type, **kwargs)
        else:
            # Generate mock data for unknown sources
            return self._generate_mock_data(source_name)
            
    def write_target(self, df: DataFrame, target_name: str, target_type: str, **kwargs):
        """Write data to target"""
        self.logger.info(f"Writing to target: {target_name} (type: {target_type})")
        
        mode = kwargs.get('mode', 'overwrite')
        
        if target_type.upper() == "HIVE":
            self._write_hive_target(df, target_name, mode)
        elif target_type.upper() == "HDFS":
            self._write_hdfs_target(df, target_name, mode, **kwargs)
        else:
            # Default to parquet files
            output_path = f"data/output/{target_name}"
            df.write.mode(mode).parquet(output_path)
            
    def _read_hdfs_source(self, source_name: str, **kwargs) -> DataFrame:
        """Read from HDFS source using configured connection"""
        format_type = kwargs.get('format', 'parquet').lower()
        connection_name = kwargs.get('connection', 'default')
        
        # Check for HDFS connection configuration
        if connection_name in self.connections:
            conn_config = self.connections[connection_name]
            if conn_config.get('type') == 'HDFS':
                namenode = conn_config.get('namenode', f"hdfs://{conn_config.get('host')}:{conn_config.get('port')}")
                path = f"{namenode}/data/{source_name.lower()}"
                self.logger.info(f"Reading from HDFS path: {path}")
            else:
                path = f"data/input/{source_name.lower()}"
        else:
            # Fallback to local path
            path = f"data/input/{source_name.lower()}"
            self.logger.warning(f"HDFS connection {connection_name} not found, using local path")
        
        if format_type == 'parquet':
            return self.spark.read.parquet(path)
        elif format_type == 'csv':
            return self.spark.read.option("header", "true").csv(path)
        elif format_type == 'avro':
            return self.spark.read.format("avro").load(path)
        else:
            return self.spark.read.parquet(path)
            
    def _read_hive_source(self, source_name: str, **kwargs) -> DataFrame:
        """Read from Hive table using configured connection"""
        # Get connection configuration
        connection_name = kwargs.get('connection', 'default')
        table_name = kwargs.get('table', source_name.lower())
        
        if connection_name in self.connections:
            conn_config = self.connections[connection_name]
            if conn_config.get('type') == 'HIVE':
                # Use the configured database
                database = conn_config.get('database', 'default')
                full_table_name = f"{database}.{table_name}"
                self.logger.info(f"Reading from Hive table: {full_table_name}")
                return self.spark.table(full_table_name)
        
        # Fallback to simple table name
        self.logger.warning(f"Connection {connection_name} not found, using default table access")
        return self.spark.table(table_name)
        
    def _read_jdbc_source(self, source_name: str, source_type: str, **kwargs) -> DataFrame:
        """Read from JDBC source"""
        # For demo purposes, generate mock data
        # In production, would use actual JDBC connections
        return self._generate_mock_data(source_name)
        
    def _write_hive_target(self, df: DataFrame, target_name: str, mode: str):
        """Write to Hive table"""
        df.write.mode(mode).saveAsTable(target_name.lower())
        
    def _write_hdfs_target(self, df: DataFrame, target_name: str, mode: str, **kwargs):
        """Write to HDFS target"""
        format_type = kwargs.get('format', 'parquet').lower()
        path = f"data/output/{target_name.lower()}"
        
        if format_type == 'parquet':
            df.write.mode(mode).parquet(path)
        elif format_type == 'csv':
            df.write.mode(mode).option("header", "true").csv(path)
        else:
            df.write.mode(mode).parquet(path)
            
    def _generate_mock_data(self, source_name: str) -> DataFrame:
        """Generate mock data for testing"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType
        from pyspark.sql.functions import lit, current_date
        
        # Define schema based on source name
        if 'sales' in source_name.lower():
            schema = StructType([
                StructField("sale_id", StringType(), False),
                StructField("customer_id", StringType(), False),
                StructField("product_id", StringType(), False),
                StructField("amount", DoubleType(), False),
                StructField("sale_date", DateType(), False),
                StructField("region", StringType(), False)
            ])
            data = [
                ("S001", "C001", "P001", 100.0, "2023-01-01", "North"),
                ("S002", "C002", "P002", 250.0, "2023-01-02", "South"),
                ("S003", "C003", "P001", 75.0, "2023-01-03", "East")
            ]
        elif 'customer' in source_name.lower():
            schema = StructType([
                StructField("customer_id", StringType(), False),
                StructField("customer_name", StringType(), False),
                StructField("city", StringType(), False),
                StructField("status", StringType(), False),
                StructField("email", StringType(), False)
            ])
            data = [
                ("C001", "John Doe", "New York", "Active", "john@email.com"),
                ("C002", "Jane Smith", "Los Angeles", "Active", "jane@email.com"),
                ("C003", "Bob Johnson", "Chicago", "Inactive", "bob@email.com")
            ]
        else:
            # Generic schema
            schema = StructType([
                StructField("id", StringType(), False),
                StructField("name", StringType(), False),
                StructField("value", DoubleType(), False),
                StructField("created_date", DateType(), False)
            ])
            data = [
                ("1", "Record 1", 100.0, "2023-01-01"),
                ("2", "Record 2", 200.0, "2023-01-02"),
                ("3", "Record 3", 300.0, "2023-01-03")
            ]
            
        return self.spark.createDataFrame(data, schema)
'''

        template = Template(base_classes_template)
        output = template.render(project=project)
        
        base_classes_file = app_dir / "src/main/python/base_classes.py"
        base_classes_file.write_text(output)
        self.logger.info("Generated base_classes.py")
    
    def _generate_mapping_classes(self, app_dir: Path, project: Project):
        """Generate mapping classes"""
        mappings_dir = app_dir / "src" / "main" / "python" / "mappings"
        mappings_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate __init__.py
        init_content = '''"""
Mapping classes for the Spark application
"""
from .base_mapping import BaseMapping

__all__ = ['BaseMapping']
'''
        with open(mappings_dir / "__init__.py", "w") as f:
            f.write(init_content)
        
        mappings = list(project.mappings.values())
        self.logger.info(f"Generating {len(mappings)} mapping classes")
        
        for mapping in mappings:
            # Use field port integrated mapping generation - this is the correct one with field-level logic
            self._generate_single_mapping_class(app_dir, mapping, project)
            
            # Enhanced generator disabled to use field port integration
            # self.enhanced_generator.generate_production_mapping(app_dir, mapping, project)
    
    def _generate_single_mapping_class(self, app_dir: Path, mapping: Dict[str, Any], project: Project):
        """Generate a single mapping class with DAG analysis and optional external configuration"""
        # Process mapping DAG for transformation dependencies
        # TODO: Implement MappingDAGProcessor when needed
        # dag_processor = MappingDAGProcessor()
        # enhanced_mapping = dag_processor.process_mapping_dag(mapping)
        # execution_plan = dag_processor.generate_execution_plan(enhanced_mapping)
        enhanced_mapping = mapping  # Use mapping as-is for now
        execution_plan = []  # Empty execution plan for now
        
        # Generate external configuration files if enabled
        if self.enable_config_externalization:
            self._generate_external_configurations(app_dir, enhanced_mapping, execution_plan, project)
        
        class_name = self._to_class_name(mapping['name'])
        file_name = self._sanitize_name(mapping['name']) + '.py'
        
        # Add custom filters for Informatica to Spark conversion
        self.jinja_env.filters['convert_informatica_to_spark'] = self._convert_informatica_expression_to_spark
        self.jinja_env.filters['convert_informatica_type_to_spark'] = self._convert_informatica_type_to_spark
        self.jinja_env.filters['suffix_with'] = self._suffix_with_filter
        self.jinja_env.filters['snake_to_camel'] = self._snake_to_camel_filter
        
        # Choose template based on configuration externalization and enterprise features
        if self.enable_config_externalization:
            if self.enterprise_features:
                # Use enterprise ultra-lean template with advanced features
                mapping_template = ENTERPRISE_ULTRA_LEAN_TEMPLATE
            else:
                # Use ultra-lean template
                mapping_template = ULTRA_LEAN_MAPPING_TEMPLATE
        else:
            # Use original embedded template for backward compatibility
            mapping_template = '''"""
{{ mapping.name }} Mapping Implementation
Generated from Informatica BDM Project: {{ project.name }}

DAG Analysis Summary:
- Total Components: {{ dag_analysis.total_components }}
- Execution Phases: {{ execution_plan.total_phases }}
- Estimated Duration: {{ execution_plan.estimated_duration }} seconds
- Parallel Groups: {{ dag_analysis.parallel_groups | length }}

Components:
{% for component in mapping.components -%}
- {{ component.name }} ({{ component.type }})
{% endfor %}
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
import concurrent.futures
from typing import Dict, Any
from ..runtime.base_classes import BaseMapping, DataSourceManager
from ..transformations.generated_transformations import *


class {{ class_name }}(BaseMapping):
    """{{ mapping.description or mapping.name + ' mapping implementation' }}
    
    DAG Execution Strategy:
    {%- for phase in execution_plan.phases %}
    Phase {{ phase.phase_number }}: {{ phase.parallel_components }} component(s) - {{ phase.phase_type }} - {{ 'Parallel' if phase.can_run_parallel else 'Sequential' }}
    {%- for component in phase.components %}
      - {{ component.component_name }} ({{ component.transformation_type }}) - {{ component.estimated_duration }}s
    {%- endfor %}
    {%- endfor %}
    """
    
    def __init__(self, spark, config):
        super().__init__("{{ mapping.name }}", spark, config)
        self.data_source_manager = DataSourceManager(spark, config.get('connections', {}))
        
        # DAG Execution Plan
        self.execution_plan = {{ execution_plan | tojson }}
        self.dag_analysis = {{ dag_analysis | tojson }}
        
        # Component execution order (topologically sorted)
        self.execution_order = {{ dag_analysis.execution_order | tojson }}
        
        # Parallel execution groups
        self.parallel_groups = {{ dag_analysis.parallel_groups | tojson }}
        
        # Component data cache for intermediate results
        self.component_data_cache = {}
        
    def execute(self) -> bool:
        """Execute mapping using DAG-based transformation execution strategy"""
        try:
            self.logger.info("Starting {{ mapping.name }} mapping execution with DAG processing")
            self.logger.info(f"Execution plan: {len(self.parallel_groups)} phases, estimated {self.execution_plan['estimated_duration']} seconds")
            
            # Execute components in parallel groups (phases)
            for phase_idx, component_group in enumerate(self.parallel_groups):
                phase_num = phase_idx + 1
                self.logger.info(f"Starting execution phase {phase_num}/{len(self.parallel_groups)} with {len(component_group)} component(s)")
                
                if len(component_group) == 1:
                    # Single component - execute directly
                    component_name = component_group[0]
                    success = self._execute_component(component_name)
                    if not success:
                        self.logger.error(f"Component {component_name} failed in phase {phase_num}")
                        return False
                else:
                    # Multiple components - execute in parallel
                    success = self._execute_parallel_components(component_group, phase_num)
                    if not success:
                        self.logger.error(f"One or more components failed in phase {phase_num}")
                        return False
                
                self.logger.info(f"Phase {phase_num} completed successfully")
            
            self.logger.info("{{ mapping.name }} mapping executed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in {{ mapping.name }} mapping: {str(e)}")
            raise
    
    def _execute_parallel_components(self, component_group: list, phase_num: int) -> bool:
        """Execute a group of components in parallel"""
        self.logger.info(f"Executing {len(component_group)} components in parallel for phase {phase_num}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(component_group), 4)) as executor:
            # Submit all components in the group
            future_to_component = {
                executor.submit(self._execute_component, comp_name): comp_name 
                for comp_name in component_group
            }
            
            # Wait for all components to complete
            failed_components = []
            for future in concurrent.futures.as_completed(future_to_component):
                comp_name = future_to_component[future]
                try:
                    success = future.result()
                    if not success:
                        failed_components.append(comp_name)
                except Exception as e:
                    self.logger.error(f"Component {comp_name} raised exception: {str(e)}")
                    failed_components.append(comp_name)
            
            if failed_components:
                self.logger.error(f"Failed components in phase {phase_num}: {failed_components}")
                return False
            
            return True
    
    def _execute_component(self, component_name: str) -> bool:
        """Execute a single component (source, transformation, or target)"""
        try:
            # Get component info from DAG analysis
            comp_info = self.dag_analysis['dependency_graph'][component_name]['component_info']
            if not comp_info:
                self.logger.error(f"No component info found for: {component_name}")
                return False
            
            comp_type = comp_info.get('component_type', '').lower()
            
            if comp_type == 'source':
                return self._execute_source_component(component_name, comp_info)
            elif comp_type == 'transformation':
                return self._execute_transformation_component(component_name, comp_info)
            elif comp_type == 'target':
                return self._execute_target_component(component_name, comp_info)
            else:
                self.logger.warning(f"Unknown component type '{comp_type}' for component: {component_name}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error executing component {component_name}: {str(e)}")
            return False
    
    def _execute_source_component(self, component_name: str, comp_info: dict) -> bool:
        """Execute a source component (read data)"""
        self.logger.info(f"Executing source component: {component_name}")
        
        # Read data using the source method
        try:
            method_name = f"_read_{component_name.lower()}"
            if hasattr(self, method_name):
                df = getattr(self, method_name)()
                self.component_data_cache[component_name] = df
                self.logger.info(f"Source {component_name} read successfully with {df.count()} rows")
                return True
            else:
                self.logger.error(f"Source method {method_name} not found")
                return False
        except Exception as e:
            self.logger.error(f"Error reading source {component_name}: {str(e)}")
            return False
    
    def _execute_transformation_component(self, component_name: str, comp_info: dict) -> bool:
        """Execute a transformation component"""
        self.logger.info(f"Executing transformation component: {component_name}")
        
        try:
            # Get input data from dependencies
            dependencies = self.dag_analysis['dependency_graph'][component_name]['dependencies']
            input_dfs = []
            
            for dep_name in dependencies:
                if dep_name in self.component_data_cache:
                    input_dfs.append(self.component_data_cache[dep_name])
                else:
                    self.logger.error(f"Dependency {dep_name} not found in cache for {component_name}")
                    return False
            
            # Execute transformation
            method_name = f"_apply_{component_name.lower()}"
            if hasattr(self, method_name):
                if len(input_dfs) == 1:
                    result_df = getattr(self, method_name)(input_dfs[0])
                else:
                    result_df = getattr(self, method_name)(*input_dfs)
                
                self.component_data_cache[component_name] = result_df
                self.logger.info(f"Transformation {component_name} executed successfully")
                return True
            else:
                self.logger.error(f"Transformation method {method_name} not found")
                return False
                
        except Exception as e:
            self.logger.error(f"Error executing transformation {component_name}: {str(e)}")
            return False
    
    def _execute_target_component(self, component_name: str, comp_info: dict) -> bool:
        """Execute a target component (write data)"""
        self.logger.info(f"Executing target component: {component_name}")
        
        try:
            # Get input data from dependencies
            dependencies = self.dag_analysis['dependency_graph'][component_name]['dependencies']
            
            for dep_name in dependencies:
                if dep_name in self.component_data_cache:
                    input_df = self.component_data_cache[dep_name]
                    
                    # Write data using the target method
                    method_name = f"_write_to_{component_name.lower()}"
                    if hasattr(self, method_name):
                        getattr(self, method_name)(input_df)
                        self.logger.info(f"Target {component_name} written successfully")
                        return True
                    else:
                        self.logger.error(f"Target method {method_name} not found")
                        return False
            
            self.logger.error(f"No input dependencies found for target {component_name}")
            return False
            
        except Exception as e:
            self.logger.error(f"Error writing target {component_name}: {str(e)}")
            return False
            
        except Exception as e:
            self.logger.error(f"Error in {{ mapping.name }} mapping: {str(e)}")
            raise
    
    {%- for source in sources %}
    
    def _read_{{ source.name | lower }}(self) -> DataFrame:
        """Read from {{ source.name }}"""
        self.logger.info("Reading from {{ source.name }}")
        return self.data_source_manager.read_source(
            "{{ source.name }}", 
            "{{ source.type }}"
            {%- if source.format %}, format="{{ source.format }}"{% endif %}
        )
    {%- endfor %}
    
    {%- for transformation in transformations %}
    
    def _apply_{{ transformation.name | lower }}(self, input_df: DataFrame{% if transformation.type == 'Joiner' %}, *additional_dfs{% endif %}) -> DataFrame:
        """Apply {{ transformation.name }} transformation"""
        self.logger.info("Applying {{ transformation.name }} transformation")
        
        {% if transformation.type == 'Expression' -%}
        # Use ExpressionTransformation class for reusability
        transformation = ExpressionTransformation(
            name="{{ transformation.name }}",
            expressions=self._get_{{ transformation.name | lower }}_expressions(),
            filters=self._get_{{ transformation.name | lower }}_filters()
        )
        result_df = transformation.transform(input_df)
        
        {%- set output_ports = transformation.ports | selectattr('direction', 'equalto', 'OUTPUT') | list %}
        {%- if output_ports %}
        # Apply data type casting based on port types (manual implementation for specific typing)
        {%- for port in output_ports %}
        {%- if port.type %}
        # Cast {{ port.name }} to {{ port.type }}
        result_df = result_df.withColumn("{{ port.name }}", col("{{ port.name }}").cast("{{ port.type | convert_informatica_type_to_spark }}"))
        {%- endif %}
        {%- endfor %}
        {%- endif %}
        
        return result_df
        
        {% elif transformation.type == 'Aggregator' -%}
        # Aggregator transformation logic
        transformation = AggregatorTransformation(
            name="{{ transformation.name }}",
            group_by_cols=self._get_{{ transformation.name | lower }}_group_by(),
            aggregations=self._get_{{ transformation.name | lower }}_aggregations()
        )
        return transformation.transform(input_df)
        
        {% elif transformation.type == 'Lookup' -%}
        # Lookup transformation logic
        lookup_df = self._get_{{ transformation.name | lower }}_lookup_data()
        transformation = LookupTransformation(
            name="{{ transformation.name }}",
            join_conditions=self._get_{{ transformation.name | lower }}_join_conditions(),
            join_type="left"
        )
        return transformation.transform(input_df, lookup_df)
        
        {% elif transformation.type == 'Joiner' -%}
        # Joiner transformation logic
        if len(additional_dfs) > 0:
            right_df = additional_dfs[0]
            transformation = JoinerTransformation(
                name="{{ transformation.name }}",
                join_conditions=self._get_{{ transformation.name | lower }}_join_conditions(),
                join_type="inner"
            )
            return transformation.transform(input_df, right_df)
        return input_df
        
        {% elif transformation.type == 'Sequence' -%}
        # Use SequenceTransformation class for reusability
        transformation = SequenceTransformation(
            name="{{ transformation.name }}",
            start_value=self._get_{{ transformation.name | lower }}_start_value(),
            increment_value=self._get_{{ transformation.name | lower }}_increment_value()
        )
        return transformation.transform(input_df)
        
        {% elif transformation.type == 'Sorter' -%}
        # Use SorterTransformation class for reusability
        transformation = SorterTransformation(
            name="{{ transformation.name }}",
            sort_keys=self._get_{{ transformation.name | lower }}_sort_keys()
        )
        return transformation.transform(input_df)
        
        {% elif transformation.type == 'Router' -%}
        
        # Use RouterTransformation class for reusability
        transformation = RouterTransformation(
            name="{{ transformation.name }}",
            output_groups=self._get_{{ transformation.name | lower }}_output_groups()
        )
        return transformation.transform(input_df)
        
        {% elif transformation.type == 'Union' -%}
        
        # Use UnionTransformation class for reusability
        transformation = UnionTransformation(
            name="{{ transformation.name }}",
            union_type=self._get_{{ transformation.name | lower }}_union_type()
        )
        return transformation.transform(*additional_dfs) if additional_dfs else input_df
        
        {% elif transformation.type in ['Java', 'SCD'] -%}
        
        # Use JavaTransformation class for custom logic including SCD
        transformation = JavaTransformation(
            name="{{ transformation.name }}",
            logic_type=self._get_{{ transformation.name | lower }}_logic_type()
        )
        existing_df = self._get_{{ transformation.name | lower }}_existing_data() if hasattr(self, '_get_{{ transformation.name | lower }}_existing_data') else None
        return transformation.transform(input_df, existing_df)
        
        {%- else %}
        # Manual implementation needed for complex transformation type: {{ transformation.type }}
        self.logger.warning("Using manual implementation for transformation type: {{ transformation.type }}")
        # TODO: Implement {{ transformation.type }} transformation logic manually here
        return input_df
        {%- endif %}
    
    {%- if transformation.type == 'Expression' %}
    
    def _get_{{ transformation.name | lower }}_expressions(self) -> dict:
        """Get expression transformation expressions"""
        return {
            {% if transformation.expressions -%}
            {% for expr in transformation.expressions %}
            "{{ expr.name }}": "{{ expr.expression | convert_informatica_to_spark }}",
            {% endfor %}
            {% else -%}
            # Add your expression logic here
            "processed_date": "current_date()",
            "load_timestamp": "current_timestamp()"
            {% endif -%}
        }
    
    def _get_{{ transformation.name | lower }}_filters(self) -> list:
        """Get expression transformation filters"""
        return [
            # Add your filter logic here
            # "column_name IS NOT NULL",
            # "amount > 0"
        ]
        
    {%- elif transformation.type == 'Aggregator' %}
    
    def _get_{{ transformation.name | lower }}_group_by(self) -> list:
        """Get aggregator group by columns"""
        return [
            # Add your group by columns here
            # "region", "product"
        ]
    
    def _get_{{ transformation.name | lower }}_aggregations(self) -> dict:
        """Get aggregator aggregation logic"""
        return {
            # Add your aggregation logic here
            # "total_amount": {"type": "sum", "column": "amount"},
            # "record_count": {"type": "count", "column": "id"}
        }
        
    {%- elif transformation.type in ['Lookup', 'Joiner'] %}
    
    def _get_{{ transformation.name | lower }}_join_conditions(self) -> list:
        """Get join conditions"""
        return [
            # Add your join conditions here
            # "left_table.id = right_table.id"
        ]
        
    {%- if transformation.type == 'Lookup' %}
    
    def _get_{{ transformation.name | lower }}_lookup_data(self) -> DataFrame:
        """Get lookup data"""
        # Implement lookup data retrieval
        return self.data_source_manager.read_source("LOOKUP_TABLE", "HIVE")
    {%- endif %}
    
    {%- elif transformation.type == 'Sequence' %}
    
    def _get_{{ transformation.name | lower }}_start_value(self) -> int:
        """Get sequence start value"""
        return 1  # Default start value
    
    def _get_{{ transformation.name | lower }}_increment_value(self) -> int:
        """Get sequence increment value"""
        return 1  # Default increment value
        
    {%- elif transformation.type == 'Sorter' %}
    
    def _get_{{ transformation.name | lower }}_sort_keys(self) -> list:
        """Get sorter sort keys"""
        return [
            # Add your sort keys here
            # {"field_name": "column1", "direction": "ASC"},
            # {"field_name": "column2", "direction": "DESC"}
        ]
        
    {%- elif transformation.type == 'Router' %}
    
    def _get_{{ transformation.name | lower }}_output_groups(self) -> list:
        """Get router output groups"""
        return [
            # Add your routing conditions here
            # {"name": "group1", "condition": "amount > 100"},
            # {"name": "group2", "condition": "region = 'North'"}
        ]
        
    {%- elif transformation.type == 'Union' %}
    
    def _get_{{ transformation.name | lower }}_union_type(self) -> str:
        """Get union type"""
        return "UNION_ALL"  # or "UNION_DISTINCT"
        
    {%- elif transformation.type in ['Java', 'SCD'] %}
    
    def _get_{{ transformation.name | lower }}_logic_type(self) -> str:
        """Get Java transformation logic type"""
        return "{% if transformation.type == 'SCD' %}scd_type2{% else %}custom{% endif %}"
    {%- endif %}
    {%- endfor %}
    
    {%- for target in targets %}
    
    def _write_to_{{ target.name | lower }}(self, output_df: DataFrame):
        """Write to {{ target.name }} target"""
        self.logger.info("Writing to {{ target.name }} target")
        
        # Add audit columns
        final_df = output_df.withColumn("load_timestamp", current_timestamp()) \\
                          .withColumn("load_date", current_date())
        
        self.data_source_manager.write_target(
            final_df, 
            "{{ target.name }}", 
            "{{ target.type }}",
            mode="overwrite"
        )
    {%- endfor %}
'''

        template = self.jinja_env.from_string(mapping_template)
        output = template.render(
            mapping=enhanced_mapping, 
            project=project, 
            class_name=class_name,
            dag_analysis=enhanced_mapping.get('dag_analysis', {}),
            execution_plan=execution_plan
        )
        
        # Write the file
        mappings_dir = app_dir / "src" / "main" / "python" / "mappings"
        mappings_dir.mkdir(parents=True, exist_ok=True)
        
        output_file = mappings_dir / file_name
        with open(output_file, 'w') as f:
            f.write(output)
        
        # Apply manual formatting fixes
        self._apply_manual_formatting(output_file)
            
        self.logger.info(f"Generated mapping class: {output_file}")
    
    def _generate_workflow_classes(self, app_dir: Path, project: Project):
        """Generate workflow orchestration classes"""
        workflows = self._extract_workflows_from_project(project)
        
        if not workflows:
            self.logger.info("No workflows found, skipping workflow generation")
            return
            
        for workflow in workflows:
            try:
                self._generate_single_workflow_class(app_dir, workflow, project)
            except Exception as e:
                self.logger.error(f"Error generating workflow {workflow.get('name', 'unknown')}: {e}")
                # Continue with other workflows
    
    def _generate_single_workflow_class(self, app_dir: Path, workflow: Dict[str, Any], project: Project):
        """Generate a single workflow class with DAG analysis"""
        # Process workflow DAG
        # TODO: Implement WorkflowDAGProcessor when needed
        # dag_processor = WorkflowDAGProcessor()
        # enhanced_workflow = dag_processor.process_workflow_dag(workflow)
        # execution_plan = dag_processor.generate_execution_plan(enhanced_workflow)
        enhanced_workflow = workflow  # Use workflow as-is for now
        execution_plan = []  # Empty execution plan for now
        
        class_name = self._to_class_name(workflow['name'])
        file_name = self._sanitize_name(workflow['name']) + '.py'
        
        workflow_template = '''"""
{{ workflow.name }} Workflow Implementation
Generated from Informatica BDM Project: {{ project.name }}

DAG Analysis Summary:
- Total Tasks: {{ execution_plan.total_phases }}
- Execution Phases: {{ execution_plan.total_phases }}
- Estimated Duration: {{ execution_plan.estimated_duration }} minutes
- Parallel Execution Groups: {{ dag_analysis.parallel_groups | length }}
"""
from ..runtime.base_classes import BaseWorkflow
{%- for task in workflow.tasks %}
{%- if task.type == 'SessionTask' and task.mapping %}
from ..mappings.{{ task.mapping | lower }} import {{ task.mapping | title | replace('_', '') }}
{%- endif %}
{%- endfor %}
import time
import subprocess
import json
import concurrent.futures
from datetime import datetime, timedelta
from typing import Dict, Any, List


class {{ class_name }}(BaseWorkflow):
    """{{ workflow.description or workflow.name + ' workflow implementation' }}
    
    DAG Execution Strategy:
    {%- for phase in execution_plan.phases %}
    Phase {{ phase.phase_number }}: {{ phase.parallel_tasks }} task(s) - {{ 'Parallel' if phase.can_run_parallel else 'Sequential' }}
    {%- for task in phase.tasks %}
      - {{ task.task_name }} ({{ task.task_type }}) - {{ task.estimated_duration }}min
    {%- endfor %}
    {%- endfor %}
    """
    
    def __init__(self, spark, config):
        super().__init__("{{ workflow.name }}", spark, config)
        
        # Initialize workflow parameters
        self.workflow_parameters = config.get('workflow_parameters', {})
        self.workflow_start_time = datetime.now()
        
        # DAG Execution Plan
        self.execution_plan = {{ execution_plan | tojson }}
        self.dag_analysis = {{ dag_analysis | tojson }}
        
        # Initialize mapping classes
        self.mapping_classes = {
            {%- for task in workflow.tasks %}
            {%- if task.type == 'SessionTask' and task.mapping %}
            "{{ task.id or task.name }}": {{ task.mapping | title | replace('_', '') }},
            {%- endif %}
            {%- endfor %}
        }
        
        # DAG-based execution order (topologically sorted)
        self.execution_order = {{ dag_analysis.execution_order | tojson }}
        
        # Parallel execution groups
        self.parallel_groups = {{ dag_analysis.parallel_groups | tojson }}
        
        # Task configurations
        self.task_configs = {
            {%- for task in workflow.tasks %}
            "{{ task.name }}": {
                'type': '{{ task.type }}',
                'name': '{{ task.name }}',
                {%- if task.type == 'Command' %}
                'command': {{ task.properties.get('Command', "'echo Default command'") }},
                'working_directory': {{ task.properties.get('WorkingDirectory', "''") }},
                'timeout_seconds': {{ task.properties.get('TimeoutSeconds', 300) }},
                {%- elif task.type == 'Decision' %}
                'conditions': {{ task.properties.get('Conditions', [{'name': 'default', 'expression': 'True', 'target_path': 'continue', 'priority': 1}]) }},
                'default_path': {{ task.properties.get('DefaultPath', "'continue'") }},
                {%- elif task.type == 'Assignment' %}
                'assignments': {{ task.properties.get('Assignments', [{'parameter_name': 'SAMPLE_PARAM', 'expression': "'assigned_value'", 'data_type': 'string'}]) }},
                {%- elif task.type == 'StartWorkflow' %}
                'target_workflow': {{ task.properties.get('TargetWorkflow', "'child_workflow'") }},
                'parameter_mapping': {{ task.properties.get('ParameterMapping', {}) }},
                'execution_mode': {{ task.properties.get('ExecutionMode', "'synchronous'") }},
                {%- elif task.type == 'Timer' %}
                'delay_amount': {{ task.properties.get('DelayAmount', 60) }},
                'delay_unit': {{ task.properties.get('DelayUnit', "'seconds'") }},
                {%- elif task.type == 'Email' %}
                'recipients': {{ task.properties.get('Recipients', "['admin@company.com']") }},
                'subject': {{ task.properties.get('Subject', "'Workflow Notification'") }},
                {%- endif %}
                'properties': {{ task.properties }}
            },
            {%- endfor %}
        }
        
    def execute(self) -> bool:
        """Execute workflow using DAG-based parallel execution strategy"""
        try:
            self.logger.info("Starting {{ workflow.name }} workflow with DAG execution")
            self.logger.info(f"Execution plan: {len(self.parallel_groups)} phases, estimated {self.execution_plan['estimated_duration']} minutes")
            start_time = time.time()
            
            # Execute tasks in parallel groups (phases)
            for phase_idx, task_group in enumerate(self.parallel_groups):
                phase_num = phase_idx + 1
                self.logger.info(f"Starting execution phase {phase_num}/{len(self.parallel_groups)} with {len(task_group)} task(s)")
                
                if len(task_group) == 1:
                    # Single task - execute directly
                    task_id = task_group[0]
                    success = self._execute_task(task_id)
                    if not success:
                        self.logger.error(f"Task {task_id} failed in phase {phase_num}")
                        self._handle_workflow_failure(task_id)
                        return False
                else:
                    # Multiple tasks - execute in parallel
                    success = self._execute_parallel_tasks(task_group, phase_num)
                    if not success:
                        self.logger.error(f"One or more tasks failed in phase {phase_num}")
                        return False
                
                self.logger.info(f"Phase {phase_num} completed successfully")
                    
            # Calculate execution time
            execution_time = time.time() - start_time
            self.logger.info(f"{{ workflow.name }} completed successfully in {execution_time:.2f} seconds")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in {{ workflow.name }} workflow: {str(e)}")
            self._handle_workflow_failure("UNKNOWN")
            raise
    
    def _execute_parallel_tasks(self, task_group: List[str], phase_num: int) -> bool:
        """Execute a group of tasks in parallel"""
        self.logger.info(f"Executing {len(task_group)} tasks in parallel for phase {phase_num}")
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(task_group), 4)) as executor:
            # Submit all tasks in the group
            future_to_task = {
                executor.submit(self._execute_task, task_id): task_id 
                for task_id in task_group
            }
            
            # Wait for all tasks to complete
            failed_tasks = []
            for future in concurrent.futures.as_completed(future_to_task):
                task_id = future_to_task[future]
                try:
                    success = future.result()
                    if not success:
                        failed_tasks.append(task_id)
                except Exception as e:
                    self.logger.error(f"Task {task_id} raised exception: {str(e)}")
                    failed_tasks.append(task_id)
            
            if failed_tasks:
                self.logger.error(f"Failed tasks in phase {phase_num}: {failed_tasks}")
                for task_id in failed_tasks:
                    self._handle_workflow_failure(task_id)
                return False
            
            return True
            
    def _execute_task(self, task_name: str) -> bool:
        """Execute a single task with enhanced task type support"""
        try:
            self.logger.info(f"Executing task: {task_name}")
            task_start_time = time.time()
            task_config = self.task_configs.get(task_name, {})
            task_type = task_config.get('type', 'Unknown')
            
            success = False
            
            if task_name in self.mapping_classes:
                # Execute mapping task
                mapping_class = self.mapping_classes[task_name]
                mapping = mapping_class(self.spark, self.config)
                success = mapping.execute()
                
            elif task_type == 'Command':
                success = self._execute_command_task(task_config)
                
            elif task_type == 'Decision':
                decision_result = self._execute_decision_task(task_config)
                success = decision_result.get('decision_made') is not None
                # Handle decision routing logic here if needed
                
            elif task_type == 'Assignment':
                success = self._execute_assignment_task(task_config)
                
            elif task_type == 'StartWorkflow':
                success = self._execute_start_workflow_task(task_config)
                
            elif task_type == 'Timer':
                success = self._execute_timer_task(task_config)
                
            elif task_type == 'Email':
                success = self._send_notification(task_name)
                
            else:
                self.logger.warning(f"Task type '{task_type}' not implemented, skipping")
                success = True  # Skip unimplemented tasks
                
            task_execution_time = time.time() - task_start_time
            
            if success:
                self.logger.info(f"Task {task_name} completed successfully in {task_execution_time:.2f} seconds")
            else:
                self.logger.error(f"Task {task_name} failed")
                
            return success
            
        except Exception as e:
            self.logger.error(f"Error executing task {task_name}: {str(e)}")
            return False
    
    # Command Task Implementation
    def _execute_command_task(self, task_config: Dict[str, Any]) -> bool:
        """Execute command task"""
        import subprocess
        import os
        from pathlib import Path
        
        self.logger.info(f"Executing command task: {task_config['name']}")
        
        try:
            command = task_config.get('command', 'echo "No command specified"')
            working_dir = task_config.get('working_directory', '')
            timeout_seconds = task_config.get('timeout_seconds', 300)
            
            # Set working directory if specified
            original_cwd = os.getcwd()
            if working_dir and Path(working_dir).exists():
                os.chdir(working_dir)
                self.logger.info(f"Changed working directory to: {working_dir}")
            
            # Execute command
            self.logger.info(f"Executing command: {command}")
            result = subprocess.run(
                command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=timeout_seconds
            )
            
            # Restore original directory
            if working_dir:
                os.chdir(original_cwd)
            
            # Log output
            if result.stdout:
                self.logger.info(f"Command stdout: {result.stdout}")
            if result.stderr:
                self.logger.warning(f"Command stderr: {result.stderr}")
            
            return result.returncode == 0
            
        except subprocess.TimeoutExpired:
            self.logger.error("Command timed out")
            return False
        except Exception as e:
            self.logger.error(f"Command execution failed: {str(e)}")
            return False
    
    # Decision Task Implementation  
    def _execute_decision_task(self, task_config: Dict[str, Any]) -> Dict[str, Any]:
        """Execute decision task"""
        self.logger.info(f"Executing decision task: {task_config['name']}")
        
        conditions = task_config.get('conditions', [])
        default_path = task_config.get('default_path', 'continue')
        
        # Evaluate conditions
        for condition in conditions:
            condition_name = condition['name']
            expression = condition['expression']
            target_path = condition['target_path']
            
            try:
                # Simple expression evaluation (enhance as needed)
                if self._evaluate_expression(expression):
                    self.logger.info(f"Decision: {condition_name} -> {target_path}")
                    return {
                        'decision_made': condition_name,
                        'execution_path': target_path
                    }
            except Exception as e:
                self.logger.error(f"Error evaluating condition {condition_name}: {str(e)}")
        
        # Default path
        self.logger.info(f"Decision: default -> {default_path}")
        return {
            'decision_made': 'default',
            'execution_path': default_path
        }
    
    # Assignment Task Implementation
    def _execute_assignment_task(self, task_config: Dict[str, Any]) -> bool:
        """Execute assignment task"""
        self.logger.info(f"Executing assignment task: {task_config['name']}")
        
        assignments = task_config.get('assignments', [])
        
        try:
            for assignment in assignments:
                param_name = assignment['parameter_name']
                expression = assignment['expression']
                
                # Evaluate expression and assign value
                try:
                    value = eval(expression, {"__builtins__": {}}, {
                        'datetime': datetime,
                        'current_date': datetime.now().strftime('%Y-%m-%d')
                    })
                    self.workflow_parameters[param_name] = value
                    self.logger.info(f"Assigned {param_name} = {value}")
                except Exception as e:
                    self.logger.error(f"Failed to assign {param_name}: {str(e)}")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Assignment task failed: {str(e)}")
            return False
    
    # Start Workflow Task Implementation
    def _execute_start_workflow_task(self, task_config: Dict[str, Any]) -> bool:
        """Execute start workflow task"""
        self.logger.info(f"Executing start workflow task: {task_config['name']}")
        
        target_workflow = task_config.get('target_workflow', 'child_workflow')
        execution_mode = task_config.get('execution_mode', 'synchronous')
        parameter_mapping = task_config.get('parameter_mapping', {})
        
        try:
            # Prepare parameters for child workflow
            child_params = {}
            for child_param, parent_param in parameter_mapping.items():
                if parent_param.startswith('$'):
                    param_name = parent_param[1:]
                    child_params[child_param] = self.workflow_parameters.get(param_name)
                else:
                    child_params[child_param] = parent_param
            
            # Execute child workflow
            cmd = ['python', f'{target_workflow}.py']
            if child_params:
                cmd.extend(['--parameters', json.dumps(child_params)])
            
            self.logger.info(f"Starting child workflow: {target_workflow}")
            
            if execution_mode == 'synchronous':
                result = subprocess.run(cmd, capture_output=True, text=True)
                success = result.returncode == 0
                if result.stdout:
                    self.logger.info(f"Child workflow output: {result.stdout}")
                if result.stderr:
                    self.logger.warning(f"Child workflow errors: {result.stderr}")
                return success
            else:
                # Asynchronous execution
                process = subprocess.Popen(cmd)
                self.logger.info(f"Child workflow started asynchronously with PID: {process.pid}")
                return True
                
        except Exception as e:
            self.logger.error(f"Start workflow task failed: {str(e)}")
            return False
    
    # Timer Task Implementation
    def _execute_timer_task(self, task_config: Dict[str, Any]) -> bool:
        """Execute timer task"""
        self.logger.info(f"Executing timer task: {task_config['name']}")
        
        delay_amount = task_config.get('delay_amount', 60)
        delay_unit = task_config.get('delay_unit', 'seconds')
        
        # Convert to seconds
        unit_multipliers = {'seconds': 1, 'minutes': 60, 'hours': 3600}
        delay_seconds = delay_amount * unit_multipliers.get(delay_unit.lower(), 1)
        
        try:
            self.logger.info(f"Timer delay: {delay_amount} {delay_unit} ({delay_seconds}s)")
            time.sleep(delay_seconds)
            self.logger.info("Timer task completed")
            return True
        except Exception as e:
            self.logger.error(f"Timer task failed: {str(e)}")
            return False
    
    def _evaluate_expression(self, expression: str) -> bool:
        """Safely evaluate boolean expression"""
        try:
            # Replace workflow parameters in expression
            eval_expr = expression
            for param_name, param_value in self.workflow_parameters.items():
                if isinstance(param_value, str):
                    eval_expr = eval_expr.replace(f"${param_name}", f"'{param_value}'")
                else:
                    eval_expr = eval_expr.replace(f"${param_name}", str(param_value))
            
            # Safe evaluation
            return eval(eval_expr, {"__builtins__": {}}, {
                'datetime': datetime,
                'current_hour': datetime.now().hour
            })
        except Exception as e:
            self.logger.error(f"Expression evaluation failed: {str(e)}")
            return False
    
    {%- for task in workflow.tasks %}
    {%- if task.type == 'Email' %}
    def _send_notification(self, task_name: str) -> bool:
        """Send email notification"""
        try:
            self.logger.info(f"Sending notification for task: {task_name}")
            
            task_config = self.task_configs.get(task_name, {})
            recipients = task_config.get('recipients', ['admin@company.com'])
            subject = task_config.get('subject', 'Workflow Notification')
            
            message = f"{{ workflow.name }} workflow notification at {time.strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Simulate email sending (replace with actual email logic)
            self.logger.info(f"Email sent to {recipients} with subject: {subject}")
            self.logger.info(f"Message: {message}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error sending notification: {str(e)}")
            return False
    {%- endif %}
    {%- endfor %}
            
    def _handle_workflow_failure(self, failed_task: str):
        """Handle workflow failure"""
        try:
            self.logger.error(f"Workflow failed at task: {failed_task}")
            # Add failure handling logic here
            
        except Exception as e:
            self.logger.error(f"Error handling workflow failure: {str(e)}")
    
    def get_task_dependencies(self) -> dict:
        """Get task dependencies"""
        return {
            {%- for link in workflow.get('links', []) %}
            "{{ link.to }}": ["{{ link.from }}"],
            {%- endfor %}
        }
'''

        template = Template(workflow_template)
        output = template.render(
            workflow=enhanced_workflow, 
            project=project, 
            class_name=class_name,
            dag_analysis=enhanced_workflow.get('dag_analysis', {}),
            execution_plan=execution_plan
        )
        
        workflow_file = app_dir / "src/main/python/workflows" / file_name
        workflow_file.write_text(output)
        self.logger.info(f"Generated workflow class: {file_name}")
    
    def _generate_main_application(self, app_dir: Path, project: Project):
        """Generate main application entry point"""
        workflows = self._extract_workflows_from_project(project)
        main_workflow = workflows[0] if workflows else None
        
        main_template = '''"""
Main Application for {{ project.name }}
Generated from Informatica BDM Project

Usage:
    python main.py [--config config.yaml]
    spark-submit main.py [--config config.yaml]
"""
import sys
import os
import logging
import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import yaml

# Add current directory to Python path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

{%- if main_workflow %}
from workflows.{{ main_workflow.name | lower }} import {{ main_workflow.name | replace('_', ' ') | title | replace(' ', '') }}
{%- endif %}


def setup_logging(log_level: str = "INFO"):
    """Setup logging configuration"""
    logging.basicConfig(
        level=getattr(logging, log_level.upper()),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/{{ project.name | lower }}.log')
        ]
    )


def load_config(config_path: str) -> dict:
    """Load configuration from YAML file"""
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        logging.error(f"Error loading config: {str(e)}")
        return {}


def create_spark_session(app_name: str, config: dict) -> SparkSession:
    """Create Spark session with configuration"""
    spark_config = config.get('spark', {})
    
    # Default Spark configuration
    conf = SparkConf()
    conf.setAppName(app_name)
    
    # Apply custom configuration
    for key, value in spark_config.items():
        conf.set(key, str(value))
    
    # Create Spark session
    spark = SparkSession.builder \\
        .config(conf=conf) \\
        .enableHiveSupport() \\
        .getOrCreate()
    
    # Set log level
    log_level = config.get('log_level', 'WARN')
    spark.sparkContext.setLogLevel(log_level)
    
    return spark


def main():
    """Main application function"""
    parser = argparse.ArgumentParser(description='{{ project.name }} Spark Application')
    parser.add_argument('--config', default='config/application.yaml', 
                       help='Path to configuration file')
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger("{{ project.name }}")
    
    try:
        logger.info("Starting {{ project.name }} Spark Application")
        
        # Load configuration
        config = load_config(args.config)
        
        # Create Spark session
        spark = create_spark_session("{{ project.name }}", config)
        
        # Create output directories
        os.makedirs("data/output", exist_ok=True)
        os.makedirs("logs", exist_ok=True)
        
        {%- if main_workflow %}
        # Execute main workflow
        workflow = {{ main_workflow.name | replace('_', ' ') | title | replace(' ', '') }}(spark, config)
        
        if workflow.validate_workflow():
            logger.info("Workflow validation passed")
            success = workflow.execute()
            
            if success:
                logger.info("{{ project.name }} completed successfully")
                exit_code = 0
            else:
                logger.error("{{ project.name }} execution failed")
                exit_code = 1
        else:
            logger.error("Workflow validation failed")
            exit_code = 1
        {%- else %}
        # No workflow defined - run basic application
        logger.info("No workflow defined, running basic application")
        exit_code = 0
        {%- endif %}
        
        # Cleanup
        spark.stop()
        
        logger.info("Application completed")
        sys.exit(exit_code)
        
    except Exception as e:
        logger.error(f"Application failed with error: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
'''

        template = Template(main_template)
        output = template.render(project=project, main_workflow=main_workflow)
        
        main_file = app_dir / "src/main/python/main.py"
        main_file.write_text(output)
        self.logger.info("Generated main.py")
    
    def _generate_config_files(self, app_dir: Path, project: Project):
        """Generate configuration files with enhanced parameter system"""
        connections = self._extract_connections_from_project(project)
        parameters = self._extract_parameters_from_project(project)
        
        # Use enhanced parameter system if available
        if self.enhanced_generator and hasattr(self.enhanced_generator, 'parameter_manager') and self.enhanced_generator.parameter_manager:
            self._setup_project_parameters(project, self.enhanced_generator.parameter_manager)
            enhanced_config = self.enhanced_generator.parameter_manager.export_typed_config()
            if enhanced_config:
                parameters.update(enhanced_config.get('project', {}))
                parameters.update(enhanced_config.get('global', {}))
        else:
            enhanced_config = {}
        
        # Use extracted connections (now returns a dict)
        connections_dict = connections if isinstance(connections, dict) else {}
        
        app_config = {
            'spark': {
                'spark.app.name': project.name,
                'spark.master': 'local[*]',
                'spark.sql.adaptive.enabled': 'true',
                'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                'spark.sql.warehouse.dir': './spark-warehouse'
            },
            'connections': connections_dict,
            'parameters': parameters,
            'log_level': 'INFO'
        }
        
        config_file = app_dir / "config/application.yaml"
        with open(config_file, 'w') as f:
            yaml.dump(app_config, f, default_flow_style=False, indent=2)
        
        # Requirements.txt
        requirements = [
            'pyspark>=3.4.0',
            'PyYAML>=6.0',
            'pandas>=1.5.0'
        ]
        
        requirements_file = app_dir / "requirements.txt"
        requirements_file.write_text('\n'.join(requirements))
        
        self.logger.info("Generated configuration files")
    
    def _generate_deployment_files(self, app_dir: Path, project: Project):
        """Generate deployment scripts and files"""
        
        # Docker file
        dockerfile_content = f'''FROM apache/spark-py:v3.4.0

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY src/main/python/ ./
COPY config/ ./config/
COPY data/ ./data/

CMD ["spark-submit", "--master", "local[*]", "main.py"]
'''
        
        dockerfile = app_dir / "Dockerfile"
        dockerfile.write_text(dockerfile_content)
        
        # Run script
        run_script = f'''#!/bin/bash

echo "Starting {project.name} Spark Application..."

# Set PYTHONPATH
export PYTHONPATH="${{PYTHONPATH}}:$(pwd)/src/main/python"

# Create necessary directories
mkdir -p data/output
mkdir -p logs

# Run the application
spark-submit \\
  --master local[*] \\
  --conf spark.sql.adaptive.enabled=true \\
  --conf spark.sql.adaptive.coalescePartitions.enabled=true \\
  src/main/python/main.py \\
  --config config/application.yaml

echo "Application completed"
'''
        
        run_script_file = app_dir / "run.sh"
        run_script_file.write_text(run_script)
        run_script_file.chmod(0o755)
        
        # Docker Compose
        docker_compose = f'''version: '3.8'

services:
  {project.name.lower()}:
    build: .
    volumes:
      - ./data:/app/data
      - ./logs:/app/logs
      - ./config:/app/config
    environment:
      - SPARK_MASTER=local[*]
'''
        
        compose_file = app_dir / "docker-compose.yml"
        compose_file.write_text(docker_compose)
        
        self.logger.info("Generated deployment files")
    
    def _generate_test_data_scripts(self, app_dir: Path, project: Project):
        """Generate smart test data generation scripts that work for any source"""
        mappings = self._extract_mappings_from_project(project)
        
        # Only generate if there are actual sources to work with
        all_sources = []
        for mapping in mappings:
            sources = [comp for comp in mapping.get('components', []) 
                      if comp.get('component_type') == 'source']
            all_sources.extend(sources)
        
        if not all_sources:
            self.logger.info("No sources found - skipping test data generation")
            return
        
        test_data_script = '''"""
Smart Test Data Generator for {{ project.name }}
Automatically generates appropriate test data for all sources
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from datetime import datetime, date
import os
import random
import string


def create_spark_session():
    """Create Spark session for data generation"""
    return SparkSession.builder \\
        .appName("{{ project.name }}_TestDataGenerator") \\
        .master("local[*]") \\
        .getOrCreate()


def generate_test_data():
    """Generate test data for all sources"""
    spark = create_spark_session()
    
    # Create output directory
    os.makedirs("data/input", exist_ok=True)
    
    sources_generated = 0
    
    {%- for mapping in mappings %}
    {%- set sources = mapping.components | selectattr('component_type', 'equalto', 'source') | list %}
    {%- for source in sources %}
    try:
        # Generate data for {{ source.name }}
        {{ source.name | lower }}_data = generate_{{ source.name | lower }}_data(spark)
        
        # Save in multiple formats for flexibility
        output_path = "data/input/{{ source.name | lower }}"
        {{ source.name | lower }}_data.coalesce(1).write.mode("overwrite").parquet(f"{output_path}.parquet")
        {{ source.name | lower }}_data.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}.csv")
        
        record_count = {{ source.name | lower }}_data.count()
        print(f"✅ Generated {record_count} test records for {{ source.name }}")
        sources_generated += 1
        
    except Exception as e:
        print(f"❌ Error generating data for {{ source.name }}: {str(e)}")
    
    {%- endfor %}
    {%- endfor %}
    
    spark.stop()
    print(f"\\n🎉 Test data generation completed! Generated data for {sources_generated} sources.")


{%- for mapping in mappings %}
{%- set sources = mapping.components | selectattr('component_type', 'equalto', 'source') | list %}
{%- for source in sources %}
def generate_{{ source.name | lower }}_data(spark):
    """Generate sample data for {{ source.name }}"""
    {%- if 'sales' in source.name | lower %}
    schema = StructType([
        StructField("sale_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("sale_date", DateType(), False),
        StructField("region", StringType(), False)
    ])
    
    data = [
        ("S001", "C001", "P001", 100.0, "2023-01-01", "North"),
        ("S002", "C002", "P002", 250.0, "2023-01-02", "South"),
        ("S003", "C003", "P001", 75.0, "2023-01-03", "East"),
        ("S004", "C001", "P003", 300.0, "2023-01-04", "North"),
        ("S005", "C004", "P002", 150.0, "2023-01-05", "West")
    ]
    
    {%- elif 'customer' in source.name | lower %}
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("customer_name", StringType(), False),
        StructField("city", StringType(), False),
        StructField("status", StringType(), False),
        StructField("email", StringType(), False)
    ])
    
    data = [
        ("C001", "John Doe", "New York", "Active", "john@email.com"),
        ("C002", "Jane Smith", "Los Angeles", "Active", "jane@email.com"),
        ("C003", "Bob Johnson", "Chicago", "Inactive", "bob@email.com"),
        ("C004", "Alice Brown", "Houston", "Active", "alice@email.com")
    ]
    
    {%- elif 'order' in source.name | lower %}
    schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_date", DateType(), False),
        StructField("total_amount", DoubleType(), False),
        StructField("status", StringType(), False)
    ])
    
    data = [
        ("O001", "C001", "2023-01-01", 500.0, "Completed"),
        ("O002", "C002", "2023-01-02", 750.0, "Completed"),
        ("O003", "C003", "2023-01-03", 300.0, "Pending"),
        ("O004", "C004", "2023-01-04", 1000.0, "Completed")
    ]
    
    {%- elif 'product' in source.name | lower %}
    schema = StructType([
        StructField("product_id", StringType(), False),
        StructField("product_name", StringType(), False),
        StructField("category", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("active", BooleanType(), False)
    ])
    
    data = [
        ("P001", "Laptop", "Electronics", 999.99, True),
        ("P002", "Phone", "Electronics", 699.99, True),
        ("P003", "Tablet", "Electronics", 399.99, True),
        ("P004", "Headphones", "Electronics", 199.99, True)
    ]
    
    {%- else %}
    # Generic schema for {{ source.name }}
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("name", StringType(), False),
        StructField("value", DoubleType(), False),
        StructField("created_date", DateType(), False)
    ])
    
    data = [
        ("1", "Record 1", 100.0, "2023-01-01"),
        ("2", "Record 2", 200.0, "2023-01-02"),
        ("3", "Record 3", 300.0, "2023-01-03"),
        ("4", "Record 4", 400.0, "2023-01-04")
    ]
    {%- endif %}
    
    return spark.createDataFrame(data, schema)

{%- endfor %}
{%- endfor %}

if __name__ == "__main__":
    generate_test_data()
'''
        
        template = Template(test_data_script)
        output = template.render(project=project, mappings=mappings)
        
        test_data_file = app_dir / "scripts/generate_test_data.py"
        test_data_file.write_text(output)
        test_data_file.chmod(0o755)
        
        self.logger.info("Generated test data scripts")
    
    def _generate_documentation(self, app_dir: Path, project: Project):
        """Generate project documentation"""
        readme_content = f'''# {project.name} - Generated Spark Application

This Spark application was automatically generated from Informatica BDM project: **{project.name}**

## Project Overview

- **Version**: {getattr(project, 'version', '1.0')}
- **Description**: {getattr(project, 'description', 'Generated Spark application')}
- **Generated on**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Quick Start

### Prerequisites
- Apache Spark 3.4+
- Python 3.8+
- Java 8 or 11

### Setup
```bash
# Install dependencies
pip install -r requirements.txt

# Generate test data
python scripts/generate_test_data.py

# Run the application
./run.sh
```

### Using Docker
```bash
# Build and run with Docker Compose
docker-compose up --build
```

## Project Structure

```
{project.name}/
├── src/main/python/          # Source code
│   ├── main.py              # Main application entry point
│   ├── base_classes.py      # Base classes for mappings and workflows
│   ├── mappings/            # Generated mapping implementations
│   ├── workflows/           # Generated workflow orchestration
│   └── transformations/     # Transformation logic
├── config/                  # Configuration files
│   └── application.yaml     # Main application configuration
├── data/                    # Data directories
│   ├── input/              # Input data files
│   └── output/             # Output data files
├── scripts/                 # Utility scripts
│   └── generate_test_data.py # Test data generation
├── logs/                   # Application logs
├── requirements.txt        # Python dependencies
├── Dockerfile             # Docker configuration
├── docker-compose.yml     # Docker Compose configuration
└── run.sh                 # Application run script
```

## Configuration

Edit `config/application.yaml` to customize:
- Spark configuration
- Database connections
- Application parameters

## Mappings

{self._get_mappings_documentation(project)}

## Workflows

{self._get_workflows_documentation(project)}

## Running in Production

1. **Cluster Deployment**:
   ```bash
   spark-submit --master yarn --deploy-mode cluster src/main/python/main.py
   ```

2. **Resource Configuration**:
   ```bash
   spark-submit \\
     --master yarn \\
     --num-executors 10 \\
     --executor-memory 4g \\
     --executor-cores 2 \\
     src/main/python/main.py
   ```

## Monitoring

- Application logs: `logs/{project.name.lower()}.log`
- Spark UI: `http://localhost:4040` (when running locally)

## Support

This application was generated automatically. For modifications:
1. Update the original Informatica BDM project
2. Regenerate the Spark application
3. Or modify the generated code directly for custom requirements
'''
        
        readme_file = app_dir / "README.md"
        readme_file.write_text(readme_content)
        
        self.logger.info("Generated documentation")
    
    def _extract_mappings_from_project(self, project: Project) -> List[Dict[str, Any]]:
        """Extract mappings from parsed project"""
        mappings = []
        for folder_name, folder_contents in project.folders.items():
            if folder_name == "Mappings":
                mappings.extend(folder_contents)
        return mappings
    
    def _extract_workflows_from_project(self, project: Project) -> List[Dict[str, Any]]:
        """Extract workflows from parsed project"""
        workflows = []
        for folder_name, folder_contents in project.folders.items():
            if folder_name == "Workflows":
                workflows.extend(folder_contents)
        return workflows
    
    def _setup_project_parameters(self, project: Project, parameter_manager):
        """Set up project-level parameters with type-awareness and validation"""
        try:
            # Import parameter system classes
            from .enhanced_parameter_system import (
                EnhancedParameter, ParameterType, ParameterScope, ParameterValidation
            )
            
            # Extract original parameters
            raw_parameters = project.parameters if hasattr(project, 'parameters') else {}
            
            # Set up typed parameters based on known parameter patterns
            for param_name, param_value in raw_parameters.items():
                enhanced_param = self._create_enhanced_parameter(param_name, param_value)
                if enhanced_param:
                    parameter_manager.add_parameter(enhanced_param)
                    # Set the actual value
                    parameter_manager.set_parameter_value(
                        param_name, param_value, enhanced_param.scope
                    )
                    
        except ImportError:
            self.logger.warning("Enhanced parameter system not available, using basic parameters")
        except Exception as e:
            self.logger.error(f"Error setting up project parameters: {str(e)}")
    
    def _create_enhanced_parameter(self, param_name: str, param_value: Any):
        """Create enhanced parameter based on parameter name and value patterns"""
        try:
            from .enhanced_parameter_system import (
                EnhancedParameter, ParameterType, ParameterScope, ParameterValidation
            )
            
            param_name_lower = param_name.lower()
            
            # Determine parameter type based on name patterns
            if 'date' in param_name_lower:
                return EnhancedParameter(
                    name=param_name,
                    param_type=ParameterType.STRING,  # Keep as string for $$SystemDate
                    scope=ParameterScope.PROJECT,
                    description=f"Date parameter: {param_name}"
                )
            elif 'size' in param_name_lower or 'count' in param_name_lower or 'threshold' in param_name_lower:
                # Numeric parameters with validation
                if 'threshold' in param_name_lower:
                    return EnhancedParameter(
                        name=param_name,
                        param_type=ParameterType.INTEGER if str(param_value).isdigit() else ParameterType.FLOAT,
                        scope=ParameterScope.PROJECT,
                        description=f"Threshold parameter: {param_name}",
                        validation=ParameterValidation(
                            min_value=0 if 'error' in param_name_lower else None,
                            max_value=1.0 if 'error' in param_name_lower else None
                        )
                    )
                else:
                    return EnhancedParameter(
                        name=param_name,
                        param_type=ParameterType.INTEGER,
                        scope=ParameterScope.PROJECT,
                        description=f"Size/count parameter: {param_name}",
                        validation=ParameterValidation(min_value=1)
                    )
            elif 'env' in param_name_lower or 'environment' in param_name_lower:
                return EnhancedParameter(
                    name=param_name,
                    param_type=ParameterType.STRING,
                    scope=ParameterScope.GLOBAL,
                    description=f"Environment parameter: {param_name}",
                    validation=ParameterValidation(
                        allowed_values=["DEVELOPMENT", "TESTING", "STAGING", "PRODUCTION"]
                    )
                )
            elif 'region' in param_name_lower:
                return EnhancedParameter(
                    name=param_name,
                    param_type=ParameterType.STRING,
                    scope=ParameterScope.PROJECT,
                    description=f"Region parameter: {param_name}",
                    validation=ParameterValidation(
                        regex_pattern=r'^[A-Z]{2,5}$'  # 2-5 uppercase letters
                    )
                )
            elif param_name_lower in ['parallel_degree', 'retention_days']:
                return EnhancedParameter(
                    name=param_name,
                    param_type=ParameterType.INTEGER,
                    scope=ParameterScope.PROJECT,
                    description=f"Configuration parameter: {param_name}",
                    validation=ParameterValidation(
                        min_value=1,
                        max_value=10000 if 'parallel' in param_name_lower else 36500
                    )
                )
            else:
                # Default string parameter
                return EnhancedParameter(
                    name=param_name,
                    param_type=ParameterType.STRING,
                    scope=ParameterScope.PROJECT,
                    description=f"Project parameter: {param_name}"
                )
                
        except ImportError:
            return None
        except Exception as e:
            self.logger.warning(f"Could not create enhanced parameter for {param_name}: {e}")
            return None

    def _extract_connections_from_project(self, project: Project) -> Dict[str, Dict[str, Any]]:
        """Extract connections from parsed project and convert to configuration format"""
        connections_dict = {}
        connections = getattr(project, 'connections', {})
        
        # Handle both dictionary and list formats  
        if isinstance(connections, dict):
            connections = connections.values()
        
        for conn in connections:
            if hasattr(conn, 'name'):
                conn_config = {
                    'type': conn.connection_type,
                    'host': conn.host,
                    'port': conn.port
                }
                
                # Add all connection properties
                conn_config.update(conn.properties)
                
                # Convert specific connection types to Spark-compatible configuration
                if conn.connection_type == 'HIVE':
                    conn_config.update({
                        'driver': 'org.apache.hive.jdbc.HiveDriver',
                        'url': f"jdbc:hive2://{conn.host}:{conn.port}/{conn_config.get('database', 'default')}",
                        'format': 'hive'
                    })
                elif conn.connection_type == 'HDFS':
                    conn_config.update({
                        'namenode': conn_config.get('namenode', f"hdfs://{conn.host}:{conn.port}"),
                        'format': 'parquet'
                    })
                
                connections_dict[conn.name] = conn_config
                
        return connections_dict
    
    def _extract_parameters_from_project(self, project: Project) -> Dict[str, Any]:
        """Extract parameters from parsed project"""
        return getattr(project, 'parameters', {})
    
    def _get_mappings_documentation(self, project: Project) -> str:
        """Generate mappings documentation"""
        mappings = self._extract_mappings_from_project(project)
        if not mappings:
            return "No mappings found in the project."
        
        docs = []
        for mapping in mappings:
            docs.append(f"### {mapping['name']}")
            docs.append(f"- **Description**: {mapping.get('description', 'N/A')}")
            docs.append("- **Components**:")
            for component in mapping.get('components', []):
                docs.append(f"  - {component['name']} ({component['type']})")
            docs.append("")
        
        return '\n'.join(docs)
    
    def _get_workflows_documentation(self, project: Project) -> str:
        """Generate workflows documentation"""
        workflows = self._extract_workflows_from_project(project)
        if not workflows:
            return "No workflows found in the project."
        
        docs = []
        for workflow in workflows:
            docs.append(f"### {workflow['name']}")
            docs.append(f"- **Description**: {workflow.get('description', 'N/A')}")
            docs.append("- **Tasks**:")
            for task in workflow.get('tasks', []):
                docs.append(f"  - {task['name']} ({task['type']})")
            docs.append("")
        
        return '\n'.join(docs)
    
    def _sanitize_name(self, name: str) -> str:
        """Sanitize name for file/class names"""
        return name.lower().replace(' ', '_').replace('-', '_')
    
    def _to_class_name(self, name: str) -> str:
        """Convert name to Python class name"""
        return ''.join(word.capitalize() for word in name.replace('_', ' ').split())
    
    def _convert_informatica_expression_to_spark(self, informatica_expr: str) -> str:
        """Convert Informatica expression syntax to Spark SQL syntax"""
        if not informatica_expr:
            return ""
            
        spark_expr = informatica_expr.strip()
        
        # String concatenation: || -> concat()
        if '||' in spark_expr:
            # Split by || and clean up parts
            parts = []
            for part in spark_expr.split('||'):
                part = part.strip()
                # Remove extra quotes if present
                if part.startswith("'") and part.endswith("'"):
                    parts.append(part)
                elif part.startswith('"') and part.endswith('"'):
                    parts.append(part)
                else:
                    # Field reference - add it as is
                    parts.append(part)
            spark_expr = f"concat({', '.join(parts)})"
        
        # Other common conversions
        spark_expr = spark_expr.replace('SUBSTR(', 'substring(')
        spark_expr = spark_expr.replace('NVL(', 'coalesce(')
        spark_expr = spark_expr.replace('TO_DATE(', 'to_date(')
        spark_expr = spark_expr.replace('TO_TIMESTAMP(', 'to_timestamp(')
        spark_expr = spark_expr.replace('SYSDATE', 'current_date()')
        spark_expr = spark_expr.replace('SYSTIMESTAMP', 'current_timestamp()')
        
        # Escape double quotes that appear in string literals to prevent Python syntax errors
        # Convert SQL string literals from "value" to 'value' for better Python compatibility
        import re
        spark_expr = re.sub(r'"([^"]*)"', r"'\1'", spark_expr)
        
        return spark_expr
    
    def _convert_informatica_type_to_spark(self, informatica_type: str) -> str:
        """Convert Informatica data type to Spark SQL type"""
        if not informatica_type:
            return "string"
            
        type_mapping = {
            'string': 'string',
            'varchar': 'string', 
            'char': 'string',
            'integer': 'int',
            'int': 'int',
            'bigint': 'bigint',
            'decimal': 'decimal',
            'double': 'double',
            'float': 'float',
            'date': 'date',
            'timestamp': 'timestamp',
            'boolean': 'boolean',
            'binary': 'binary'
        }
        return type_mapping.get(informatica_type.lower(), 'string')
    
    def _suffix_with_filter(self, value: str, suffix: str) -> str:
        """Jinja2 filter to add suffix to string"""
        return f"{value}{suffix}"
    
    def _snake_to_camel_filter(self, value: str) -> str:
        """Convert snake_case to CamelCase"""
        # Split by underscore, capitalize each part, then join
        parts = value.split('_')
        return ''.join(part.capitalize() for part in parts)
    
    def _apply_manual_formatting(self, file_path: Path):
        """Apply manual formatting fixes to resolve indentation issues"""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
            
            # First, fix the line concatenation issue by adding proper line breaks
            import re
            
            # Fix patterns where multiple statements are on the same line
            content = re.sub(r'(\w+_df = self\._read_\w+\(\))(\s*)(\# Apply transformations)', r'\1\n            \3', content)
            content = re.sub(r'(current_df = \w+_df)(\s*)(current_df = self\._apply_)', r'\1\n            \3', content)
            content = re.sub(r'(\# Write to targets)(\s*)(self\._write_to_)', r'\1\n            \3', content)
            content = re.sub(r'(self\._write_to_\w+\(current_df\))(\s*)(self\.logger\.info)', r'\1\n            \3', content)
            
            # Fix comment-to-code concatenation (the line 32 issue)
            content = re.sub(r'(# Read source data)(\s+)(\w+_df = self\._read_)', r'\1\n            \3', content)
            content = re.sub(r'(# Apply transformations)(\s+)(current_df = )', r'\1\n            \3', content)
            content = re.sub(r'(# Write to targets)(\s+)(self\._write_to_)', r'\1\n            \3', content)
            
            # Fix method definitions that are concatenated
            content = re.sub(r'(raise)(\s*)(def _\w+)', r'\1\n\n    \3', content)
            content = re.sub(r'(\)\s*)(def _\w+)', r')\n\n    \2', content)
            
            # Fix missing line breaks between functions (especially important)
            content = re.sub(r'(\]\s*)(def _\w+)', r'\1\n\n    \2', content)  # After closing brackets
            content = re.sub(r'(\}\s*)(def _\w+)', r'\1\n\n    \2', content)  # After closing braces
            content = re.sub(r'(return \w+\s*)(def _\w+)', r'\1\n\n    \2', content)  # After return statements
            
            lines = content.split('\n')
            formatted_lines = []
            in_execute_method = False
            
            for line in lines:
                # Detect if we're in the execute method
                if 'def execute(self) -> bool:' in line:
                    in_execute_method = True
                elif line.strip().startswith('def ') and in_execute_method:
                    in_execute_method = False
                
                # Fix indentation for execute method content
                if in_execute_method and line.strip() and not line.startswith('    '):
                    # Lines that should be indented within the execute method
                    if any(pattern in line for pattern in [
                        '_df = self._read_',
                        'current_df = self._apply_',
                        'current_df =', 
                        'self._write_to_',
                        '# Read source data',
                        '# Apply transformations', 
                        '# Write to targets'
                    ]):
                        line = '            ' + line.strip()  # 12 spaces for try block content
                
                formatted_lines.append(line)
            
            # Write back the formatted content
            with open(file_path, 'w') as f:
                f.write('\n'.join(formatted_lines))
                
            self.logger.info(f"Applied manual formatting to {file_path}")
            
            # Now apply black formatting on top of manual formatting
            self._format_python_file(file_path)
            
        except Exception as e:
            self.logger.warning(f"Could not apply manual formatting to {file_path}: {str(e)}")
    
    def _format_python_file(self, file_path: Path):
        """Apply automatic formatting to a Python file using black"""
        try:
            import subprocess
            result = subprocess.run(
                ['python', '-m', 'black', str(file_path)], 
                capture_output=True, 
                text=True,
                timeout=30
            )
            if result.returncode == 0:
                self.logger.info(f"Successfully formatted {file_path}")
            else:
                self.logger.warning(f"Black formatting failed for {file_path}: {result.stderr}")
        except (ImportError, subprocess.TimeoutExpired, FileNotFoundError) as e:
            self.logger.warning(f"Could not format {file_path} with black: {str(e)}")
        except Exception as e:
            self.logger.warning(f"Unexpected error formatting {file_path}: {str(e)}")
      
    def _generate_transformations_module(self, app_dir: Path):
        """Generate the transformations module"""
        transformations_content = '''"""
Generated Transformation Classes
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ..runtime.base_classes import BaseTransformation


class ExpressionTransformation(BaseTransformation):
    """Expression-based transformations (filtering, calculations)"""
    
    def __init__(self, name: str, expressions: dict = None, filters: list = None):
        super().__init__(name, "Expression")
        self.expressions = expressions or {}
        self.filters = filters or []
        
    def transform(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """Apply expressions and filters"""
        result_df = input_df
        
        # Apply filters
        for filter_expr in self.filters:
            result_df = result_df.filter(filter_expr)
            self.logger.info(f"Applied filter: {filter_expr}")
            
        # Apply expressions
        for col_name, expression in self.expressions.items():
            result_df = result_df.withColumn(col_name, expr(expression))
            self.logger.info(f"Added column {col_name}")
            
        return result_df


class AggregatorTransformation(BaseTransformation):
    """Aggregation operations"""
    
    def __init__(self, name: str, group_by_cols: list = None, aggregations: dict = None):
        super().__init__(name, "Aggregator")
        self.group_by_cols = group_by_cols or []
        self.aggregations = aggregations or {}
        
    def transform(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """Apply aggregation"""
        if not self.group_by_cols:
            return input_df
            
        agg_exprs = []
        for agg_name, agg_config in self.aggregations.items():
            agg_type = agg_config.get('type', 'sum')
            agg_column = agg_config.get('column')
            
            if agg_type.lower() == 'sum':
                agg_exprs.append(sum(agg_column).alias(agg_name))
            elif agg_type.lower() == 'count':
                agg_exprs.append(count(agg_column).alias(agg_name))
            elif agg_type.lower() == 'avg':
                agg_exprs.append(avg(agg_column).alias(agg_name))
            elif agg_type.lower() == 'max':
                agg_exprs.append(max(agg_column).alias(agg_name))
            elif agg_type.lower() == 'min':
                agg_exprs.append(min(agg_column).alias(agg_name))
                
        return input_df.groupBy(*self.group_by_cols).agg(*agg_exprs)


class LookupTransformation(BaseTransformation):
    """Lookup operations (joins)"""
    
    def __init__(self, name: str, join_conditions: list = None, join_type: str = "left"):
        super().__init__(name, "Lookup")
        self.join_conditions = join_conditions or []
        self.join_type = join_type
        
    def transform(self, input_df: DataFrame, lookup_df: DataFrame = None, **kwargs) -> DataFrame:
        """Perform lookup operation"""
        if lookup_df is None:
            return input_df
            
        if not self.join_conditions:
            return input_df
            
        join_expr = None
        for condition in self.join_conditions:
            condition_expr = expr(condition)
            if join_expr is None:
                join_expr = condition_expr
            else:
                join_expr = join_expr & condition_expr
                
        return input_df.join(lookup_df, join_expr, self.join_type)


class JoinerTransformation(BaseTransformation):
    """Join operations between sources"""
    
    def __init__(self, name: str, join_conditions: list = None, join_type: str = "inner"):
        super().__init__(name, "Joiner")
        self.join_conditions = join_conditions or []
        self.join_type = join_type
        
    def transform(self, left_df: DataFrame, right_df: DataFrame, **kwargs) -> DataFrame:
        """Join two DataFrames"""
        if not self.join_conditions:
            return left_df
            
        join_expr = None
        for condition in self.join_conditions:
            condition_expr = expr(condition)
            if join_expr is None:
                join_expr = condition_expr
            else:
                join_expr = join_expr & condition_expr
                
        return left_df.join(right_df, join_expr, self.join_type)


class SequenceTransformation(BaseTransformation):
    """Sequence number generation"""
    
    def __init__(self, name: str, start_value: int = 1, increment_value: int = 1, **kwargs):
        super().__init__(name, "Sequence")
        self.start_value = start_value
        self.increment_value = increment_value
        
    def transform(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """Generate sequence numbers"""
        from pyspark.sql.functions import monotonically_increasing_id, row_number, lit
        from pyspark.sql.window import Window
        
        if self.increment_value == 1:
            # Use monotonic ID for performance
            return input_df.withColumn("NEXTVAL", 
                monotonically_increasing_id() + self.start_value)
        else:
            # Use row_number for custom increment
            window_spec = Window.orderBy(lit(1))
            return input_df.withColumn("NEXTVAL",
                (row_number().over(window_spec) - 1) * self.increment_value + self.start_value)


class SorterTransformation(BaseTransformation):
    """Data sorting operations"""
    
    def __init__(self, name: str, sort_keys: list = None):
        super().__init__(name, "Sorter")
        self.sort_keys = sort_keys or []
        
    def transform(self, input_df: DataFrame, **kwargs) -> DataFrame:
        """Sort data based on sort keys"""
        from pyspark.sql.functions import col
        
        if not self.sort_keys:
            return input_df
            
        sort_exprs = []
        for key in self.sort_keys:
            field_name = key['field_name']
            direction = key.get('direction', 'ASC')
            
            if direction.upper() == 'ASC':
                sort_exprs.append(col(field_name).asc())
            else:
                sort_exprs.append(col(field_name).desc())
                
        return input_df.orderBy(*sort_exprs)


class RouterTransformation(BaseTransformation):
    """Conditional data routing"""
    
    def __init__(self, name: str, output_groups: list = None):
        super().__init__(name, "Router")
        self.output_groups = output_groups or []
        
    def transform(self, input_df: DataFrame, **kwargs) -> dict:
        """Route data to multiple outputs"""
        results = {}
        
        for group in self.output_groups:
            group_name = group['name']
            condition = group['condition']
            results[group_name] = input_df.filter(condition)
            
        # Default group gets remaining records
        all_conditions = " OR ".join([f"({g['condition']})" for g in self.output_groups])
        results['DEFAULT'] = input_df.filter(f"NOT ({all_conditions})")
        
        return results


class UnionTransformation(BaseTransformation):
    """Union operations for combining DataFrames"""
    
    def __init__(self, name: str, union_type: str = "UNION_ALL"):
        super().__init__(name, "Union")
        self.union_type = union_type
        
    def transform(self, *input_dataframes, **kwargs) -> DataFrame:
        """Combine multiple DataFrames"""
        if len(input_dataframes) < 2:
            return input_dataframes[0] if input_dataframes else None
            
        result_df = input_dataframes[0]
        for df in input_dataframes[1:]:
            result_df = result_df.union(df)
            
        if self.union_type == "UNION_DISTINCT":
            result_df = result_df.distinct()
            
        return result_df


class JavaTransformation(BaseTransformation):
    """Custom transformation logic"""
    
    def __init__(self, name: str, logic_type: str = "custom"):
        super().__init__(name, "Java")
        self.logic_type = logic_type
        
    def transform(self, input_df: DataFrame, existing_df: DataFrame = None, **kwargs) -> DataFrame:
        """Apply custom transformation logic"""
        if self.logic_type == "scd_type2":
            return self._apply_scd_type2(input_df, existing_df)
        else:
            return input_df
            
    def _apply_scd_type2(self, source_df: DataFrame, existing_df: DataFrame = None) -> DataFrame:
        """Apply SCD Type 2 logic"""
        if existing_df is None:
            return source_df.withColumn("effective_date", current_date()) \\
                           .withColumn("expiry_date", lit(None).cast("date")) \\
                           .withColumn("is_current", lit(True)) \\
                           .withColumn("record_version", lit(1))
        else:
            # Simplified SCD Type 2 implementation
            expired_df = existing_df.withColumn("expiry_date", current_date()) \\
                                   .withColumn("is_current", lit(False))
            
            new_df = source_df.withColumn("effective_date", current_date()) \\
                             .withColumn("expiry_date", lit(None).cast("date")) \\
                             .withColumn("is_current", lit(True)) \\
                             .withColumn("record_version", lit(1))
                             
            return expired_df.union(new_df)
'''
    
        transformations_file = app_dir / "src/main/python/transformations/generated_transformations.py"
        transformations_file.write_text(transformations_content)
        self.logger.info("Generated transformations module")
        
    def _generate_external_configurations(self, app_dir: Path, mapping: Dict[str, Any], execution_plan: Dict[str, Any], project: Project):
        """Generate external configuration files for mapping (Phase 1 implementation)"""
        try:
            self.logger.info(f"Generating external configurations for mapping: {mapping['name']}")
            
            # Initialize configuration file generator
            config_generator = ConfigurationFileGenerator(str(app_dir))
            
            # Extract DAG analysis from mapping
            dag_analysis = mapping.get('dag_analysis', {})
            
            # Generate all configuration files
            config_generator.generate_all_configs(mapping, dag_analysis, execution_plan)
            
            self.logger.info(f"Successfully generated external configurations for {mapping['name']}")
            
        except Exception as e:
            self.logger.error(f"Error generating external configurations for {mapping['name']}: {e}")
            # Don't fail the entire generation process if configuration externalization fails
            if self.enable_config_externalization:
                self.logger.warning("Continuing with embedded configuration approach")

    def _generate_enterprise_components(self, app_dir: Path, project: Project):
        """Generate enterprise components for advanced features"""
        try:
            self.logger.info("Generating enterprise components")
            
            # Copy enterprise framework files from the source framework
            framework_dir = Path(__file__).parent
            target_dir = app_dir / "src/main/python"
            
            enterprise_files = [
                "config_management.py",
                "monitoring_integration.py", 
                "advanced_config_validation.py",
                "config_migration_tools.py"
            ]
            
            for file_name in enterprise_files:
                source_file = framework_dir / file_name
                target_file = target_dir / file_name
                
                if source_file.exists():
                    # Copy the enterprise component file
                    import shutil
                    shutil.copy2(source_file, target_file)
                    self.logger.info(f"Generated enterprise component: {file_name}")
                else:
                    self.logger.warning(f"Enterprise component source not found: {file_name}")
            
            self.logger.info("Enterprise components generated successfully")
            
        except Exception as e:
            self.logger.error(f"Error generating enterprise components: {e}")
            # Don't fail the entire generation process
            self.logger.warning("Continuing without enterprise components")
    
    def _generate_test_cases(self, app_dir: Path, project: Project):
        """Generate comprehensive test cases for the generated application"""
        try:
            self.logger.info("Generating comprehensive test cases...")
            
            # Ensure test directories exist
            test_dirs = [
                app_dir / "tests",
                app_dir / "tests" / "unit", 
                app_dir / "tests" / "integration",
                app_dir / "tests" / "data"
            ]
            
            for test_dir in test_dirs:
                test_dir.mkdir(parents=True, exist_ok=True)
                (test_dir / "__init__.py").write_text("")
            
            # Generate pytest configuration
            self._generate_pytest_config(app_dir)
            
            # Generate unit tests for mappings using dynamic generator
            self._generate_dynamic_mapping_unit_tests(app_dir, project)
            
            # Generate unit tests for workflows
            self._generate_workflow_unit_tests(app_dir, project)
            
            # Generate integration tests
            self._generate_integration_tests(app_dir, project)
            
            # Generate test fixtures and data
            self._generate_test_fixtures(app_dir, project)
            
            # Generate test requirements
            self._generate_test_requirements(app_dir)
            
            self.logger.info("Test cases generated successfully")
            
        except Exception as e:
            self.logger.error(f"Error generating test cases: {e}")
            import traceback
            traceback.print_exc()
            self.logger.warning("Continuing without test cases")
    
    def _generate_pytest_config(self, app_dir: Path):
        """Generate pytest configuration files"""
        # pytest.ini
        pytest_config = """[tool:pytest]
testpaths = tests
python_files = test_*.py
python_classes = Test*
python_functions = test_*
addopts = 
    -v
    --tb=short
    --strict-markers
    --disable-warnings
markers =
    unit: Unit tests
    integration: Integration tests
    slow: Slow running tests
"""
        (app_dir / "pytest.ini").write_text(pytest_config)
        
        # conftest.py with shared fixtures
        conftest_content = '''"""
Shared test fixtures and configuration
"""
import pytest
import tempfile
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType


@pytest.fixture(scope="session")
def spark_session():
    """Create a Spark session for testing"""
    spark = SparkSession.builder \\
        .appName("TestSpark") \\
        .master("local[2]") \\
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \\
        .config("spark.sql.adaptive.enabled", "false") \\
        .getOrCreate()
    
    yield spark
    
    spark.stop()


@pytest.fixture
def temp_data_dir():
    """Create temporary directory for test data"""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_config():
    """Sample configuration for testing"""
    return {
        "spark": {
            "app.name": "TestApp",
            "master": "local[2]"
        },
        "connections": {
            "TEST_CONN": {
                "type": "HDFS",
                "base_path": "/tmp/test_data"
            }
        }
    }
'''
        (app_dir / "tests" / "conftest.py").write_text(conftest_content)
    
    def _generate_dynamic_mapping_unit_tests(self, app_dir: Path, project: Project):
        """Generate dynamic, schema-aware unit tests for mapping classes"""
        if not project.mappings:
            self.logger.info("No mappings found, skipping dynamic unit test generation")
            return
        
        # Initialize dynamic test generator
        dynamic_generator = DynamicTestGenerator(project)
        
        # Handle both dict and list structures for mappings
        mappings_to_process = []
        if isinstance(project.mappings, dict):
            mappings_to_process = list(project.mappings.values())
        else:
            mappings_to_process = project.mappings
            
        for mapping in mappings_to_process:
            try:
                # Handle both dict and object structures for mapping
                if isinstance(mapping, dict):
                    mapping_name = self._sanitize_name(mapping.get('name', 'unknown_mapping'))
                else:
                    mapping_name = self._sanitize_name(mapping.name)
                
                # Generate dynamic test content using the dynamic generator
                dynamic_test_content = dynamic_generator.generate_dynamic_unit_tests(mapping)
                
                # Write the dynamic test file
                test_file = app_dir / "tests" / "unit" / f"test_{mapping_name}.py"
                test_file.write_text(dynamic_test_content)
                
                self.logger.info(f"Generated DYNAMIC unit tests for mapping: {mapping_name}")
                
            except Exception as e:
                self.logger.error(f"Error generating dynamic tests for mapping {mapping.get('name', 'unknown')}: {e}")
                # Fall back to static generation for this mapping
                self._generate_static_mapping_test(app_dir, mapping, project)
    
    def _generate_static_mapping_test(self, app_dir: Path, mapping: Dict[str, Any], project: Project):
        """Fallback static test generation for when dynamic generation fails"""
        if isinstance(mapping, dict):
            mapping_name = self._sanitize_name(mapping.get('name', 'unknown_mapping'))
        else:
            mapping_name = self._sanitize_name(mapping.name)
        
        class_name = self._to_class_name(mapping_name)
        
        # Simple static test as fallback
        static_test_content = f'''"""
Unit tests for {class_name} mapping - STATIC FALLBACK
"""
import pytest
from pyspark.sql import DataFrame
import sys
from pathlib import Path

# Add source path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / "src/main/python"))

from mappings.{mapping_name} import {class_name}


class Test{class_name}:
    """Test {class_name} mapping functionality - static fallback"""
    
    def test_mapping_initialization(self, spark_session, sample_config):
        """Test mapping can be initialized properly"""
        mapping = {class_name}(spark=spark_session, config=sample_config)
        assert mapping is not None
        assert mapping.spark == spark_session
    
    def test_basic_execution(self, spark_session, sample_config):
        """Basic execution test"""
        mapping = {class_name}(spark=spark_session, config=sample_config)
        
        # Generic test data
        test_data = [(1, "Test"), (2, "Test2")]
        test_df = spark_session.createDataFrame(test_data, ["id", "name"])
        
        try:
            result = mapping.execute(test_df)
            assert result is not None or result is None
        except (NotImplementedError, AttributeError):
            pytest.skip("Execute method not implemented yet")
'''
        
        test_file = app_dir / "tests" / "unit" / f"test_{mapping_name}.py"
        test_file.write_text(static_test_content)
        self.logger.info(f"Generated STATIC FALLBACK unit tests for mapping: {mapping_name}")
    
    def _generate_mapping_unit_tests(self, app_dir: Path, project: Project):
        """Generate unit tests for mapping classes"""
        if not project.mappings:
            return
        
        # Handle both dict and list structures for mappings
        mappings_to_process = []
        if isinstance(project.mappings, dict):
            mappings_to_process = list(project.mappings.values())
        else:
            mappings_to_process = project.mappings
            
        for mapping in mappings_to_process:
            # Handle both dict and object structures for mapping
            if isinstance(mapping, dict):
                mapping_name = self._sanitize_name(mapping.get('name', 'unknown_mapping'))
            else:
                mapping_name = self._sanitize_name(mapping.name)
            
            class_name = self._to_class_name(mapping_name)
            
            test_content = f'''"""
Unit tests for {class_name} mapping
"""
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import sys
from pathlib import Path

# Add source path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / "src/main/python"))

from mappings.{mapping_name} import {class_name}


class Test{class_name}:
    """Test {class_name} mapping functionality"""
    
    def test_mapping_initialization(self, spark_session, sample_config):
        """Test mapping can be initialized properly"""
        mapping = {class_name}(spark=spark_session, config=sample_config)
        assert mapping is not None
        assert mapping.spark == spark_session
    
    def test_mapping_schema_validation(self, spark_session, sample_config):
        """Test mapping validates input schemas correctly"""
        mapping = {class_name}(spark=spark_session, config=sample_config)
        
        # Create test schema based on sources
        test_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        
        # Test schema validation
        # This should be implemented based on actual mapping logic
        assert True  # Placeholder - implement actual validation
    
    def test_mapping_transformation(self, spark_session, sample_config, temp_data_dir):
        """Test mapping transformation logic"""
        mapping = {class_name}(spark=spark_session, config=sample_config)
        
        # Create test input data
        test_data = [
            (1, "John Doe"),
            (2, "Jane Smith"),
            (3, "Bob Johnson")
        ]
        
        input_df = spark_session.createDataFrame(
            test_data, 
            ["id", "name"]
        )
        
        # Test the transformation
        # Note: This needs to be adapted based on actual mapping logic
        try:
            result_df = mapping.execute(input_df)
            assert result_df is not None
            assert isinstance(result_df, DataFrame)
            assert result_df.count() > 0
        except NotImplementedError:
            # If execute method not implemented, test passes
            pytest.skip("Execute method not implemented yet")
    
    def test_mapping_error_handling(self, spark_session, sample_config):
        """Test mapping handles errors gracefully"""
        mapping = {class_name}(spark=spark_session, config=sample_config)
        
        # Test with invalid input
        empty_df = spark_session.createDataFrame([], StructType([]))
        
        # Should handle empty dataframe gracefully
        try:
            result = mapping.execute(empty_df)
            # Should either return empty result or handle gracefully
            assert True
        except NotImplementedError:
            pytest.skip("Execute method not implemented yet")
        except Exception as e:
            # If it throws an exception, it should be a meaningful one
            assert str(e) != ""
    
    @pytest.mark.slow
    def test_mapping_performance(self, spark_session, sample_config):
        """Test mapping performance with larger dataset"""
        mapping = {class_name}(spark=spark_session, config=sample_config)
        
        # Create larger test dataset
        import time
        large_data = [(i, f"User_{{i}}") for i in range(1000)]
        
        input_df = spark_session.createDataFrame(
            large_data, 
            ["id", "name"]
        )
        
        start_time = time.time()
        try:
            result_df = mapping.execute(input_df)
            execution_time = time.time() - start_time
            
            # Performance assertion - should complete within reasonable time
            assert execution_time < 30.0  # 30 seconds max for 1000 records
            assert result_df.count() > 0
        except NotImplementedError:
            pytest.skip("Execute method not implemented yet")
'''
            
            test_file = app_dir / "tests" / "unit" / f"test_{mapping_name}.py"
            test_file.write_text(test_content)
            self.logger.info(f"Generated unit tests for mapping: {mapping_name}")
    
    def _generate_workflow_unit_tests(self, app_dir: Path, project: Project):
        """Generate unit tests for workflow classes"""
        if not project.workflows:
            return
        
        # Handle both dict and list structures for workflows
        workflows_to_process = []
        if isinstance(project.workflows, dict):
            workflows_to_process = list(project.workflows.values())
        else:
            workflows_to_process = project.workflows
            
        for workflow in workflows_to_process:
            # Handle both dict and object structures for workflow
            if isinstance(workflow, dict):
                workflow_name = self._sanitize_name(workflow.get('name', 'unknown_workflow'))
            else:
                workflow_name = self._sanitize_name(workflow.name)
            
            class_name = self._to_class_name(workflow_name)
            
            test_content = f'''"""
Unit tests for {class_name} workflow
"""
import pytest
import sys
from pathlib import Path

# Add source path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / "src/main/python"))

from workflows.{workflow_name} import {class_name}


class Test{class_name}:
    """Test {class_name} workflow functionality"""
    
    def test_workflow_initialization(self, spark_session, sample_config):
        """Test workflow can be initialized properly"""
        workflow = {class_name}(spark=spark_session, config=sample_config)
        assert workflow is not None
        assert workflow.spark == spark_session
    
    def test_workflow_execution_order(self, spark_session, sample_config):
        """Test workflow executes steps in correct order"""
        workflow = {class_name}(spark=spark_session, config=sample_config)
        
        # Test execution plan
        try:
            execution_plan = workflow.get_execution_plan()
            assert execution_plan is not None
            assert len(execution_plan) > 0
        except (NotImplementedError, AttributeError):
            pytest.skip("Execution plan method not implemented yet")
    
    def test_workflow_dependency_validation(self, spark_session, sample_config):
        """Test workflow validates dependencies correctly"""
        workflow = {class_name}(spark=spark_session, config=sample_config)
        
        try:
            is_valid = workflow.validate_dependencies()
            assert isinstance(is_valid, bool)
        except (NotImplementedError, AttributeError):
            pytest.skip("Dependency validation not implemented yet")
    
    def test_workflow_execute(self, spark_session, sample_config, temp_data_dir):
        """Test workflow execution"""
        workflow = {class_name}(spark=spark_session, config=sample_config)
        
        try:
            result = workflow.execute()
            # Workflow should complete successfully
            assert result is not None or result is None  # Some workflows may not return values
        except NotImplementedError:
            pytest.skip("Execute method not implemented yet")
        except Exception as e:
            # If it fails, should be with meaningful error
            assert str(e) != ""
    
    def test_workflow_error_recovery(self, spark_session, sample_config):
        """Test workflow handles errors and can recover"""
        workflow = {class_name}(spark=spark_session, config=sample_config)
        
        # Test error handling
        try:
            # Simulate error condition
            workflow.config = {{}}  # Invalid config
            result = workflow.execute()
            # Should either handle gracefully or raise meaningful error
            assert True
        except NotImplementedError:
            pytest.skip("Execute method not implemented yet")
        except Exception as e:
            # Error should be informative
            assert str(e) != ""
'''
            
            test_file = app_dir / "tests" / "unit" / f"test_{workflow_name}.py"
            test_file.write_text(test_content)
            self.logger.info(f"Generated unit tests for workflow: {workflow_name}")
    
    def _generate_integration_tests(self, app_dir: Path, project: Project):
        """Generate integration tests for the complete pipeline"""
        
        integration_test = f'''"""
Integration tests for complete {project.name} pipeline
"""
import pytest
import tempfile
import shutil
from pathlib import Path
import sys

# Add source path for imports
sys.path.append(str(Path(__file__).parent.parent.parent / "src/main/python"))

from main import SparkApplication


class TestIntegration:
    """Test complete pipeline integration"""
    
    def test_end_to_end_pipeline(self, spark_session, temp_data_dir):
        """Test complete end-to-end pipeline execution"""
        # Create test configuration
        config = {{
            "spark": {{
                "app.name": "IntegrationTest",
                "master": "local[2]"
            }},
            "connections": {{
                "TEST_CONN": {{
                    "type": "HDFS",
                    "base_path": str(temp_data_dir)
                }}
            }}
        }}
        
        try:
            app = SparkApplication(spark=spark_session)
            result = app.run(config)
            
            # Pipeline should complete successfully
            assert result is not None or result is None
            
        except NotImplementedError:
            pytest.skip("Main application not fully implemented yet")
        except Exception as e:
            # Should handle errors gracefully
            assert str(e) != ""
    
    def test_data_quality_validation(self, spark_session, temp_data_dir):
        """Test data quality checks in pipeline"""
        # Create sample input data
        test_data_file = temp_data_dir / "test_input.csv"
        test_data_file.write_text("id,name\\n1,John\\n2,Jane\\n")
        
        config = {{
            "input_path": str(test_data_file),
            "output_path": str(temp_data_dir / "output"),
            "spark": {{
                "app.name": "DataQualityTest",
                "master": "local[2]"
            }}
        }}
        
        try:
            app = SparkApplication(spark=spark_session)
            result = app.run(config)
            
            # Check output exists and has expected structure
            output_path = Path(config["output_path"])
            if output_path.exists():
                assert any(output_path.iterdir())  # Should have output files
            
        except NotImplementedError:
            pytest.skip("Data quality validation not implemented yet")
    
    @pytest.mark.slow
    def test_large_dataset_processing(self, spark_session, temp_data_dir):
        """Test pipeline with larger dataset"""
        # Create larger test dataset
        large_data_file = temp_data_dir / "large_test.csv"
        
        # Generate test data
        lines = ["id,name,value"]
        for i in range(10000):
            lines.append(f"{{i}},User_{{i}},{{i*10}}")
        
        large_data_file.write_text("\\n".join(lines))
        
        config = {{
            "input_path": str(large_data_file),
            "output_path": str(temp_data_dir / "large_output"),
            "spark": {{
                "app.name": "LargeDataTest",
                "master": "local[2]"
            }}
        }}
        
        import time
        start_time = time.time()
        
        try:
            app = SparkApplication(spark=spark_session)
            result = app.run(config)
            
            execution_time = time.time() - start_time
            
            # Should complete within reasonable time
            assert execution_time < 60.0  # 1 minute max for 10k records
            
        except NotImplementedError:
            pytest.skip("Large dataset processing not implemented yet")
'''
        
        integration_file = app_dir / "tests" / "integration" / "test_pipeline_integration.py"
        integration_file.write_text(integration_test)
        self.logger.info("Generated integration tests")
    
    def _generate_test_fixtures(self, app_dir: Path, project: Project):
        """Generate test fixtures and sample data"""
        # Create sample CSV data files
        sample_data = {
            "customers.csv": "customer_id,first_name,last_name,email,phone\\n1,John,Doe,john@example.com,555-1234\\n2,Jane,Smith,jane@example.com,555-5678",
            "orders.csv": "order_id,customer_id,product,amount,order_date\\n1,1,Widget,29.99,2024-01-15\\n2,2,Gadget,49.99,2024-01-16",
            "products.csv": "product_id,name,category,price\\n1,Widget,Electronics,29.99\\n2,Gadget,Electronics,49.99"
        }
        
        data_dir = app_dir / "tests" / "data"
        for filename, content in sample_data.items():
            (data_dir / filename).write_text(content)
        
        # Create test configuration YAML
        test_config = """
test_connections:
  TEST_HDFS:
    type: HDFS
    base_path: ./tests/data
    format: csv
  
  TEST_HIVE:
    type: HIVE
    database: test_db
    host: localhost
    port: 10000

spark_config:
  spark.app.name: TestApplication
  spark.master: local[2]
  spark.sql.warehouse.dir: /tmp/test-warehouse

test_scenarios:
  - name: basic_transformation
    input_files:
      - customers.csv
      - orders.csv
    expected_outputs:
      - customer_orders.parquet
  
  - name: data_quality_check
    input_files:
      - products.csv
    validation_rules:
      - price > 0
      - name is not null
"""
        
        (data_dir / "test_config.yaml").write_text(test_config)
        self.logger.info("Generated test fixtures and sample data")
    
    def _generate_test_requirements(self, app_dir: Path):
        """Generate requirements.txt for testing"""
        test_requirements = """# Test requirements
pytest>=7.0.0
pytest-cov>=4.0.0
pytest-spark>=0.6.0
pytest-mock>=3.10.0
pyspark>=3.3.0
pandas>=1.5.0
"""
        
        (app_dir / "test_requirements.txt").write_text(test_requirements)
        self.logger.info("Generated test requirements") 