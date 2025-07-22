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

from .xml_parser import InformaticaXMLParser
from .base_classes import Project
from .enhanced_spark_generator import EnhancedSparkCodeGenerator


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
    
    def __init__(self, output_base_dir: str = "generated_spark_app"):
        self.output_base_dir = Path(output_base_dir)
        self.logger = logging.getLogger("SparkCodeGenerator")
        self.xml_parser = InformaticaXMLParser()
        self.enhanced_generator = EnhancedSparkCodeGenerator(output_base_dir)
        
        # Template environment for code generation
        self.jinja_env = Environment(
            loader=FileSystemLoader(Path(__file__).parent / "templates"),
            trim_blocks=True,
            lstrip_blocks=True
        )
        
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
            "logs"
        ]
        
        for dir_path in directories:
            (app_dir / dir_path).mkdir(parents=True, exist_ok=True)
            
        # Only create __init__.py files where they're actually needed (with content)
        python_packages = [
            "src/main/python",
            "src/main/python/mappings",
            "src/main/python/transformations",
            "src/main/python/workflows"
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
        """Read from HDFS source"""
        format_type = kwargs.get('format', 'parquet').lower()
        path = f"data/input/{source_name.lower()}"
        
        if format_type == 'parquet':
            return self.spark.read.parquet(path)
        elif format_type == 'csv':
            return self.spark.read.option("header", "true").csv(path)
        elif format_type == 'avro':
            return self.spark.read.format("avro").load(path)
        else:
            return self.spark.read.parquet(path)
            
    def _read_hive_source(self, source_name: str, **kwargs) -> DataFrame:
        """Read from Hive table"""
        return self.spark.table(source_name.lower())
        
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
        """Generate mapping classes from project mappings with production-ready code"""
        mappings = self._extract_mappings_from_project(project)
        
        self.logger.info(f"Generating {len(mappings)} production-ready mapping classes...")
        
        for mapping in mappings:
            # Use enhanced generator for production-ready transformations
            self.enhanced_generator.generate_production_mapping(app_dir, mapping, project)
            
            # Also generate the original version for comparison (can be removed later)
            # self._generate_single_mapping_class(app_dir, mapping, project)
    
    def _generate_single_mapping_class(self, app_dir: Path, mapping: Dict[str, Any], project: Project):
        """Generate a single mapping class"""
        class_name = self._to_class_name(mapping['name'])
        file_name = self._sanitize_name(mapping['name']) + '.py'
        
        mapping_template = '''"""
{{ mapping.name }} Mapping Implementation
Generated from Informatica BDM Project: {{ project.name }}

Components:
{%- for component in mapping.components %}
- {{ component.name }} ({{ component.type }})
{%- endfor %}
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ..base_classes import BaseMapping, DataSourceManager
from ..transformations.generated_transformations import *


class {{ class_name }}(BaseMapping):
    """{{ mapping.description or mapping.name + ' mapping implementation' }}"""
    
    def __init__(self, spark, config):
        super().__init__("{{ mapping.name }}", spark, config)
        self.data_source_manager = DataSourceManager(spark, config.get('connections', {}))
        
    def execute(self) -> bool:
        """Execute the {{ mapping.name }} mapping"""
        try:
            self.logger.info("Starting {{ mapping.name }} mapping execution")
            
            {%- set sources = mapping.components | selectattr('component_type', 'equalto', 'source') | list %}
            {%- set transformations = mapping.components | selectattr('component_type', 'equalto', 'transformation') | list %}
            {%- set targets = mapping.components | selectattr('component_type', 'equalto', 'target') | list %}
            
            # Read source data
            {%- for source in sources %}
            {{ source.name | lower }}_df = self._read_{{ source.name | lower }}()
            {%- endfor %}
            
            {%- if sources | length == 1 %}
            # Apply transformations
            current_df = {{ sources[0].name | lower }}_df
            {%- for transformation in transformations %}
            current_df = self._apply_{{ transformation.name | lower }}(current_df)
            {%- endfor %}
            
            # Write to targets
            {%- for target in targets %}
            self._write_to_{{ target.name | lower }}(current_df)
            {%- endfor %}
            
            {%- else %}
            # Multiple sources - implement join logic
            {%- for transformation in transformations %}
            {%- if transformation.type == 'Joiner' %}
            joined_df = self._apply_{{ transformation.name | lower }}({{ sources | map(attribute='name') | map('lower') | map('suffix_with', '_df') | join(', ') }})
            {%- endif %}
            {%- endfor %}
            
            # Apply remaining transformations
            current_df = joined_df
            {%- for transformation in transformations %}
            {%- if transformation.type != 'Joiner' %}
            current_df = self._apply_{{ transformation.name | lower }}(current_df)
            {%- endif %}
            {%- endfor %}
            
            # Write to targets
            {%- for target in targets %}
            self._write_to_{{ target.name | lower }}(current_df)
            {%- endfor %}
            {%- endif %}
            
            self.logger.info("{{ mapping.name }} mapping executed successfully")
            return True
            
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
        
        {%- if transformation.type == 'Expression' %}
        # Expression transformation logic
        transformation = ExpressionTransformation(
            name="{{ transformation.name }}",
            expressions=self._get_{{ transformation.name | lower }}_expressions(),
            filters=self._get_{{ transformation.name | lower }}_filters()
        )
        return transformation.transform(input_df)
        
        {%- elif transformation.type == 'Aggregator' %}
        # Aggregator transformation logic
        transformation = AggregatorTransformation(
            name="{{ transformation.name }}",
            group_by_cols=self._get_{{ transformation.name | lower }}_group_by(),
            aggregations=self._get_{{ transformation.name | lower }}_aggregations()
        )
        return transformation.transform(input_df)
        
        {%- elif transformation.type == 'Lookup' %}
        # Lookup transformation logic
        lookup_df = self._get_{{ transformation.name | lower }}_lookup_data()
        transformation = LookupTransformation(
            name="{{ transformation.name }}",
            join_conditions=self._get_{{ transformation.name | lower }}_join_conditions(),
            join_type="left"
        )
        return transformation.transform(input_df, lookup_df)
        
        {%- elif transformation.type == 'Joiner' %}
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
        
        {%- elif transformation.type == 'Java' %}
        # Java/Custom transformation logic
        transformation = JavaTransformation(
            name="{{ transformation.name }}",
            logic_type="custom"
        )
        return transformation.transform(input_df)
        
        {%- else %}
        # Generic transformation
        self.logger.warning("Unknown transformation type: {{ transformation.type }}")
        return input_df
        {%- endif %}
    
    {%- if transformation.type == 'Expression' %}
    def _get_{{ transformation.name | lower }}_expressions(self) -> dict:
        """Get expression transformation expressions"""
        return {
            # Add your expression logic here
            "processed_date": "current_date()",
            "load_timestamp": "current_timestamp()"
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
    {%- endif %}
    {%- endfor %}
    
    {%- for target in targets %}
    def _write_to_{{ target.name | lower }}(self, output_df: DataFrame):
        """Write to {{ target.name }} target"""
        self.logger.info("Writing to {{ target.name }} target")
        
        # Add audit columns
        final_df = output_df.withColumn("load_date", current_date()) \\
                           .withColumn("load_timestamp", current_timestamp()) \\
                           .withColumn("source_system", lit("{{ project.name | upper }}"))
        
        self.data_source_manager.write_target(
            final_df, 
            "{{ target.name }}", 
            "{{ target.type }}",
            mode="overwrite"
        )
    {%- endfor %}
'''

        template = Template(mapping_template)
        output = template.render(
            mapping=mapping, 
            project=project, 
            class_name=class_name
        )
        
        mapping_file = app_dir / "src/main/python/mappings" / file_name
        mapping_file.write_text(output)
        self.logger.info(f"Generated mapping class: {file_name}")
    
    def _generate_workflow_classes(self, app_dir: Path, project: Project):
        """Generate workflow orchestration classes"""
        workflows = self._extract_workflows_from_project(project)
        
        for workflow in workflows:
            self._generate_single_workflow_class(app_dir, workflow, project)
    
    def _generate_single_workflow_class(self, app_dir: Path, workflow: Dict[str, Any], project: Project):
        """Generate a single workflow class"""
        class_name = self._to_class_name(workflow['name'])
        file_name = self._sanitize_name(workflow['name']) + '.py'
        
        workflow_template = '''"""
{{ workflow.name }} Workflow Implementation
Generated from Informatica BDM Project: {{ project.name }}
"""
from ..base_classes import BaseWorkflow
{%- for task in workflow.tasks %}
{%- if task.type == 'Mapping' %}
from ..mappings.{{ task.mapping | lower }} import {{ task.mapping | title | replace('_', '') }}
{%- endif %}
{%- endfor %}
import time


class {{ class_name }}(BaseWorkflow):
    """{{ workflow.description or workflow.name + ' workflow implementation' }}"""
    
    def __init__(self, spark, config):
        super().__init__("{{ workflow.name }}", spark, config)
        
        # Initialize mapping classes
        self.mapping_classes = {
            {%- for task in workflow.tasks %}
            {%- if task.type == 'Mapping' %}
            "{{ task.name }}": {{ task.mapping | title | replace('_', '') }},
            {%- endif %}
            {%- endfor %}
        }
        
        # Task execution order based on dependencies
        self.execution_order = [
            {%- for task in workflow.tasks %}
            "{{ task.name }}",
            {%- endfor %}
        ]
        
    def execute(self) -> bool:
        """Execute the complete workflow"""
        try:
            self.logger.info("Starting {{ workflow.name }} workflow")
            start_time = time.time()
            
            # Execute tasks in order
            for task_name in self.execution_order:
                success = self._execute_task(task_name)
                if not success:
                    self.logger.error(f"Task {task_name} failed. Stopping workflow.")
                    self._handle_workflow_failure(task_name)
                    return False
                    
            # Calculate execution time
            execution_time = time.time() - start_time
            self.logger.info(f"{{ workflow.name }} completed successfully in {execution_time:.2f} seconds")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in {{ workflow.name }} workflow: {str(e)}")
            self._handle_workflow_failure("UNKNOWN")
            raise
            
    def _execute_task(self, task_name: str) -> bool:
        """Execute a single task"""
        try:
            self.logger.info(f"Executing task: {task_name}")
            task_start_time = time.time()
            
            if task_name in self.mapping_classes:
                # Execute mapping task
                mapping_class = self.mapping_classes[task_name]
                mapping = mapping_class(self.spark, self.config)
                success = mapping.execute()
                
            {%- for task in workflow.tasks %}
            {%- if task.type == 'Email' %}
            elif task_name == "{{ task.name }}":
                # Execute email notification task
                success = self._send_notification("{{ task.name }}")
            {%- endif %}
            {%- endfor %}
                
            else:
                self.logger.warning(f"Task {task_name} not implemented")
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
    
    {%- for task in workflow.tasks %}
    {%- if task.type == 'Email' %}
    def _send_notification(self, task_name: str) -> bool:
        """Send email notification"""
        try:
            self.logger.info(f"Sending notification for task: {task_name}")
            
            # Email notification logic
            {%- if task.properties %}
            {%- for prop in task.properties %}
            {%- if prop.name == 'Recipient' %}
            recipients = ["{{ prop.value }}"]
            {%- endif %}
            {%- if prop.name == 'Subject' %}
            subject = "{{ prop.value }}"
            {%- endif %}
            {%- endfor %}
            {%- else %}
            recipients = ["admin@company.com"]
            subject = "Workflow Completed"
            {%- endif %}
            
            message = f"{{ workflow.name }} workflow completed successfully at {time.strftime('%Y-%m-%d %H:%M:%S')}"
            
            # Simulate email sending (replace with actual email logic)
            self.logger.info(f"Email sent to {recipients} with subject: {subject}")
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
            workflow=workflow, 
            project=project, 
            class_name=class_name
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
from workflows.{{ main_workflow.name | lower }} import {{ main_workflow.name | title | replace('_', '') }}
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
        workflow = {{ main_workflow.name | title | replace('_', '') }}(spark, config)
        
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
        if hasattr(self.enhanced_generator, 'parameter_manager') and self.enhanced_generator.parameter_manager:
            self._setup_project_parameters(project, self.enhanced_generator.parameter_manager)
            enhanced_config = self.enhanced_generator.parameter_manager.export_typed_config()
            if enhanced_config:
                parameters.update(enhanced_config.get('project', {}))
                parameters.update(enhanced_config.get('global', {}))
        
        # Application configuration
        connections_dict = {}
        if connections:
            for conn in connections:
                if isinstance(conn, dict):
                    # Connection is properly formatted as dictionary
                    conn_name = conn.get('name', 'unknown_connection')
                    connections_dict[conn_name] = {
                        'type': conn.get('type', 'UNKNOWN'),
                        'host': conn.get('host', 'localhost'),
                        'port': conn.get('port', ''),
                        'database': conn.get('database', '')
                    }
                elif isinstance(conn, str):
                    # Connection is just a string, create a basic entry
                    connections_dict[conn] = {
                        'type': 'UNKNOWN',
                        'host': 'localhost',
                        'port': '',
                        'database': ''
                    }
        
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

    def _extract_connections_from_project(self, project: Project) -> List[Dict[str, Any]]:
        """Extract connections from parsed project"""
        return getattr(project, 'connections', [])
    
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
    
    def _generate_transformations_module(self, app_dir: Path):
        """Generate the transformations module"""
        transformations_content = '''"""
Generated Transformation Classes
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ..base_classes import BaseTransformation


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