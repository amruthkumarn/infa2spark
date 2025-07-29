"""
Configuration-Driven Mapping Template for Phase 2 Implementation
Generates lean mapping classes that load configurations from external files
"""

LEAN_MAPPING_TEMPLATE = '''"""
{{ mapping.name }} Configuration-Driven Mapping Implementation
Generated from Informatica BDM Project: {{ project.name }}

Configuration-driven mapping with externalized execution plans.
All execution plans, DAG analysis, and component configurations 
are loaded from external configuration files in config/ directory.

External Configuration Files:
- config/execution-plans/{{ mapping.name | lower }}_execution_plan.json
- config/dag-analysis/{{ mapping.name | lower }}_dag_analysis.json  
- config/component-metadata/{{ mapping.name | lower }}_components.json
- config/runtime/memory-profiles.yaml

Usage:
    mapping = {{ class_name }}(spark, {"environment": "production", "config_dir": "config"})
    success = mapping.execute()
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import concurrent.futures
from typing import Dict, Any, List, Optional
import logging
import os
from pathlib import Path

from ..runtime.base_classes import BaseMapping, DataSourceManager
from ..transformations.generated_transformations import *
from ..runtime.config_externalization import MappingConfigurationManager, RuntimeConfigResolver


class {{ class_name }}(BaseMapping):
    """{{ mapping.name }} configuration-driven mapping implementation"""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__("{{ mapping.name }}", spark, config)
        
        # Initialize configuration management
        config_dir = config.get("config_dir", "config")
        environment = config.get("environment", "default")
        
        self.config_manager = MappingConfigurationManager(
            mapping_name="{{ mapping.name }}",
            config_dir=config_dir,
            environment=environment
        )
        
        self.config_resolver = RuntimeConfigResolver(self.config_manager)
        
        # Load external configurations
        self.execution_plan = self.config_manager.load_execution_plan()
        self.dag_analysis = self.config_manager.load_dag_analysis()
        self.component_metadata = self.config_manager.load_component_metadata()
        
        # Resolve runtime configurations
        self.memory_config = self.config_manager.load_memory_profile(environment)
        self.monitoring_config = self.config_resolver.resolve_monitoring_config()
        
        # Initialize data source manager
        self.data_source_manager = DataSourceManager(
            spark, config.get("connections", {})
        )
        
        # Component data cache for intermediate results
        self.component_data_cache = {}
        
        # Initialize monitoring if enabled
        if self.monitoring_config.get("monitoring_enabled"):
            self._setup_monitoring()

    def execute(self) -> bool:
        """Execute mapping using configuration-driven DAG execution strategy"""
        try:
            self.logger.info(
                f"Starting {self.execution_plan['mapping_name']} mapping execution"
            )
            
            # Get execution strategy from external configuration
            strategy = self.config_resolver.resolve_execution_strategy()
            
            self.logger.info(
                f"Execution plan: {len(self.execution_plan['phases'])} phases, "
                f"estimated {self.execution_plan.get('estimated_duration', 'unknown')} seconds"
            )

            # Execute phases based on external configuration
            phases = self.execution_plan.get("phases", [])
            for phase in phases:
                phase_num = phase["phase_number"]
                components = phase["components"]
                
                self.logger.info(
                    f"Starting execution phase {phase_num} with {len(components)} component(s)"
                )

                if phase.get("can_run_parallel", False) and len(components) > 1:
                    success = self._execute_parallel_components(components, phase_num)
                else:
                    success = self._execute_sequential_components(components, phase_num)
                
                if not success:
                    self.logger.error(f"Phase {phase_num} failed")
                    return False

                self.logger.info(f"Phase {phase_num} completed successfully")

            self.logger.info(f"{self.execution_plan['mapping_name']} mapping executed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in {self.execution_plan['mapping_name']} mapping: {str(e)}")
            raise

    def _execute_parallel_components(self, component_group: List[Dict], phase_num: int) -> bool:
        """Execute components in parallel using ThreadPoolExecutor"""
        try:
            strategy = self.config_resolver.resolve_execution_strategy()
            max_workers = min(len(component_group), strategy.get("max_parallel_components", 4))
            
            self.logger.info(f"Executing {len(component_group)} components in parallel (max_workers={max_workers})")
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all component executions
                future_to_component = {
                    executor.submit(self._execute_component, comp["component_name"]): comp["component_name"]
                    for comp in component_group
                }
                
                # Wait for all to complete
                success = True
                for future in concurrent.futures.as_completed(future_to_component):
                    component_name = future_to_component[future]
                    try:
                        result = future.result()
                        if not result:
                            self.logger.error(f"Component {component_name} failed")
                            success = False
                    except Exception as e:
                        self.logger.error(f"Component {component_name} threw exception: {str(e)}")
                        success = False
                        
            return success
            
        except Exception as e:
            self.logger.error(f"Error in parallel execution of phase {phase_num}: {str(e)}")
            return False

    def _execute_sequential_components(self, component_group: List[Dict], phase_num: int) -> bool:
        """Execute components sequentially"""
        try:
            self.logger.info(f"Executing {len(component_group)} components sequentially")
            
            for comp in component_group:
                component_name = comp["component_name"]
                success = self._execute_component(component_name)
                if not success:
                    self.logger.error(f"Sequential execution failed at component: {component_name}")
                    return False
                    
            return True
            
        except Exception as e:
            self.logger.error(f"Error in sequential execution of phase {phase_num}: {str(e)}")
            return False

    def _execute_component(self, component_name: str) -> bool:
        """Execute a single component using configuration-driven approach"""
        try:
            # Get component configuration from external files
            component_config = self._get_component_config(component_name)
            
            if not component_config:
                self.logger.error(f"No configuration found for component: {component_name}")
                return False

            # Resolve component memory requirements from external configuration
            comp_type = component_config.get("transformation_type") or component_config.get("component_type", "")
            memory_config = self.config_resolver.resolve_component_memory_requirements(
                component_name, comp_type
            )
            
            # Apply memory configuration to Spark session if needed
            self._apply_component_memory_config(memory_config)
            
            # Execute based on component type from metadata
            component_metadata = self._get_component_metadata(component_name)
            if not component_metadata:
                self.logger.warning(f"No metadata found for component: {component_name}")
                return False
                
            comp_category = component_metadata.get("component_type", "").lower()
            
            if comp_category == "source":
                return self._execute_source_component(component_name, component_metadata)
            elif comp_category == "transformation":
                return self._execute_transformation_component(component_name, component_metadata)
            elif comp_category == "target":
                return self._execute_target_component(component_name, component_metadata)
            else:
                self.logger.warning(f"Unknown component type '{comp_category}' for component: {component_name}")
                return False

        except Exception as e:
            self.logger.error(f"Error executing component {component_name}: {str(e)}")
            return False

    def _get_component_config(self, component_name: str) -> Optional[Dict[str, Any]]:
        """Get component configuration from execution plan"""
        for phase in self.execution_plan.get("phases", []):
            for comp in phase.get("components", []):
                if comp.get("component_name") == component_name:
                    return comp
        return None

    def _get_component_metadata(self, component_name: str) -> Optional[Dict[str, Any]]:
        """Get component metadata from external configuration"""
        # Check sources
        for source in self.component_metadata.get("sources", []):
            if source.get("name") == component_name:
                source["component_type"] = "source"
                return source
                
        # Check transformations  
        for transformation in self.component_metadata.get("transformations", []):
            if transformation.get("name") == component_name:
                transformation["component_type"] = "transformation"
                return transformation
                
        # Check targets
        for target in self.component_metadata.get("targets", []):
            if target.get("name") == component_name:
                target["component_type"] = "target"
                return target
                
        return None

    def _execute_source_component(self, component_name: str, metadata: Dict[str, Any]) -> bool:
        """Execute source component using external metadata"""
        try:
            self.logger.info(f"Reading from source: {component_name}")
            
            # Get source configuration from metadata
            connection = metadata.get("connection", "default")
            table = metadata.get("table", component_name.lower())
            source_format = metadata.get("format", "hive")
            
            # Read data using DataSourceManager
            df = self.data_source_manager.read_source(
                component_name, 
                source_format,
                connection=connection,
                table=table
            )
            
            # Cache the result
            self.component_data_cache[component_name] = df
            
            self.logger.info(f"Successfully read {df.count()} rows from {component_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error reading source {component_name}: {str(e)}")
            return False

    def _execute_transformation_component(self, component_name: str, metadata: Dict[str, Any]) -> bool:
        """Execute transformation component using external metadata"""
        try:
            self.logger.info(f"Applying transformation: {component_name}")
            
            # Get transformation type from metadata
            transformation_type = metadata.get("type", "").replace("Transformation", "")
            
            # Get input dependencies from DAG analysis
            dependencies = self.config_resolver.get_component_dependencies(component_name)
            
            # Get input DataFrames
            input_dfs = []
            for dep in dependencies:
                if dep in self.component_data_cache:
                    input_dfs.append(self.component_data_cache[dep])
                else:
                    self.logger.error(f"Dependency {dep} not found in cache for {component_name}")
                    return False
            
            if not input_dfs:
                self.logger.error(f"No input DataFrames found for transformation: {component_name}")
                return False
                
            # Apply transformation based on type
            if transformation_type == "Expression":
                output_df = self._apply_expression_transformation(component_name, input_dfs[0], metadata)
            elif transformation_type == "Aggregator":
                output_df = self._apply_aggregator_transformation(component_name, input_dfs[0], metadata)
            elif transformation_type == "Joiner":
                output_df = self._apply_joiner_transformation(component_name, input_dfs, metadata)
            elif transformation_type == "Lookup":
                output_df = self._apply_lookup_transformation(component_name, input_dfs[0], metadata)
            elif transformation_type == "Sequence":
                output_df = self._apply_sequence_transformation(component_name, input_dfs[0], metadata)
            elif transformation_type == "Sorter":
                output_df = self._apply_sorter_transformation(component_name, input_dfs[0], metadata)
            elif transformation_type == "Router":
                output_df = self._apply_router_transformation(component_name, input_dfs[0], metadata)
            elif transformation_type == "Union":
                output_df = self._apply_union_transformation(component_name, input_dfs, metadata)
            elif transformation_type == "Java":
                output_df = self._apply_java_transformation(component_name, input_dfs[0], metadata)
            else:
                self.logger.warning(f"Unsupported transformation type: {transformation_type}")
                output_df = input_dfs[0]  # Pass through
            
            # Cache the result
            self.component_data_cache[component_name] = output_df
            
            self.logger.info(f"Successfully applied transformation: {component_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error applying transformation {component_name}: {str(e)}")
            return False

    def _execute_target_component(self, component_name: str, metadata: Dict[str, Any]) -> bool:
        """Execute target component using external metadata"""
        try:
            self.logger.info(f"Writing to target: {component_name}")
            
            # Get input dependencies
            dependencies = self.config_resolver.get_component_dependencies(component_name)
            
            if not dependencies:
                self.logger.error(f"No dependencies found for target: {component_name}")
                return False
                
            # Get input DataFrame (should be exactly one for targets)
            input_component = dependencies[0]
            if input_component not in self.component_data_cache:
                self.logger.error(f"Input component {input_component} not found in cache for target {component_name}")
                return False
                
            input_df = self.component_data_cache[input_component]
            
            # Get target configuration from metadata
            connection = metadata.get("connection", "default")
            table = metadata.get("table", component_name.lower())
            target_format = metadata.get("format", "hive")
            load_type = metadata.get("load_type", "BULK")
            
            # Write data using DataSourceManager
            self.data_source_manager.write_target(
                input_df,
                component_name,
                target_format,
                connection=connection,
                table=table,
                mode="overwrite" if load_type == "BULK" else "append"
            )
            
            self.logger.info(f"Successfully wrote {input_df.count()} rows to {component_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing target {component_name}: {str(e)}")
            return False

    def _apply_component_memory_config(self, memory_config: Dict[str, Any]):
        """Apply component-specific memory configuration to Spark session"""
        if not memory_config:
            return
            
        # Apply dynamic configuration if supported
        try:
            if "shuffle_partitions" in memory_config:
                self.spark.sql(f"SET spark.sql.shuffle.partitions={memory_config['shuffle_partitions']}")
                
            # Log configuration application
            self.logger.debug(f"Applied memory configuration: {memory_config}")
            
        except Exception as e:
            self.logger.warning(f"Could not apply memory configuration: {e}")

    def _setup_monitoring(self):
        """Setup monitoring and metrics collection"""
        try:
            # Initialize monitoring based on external configuration
            namespace = self.monitoring_config.get("metrics_namespace", "spark.etl")
            self.logger.info(f"Monitoring enabled with namespace: {namespace}")
            
            # Set checkpoint directory from configuration
            checkpoint_dir = self.monitoring_config.get("checkpoint_dir", "/tmp/spark-checkpoints")
            self.spark.sparkContext.setCheckpointDir(checkpoint_dir)
            
        except Exception as e:
            self.logger.warning(f"Could not setup monitoring: {e}")

    # Transformation methods that delegate to generated transformation classes
    def _apply_expression_transformation(self, name: str, input_df: DataFrame, metadata: Dict) -> DataFrame:
        """Apply expression transformation using metadata configuration"""
        expressions = {expr["name"]: expr["formula"] for expr in metadata.get("expressions", [])}
        transformation = ExpressionTransformation(name, expressions)
        return transformation.transform(input_df)

    def _apply_aggregator_transformation(self, name: str, input_df: DataFrame, metadata: Dict) -> DataFrame:
        """Apply aggregator transformation using metadata configuration"""
        group_by_cols = metadata.get("group_by", [])
        aggregations = metadata.get("aggregations", {})
        transformation = AggregatorTransformation(name, group_by_cols, aggregations)
        return transformation.transform(input_df)

    def _apply_joiner_transformation(self, name: str, input_dfs: List[DataFrame], metadata: Dict) -> DataFrame:
        """Apply joiner transformation using metadata configuration"""
        join_conditions = metadata.get("join_conditions", [])
        join_type = metadata.get("join_type", "inner")
        transformation = JoinerTransformation(name, join_conditions, join_type)
        return transformation.transform(input_dfs[0], input_dfs[1] if len(input_dfs) > 1 else None)

    def _apply_lookup_transformation(self, name: str, input_df: DataFrame, metadata: Dict) -> DataFrame:
        """Apply lookup transformation using metadata configuration"""
        join_conditions = metadata.get("lookup_conditions", [])
        transformation = LookupTransformation(name, join_conditions)
        return transformation.transform(input_df)

    def _apply_sequence_transformation(self, name: str, input_df: DataFrame, metadata: Dict) -> DataFrame:
        """Apply sequence transformation using metadata configuration"""
        start_value = metadata.get("start_value", 1)
        increment = metadata.get("increment_value", 1)
        transformation = SequenceTransformation(name, start_value, increment)
        return transformation.transform(input_df)

    def _apply_sorter_transformation(self, name: str, input_df: DataFrame, metadata: Dict) -> DataFrame:
        """Apply sorter transformation using metadata configuration"""
        sort_keys = metadata.get("sort_keys", [])
        transformation = SorterTransformation(name, sort_keys)
        return transformation.transform(input_df)

    def _apply_router_transformation(self, name: str, input_df: DataFrame, metadata: Dict) -> DataFrame:
        """Apply router transformation using metadata configuration"""
        output_groups = metadata.get("output_groups", [])
        transformation = RouterTransformation(name, output_groups)
        return transformation.transform(input_df)

    def _apply_union_transformation(self, name: str, input_dfs: List[DataFrame], metadata: Dict) -> DataFrame:
        """Apply union transformation using metadata configuration"""
        union_type = metadata.get("union_type", "UNION_ALL")
        transformation = UnionTransformation(name, union_type)
        return transformation.transform(input_dfs[0], *input_dfs[1:])

    def _apply_java_transformation(self, name: str, input_df: DataFrame, metadata: Dict) -> DataFrame:
        """Apply Java transformation using metadata configuration"""
        logic_type = metadata.get("logic_type", "custom")
        transformation = JavaTransformation(name, logic_type)
        return transformation.transform(input_df)
'''