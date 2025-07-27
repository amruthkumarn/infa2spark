"""
Ultra-Lean Configuration-Driven Mapping Template for Phase 2 Optimization
Generates minimal mapping classes by maximizing delegation to transformation classes
"""

ULTRA_LEAN_MAPPING_TEMPLATE = '''"""
{{ mapping.name }} Configuration-Driven Mapping Implementation
Generated from Informatica BDM Project: {{ project.name }}

Configuration-driven mapping with externalized execution plans.
All configurations loaded from external files in config/ directory.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from typing import Dict, Any, List, Optional
import concurrent.futures
import logging

from ..base_classes import BaseMapping, DataSourceManager
from ..transformations.generated_transformations import *
from ..config_externalization import MappingConfigurationManager, RuntimeConfigResolver


class {{ class_name }}(BaseMapping):
    """{{ mapping.name }} configuration-driven mapping implementation"""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__("{{ mapping.name }}", spark, config)
        
        # Initialize configuration management
        self.config_manager = MappingConfigurationManager(
            mapping_name="{{ mapping.name }}",
            config_dir=config.get("config_dir", "config"),
            environment=config.get("environment", "default")
        )
        
        self.config_resolver = RuntimeConfigResolver(self.config_manager)
        
        # Load external configurations
        self.execution_plan = self.config_manager.load_execution_plan()
        self.dag_analysis = self.config_manager.load_dag_analysis()
        self.component_metadata = self.config_manager.load_component_metadata()
        self.memory_config = self.config_manager.load_memory_profile()
        self.monitoring_config = self.config_resolver.resolve_monitoring_config()
        
        # Initialize data source manager
        self.data_source_manager = DataSourceManager(spark, config.get("connections", {}))
        
        # Component data cache
        self.component_data_cache = {}
        
        # Setup monitoring if enabled
        if self.monitoring_config.get("monitoring_enabled"):
            self._setup_monitoring()

    def execute(self) -> bool:
        """Execute mapping using configuration-driven DAG execution strategy"""
        try:
            self.logger.info(f"Starting {self.execution_plan['mapping_name']} mapping execution")
            
            # Execute phases from external configuration
            for phase in self.execution_plan.get("phases", []):
                phase_num = phase["phase_number"]
                components = phase["components"]
                
                self.logger.info(f"Starting phase {phase_num} with {len(components)} component(s)")

                success = (self._execute_parallel_components(components, phase_num) 
                          if phase.get("can_run_parallel", False) and len(components) > 1
                          else self._execute_sequential_components(components, phase_num))
                
                if not success:
                    self.logger.error(f"Phase {phase_num} failed")
                    return False

                self.logger.info(f"Phase {phase_num} completed successfully")

            self.logger.info(f"{self.execution_plan['mapping_name']} mapping executed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in {self.execution_plan['mapping_name']} mapping: {str(e)}")
            raise

    def _execute_parallel_components(self, components: List[Dict], phase_num: int) -> bool:
        """Execute components in parallel"""
        strategy = self.config_resolver.resolve_execution_strategy()
        max_workers = min(len(components), strategy.get("max_parallel_components", 4))
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self._execute_component, comp["component_name"]): comp["component_name"]
                      for comp in components}
            
            return all(future.result() for future in concurrent.futures.as_completed(futures))

    def _execute_sequential_components(self, components: List[Dict], phase_num: int) -> bool:
        """Execute components sequentially"""
        return all(self._execute_component(comp["component_name"]) for comp in components)

    def _execute_component(self, component_name: str) -> bool:
        """Execute a single component using configuration-driven approach"""
        try:
            # Get component metadata
            metadata = self._get_component_metadata(component_name)
            if not metadata:
                self.logger.error(f"No metadata found for component: {component_name}")
                return False

            # Apply memory configuration
            config = self._get_component_config(component_name)
            if config:
                memory_config = self.config_resolver.resolve_component_memory_requirements(
                    component_name, config.get("transformation_type", "")
                )
                self._apply_memory_config(memory_config)

            # Execute based on component type
            component_type = metadata.get("component_type", "").lower()
            
            if component_type == "source":
                return self._execute_source(component_name, metadata)
            elif component_type == "transformation":
                return self._execute_transformation(component_name, metadata)
            elif component_type == "target":
                return self._execute_target(component_name, metadata)
            else:
                self.logger.warning(f"Unknown component type: {component_type}")
                return False

        except Exception as e:
            self.logger.error(f"Error executing component {component_name}: {str(e)}")
            return False

    def _execute_source(self, name: str, metadata: Dict) -> bool:
        """Execute source component"""
        try:
            df = self.data_source_manager.read_source(
                name, 
                metadata.get("format", "hive"),
                connection=metadata.get("connection", "default"),
                table=metadata.get("table", name.lower())
            )
            self.component_data_cache[name] = df
            self.logger.info(f"Read {df.count()} rows from {name}")
            return True
        except Exception as e:
            self.logger.error(f"Error reading source {name}: {e}")
            return False

    def _execute_transformation(self, name: str, metadata: Dict) -> bool:
        """Execute transformation component using generated transformation classes"""
        try:
            # Get input dependencies and DataFrames
            dependencies = self.config_resolver.get_component_dependencies(name)
            input_dfs = [self.component_data_cache[dep] for dep in dependencies if dep in self.component_data_cache]
            
            if not input_dfs:
                self.logger.error(f"No input DataFrames for {name}")
                return False

            # Create and apply transformation using generated classes
            transformation_type = metadata.get("type", "").replace("Transformation", "")
            output_df = self._apply_transformation(name, transformation_type, input_dfs, metadata)
            
            self.component_data_cache[name] = output_df
            self.logger.info(f"Applied transformation: {name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error applying transformation {name}: {e}")
            return False

    def _execute_target(self, name: str, metadata: Dict) -> bool:
        """Execute target component"""
        try:
            dependencies = self.config_resolver.get_component_dependencies(name)
            if not dependencies or dependencies[0] not in self.component_data_cache:
                self.logger.error(f"No input data for target {name}")
                return False
                
            input_df = self.component_data_cache[dependencies[0]]
            
            self.data_source_manager.write_target(
                input_df,
                name,
                metadata.get("format", "hive"),
                connection=metadata.get("connection", "default"),
                table=metadata.get("table", name.lower()),
                mode="overwrite" if metadata.get("load_type") == "BULK" else "append"
            )
            
            self.logger.info(f"Wrote {input_df.count()} rows to {name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error writing target {name}: {e}")
            return False

    def _apply_transformation(self, name: str, transformation_type: str, input_dfs: List[DataFrame], metadata: Dict) -> DataFrame:
        """Apply transformation using generated transformation classes"""
        # Transformation factory pattern - delegate to generated classes
        transformations = {
            "Expression": lambda: ExpressionTransformation(name, {expr["name"]: expr["formula"] for expr in metadata.get("expressions", [])}),
            "Aggregator": lambda: AggregatorTransformation(name, metadata.get("group_by", []), metadata.get("aggregations", {})),
            "Joiner": lambda: JoinerTransformation(name, metadata.get("join_conditions", []), metadata.get("join_type", "inner")),
            "Lookup": lambda: LookupTransformation(name, metadata.get("lookup_conditions", [])),
            "Sequence": lambda: SequenceTransformation(name, metadata.get("start_value", 1), metadata.get("increment_value", 1)),
            "Sorter": lambda: SorterTransformation(name, metadata.get("sort_keys", [])),
            "Router": lambda: RouterTransformation(name, metadata.get("output_groups", [])),
            "Union": lambda: UnionTransformation(name, metadata.get("union_type", "UNION_ALL")),
            "Java": lambda: JavaTransformation(name, metadata.get("logic_type", "custom"))
        }
        
        transformation = transformations.get(transformation_type, lambda: None)()
        if transformation:
            return transformation.transform(input_dfs[0], *input_dfs[1:] if len(input_dfs) > 1 else [])
        else:
            self.logger.warning(f"Unsupported transformation type: {transformation_type}")
            return input_dfs[0]  # Pass through

    def _get_component_config(self, component_name: str) -> Optional[Dict]:
        """Get component configuration from execution plan"""
        for phase in self.execution_plan.get("phases", []):
            for comp in phase.get("components", []):
                if comp.get("component_name") == component_name:
                    return comp
        return None

    def _get_component_metadata(self, component_name: str) -> Optional[Dict]:
        """Get component metadata from external configuration"""
        for category in ["sources", "transformations", "targets"]:
            for component in self.component_metadata.get(category, []):
                if component.get("name") == component_name:
                    component["component_type"] = category[:-1]  # Remove 's' suffix
                    return component
        return None

    def _apply_memory_config(self, memory_config: Dict):
        """Apply component memory configuration"""
        if memory_config and "shuffle_partitions" in memory_config:
            try:
                self.spark.sql(f"SET spark.sql.shuffle.partitions={memory_config['shuffle_partitions']}")
                self.logger.debug(f"Applied memory config: {memory_config}")
            except Exception as e:
                self.logger.warning(f"Could not apply memory config: {e}")

    def _setup_monitoring(self):
        """Setup monitoring from external configuration"""
        try:
            checkpoint_dir = self.monitoring_config.get("checkpoint_dir", "/tmp/spark-checkpoints")
            self.spark.sparkContext.setCheckpointDir(checkpoint_dir)
            self.logger.info("Monitoring enabled")
        except Exception as e:
            self.logger.warning(f"Could not setup monitoring: {e}")
'''