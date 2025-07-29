"""
m_Complete_Transformation_Showcase Enterprise Configuration-Driven Mapping Implementation
Generated from Informatica BDM Project: Enterprise_Complete_Transformations

Enterprise Features:
- Advanced configuration validation and static loading
- Comprehensive monitoring and metrics collection
- Configuration migration and versioning support
- Enterprise-grade error handling and alerting

Configuration Files:
- config/execution-plans/m_complete_transformation_showcase_execution_plan.json
- config/dag-analysis/m_complete_transformation_showcase_dag_analysis.json
- config/component-metadata/m_complete_transformation_showcase_components.json
- config/runtime/memory-profiles.yaml
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from typing import Dict, Any, List, Optional
import concurrent.futures
import logging
import time

from ..runtime.base_classes import BaseMapping, DataSourceManager
from ..transformations.generated_transformations import *
from ..runtime.config_management import EnterpriseConfigurationManager
from ..runtime.monitoring_integration import MonitoringIntegration
from ..runtime.advanced_config_validation import (
    ConfigurationSchemaValidator,
    ConfigurationIntegrityChecker,
)


class MCompleteTransformationShowcase(BaseMapping):
    """m_Complete_Transformation_Showcase enterprise configuration-driven mapping with advanced features"""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__("m_Complete_Transformation_Showcase", spark, config)

        # Enhanced configuration management (static loading for Spark)
        self.config_manager = EnterpriseConfigurationManager(
            mapping_name="m_Complete_Transformation_Showcase",
            config_dir=config.get("config_dir", "config"),
            environment=config.get("environment", "default"),
        )

        # Monitoring and metrics integration
        self.monitoring = MonitoringIntegration(
            enable_metrics=config.get("enable_monitoring", True),
            enable_alerts=config.get("enable_alerts", True),
        )

        # Configuration validation
        self.validator = ConfigurationSchemaValidator()
        self.integrity_checker = ConfigurationIntegrityChecker()

        # Load and validate configurations
        self._load_and_validate_configurations()

        # Initialize data source manager
        self.data_source_manager = DataSourceManager(
            spark, config.get("connections", {})
        )

        # Component data cache
        self.component_data_cache = {}

        # Start enterprise features
        self._start_enterprise_features()

    def _load_and_validate_configurations(self):
        """Load and validate all configurations with enterprise validation"""
        start_time = time.time()

        try:
            # Load configurations
            self.execution_plan = self.config_manager.load_execution_plan()
            self.dag_analysis = self.config_manager.load_dag_analysis()
            self.component_metadata = self.config_manager.load_component_metadata()
            self.memory_config = self.config_manager.load_memory_profile()

            load_time = time.time() - start_time

            # Record metrics
            if hasattr(self, "monitoring") and self.monitoring.config_monitor:
                self.monitoring.config_monitor.record_config_load_time(
                    "all_configs", load_time, "m_Complete_Transformation_Showcase"
                )

            # Validate configurations
            self._validate_configurations()

            self.logger.info(f"Loaded and validated configurations in {load_time:.3f}s")

        except Exception as e:
            self.logger.error(f"Error loading configurations: {e}")
            raise

    def _validate_configurations(self):
        """Validate configurations using enterprise validation framework"""
        validation_start = time.time()
        all_errors = 0
        all_warnings = 0

        try:
            # Validate execution plan
            if self.execution_plan:
                results = self.validator.validate_execution_plan(
                    self.execution_plan, "execution_plan"
                )
                errors = sum(1 for r in results if r.severity.value == "error")
                warnings = sum(1 for r in results if r.severity.value == "warning")
                all_errors += errors
                all_warnings += warnings

                if results:
                    self.logger.warning(
                        f"Execution plan validation: {errors} errors, {warnings} warnings"
                    )

            # Validate memory profiles
            if self.memory_config:
                results = self.validator.validate_memory_profiles(
                    self.memory_config, "memory_profiles"
                )
                errors = sum(1 for r in results if r.severity.value == "error")
                warnings = sum(1 for r in results if r.severity.value == "warning")
                all_errors += errors
                all_warnings += warnings

                if results:
                    self.logger.warning(
                        f"Memory profiles validation: {errors} errors, {warnings} warnings"
                    )

            # Validate component metadata
            if self.component_metadata:
                results = self.validator.validate_component_metadata(
                    self.component_metadata, "component_metadata"
                )
                errors = sum(1 for r in results if r.severity.value == "error")
                warnings = sum(1 for r in results if r.severity.value == "warning")
                all_errors += errors
                all_warnings += warnings

                if results:
                    self.logger.warning(
                        f"Component metadata validation: {errors} errors, {warnings} warnings"
                    )

            # Record validation metrics
            if hasattr(self, "monitoring") and self.monitoring.config_monitor:
                self.monitoring.config_monitor.record_config_validation_result(
                    "all_configs",
                    all_errors,
                    all_warnings,
                    "m_Complete_Transformation_Showcase",
                )

            validation_time = time.time() - validation_start
            self.logger.debug(
                f"Configuration validation completed in {validation_time:.3f}s"
            )

            if all_errors > 0:
                raise ValueError(
                    f"Configuration validation failed with {all_errors} errors"
                )

        except Exception as e:
            self.logger.error(f"Configuration validation error: {e}")
            raise

    def _start_enterprise_features(self):
        """Start enterprise features"""
        try:
            # Start monitoring
            if self.monitoring:
                self.monitoring.start()
                self.logger.info("Monitoring and metrics collection enabled")

        except Exception as e:
            self.logger.warning(f"Could not start enterprise features: {e}")

    def execute(self) -> bool:
        """Execute mapping with enterprise monitoring and metrics"""
        execution_start = time.time()
        success = False

        try:
            self.logger.info(
                f"Starting {self.execution_plan['mapping_name']} mapping execution"
            )

            # Record execution start metrics
            if self.monitoring and self.monitoring.config_monitor:
                self.monitoring.config_monitor.record_mapping_execution_metrics(
                    "m_Complete_Transformation_Showcase",
                    0,
                    len(self.execution_plan.get("phases", [])),
                    0,
                    False,
                )

            # Execute phases with enhanced monitoring
            phases = self.execution_plan.get("phases", [])
            for phase in phases:
                phase_start = time.time()
                phase_num = phase["phase_number"]
                components = phase["components"]

                self.logger.info(
                    f"Starting phase {phase_num} with {len(components)} component(s)"
                )

                # Execute phase with monitoring
                success = (
                    self._execute_parallel_components(components, phase_num)
                    if phase.get("can_run_parallel", False) and len(components) > 1
                    else self._execute_sequential_components(components, phase_num)
                )

                phase_duration = time.time() - phase_start

                # Record phase metrics
                if self.monitoring and self.monitoring.config_monitor:
                    self.monitoring.config_monitor.record_mapping_execution_metrics(
                        "m_Complete_Transformation_Showcase",
                        phase_num,
                        len(components),
                        phase_duration,
                        success,
                    )

                if not success:
                    self.logger.error(f"Phase {phase_num} failed")
                    return False

                self.logger.info(
                    f"Phase {phase_num} completed in {phase_duration:.3f}s"
                )

            execution_duration = time.time() - execution_start

            # Record final execution metrics
            if self.monitoring and self.monitoring.config_monitor:
                self.monitoring.config_monitor.record_mapping_execution_metrics(
                    "m_Complete_Transformation_Showcase",
                    -1,
                    len(phases),
                    execution_duration,
                    True,
                )

            self.logger.info(
                f"{self.execution_plan['mapping_name']} executed successfully in {execution_duration:.3f}s"
            )
            return True

        except Exception as e:
            execution_duration = time.time() - execution_start

            # Record failure metrics
            if self.monitoring and self.monitoring.config_monitor:
                self.monitoring.config_monitor.record_mapping_execution_metrics(
                    "m_Complete_Transformation_Showcase",
                    -1,
                    0,
                    execution_duration,
                    False,
                )

            self.logger.error(
                f"Error in {self.execution_plan['mapping_name']} mapping: {str(e)}"
            )
            raise

    def _execute_parallel_components(
        self, components: List[Dict], phase_num: int
    ) -> bool:
        """Execute components in parallel with enhanced monitoring"""
        max_workers = min(len(components), 4)

        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit with monitoring
            futures = {}
            for comp in components:
                component_name = comp["component_name"]
                future = executor.submit(
                    self._execute_component_with_monitoring, component_name
                )
                futures[future] = component_name

            # Collect results with monitoring
            success = True
            for future in concurrent.futures.as_completed(futures):
                component_name = futures[future]
                try:
                    result = future.result()
                    if not result:
                        self.logger.error(f"Component {component_name} failed")
                        success = False
                except Exception as e:
                    self.logger.error(
                        f"Component {component_name} threw exception: {str(e)}"
                    )
                    success = False

        return success

    def _execute_sequential_components(
        self, components: List[Dict], phase_num: int
    ) -> bool:
        """Execute components sequentially with enhanced monitoring"""
        for comp in components:
            component_name = comp["component_name"]
            success = self._execute_component_with_monitoring(component_name)
            if not success:
                self.logger.error(
                    f"Sequential execution failed at component: {component_name}"
                )
                return False
        return True

    def _execute_component_with_monitoring(self, component_name: str) -> bool:
        """Execute component with enterprise monitoring"""
        start_time = time.time()
        success = False

        try:
            # Get component metadata
            metadata = self._get_component_metadata(component_name)
            if not metadata:
                self.logger.error(f"No metadata found for component: {component_name}")
                return False

            component_type = metadata.get("component_type", "").lower()

            # Execute component
            if component_type == "source":
                success = self._execute_source(component_name, metadata)
            elif component_type == "transformation":
                success = self._execute_transformation(component_name, metadata)
            elif component_type == "target":
                success = self._execute_target(component_name, metadata)
            else:
                self.logger.warning(f"Unknown component type: {component_type}")
                return False

            duration = time.time() - start_time

            # Record component execution metrics
            if self.monitoring and self.monitoring.config_monitor:
                self.monitoring.config_monitor.record_component_execution_metrics(
                    "m_Complete_Transformation_Showcase",
                    component_name,
                    component_type,
                    duration,
                    success,
                )

            return success

        except Exception as e:
            duration = time.time() - start_time

            # Record failure metrics
            if self.monitoring and self.monitoring.config_monitor:
                self.monitoring.config_monitor.record_component_execution_metrics(
                    "m_Complete_Transformation_Showcase",
                    component_name,
                    "unknown",
                    duration,
                    False,
                )

            self.logger.error(f"Error executing component {component_name}: {str(e)}")
            return False

    def _execute_source(self, name: str, metadata: Dict) -> bool:
        """Execute source component with monitoring"""
        try:
            with self.monitoring.metrics_collector.timer(
                "source.read_time", {"source_name": name}
            ):
                df = self.data_source_manager.read_source(
                    name,
                    metadata.get("format", "hive"),
                    connection=metadata.get("connection", "default"),
                    table=metadata.get("table", name.lower()),
                )

                row_count = df.count()
                self.component_data_cache[name] = df

                # Record data metrics
                self.monitoring.metrics_collector.gauge(
                    "source.row_count", row_count, {"source_name": name}
                )

                self.logger.info(f"Read {row_count} rows from {name}")
                return True

        except Exception as e:
            self.logger.error(f"Error reading source {name}: {e}")
            return False

    def _execute_transformation(self, name: str, metadata: Dict) -> bool:
        """Execute transformation with monitoring"""
        try:
            # Get input dependencies
            dependencies = self._get_component_dependencies(name)
            input_dfs = [
                self.component_data_cache[dep]
                for dep in dependencies
                if dep in self.component_data_cache
            ]

            if not input_dfs:
                self.logger.error(f"No input DataFrames for {name}")
                return False

            # Apply transformation with timing
            transformation_type = metadata.get("type", "").replace("Transformation", "")

            with self.monitoring.metrics_collector.timer(
                "transformation.execution_time",
                {"transformation_name": name, "type": transformation_type},
            ):
                output_df = self._apply_transformation(
                    name, transformation_type, input_dfs, metadata
                )

                output_count = output_df.count()
                self.component_data_cache[name] = output_df

                # Record transformation metrics
                input_count = input_dfs[0].count() if input_dfs else 0
                selectivity = output_count / input_count if input_count > 0 else 0

                self.monitoring.metrics_collector.gauge(
                    "transformation.input_rows",
                    input_count,
                    {"transformation_name": name},
                )
                self.monitoring.metrics_collector.gauge(
                    "transformation.output_rows",
                    output_count,
                    {"transformation_name": name},
                )
                self.monitoring.metrics_collector.gauge(
                    "transformation.selectivity",
                    selectivity,
                    {"transformation_name": name},
                )

                self.logger.info(
                    f"Applied transformation: {name} ({input_count} -> {output_count} rows)"
                )
                return True

        except Exception as e:
            self.logger.error(f"Error applying transformation {name}: {e}")
            return False

    def _execute_target(self, name: str, metadata: Dict) -> bool:
        """Execute target component with monitoring"""
        try:
            dependencies = self._get_component_dependencies(name)
            if not dependencies or dependencies[0] not in self.component_data_cache:
                self.logger.error(f"No input data for target {name}")
                return False

            input_df = self.component_data_cache[dependencies[0]]

            with self.monitoring.metrics_collector.timer(
                "target.write_time", {"target_name": name}
            ):
                self.data_source_manager.write_target(
                    input_df,
                    name,
                    metadata.get("format", "hive"),
                    connection=metadata.get("connection", "default"),
                    table=metadata.get("table", name.lower()),
                    mode=(
                        "overwrite" if metadata.get("load_type") == "BULK" else "append"
                    ),
                )

                row_count = input_df.count()

                # Record target metrics
                self.monitoring.metrics_collector.gauge(
                    "target.written_rows", row_count, {"target_name": name}
                )

                self.logger.info(f"Wrote {row_count} rows to {name}")
                return True

        except Exception as e:
            self.logger.error(f"Error writing target {name}: {e}")
            return False

    def _apply_transformation(
        self,
        name: str,
        transformation_type: str,
        input_dfs: List[DataFrame],
        metadata: Dict,
    ) -> DataFrame:
        """Apply transformation using generated classes with monitoring"""
        # Record transformation attempt
        self.monitoring.metrics_collector.counter(
            "transformation.attempts", 1, {"type": transformation_type, "name": name}
        )

        # Transformation factory pattern with monitoring
        transformations = {
            "Expression": lambda: ExpressionTransformation(
                name,
                {
                    expr["name"]: expr["formula"]
                    for expr in metadata.get("expressions", [])
                },
            ),
            "Aggregator": lambda: AggregatorTransformation(
                name, metadata.get("group_by", []), metadata.get("aggregations", {})
            ),
            "Joiner": lambda: JoinerTransformation(
                name,
                metadata.get("join_conditions", []),
                metadata.get("join_type", "inner"),
            ),
            "Lookup": lambda: LookupTransformation(
                name, metadata.get("lookup_conditions", [])
            ),
            "Sequence": lambda: SequenceTransformation(
                name, metadata.get("start_value", 1), metadata.get("increment_value", 1)
            ),
            "Sorter": lambda: SorterTransformation(name, metadata.get("sort_keys", [])),
            "Router": lambda: RouterTransformation(
                name, metadata.get("output_groups", [])
            ),
            "Union": lambda: UnionTransformation(
                name, metadata.get("union_type", "UNION_ALL")
            ),
            "Java": lambda: JavaTransformation(
                name, metadata.get("logic_type", "custom")
            ),
        }

        transformation = transformations.get(transformation_type, lambda: None)()
        if transformation:
            result = transformation.transform(
                input_dfs[0], *input_dfs[1:] if len(input_dfs) > 1 else []
            )

            # Record successful transformation
            self.monitoring.metrics_collector.counter(
                "transformation.success", 1, {"type": transformation_type, "name": name}
            )
            return result
        else:
            self.logger.warning(
                f"Unsupported transformation type: {transformation_type}"
            )

            # Record unsupported transformation
            self.monitoring.metrics_collector.counter(
                "transformation.unsupported",
                1,
                {"type": transformation_type, "name": name},
            )
            return input_dfs[0]  # Pass through

    def _get_component_dependencies(self, component_name: str) -> List[str]:
        """Get component dependencies from DAG analysis"""
        dependency_graph = self.dag_analysis.get("dependency_graph", {})
        component_info = dependency_graph.get(component_name, {})
        return component_info.get("dependencies", [])

    def _get_component_metadata(self, component_name: str) -> Optional[Dict]:
        """Get component metadata from external configuration"""
        for category in ["sources", "transformations", "targets"]:
            for component in self.component_metadata.get(category, []):
                if component.get("name") == component_name:
                    component["component_type"] = category[:-1]  # Remove 's' suffix
                    return component
        return None

    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get current monitoring status and metrics"""
        if not self.monitoring:
            return {"monitoring_enabled": False}

        return self.monitoring.get_monitoring_status()

    def stop(self):
        """Stop enterprise features gracefully"""
        try:
            # Stop monitoring
            if self.monitoring:
                self.monitoring.stop()

            self.logger.info("Enterprise features stopped")

        except Exception as e:
            self.logger.warning(f"Error stopping enterprise features: {e}")

    def __enter__(self):
        """Context manager entry"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()
