"""
Configuration management for externalized mapping configurations
Implements Phase 1 of the Configuration Externalization Plan
"""
import json
import yaml
import os
import re
from typing import Dict, Any, Optional, List
from pathlib import Path
from datetime import datetime
import logging


class MappingConfigurationManager:
    """Manages loading and resolution of mapping configurations"""
    
    def __init__(self, mapping_name: str, config_dir: str = "config", environment: str = "default"):
        self.mapping_name = mapping_name
        self.config_dir = Path(config_dir)
        self.environment = environment
        self.logger = logging.getLogger(f"ConfigManager.{mapping_name}")
        
        # Cache for loaded configurations
        self._execution_plan = None
        self._dag_analysis = None
        self._memory_config = None
        self._component_metadata = None
        
    def load_execution_plan(self) -> Dict[str, Any]:
        """Load execution plan from JSON file with caching"""
        if self._execution_plan is None:
            plan_file = self.config_dir / "execution-plans" / f"{self.mapping_name}_execution_plan.json"
            
            if not plan_file.exists():
                self.logger.warning(f"Execution plan file not found: {plan_file}")
                return self._get_default_execution_plan()
                
            try:
                with open(plan_file, 'r') as f:
                    self._execution_plan = json.load(f)
                self.logger.info(f"Loaded execution plan from {plan_file}")
            except Exception as e:
                self.logger.error(f"Error loading execution plan: {e}")
                return self._get_default_execution_plan()
                
        return self._execution_plan
        
    def load_dag_analysis(self) -> Dict[str, Any]:
        """Load DAG analysis from JSON file with caching"""
        if self._dag_analysis is None:
            dag_file = self.config_dir / "dag-analysis" / f"{self.mapping_name}_dag_analysis.json"
            
            if not dag_file.exists():
                self.logger.warning(f"DAG analysis file not found: {dag_file}")
                return self._get_default_dag_analysis()
                
            try:
                with open(dag_file, 'r') as f:
                    self._dag_analysis = json.load(f)
                self.logger.info(f"Loaded DAG analysis from {dag_file}")
            except Exception as e:
                self.logger.error(f"Error loading DAG analysis: {e}")
                return self._get_default_dag_analysis()
                
        return self._dag_analysis
        
    def load_memory_profile(self, environment: str = None) -> Dict[str, Any]:
        """Load memory configuration with environment overrides"""
        env = environment or self.environment
        
        if self._memory_config is None:
            memory_file = self.config_dir / "runtime" / "memory-profiles.yaml"
            
            if not memory_file.exists():
                self.logger.warning(f"Memory profiles file not found: {memory_file}")
                return self._get_default_memory_config()
                
            try:
                with open(memory_file, 'r') as f:
                    base_config = yaml.safe_load(f)
                
                # Apply environment-specific overrides
                self._memory_config = self._apply_environment_overrides(base_config, env)
                self.logger.info(f"Loaded memory profile for environment: {env}")
            except Exception as e:
                self.logger.error(f"Error loading memory profile: {e}")
                return self._get_default_memory_config()
                
        return self._memory_config
        
    def load_component_metadata(self) -> Dict[str, Any]:
        """Load component metadata from JSON file"""
        if self._component_metadata is None:
            metadata_file = self.config_dir / "component-metadata" / f"{self.mapping_name}_components.json"
            
            if not metadata_file.exists():
                self.logger.warning(f"Component metadata file not found: {metadata_file}")
                return self._get_default_component_metadata()
                
            try:
                with open(metadata_file, 'r') as f:
                    self._component_metadata = json.load(f)
                self.logger.info(f"Loaded component metadata from {metadata_file}")
            except Exception as e:
                self.logger.error(f"Error loading component metadata: {e}")
                return self._get_default_component_metadata()
                
        return self._component_metadata
        
    def get_component_config(self, component_name: str, component_type: str = None) -> Dict[str, Any]:
        """Get configuration for specific component"""
        memory_config = self.load_memory_profile()
        
        # Get base configuration by component type
        base_config = {}
        if component_type:
            profiles = memory_config.get("memory_profiles", {})
            base_config = profiles.get(component_type.lower().replace("transformation", "_transformation"), {})
            
        # Apply component-specific overrides
        overrides = memory_config.get("component_overrides", {}).get(component_name, {})
        
        final_config = {**base_config, **overrides}
        self.logger.debug(f"Component config for {component_name}: {final_config}")
        
        return final_config
        
    def _apply_environment_overrides(self, base_config: Dict, environment: str) -> Dict[str, Any]:
        """Apply environment-specific configuration overrides"""
        env_config = base_config.get("environments", {}).get(environment, {})
        scale_factor = env_config.get("global_scale_factor", 1.0)
        
        # Apply scaling to memory configurations
        scaled_config = base_config.copy()
        
        if "memory_profiles" in base_config:
            scaled_config["memory_profiles"] = {}
            for profile_name, profile_config in base_config["memory_profiles"].items():
                scaled_profile = profile_config.copy()
                
                # Scale memory values
                for memory_key in ["driver_memory", "executor_memory"]:
                    if memory_key in scaled_profile:
                        memory_value = scaled_profile[memory_key]
                        scaled_value = self._scale_memory_value(memory_value, scale_factor)
                        scaled_profile[memory_key] = scaled_value
                        
                # Scale partition counts
                if "shuffle_partitions" in scaled_profile:
                    scaled_profile["shuffle_partitions"] = int(
                        scaled_profile["shuffle_partitions"] * scale_factor
                    )
                    
                scaled_config["memory_profiles"][profile_name] = scaled_profile
                
        # Merge environment-specific settings
        scaled_config.update(env_config)
        
        return scaled_config
        
    def _scale_memory_value(self, memory_str: str, scale_factor: float) -> str:
        """Scale memory value string (e.g., '2g' -> '4g')"""
        if not memory_str or scale_factor == 1.0:
            return memory_str
            
        # Parse memory value
        match = re.match(r'(\d+)([gmk])', str(memory_str).lower())
        if not match:
            return memory_str
            
        value, unit = match.groups()
        scaled_value = max(1, int(int(value) * scale_factor))  # Minimum 1
        return f"{scaled_value}{unit}"
        
    def _get_default_execution_plan(self) -> Dict[str, Any]:
        """Return default execution plan when file not found"""
        return {
            "mapping_name": self.mapping_name,
            "metadata": {
                "generated_date": datetime.now().isoformat(),
                "generator_version": "1.0.0",
                "source": "default_fallback"
            },
            "execution_strategy": {
                "data_flow_strategy": "port_based",
                "estimated_duration": 30,
                "parallel_execution_enabled": True,
                "max_parallel_components": 4,
                "failure_strategy": "fail_fast"
            },
            "phases": [
                {
                    "phase_number": 1,
                    "phase_type": "mixed_phase",
                    "can_run_parallel": False,
                    "estimated_phase_duration": 30,
                    "parallel_components": 1,
                    "components": []
                }
            ]
        }
        
    def _get_default_dag_analysis(self) -> Dict[str, Any]:
        """Return default DAG analysis when file not found"""
        return {
            "mapping_name": self.mapping_name,
            "dag_metadata": {
                "total_components": 0,
                "total_phases": 1,
                "is_valid_dag": True,
                "has_cycles": False
            },
            "dependency_graph": {},
            "execution_order": [],
            "parallel_groups": [[]],
            "execution_levels": {},
            "port_connections": []
        }
        
    def _get_default_memory_config(self) -> Dict[str, Any]:
        """Return default memory configuration when file not found"""
        return {
            "memory_profiles": {
                "expression_transformation": {
                    "driver_memory": "1g",
                    "executor_memory": "1g",
                    "executor_cores": 2,
                    "shuffle_partitions": 100
                },
                "aggregator_transformation": {
                    "driver_memory": "2g",
                    "executor_memory": "4g",
                    "executor_cores": 4,
                    "shuffle_partitions": 400
                },
                "lookup_transformation": {
                    "driver_memory": "2g",
                    "executor_memory": "3g",
                    "executor_cores": 3,
                    "shuffle_partitions": 200
                }
            },
            "component_overrides": {},
            "environments": {
                "default": {
                    "global_scale_factor": 1.0,
                    "max_executors": 4,
                    "log_level": "INFO"
                }
            }
        }
        
    def _get_default_component_metadata(self) -> Dict[str, Any]:
        """Return default component metadata when file not found"""
        return {
            "mapping_name": self.mapping_name,
            "sources": [],
            "transformations": [],
            "targets": []
        }


class RuntimeConfigResolver:
    """Resolves runtime configuration with environment-specific overrides"""
    
    def __init__(self, config_manager: MappingConfigurationManager):
        self.config_manager = config_manager
        self.logger = logging.getLogger(f"ConfigResolver.{config_manager.mapping_name}")
        
    def resolve_component_memory_requirements(self, component_name: str, component_type: str) -> Dict[str, Any]:
        """Resolve memory requirements for a component based on type and environment"""
        config = self.config_manager.get_component_config(component_name, component_type)
        self.logger.debug(f"Resolved memory config for {component_name}: {config}")
        return config
        
    def resolve_execution_strategy(self) -> Dict[str, Any]:
        """Resolve execution strategy with environment-specific optimizations"""
        execution_plan = self.config_manager.load_execution_plan()
        memory_config = self.config_manager.load_memory_profile()
        
        strategy = execution_plan.get("execution_strategy", {}).copy()
        
        # Apply environment-specific execution parameters
        env_settings = memory_config.get("environments", {}).get(self.config_manager.environment, {})
        if "max_executors" in env_settings:
            strategy["max_executors"] = env_settings["max_executors"]
            
        self.logger.info(f"Resolved execution strategy: {strategy}")
        return strategy
        
    def resolve_monitoring_config(self) -> Dict[str, Any]:
        """Resolve monitoring and metrics configuration"""
        memory_config = self.config_manager.load_memory_profile()
        env_settings = memory_config.get("environments", {}).get(self.config_manager.environment, {})
        
        monitoring_config = {
            "monitoring_enabled": env_settings.get("monitoring_enabled", False),
            "metrics_namespace": env_settings.get("metrics_namespace", "spark.etl"),
            "log_level": env_settings.get("log_level", "INFO"),
            "checkpoint_dir": env_settings.get("checkpoint_dir", "/tmp/spark-checkpoints")
        }
        
        self.logger.debug(f"Resolved monitoring config: {monitoring_config}")
        return monitoring_config
        
    def resolve_phase_execution_plan(self, phase_number: int) -> Optional[Dict[str, Any]]:
        """Resolve execution plan for a specific phase"""
        execution_plan = self.config_manager.load_execution_plan()
        
        for phase in execution_plan.get("phases", []):
            if phase.get("phase_number") == phase_number:
                return phase
                
        self.logger.warning(f"Phase {phase_number} not found in execution plan")
        return None
        
    def get_component_dependencies(self, component_name: str) -> List[str]:
        """Get dependencies for a specific component"""
        dag_analysis = self.config_manager.load_dag_analysis()
        dependency_graph = dag_analysis.get("dependency_graph", {})
        
        component_info = dependency_graph.get(component_name, {})
        dependencies = component_info.get("dependencies", [])
        
        self.logger.debug(f"Dependencies for {component_name}: {dependencies}")
        return dependencies