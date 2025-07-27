"""
Configuration File Generator for Externalized Mapping Configurations
Implements Phase 1 of the Configuration Externalization Plan
"""
import json
import yaml
import os
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime
import logging


class ConfigurationFileGenerator:
    """Generates external configuration files for mappings"""
    
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.conf_dir = self.output_dir / "config"
        self.logger = logging.getLogger("ConfigFileGenerator")
        
    def generate_all_configs(self, mapping_info: Dict, dag_analysis: Dict, execution_plan: Dict):
        """Generate all configuration files for a mapping"""
        mapping_name = mapping_info.get("name", "unknown_mapping")
        
        self.logger.info(f"Generating external configurations for mapping: {mapping_name}")
        
        # Create configuration directories
        self._create_config_directories()
        
        # Generate configuration files
        self.generate_execution_plan_config(execution_plan, mapping_name)
        self.generate_dag_analysis_config(dag_analysis, mapping_name)
        self.generate_component_metadata_config(mapping_info, mapping_name)
        self.generate_memory_profiles_config(mapping_info.get("components", []))
        
        self.logger.info(f"Successfully generated all configuration files for {mapping_name}")
        
    def _create_config_directories(self):
        """Create configuration directory structure"""
        dirs = [
            "execution-plans",
            "dag-analysis", 
            "component-metadata",
            "runtime"
        ]
        
        self.conf_dir.mkdir(parents=True, exist_ok=True)
        
        for dir_name in dirs:
            dir_path = self.conf_dir / dir_name
            dir_path.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Created configuration directory: {dir_path}")
            
    def generate_execution_plan_config(self, execution_plan: Dict, mapping_name: str):
        """Generate execution plan JSON file"""
        config_file = self.conf_dir / "execution-plans" / f"{mapping_name}_execution_plan.json"
        
        # Enhance execution plan with metadata
        enhanced_plan = {
            "mapping_name": mapping_name,
            "metadata": {
                "generated_date": datetime.now().isoformat(),
                "generator_version": "1.0.0",
                "description": f"Execution plan for {mapping_name} mapping"
            },
            **execution_plan
        }
        
        # Add optimization hints if not present
        if "optimization_hints" not in enhanced_plan:
            enhanced_plan["optimization_hints"] = {
                "cache_intermediate_results": True,
                "checkpoint_frequency": "per_phase",
                "resource_scaling": "auto"
            }
        
        try:
            with open(config_file, 'w') as f:
                json.dump(enhanced_plan, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Generated execution plan config: {config_file}")
        except Exception as e:
            self.logger.error(f"Error generating execution plan config: {e}")
            raise
            
    def generate_dag_analysis_config(self, dag_analysis: Dict, mapping_name: str):
        """Generate DAG analysis JSON file"""
        config_file = self.conf_dir / "dag-analysis" / f"{mapping_name}_dag_analysis.json"
        
        # Enhance DAG analysis with metadata
        enhanced_dag = {
            "mapping_name": mapping_name,
            "dag_metadata": {
                "total_components": dag_analysis.get("total_components", 0),
                "total_phases": len(dag_analysis.get("parallel_groups", [])),
                "is_valid_dag": dag_analysis.get("is_valid_dag", True),
                "has_cycles": not dag_analysis.get("is_valid_dag", True),
                "generated_date": datetime.now().isoformat(),
                **dag_analysis.get("dag_metadata", {})
            },
            **{k: v for k, v in dag_analysis.items() if k != "dag_metadata"}
        }
        
        # Add performance analysis if not present
        if "performance_analysis" not in enhanced_dag:
            enhanced_dag["performance_analysis"] = {
                "bottlenecks": self._identify_bottlenecks(dag_analysis),
                "optimization_opportunities": self._identify_optimization_opportunities(dag_analysis)
            }
        
        try:
            with open(config_file, 'w') as f:
                json.dump(enhanced_dag, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Generated DAG analysis config: {config_file}")
        except Exception as e:
            self.logger.error(f"Error generating DAG analysis config: {e}")
            raise
            
    def generate_component_metadata_config(self, mapping_info: Dict, mapping_name: str):
        """Generate component metadata JSON file"""
        config_file = self.conf_dir / "component-metadata" / f"{mapping_name}_components.json"
        
        # Extract and structure component metadata
        metadata = {
            "mapping_name": mapping_name,
            "metadata": {
                "generated_date": datetime.now().isoformat(),
                "total_components": len(mapping_info.get("components", [])),
                "description": f"Component metadata for {mapping_name} mapping"
            },
            "sources": [],
            "transformations": [],
            "targets": []
        }
        
        # Categorize components
        for component in mapping_info.get("components", []):
            comp_type = component.get("component_type", "").lower()
            
            # Add data profiling estimates
            enhanced_component = self._enhance_component_metadata(component)
            
            if comp_type == "source":
                metadata["sources"].append(enhanced_component)
            elif comp_type == "transformation":
                metadata["transformations"].append(enhanced_component)
            elif comp_type == "target":
                metadata["targets"].append(enhanced_component)
            else:
                # Default to transformation if type is unclear
                metadata["transformations"].append(enhanced_component)
                
        try:
            with open(config_file, 'w') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Generated component metadata config: {config_file}")
        except Exception as e:
            self.logger.error(f"Error generating component metadata config: {e}")
            raise
            
    def generate_memory_profiles_config(self, components: List[Dict]):
        """Generate memory profiles YAML file"""
        config_file = self.conf_dir / "runtime" / "memory-profiles.yaml"
        
        # Build memory profiles from component analysis
        memory_config = {
            "memory_profiles": self._build_memory_profiles(components),
            "component_overrides": self._build_component_overrides(components),
            "environments": {
                "development": {
                    "global_scale_factor": 0.5,
                    "max_executors": 2,
                    "checkpoint_dir": "/tmp/spark-checkpoints",
                    "log_level": "DEBUG",
                    "monitoring_enabled": False
                },
                "testing": {
                    "global_scale_factor": 0.7,
                    "max_executors": 4,
                    "checkpoint_dir": "/tmp/spark-checkpoints",
                    "log_level": "INFO",
                    "monitoring_enabled": True
                },
                "production": {
                    "global_scale_factor": 2.0,
                    "max_executors": 20,
                    "checkpoint_dir": "s3a://spark-checkpoints/",
                    "log_level": "WARN",
                    "monitoring_enabled": True,
                    "metrics_namespace": "spark.etl"
                }
            },
            "metadata": {
                "generated_date": datetime.now().isoformat(),
                "description": "Memory and resource profiles for Spark transformations"
            }
        }
        
        try:
            with open(config_file, 'w') as f:
                yaml.dump(memory_config, f, default_flow_style=False, indent=2, allow_unicode=True)
            self.logger.info(f"Generated memory profiles config: {config_file}")
        except Exception as e:
            self.logger.error(f"Error generating memory profiles config: {e}")
            raise
            
    def _build_memory_profiles(self, components: List[Dict]) -> Dict[str, Dict]:
        """Build memory profiles from component analysis"""
        profiles = {
            "expression_transformation": {
                "driver_memory": "1g",
                "executor_memory": "1g",
                "executor_cores": 2,
                "shuffle_partitions": 100,
                "dynamic_allocation": False,
                "description": "Standard configuration for expression transformations"
            },
            "aggregator_transformation": {
                "driver_memory": "2g",
                "executor_memory": "4g",
                "executor_cores": 4,
                "shuffle_partitions": 400,
                "dynamic_allocation": True,
                "description": "High-memory configuration for aggregation operations"
            },
            "lookup_transformation": {
                "driver_memory": "2g",
                "executor_memory": "3g",
                "executor_cores": 3,
                "shuffle_partitions": 200,
                "cache_tables": True,
                "broadcast_threshold": "100MB",
                "description": "Optimized configuration for lookup operations"
            },
            "joiner_transformation": {
                "driver_memory": "2g",
                "executor_memory": "3g",
                "executor_cores": 4,
                "shuffle_partitions": 200,
                "broadcast_threshold": "100MB",
                "description": "Configuration for join operations"
            },
            "sequence_transformation": {
                "driver_memory": "1g",
                "executor_memory": "1g",
                "executor_cores": 1,
                "shuffle_partitions": 50,
                "dynamic_allocation": False,
                "description": "Lightweight configuration for sequence generation"
            },
            "sorter_transformation": {
                "driver_memory": "1g",
                "executor_memory": "2g",
                "executor_cores": 3,
                "shuffle_partitions": 200,
                "description": "Configuration for sorting operations"
            },
            "router_transformation": {
                "driver_memory": "1g",
                "executor_memory": "1g",
                "executor_cores": 2,
                "shuffle_partitions": 100,
                "description": "Configuration for routing/filtering operations"
            },
            "union_transformation": {
                "driver_memory": "1g",
                "executor_memory": "1g",
                "executor_cores": 2,
                "shuffle_partitions": 100,
                "description": "Configuration for union operations"
            },
            "java_transformation": {
                "driver_memory": "1g",
                "executor_memory": "1g",
                "executor_cores": 2,
                "shuffle_partitions": 100,
                "description": "Configuration for custom Java transformations"
            }
        }
        
        # Analyze actual components to adjust profiles if needed
        type_counts = {}
        for component in components:
            comp_type = component.get("type", "").lower()
            if "transformation" in comp_type:
                clean_type = comp_type.replace("transformation", "_transformation")
                type_counts[clean_type] = type_counts.get(clean_type, 0) + 1
        
        self.logger.debug(f"Component type distribution: {type_counts}")
        
        return profiles
        
    def _build_component_overrides(self, components: List[Dict]) -> Dict[str, Dict]:
        """Build component-specific overrides based on analysis"""
        overrides = {}
        
        for component in components:
            comp_name = component.get("name", "")
            comp_type = component.get("type", "").lower()
            
            # Add overrides for known resource-intensive components
            if "aggregator" in comp_type and "customer" in comp_name.lower():
                overrides[comp_name] = {
                    "executor_memory": "6g",
                    "shuffle_partitions": 800,
                    "reason": "Large customer aggregation dataset"
                }
            elif "lookup" in comp_type and "demographic" in comp_name.lower():
                overrides[comp_name] = {
                    "cache_tables": True,
                    "broadcast_threshold": "200MB",
                    "reason": "Frequently accessed lookup table"
                }
            elif "scd" in comp_name.lower() or "java" in comp_type:
                overrides[comp_name] = {
                    "executor_memory": "2g",
                    "shuffle_partitions": 300,
                    "reason": "Complex SCD logic requires additional memory"
                }
        
        return overrides
        
    def _enhance_component_metadata(self, component: Dict) -> Dict:
        """Enhance component metadata with additional information"""
        enhanced = component.copy()
        
        # Add data profiling estimates
        comp_name = component.get("name", "")
        comp_type = component.get("type", "").lower()
        
        if component.get("component_type", "").lower() == "source":
            enhanced["data_profile"] = {
                "estimated_row_count": self._estimate_row_count(comp_name),
                "estimated_size_mb": self._estimate_data_size(comp_name),
                "partitioning": self._suggest_partitioning(comp_name)
            }
        elif "aggregator" in comp_type:
            enhanced["performance_hints"] = {
                "use_columnar_format": True,
                "enable_predicate_pushdown": True,
                "estimated_reduction_factor": 0.1
            }
        elif "lookup" in comp_type:
            enhanced["performance_hints"] = {
                "cache_strategy": "broadcast_if_small",
                "index_recommendations": ["primary_key"],
                "estimated_lookup_ratio": 0.8
            }
            
        return enhanced
        
    def _estimate_row_count(self, component_name: str) -> int:
        """Estimate row count based on component name patterns"""
        name_lower = component_name.lower()
        
        if "customer" in name_lower:
            return 1000000  # 1M customers
        elif "transaction" in name_lower:
            return 10000000  # 10M transactions
        elif "product" in name_lower:
            return 100000  # 100K products
        else:
            return 500000  # Default 500K rows
            
    def _estimate_data_size(self, component_name: str) -> int:
        """Estimate data size in MB based on component name patterns"""
        row_count = self._estimate_row_count(component_name)
        
        # Estimate ~150 bytes per row on average
        size_mb = int((row_count * 150) / (1024 * 1024))
        return max(10, size_mb)  # Minimum 10MB
        
    def _suggest_partitioning(self, component_name: str) -> str:
        """Suggest partitioning strategy based on component name"""
        name_lower = component_name.lower()
        
        if "customer" in name_lower:
            return "customer_id"
        elif "transaction" in name_lower:
            return "transaction_date"
        elif "product" in name_lower:
            return "category"
        else:
            return "id"
            
    def _identify_bottlenecks(self, dag_analysis: Dict) -> List[str]:
        """Identify potential bottlenecks in the DAG"""
        bottlenecks = []
        
        # Components with many dependents could be bottlenecks
        dependency_graph = dag_analysis.get("dependency_graph", {})
        
        for comp_name, comp_info in dependency_graph.items():
            dependents = comp_info.get("dependents", [])
            if len(dependents) > 2:  # More than 2 dependents
                bottlenecks.append(comp_name)
                
        return bottlenecks
        
    def _identify_optimization_opportunities(self, dag_analysis: Dict) -> List[Dict]:
        """Identify optimization opportunities in the DAG"""
        opportunities = []
        
        dependency_graph = dag_analysis.get("dependency_graph", {})
        
        for comp_name, comp_info in dependency_graph.items():
            component_data = comp_info.get("component_info", {})
            comp_type = component_data.get("type", "").lower()
            
            if "lookup" in comp_type:
                opportunities.append({
                    "component": comp_name,
                    "suggestion": "Consider broadcast join for small lookup tables",
                    "estimated_improvement": "30%"
                })
            elif "aggregator" in comp_type:
                opportunities.append({
                    "component": comp_name,
                    "suggestion": "Enable adaptive query execution for dynamic optimization",
                    "estimated_improvement": "20%"
                })
                
        return opportunities


class ConfigurationValidator:
    """Validates configuration files for correctness and completeness"""
    
    def __init__(self):
        self.logger = logging.getLogger("ConfigValidator")
        
    def validate_execution_plan(self, config_file: Path) -> List[str]:
        """Validate execution plan configuration file"""
        errors = []
        
        try:
            with open(config_file, 'r') as f:
                config = json.load(f)
                
            # Required fields validation
            required_fields = ["mapping_name", "execution_strategy", "phases"]
            for field in required_fields:
                if field not in config:
                    errors.append(f"Missing required field: {field}")
                    
            # Validate phases
            phases = config.get("phases", [])
            if not phases:
                errors.append("No execution phases defined")
            else:
                for i, phase in enumerate(phases):
                    phase_errors = self._validate_phase(phase, i + 1)
                    errors.extend(phase_errors)
                    
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON format: {e}")
        except Exception as e:
            errors.append(f"Error reading config file: {e}")
            
        return errors
        
    def _validate_phase(self, phase: Dict, phase_num: int) -> List[str]:
        """Validate a single execution phase"""
        errors = []
        
        required_fields = ["phase_number", "components"]
        for field in required_fields:
            if field not in phase:
                errors.append(f"Phase {phase_num}: Missing required field '{field}'")
                
        if phase.get("phase_number") != phase_num:
            errors.append(f"Phase {phase_num}: Phase number mismatch")
            
        components = phase.get("components", [])
        if not components:
            errors.append(f"Phase {phase_num}: No components defined")
            
        return errors