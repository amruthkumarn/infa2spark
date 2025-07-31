"""
Advanced Configuration Validation Framework for Phase 3
Implements comprehensive validation, schema checking, and configuration integrity
"""
import json
import yaml
import re
import os
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
from enum import Enum
import logging
from datetime import datetime


class ValidationSeverity(Enum):
    """Validation severity levels"""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


@dataclass
class ValidationResult:
    """Validation result container"""
    severity: ValidationSeverity
    message: str
    file_path: Optional[str] = None
    field_path: Optional[str] = None
    suggested_fix: Optional[str] = None
    rule_id: Optional[str] = None


class ConfigurationSchemaValidator:
    """Advanced schema validation for configuration files"""
    
    def __init__(self):
        self.logger = logging.getLogger("ConfigSchemaValidator")
        self.validation_rules = self._load_validation_rules()
        
    def _load_validation_rules(self) -> Dict[str, Any]:
        """Load validation rules for different configuration types"""
        return {
            "execution_plan": {
                "required_fields": ["mapping_name", "metadata", "phases"],
                "field_types": {
                    "mapping_name": str,
                    "total_phases": int,
                    "estimated_duration": (int, float),
                    "phases": list
                },
                "constraints": {
                    "total_phases": {"min": 1, "max": 20},
                    "estimated_duration": {"min": 1, "max": 7200}  # 2 hours max
                }
            },
            "memory_profiles": {
                "required_sections": ["memory_profiles", "environments"],
                "memory_format_pattern": r"^\d+[gmk]$",
                "valid_environments": ["development", "testing", "production"],
                "scale_factor_range": {"min": 0.1, "max": 10.0}
            },
            "component_metadata": {
                "required_sections": ["mapping_name", "sources", "transformations", "targets"],
                "component_types": ["source", "transformation", "target"],
                "transformation_types": [
                    "ExpressionTransformation", "AggregatorTransformation", 
                    "JoinerTransformation", "LookupTransformation",
                    "SequenceTransformation", "SorterTransformation",
                    "RouterTransformation", "UnionTransformation", "JavaTransformation"
                ]
            },
            "dag_analysis": {
                "required_fields": ["mapping_name", "dag_metadata", "dependency_graph"],
                "dag_metadata_fields": ["total_components", "total_phases", "is_valid_dag"],
                "max_components": 100,
                "max_dependency_depth": 20
            }
        }
    
    def validate_execution_plan(self, config_data: Dict[str, Any], file_path: str) -> List[ValidationResult]:
        """Validate execution plan configuration"""
        results = []
        rules = self.validation_rules["execution_plan"]
        
        # Check required fields
        for field in rules["required_fields"]:
            if field not in config_data:
                results.append(ValidationResult(
                    severity=ValidationSeverity.ERROR,
                    message=f"Missing required field: {field}",
                    file_path=file_path,
                    field_path=field,
                    suggested_fix=f"Add '{field}' field to configuration",
                    rule_id="EP001"
                ))
        
        # Validate field types
        for field, expected_type in rules["field_types"].items():
            if field in config_data:
                value = config_data[field]
                if isinstance(expected_type, tuple):
                    if not isinstance(value, expected_type):
                        results.append(ValidationResult(
                            severity=ValidationSeverity.ERROR,
                            message=f"Field '{field}' should be one of types {expected_type}, got {type(value).__name__}",
                            file_path=file_path,
                            field_path=field,
                            rule_id="EP002"
                        ))
                else:
                    if not isinstance(value, expected_type):
                        results.append(ValidationResult(
                            severity=ValidationSeverity.ERROR,
                            message=f"Field '{field}' should be {expected_type.__name__}, got {type(value).__name__}",
                            file_path=file_path,
                            field_path=field,
                            rule_id="EP002"
                        ))
        
        # Validate constraints
        for field, constraint in rules["constraints"].items():
            if field in config_data:
                value = config_data[field]
                if "min" in constraint and value < constraint["min"]:
                    results.append(ValidationResult(
                        severity=ValidationSeverity.ERROR,
                        message=f"Field '{field}' value {value} is below minimum {constraint['min']}",
                        file_path=file_path,
                        field_path=field,
                        suggested_fix=f"Set '{field}' to at least {constraint['min']}",
                        rule_id="EP003"
                    ))
                if "max" in constraint and value > constraint["max"]:
                    results.append(ValidationResult(
                        severity=ValidationSeverity.ERROR,
                        message=f"Field '{field}' value {value} exceeds maximum {constraint['max']}",
                        file_path=file_path,
                        field_path=field,
                        suggested_fix=f"Set '{field}' to at most {constraint['max']}",
                        rule_id="EP003"
                    ))
        
        # Validate phases structure
        if "phases" in config_data:
            results.extend(self._validate_phases(config_data["phases"], file_path))
            
        return results
    
    def validate_memory_profiles(self, config_data: Dict[str, Any], file_path: str) -> List[ValidationResult]:
        """Validate memory profiles configuration"""
        results = []
        rules = self.validation_rules["memory_profiles"]
        
        # Check required sections
        for section in rules["required_sections"]:
            if section not in config_data:
                results.append(ValidationResult(
                    severity=ValidationSeverity.ERROR,
                    message=f"Missing required section: {section}",
                    file_path=file_path,
                    field_path=section,
                    rule_id="MP001"
                ))
        
        # Validate memory format patterns
        memory_pattern = re.compile(rules["memory_format_pattern"])
        if "memory_profiles" in config_data:
            for profile_name, profile_config in config_data["memory_profiles"].items():
                for memory_field in ["driver_memory", "executor_memory"]:
                    if memory_field in profile_config:
                        memory_value = str(profile_config[memory_field])
                        if not memory_pattern.match(memory_value.lower()):
                            results.append(ValidationResult(
                                severity=ValidationSeverity.ERROR,
                                message=f"Invalid memory format '{memory_value}' in {profile_name}.{memory_field}",
                                file_path=file_path,
                                field_path=f"memory_profiles.{profile_name}.{memory_field}",
                                suggested_fix="Use format like '2g', '512m', '1024k'",
                                rule_id="MP002"
                            ))
        
        # Validate environments
        if "environments" in config_data:
            for env_name, env_config in config_data["environments"].items():
                if env_name not in rules["valid_environments"]:
                    results.append(ValidationResult(
                        severity=ValidationSeverity.WARNING,
                        message=f"Non-standard environment name: {env_name}",
                        file_path=file_path,
                        field_path=f"environments.{env_name}",
                        suggested_fix=f"Consider using one of: {', '.join(rules['valid_environments'])}",
                        rule_id="MP003"
                    ))
                
                # Validate scale factors
                if "global_scale_factor" in env_config:
                    scale_factor = env_config["global_scale_factor"]
                    if not (rules["scale_factor_range"]["min"] <= scale_factor <= rules["scale_factor_range"]["max"]):
                        results.append(ValidationResult(
                            severity=ValidationSeverity.WARNING,
                            message=f"Scale factor {scale_factor} in {env_name} is outside recommended range",
                            file_path=file_path,
                            field_path=f"environments.{env_name}.global_scale_factor",
                            suggested_fix=f"Use scale factor between {rules['scale_factor_range']['min']} and {rules['scale_factor_range']['max']}",
                            rule_id="MP004"
                        ))
        
        return results
    
    def validate_component_metadata(self, config_data: Dict[str, Any], file_path: str) -> List[ValidationResult]:
        """Validate component metadata configuration"""
        results = []
        rules = self.validation_rules["component_metadata"]
        
        # Check required sections
        for section in rules["required_sections"]:
            if section not in config_data:
                results.append(ValidationResult(
                    severity=ValidationSeverity.ERROR,
                    message=f"Missing required section: {section}",
                    file_path=file_path,
                    field_path=section,
                    rule_id="CM001"
                ))
        
        # Validate transformation types
        if "transformations" in config_data:
            for transformation in config_data["transformations"]:
                if "type" in transformation:
                    trans_type = transformation["type"]
                    if trans_type not in rules["transformation_types"]:
                        results.append(ValidationResult(
                            severity=ValidationSeverity.WARNING,
                            message=f"Unknown transformation type: {trans_type}",
                            file_path=file_path,
                            field_path=f"transformations.{transformation.get('name', 'unknown')}.type",
                            suggested_fix=f"Use one of: {', '.join(rules['transformation_types'])}",
                            rule_id="CM002"
                        ))
        
        return results
    
    def validate_dag_analysis(self, config_data: Dict[str, Any], file_path: str) -> List[ValidationResult]:
        """Validate DAG analysis configuration"""
        results = []
        rules = self.validation_rules["dag_analysis"]
        
        # Check required fields
        for field in rules["required_fields"]:
            if field not in config_data:
                results.append(ValidationResult(
                    severity=ValidationSeverity.ERROR,
                    message=f"Missing required field: {field}",
                    file_path=file_path,
                    field_path=field,
                    rule_id="DA001"
                ))
        
        # Validate DAG metadata
        if "dag_metadata" in config_data:
            dag_metadata = config_data["dag_metadata"]
            for field in rules["dag_metadata_fields"]:
                if field not in dag_metadata:
                    results.append(ValidationResult(
                        severity=ValidationSeverity.ERROR,
                        message=f"Missing DAG metadata field: {field}",
                        file_path=file_path,
                        field_path=f"dag_metadata.{field}",
                        rule_id="DA002"
                    ))
            
            # Check component count limits
            if "total_components" in dag_metadata:
                component_count = dag_metadata["total_components"]
                if component_count > rules["max_components"]:
                    results.append(ValidationResult(
                        severity=ValidationSeverity.WARNING,
                        message=f"High component count: {component_count} (limit: {rules['max_components']})",
                        file_path=file_path,
                        field_path="dag_metadata.total_components",
                        suggested_fix="Consider breaking down into smaller mappings",
                        rule_id="DA003"
                    ))
        
        # Validate dependency graph structure
        if "dependency_graph" in config_data:
            results.extend(self._validate_dependency_graph(config_data["dependency_graph"], file_path))
        
        return results
    
    def _validate_phases(self, phases: List[Dict], file_path: str) -> List[ValidationResult]:
        """Validate phases structure in execution plan"""
        results = []
        
        for i, phase in enumerate(phases):
            phase_path = f"phases[{i}]"
            
            # Check required phase fields
            required_phase_fields = ["phase_number", "components"]
            for field in required_phase_fields:
                if field not in phase:
                    results.append(ValidationResult(
                        severity=ValidationSeverity.ERROR,
                        message=f"Missing field '{field}' in phase {i+1}",
                        file_path=file_path,
                        field_path=f"{phase_path}.{field}",
                        rule_id="EP004"
                    ))
            
            # Validate phase number sequence
            if "phase_number" in phase and phase["phase_number"] != i + 1:
                results.append(ValidationResult(
                    severity=ValidationSeverity.ERROR,
                    message=f"Phase number mismatch: expected {i+1}, got {phase['phase_number']}",
                    file_path=file_path,
                    field_path=f"{phase_path}.phase_number",
                    suggested_fix=f"Set phase_number to {i+1}",
                    rule_id="EP005"
                ))
            
            # Validate components list
            if "components" in phase:
                if not isinstance(phase["components"], list):
                    results.append(ValidationResult(
                        severity=ValidationSeverity.ERROR,
                        message=f"Components in phase {i+1} must be a list",
                        file_path=file_path,
                        field_path=f"{phase_path}.components",
                        rule_id="EP006"
                    ))
                elif len(phase["components"]) == 0:
                    results.append(ValidationResult(
                        severity=ValidationSeverity.WARNING,
                        message=f"Phase {i+1} has no components",
                        file_path=file_path,
                        field_path=f"{phase_path}.components",
                        rule_id="EP007"
                    ))
        
        return results
    
    def _validate_dependency_graph(self, dependency_graph: Dict, file_path: str) -> List[ValidationResult]:
        """Validate dependency graph structure"""
        results = []
        
        # Check for circular dependencies
        visited = set()
        rec_stack = set()
        
        def has_cycle(node):
            if node in rec_stack:
                return True
            if node in visited:
                return False
                
            visited.add(node)
            rec_stack.add(node)
            
            if node in dependency_graph:
                dependencies = dependency_graph[node].get("dependencies", [])
                for dep in dependencies:
                    if has_cycle(dep):
                        return True
            
            rec_stack.remove(node)
            return False
        
        for component in dependency_graph:
            if component not in visited:
                if has_cycle(component):
                    results.append(ValidationResult(
                        severity=ValidationSeverity.ERROR,
                        message=f"Circular dependency detected involving component: {component}",
                        file_path=file_path,
                        field_path=f"dependency_graph.{component}",
                        suggested_fix="Review and break circular dependencies",
                        rule_id="DA004"
                    ))
                    break
        
        return results


class ConfigurationIntegrityChecker:
    """Checks integrity and consistency across configuration files"""
    
    def __init__(self):
        self.logger = logging.getLogger("ConfigIntegrityChecker")
    
    def check_cross_file_consistency(self, config_dir: Path, mapping_name: str) -> List[ValidationResult]:
        """Check consistency across all configuration files for a mapping"""
        results = []
        
        # Load all configuration files
        config_files = {
            "execution_plan": config_dir / "execution-plans" / f"{mapping_name}_execution_plan.json",
            "dag_analysis": config_dir / "dag-analysis" / f"{mapping_name}_dag_analysis.json",
            "component_metadata": config_dir / "component-metadata" / f"{mapping_name}_components.json",
            "memory_profiles": config_dir / "runtime" / "memory-profiles.yaml"
        }
        
        loaded_configs = {}
        for config_type, file_path in config_files.items():
            if file_path.exists():
                try:
                    if file_path.suffix == ".json":
                        with open(file_path, 'r') as f:
                            loaded_configs[config_type] = json.load(f)
                    elif file_path.suffix == ".yaml":
                        with open(file_path, 'r') as f:
                            loaded_configs[config_type] = yaml.safe_load(f)
                except Exception as e:
                    results.append(ValidationResult(
                        severity=ValidationSeverity.ERROR,
                        message=f"Failed to load {config_type}: {e}",
                        file_path=str(file_path),
                        rule_id="IC001"
                    ))
        
        # Check mapping name consistency
        mapping_names = set()
        for config_type, config_data in loaded_configs.items():
            if "mapping_name" in config_data:
                mapping_names.add(config_data["mapping_name"])
        
        if len(mapping_names) > 1:
            results.append(ValidationResult(
                severity=ValidationSeverity.ERROR,
                message=f"Inconsistent mapping names across files: {mapping_names}",
                field_path="mapping_name",
                suggested_fix=f"Ensure all files use mapping name: {mapping_name}",
                rule_id="IC002"
            ))
        
        # Check component consistency between execution plan and metadata
        if "execution_plan" in loaded_configs and "component_metadata" in loaded_configs:
            results.extend(self._check_component_consistency(
                loaded_configs["execution_plan"],
                loaded_configs["component_metadata"],
                config_files["execution_plan"],
                config_files["component_metadata"]
            ))
        
        # Check DAG analysis consistency
        if "dag_analysis" in loaded_configs and "execution_plan" in loaded_configs:
            results.extend(self._check_dag_consistency(
                loaded_configs["dag_analysis"],
                loaded_configs["execution_plan"],
                config_files["dag_analysis"],
                config_files["execution_plan"]
            ))
        
        return results
    
    def _check_component_consistency(self, execution_plan: Dict, component_metadata: Dict, 
                                   ep_file: Path, cm_file: Path) -> List[ValidationResult]:
        """Check consistency between execution plan and component metadata"""
        results = []
        
        # Extract component names from execution plan
        ep_components = set()
        for phase in execution_plan.get("phases", []):
            for component in phase.get("components", []):
                ep_components.add(component.get("component_name", ""))
        
        # Extract component names from metadata
        cm_components = set()
        for category in ["sources", "transformations", "targets"]:
            for component in component_metadata.get(category, []):
                cm_components.add(component.get("name", ""))
        
        # Check for missing components
        missing_in_metadata = ep_components - cm_components
        missing_in_execution_plan = cm_components - ep_components
        
        for component in missing_in_metadata:
            if component:  # Skip empty names
                results.append(ValidationResult(
                    severity=ValidationSeverity.ERROR,
                    message=f"Component '{component}' in execution plan but not in metadata",
                    file_path=str(cm_file),
                    suggested_fix=f"Add component '{component}' to component metadata",
                    rule_id="IC003"
                ))
        
        for component in missing_in_execution_plan:
            if component:  # Skip empty names
                results.append(ValidationResult(
                    severity=ValidationSeverity.WARNING,
                    message=f"Component '{component}' in metadata but not in execution plan",
                    file_path=str(ep_file),
                    suggested_fix=f"Add component '{component}' to execution plan or remove from metadata",
                    rule_id="IC004"
                ))
        
        return results
    
    def _check_dag_consistency(self, dag_analysis: Dict, execution_plan: Dict,
                              da_file: Path, ep_file: Path) -> List[ValidationResult]:
        """Check consistency between DAG analysis and execution plan"""
        results = []
        
        # Check component count consistency
        dag_component_count = dag_analysis.get("dag_metadata", {}).get("total_components", 0)
        
        ep_component_count = 0
        for phase in execution_plan.get("phases", []):
            ep_component_count += len(phase.get("components", []))
        
        if dag_component_count != ep_component_count:
            results.append(ValidationResult(
                severity=ValidationSeverity.WARNING,
                message=f"Component count mismatch: DAG analysis ({dag_component_count}) vs execution plan ({ep_component_count})",
                file_path=str(da_file),
                field_path="dag_metadata.total_components",
                suggested_fix="Update component count in DAG analysis",
                rule_id="IC005"
            ))
        
        # Check phase count consistency
        dag_phase_count = dag_analysis.get("dag_metadata", {}).get("total_phases", 0)
        ep_phase_count = len(execution_plan.get("phases", []))
        
        if dag_phase_count != ep_phase_count:
            results.append(ValidationResult(
                severity=ValidationSeverity.WARNING,
                message=f"Phase count mismatch: DAG analysis ({dag_phase_count}) vs execution plan ({ep_phase_count})",
                file_path=str(da_file),
                field_path="dag_metadata.total_phases",
                suggested_fix="Update phase count in DAG analysis",
                rule_id="IC006"
            ))
        
        return results


class ConfigurationValidationReporter:
    """Generates validation reports in various formats"""
    
    def __init__(self):
        self.logger = logging.getLogger("ConfigValidationReporter")
    
    def generate_console_report(self, results: List[ValidationResult]) -> str:
        """Generate console-friendly validation report"""
        if not results:
            return "✅ All validations passed!"
        
        error_count = sum(1 for r in results if r.severity == ValidationSeverity.ERROR)
        warning_count = sum(1 for r in results if r.severity == ValidationSeverity.WARNING)
        info_count = sum(1 for r in results if r.severity == ValidationSeverity.INFO)
        
        report = []
        report.append(f"🔍 Configuration Validation Report")
        report.append(f"=" * 50)
        report.append(f"📊 Summary: {error_count} errors, {warning_count} warnings, {info_count} info")
        report.append("")
        
        # Group by severity
        for severity in [ValidationSeverity.ERROR, ValidationSeverity.WARNING, ValidationSeverity.INFO]:
            severity_results = [r for r in results if r.severity == severity]
            if severity_results:
                icon = "❌" if severity == ValidationSeverity.ERROR else "⚠️" if severity == ValidationSeverity.WARNING else "ℹ️"
                report.append(f"{icon} {severity.value.upper()}S:")
                for result in severity_results:
                    report.append(f"  • {result.message}")
                    if result.file_path:
                        report.append(f"    📁 File: {result.file_path}")
                    if result.field_path:
                        report.append(f"    🔍 Field: {result.field_path}")
                    if result.suggested_fix:
                        report.append(f"    💡 Fix: {result.suggested_fix}")
                    if result.rule_id:
                        report.append(f"    🏷️  Rule: {result.rule_id}")
                    report.append("")
        
        return "\n".join(report)
    
    def generate_json_report(self, results: List[ValidationResult]) -> Dict[str, Any]:
        """Generate JSON validation report"""
        return {
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_issues": len(results),
                "errors": sum(1 for r in results if r.severity == ValidationSeverity.ERROR),
                "warnings": sum(1 for r in results if r.severity == ValidationSeverity.WARNING),
                "info": sum(1 for r in results if r.severity == ValidationSeverity.INFO)
            },
            "issues": [
                {
                    "severity": result.severity.value,
                    "message": result.message,
                    "file_path": result.file_path,
                    "field_path": result.field_path,
                    "suggested_fix": result.suggested_fix,
                    "rule_id": result.rule_id
                }
                for result in results
            ]
        }