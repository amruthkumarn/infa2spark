"""
Configuration Migration Tools for Phase 3
Implements configuration versioning, migration, and upgrade utilities
"""
import json
import yaml
import shutil
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import logging
import re
from packaging import version


class MigrationType(Enum):
    """Types of configuration migrations"""
    SCHEMA_UPGRADE = "schema_upgrade"
    FORMAT_CHANGE = "format_change"
    FIELD_RENAME = "field_rename"
    FIELD_REMOVAL = "field_removal"
    FIELD_ADDITION = "field_addition"
    VALIDATION_UPDATE = "validation_update"


@dataclass
class MigrationRule:
    """Configuration migration rule"""
    name: str
    description: str
    from_version: str
    to_version: str
    migration_type: MigrationType
    config_types: List[str]  # Which config types this applies to
    transformation: Dict[str, Any]  # Migration transformation details
    required: bool = True  # Whether this migration is required


@dataclass
class ConfigurationVersion:
    """Configuration version information"""
    version: str
    timestamp: datetime
    description: str
    schema_changes: List[str]
    breaking_changes: bool = False


class ConfigurationMigrator:
    """Handles configuration migrations between versions"""
    
    def __init__(self):
        self.logger = logging.getLogger("ConfigurationMigrator")
        self.migration_rules = self._load_migration_rules()
        self.version_history = self._load_version_history()
    
    def _load_migration_rules(self) -> List[MigrationRule]:
        """Load migration rules for different versions"""
        return [
            # Example migration from v1.0.0 to v1.1.0
            MigrationRule(
                name="add_optimization_hints",
                description="Add optimization_hints section to execution plans",
                from_version="1.0.0",
                to_version="1.1.0",
                migration_type=MigrationType.FIELD_ADDITION,
                config_types=["execution_plan"],
                transformation={
                    "add_fields": {
                        "optimization_hints": {
                            "cache_intermediate_results": True,
                            "checkpoint_frequency": "per_phase",
                            "resource_scaling": "auto"
                        }
                    }
                }
            ),
            
            # Migration from v1.1.0 to v1.2.0
            MigrationRule(
                name="rename_memory_fields",
                description="Rename memory configuration field names for consistency",
                from_version="1.1.0", 
                to_version="1.2.0",
                migration_type=MigrationType.FIELD_RENAME,
                config_types=["memory_profiles"],
                transformation={
                    "field_renames": {
                        "driver_mem": "driver_memory",
                        "executor_mem": "executor_memory",
                        "shuffle_parts": "shuffle_partitions"
                    }
                }
            ),
            
            # Migration from v1.2.0 to v1.3.0
            MigrationRule(
                name="add_performance_analysis",
                description="Add performance analysis section to DAG analysis",
                from_version="1.2.0",
                to_version="1.3.0", 
                migration_type=MigrationType.FIELD_ADDITION,
                config_types=["dag_analysis"],
                transformation={
                    "add_fields": {
                        "performance_analysis": {
                            "bottlenecks": [],
                            "optimization_opportunities": []
                        }
                    }
                }
            ),
            
            # Migration from v1.3.0 to v2.0.0 (breaking changes)
            MigrationRule(
                name="restructure_component_metadata",
                description="Restructure component metadata for better organization",
                from_version="1.3.0",
                to_version="2.0.0",
                migration_type=MigrationType.SCHEMA_UPGRADE,
                config_types=["component_metadata"],
                transformation={
                    "schema_restructure": {
                        "group_by_type": True,
                        "add_metadata_sections": ["data_profile", "performance_hints"],
                        "remove_deprecated_fields": ["legacy_config"]
                    }
                }
            )
        ]
    
    def _load_version_history(self) -> List[ConfigurationVersion]:
        """Load configuration version history"""
        return [
            ConfigurationVersion(
                version="1.0.0",
                timestamp=datetime(2025, 1, 1),
                description="Initial configuration externalization implementation",
                schema_changes=[
                    "Created execution_plan.json schema",
                    "Created memory-profiles.yaml schema", 
                    "Created component_metadata.json schema",
                    "Created dag_analysis.json schema"
                ]
            ),
            ConfigurationVersion(
                version="1.1.0",
                timestamp=datetime(2025, 2, 1),
                description="Added optimization hints and performance tuning",
                schema_changes=[
                    "Added optimization_hints to execution plans",
                    "Enhanced memory profiles with dynamic allocation",
                    "Added component-specific overrides"
                ]
            ),
            ConfigurationVersion(
                version="1.2.0",
                timestamp=datetime(2025, 3, 1),
                description="Standardized field naming and validation",
                schema_changes=[
                    "Renamed memory configuration fields for consistency",
                    "Added validation rules and constraints",
                    "Enhanced environment configuration"
                ]
            ),
            ConfigurationVersion(
                version="1.3.0",
                timestamp=datetime(2025, 4, 1),
                description="Performance analysis and monitoring integration",
                schema_changes=[
                    "Added performance analysis to DAG configurations",
                    "Enhanced monitoring and metrics support",
                    "Added hot-reloading capabilities"
                ]
            ),
            ConfigurationVersion(
                version="2.0.0",
                timestamp=datetime(2025, 7, 26),
                description="Major restructuring and enterprise features",
                schema_changes=[
                    "Restructured component metadata organization",
                    "Enhanced validation framework",
                    "Added configuration migration tools",
                    "Implemented advanced monitoring"
                ],
                breaking_changes=True
            )
        ]
    
    def get_current_version(self, config_dir: Path) -> Optional[str]:
        """Get current configuration version from metadata"""
        version_file = config_dir / ".config_version"
        if version_file.exists():
            try:
                with open(version_file, 'r') as f:
                    return f.read().strip()
            except Exception as e:
                self.logger.warning(f"Could not read version file: {e}")
        
        # Try to infer version from configuration structure
        return self._infer_version_from_structure(config_dir)
    
    def _infer_version_from_structure(self, config_dir: Path) -> str:
        """Infer configuration version from file structure and content"""
        # Check for version-specific features
        execution_plans_dir = config_dir / "execution-plans"
        if execution_plans_dir.exists():
            # Look for a sample execution plan file
            for plan_file in execution_plans_dir.glob("*.json"):
                try:
                    with open(plan_file, 'r') as f:
                        plan_data = json.load(f)
                    
                    # Check for version-specific features
                    if "optimization_hints" in plan_data:
                        if "performance_analysis" in plan_data:
                            return "1.3.0"
                        else:
                            return "1.1.0"
                    else:
                        return "1.0.0"
                except Exception:
                    continue
        
        return "1.0.0"  # Default to earliest version
    
    def set_version(self, config_dir: Path, version: str):
        """Set configuration version"""
        version_file = config_dir / ".config_version"
        with open(version_file, 'w') as f:
            f.write(version)
        self.logger.info(f"Set configuration version to {version}")
    
    def get_migration_path(self, from_version: str, to_version: str) -> List[MigrationRule]:
        """Get sequence of migrations needed to upgrade from one version to another"""
        migration_path = []
        current_version = from_version
        
        while version.parse(current_version) < version.parse(to_version):
            # Find next migration
            next_migration = None
            for rule in self.migration_rules:
                if rule.from_version == current_version:
                    if not next_migration or version.parse(rule.to_version) < version.parse(next_migration.to_version):
                        next_migration = rule
            
            if not next_migration:
                self.logger.warning(f"No migration path found from {current_version} to {to_version}")
                break
            
            migration_path.append(next_migration)
            current_version = next_migration.to_version
        
        return migration_path
    
    def migrate_configuration(self, config_dir: Path, target_version: str = None, 
                            backup: bool = True) -> bool:
        """Migrate configuration to target version"""
        if target_version is None:
            target_version = self.version_history[-1].version  # Latest version
        
        current_version = self.get_current_version(config_dir)
        if not current_version:
            self.logger.error("Could not determine current configuration version")
            return False
        
        if version.parse(current_version) >= version.parse(target_version):
            self.logger.info(f"Configuration already at version {current_version} (target: {target_version})")
            return True
        
        self.logger.info(f"Migrating configuration from {current_version} to {target_version}")
        
        # Create backup if requested
        if backup:
            backup_dir = self._create_backup(config_dir, current_version)
            self.logger.info(f"Created backup at {backup_dir}")
        
        # Get migration path
        migration_path = self.get_migration_path(current_version, target_version)
        if not migration_path:
            self.logger.error(f"No migration path found from {current_version} to {target_version}")
            return False
        
        # Apply migrations
        for migration_rule in migration_path:
            try:
                self.logger.info(f"Applying migration: {migration_rule.name}")
                success = self._apply_migration(config_dir, migration_rule)
                if not success:
                    self.logger.error(f"Migration failed: {migration_rule.name}")
                    return False
            except Exception as e:
                self.logger.error(f"Error applying migration {migration_rule.name}: {e}")
                return False
        
        # Update version
        self.set_version(config_dir, target_version)
        self.logger.info(f"Successfully migrated configuration to version {target_version}")
        return True
    
    def _create_backup(self, config_dir: Path, version: str) -> Path:
        """Create backup of current configuration"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_dir = config_dir.parent / f"config_backup_{version}_{timestamp}"
        
        shutil.copytree(config_dir, backup_dir)
        return backup_dir
    
    def _apply_migration(self, config_dir: Path, migration_rule: MigrationRule) -> bool:
        """Apply a single migration rule"""
        success = True
        
        for config_type in migration_rule.config_types:
            config_files = self._get_config_files(config_dir, config_type)
            
            for config_file in config_files:
                try:
                    if not self._migrate_config_file(config_file, migration_rule):
                        success = False
                except Exception as e:
                    self.logger.error(f"Error migrating {config_file}: {e}")
                    success = False
        
        return success
    
    def _get_config_files(self, config_dir: Path, config_type: str) -> List[Path]:
        """Get all configuration files of a specific type"""
        config_files = []
        
        if config_type == "execution_plan":
            pattern = "*_execution_plan.json"
            config_files.extend((config_dir / "execution-plans").glob(pattern))
        elif config_type == "dag_analysis":
            pattern = "*_dag_analysis.json"
            config_files.extend((config_dir / "dag-analysis").glob(pattern))
        elif config_type == "component_metadata":
            pattern = "*_components.json"
            config_files.extend((config_dir / "component-metadata").glob(pattern))
        elif config_type == "memory_profiles":
            config_files.append(config_dir / "runtime" / "memory-profiles.yaml")
        
        return [f for f in config_files if f.exists()]
    
    def _migrate_config_file(self, config_file: Path, migration_rule: MigrationRule) -> bool:
        """Migrate a single configuration file"""
        try:
            # Load configuration
            if config_file.suffix == ".json":
                with open(config_file, 'r') as f:
                    config_data = json.load(f)
            elif config_file.suffix == ".yaml":
                with open(config_file, 'r') as f:
                    config_data = yaml.safe_load(f)
            else:
                self.logger.warning(f"Unsupported file format: {config_file}")
                return False
            
            # Apply transformation
            migrated_data = self._apply_transformation(config_data, migration_rule.transformation, migration_rule.migration_type)
            
            # Write back
            if config_file.suffix == ".json":
                with open(config_file, 'w') as f:
                    json.dump(migrated_data, f, indent=2, ensure_ascii=False)
            elif config_file.suffix == ".yaml":
                with open(config_file, 'w') as f:
                    yaml.dump(migrated_data, f, default_flow_style=False, indent=2, allow_unicode=True)
            
            self.logger.debug(f"Migrated {config_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error migrating {config_file}: {e}")
            return False
    
    def _apply_transformation(self, config_data: Dict[str, Any], transformation: Dict[str, Any], 
                            migration_type: MigrationType) -> Dict[str, Any]:
        """Apply migration transformation to configuration data"""
        result = config_data.copy()
        
        if migration_type == MigrationType.FIELD_ADDITION:
            # Add new fields
            add_fields = transformation.get("add_fields", {})
            for field_path, field_value in add_fields.items():
                self._set_nested_field(result, field_path, field_value)
        
        elif migration_type == MigrationType.FIELD_RENAME:
            # Rename fields
            field_renames = transformation.get("field_renames", {})
            for old_name, new_name in field_renames.items():
                self._rename_field(result, old_name, new_name)
        
        elif migration_type == MigrationType.FIELD_REMOVAL:
            # Remove fields
            remove_fields = transformation.get("remove_fields", [])
            for field_path in remove_fields:
                self._remove_nested_field(result, field_path)
        
        elif migration_type == MigrationType.SCHEMA_UPGRADE:
            # Complex schema restructuring
            schema_config = transformation.get("schema_restructure", {})
            result = self._apply_schema_restructure(result, schema_config)
        
        # Update metadata version info if present
        if "metadata" in result:
            result["metadata"]["migrated_from"] = transformation.get("from_version")
            result["metadata"]["migration_date"] = datetime.now().isoformat()
        
        return result
    
    def _set_nested_field(self, data: Dict[str, Any], field_path: str, value: Any):
        """Set value in nested dictionary using dot notation"""
        keys = field_path.split('.')
        current = data
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
        
        current[keys[-1]] = value
    
    def _rename_field(self, data: Dict[str, Any], old_name: str, new_name: str):
        """Rename field in dictionary (supports nested structures)"""
        def rename_recursive(obj):
            if isinstance(obj, dict):
                if old_name in obj:
                    obj[new_name] = obj.pop(old_name)
                for value in obj.values():
                    rename_recursive(value)
            elif isinstance(obj, list):
                for item in obj:
                    rename_recursive(item)
        
        rename_recursive(data)
    
    def _remove_nested_field(self, data: Dict[str, Any], field_path: str):
        """Remove nested field using dot notation"""
        keys = field_path.split('.')
        current = data
        
        for key in keys[:-1]:
            if key not in current:
                return  # Field doesn't exist
            current = current[key]
        
        if keys[-1] in current:
            del current[keys[-1]]
    
    def _apply_schema_restructure(self, data: Dict[str, Any], schema_config: Dict[str, Any]) -> Dict[str, Any]:
        """Apply complex schema restructuring"""
        result = data.copy()
        
        # Group by type (for component metadata)
        if schema_config.get("group_by_type") and "components" in result:
            components = result.pop("components", [])
            result["sources"] = [c for c in components if c.get("component_type") == "source"]
            result["transformations"] = [c for c in components if c.get("component_type") == "transformation"]
            result["targets"] = [c for c in components if c.get("component_type") == "target"]
        
        # Add metadata sections
        add_sections = schema_config.get("add_metadata_sections", [])
        for section in add_sections:
            if section == "data_profile":
                # Add data profile to sources
                for source in result.get("sources", []):
                    if "data_profile" not in source:
                        source["data_profile"] = {
                            "estimated_row_count": 100000,
                            "estimated_size_mb": 50,
                            "partitioning": "id"
                        }
            elif section == "performance_hints":
                # Add performance hints to transformations
                for transformation in result.get("transformations", []):
                    if "performance_hints" not in transformation:
                        transformation["performance_hints"] = {
                            "cache_strategy": "auto",
                            "optimization_level": "standard"
                        }
        
        # Remove deprecated fields
        remove_fields = schema_config.get("remove_deprecated_fields", [])
        for field in remove_fields:
            self._remove_nested_field(result, field)
        
        return result


class ConfigurationValidator:
    """Enhanced configuration validator with migration support"""
    
    def __init__(self, migrator: ConfigurationMigrator):
        self.migrator = migrator
        self.logger = logging.getLogger("ConfigurationValidator")
    
    def validate_and_migrate(self, config_dir: Path, target_version: str = None) -> Tuple[bool, List[str]]:
        """Validate configuration and migrate if necessary"""
        issues = []
        
        # Check current version
        current_version = self.migrator.get_current_version(config_dir)
        if not current_version:
            issues.append("Could not determine configuration version")
            return False, issues
        
        # Check if migration is needed
        if target_version and version.parse(current_version) < version.parse(target_version):
            self.logger.info(f"Migration required from {current_version} to {target_version}")
            
            # Perform migration
            success = self.migrator.migrate_configuration(config_dir, target_version)
            if not success:
                issues.append(f"Migration failed from {current_version} to {target_version}")
                return False, issues
            
            issues.append(f"Successfully migrated from {current_version} to {target_version}")
        
        # Validate migrated/current configuration
        validation_issues = self._validate_configuration_structure(config_dir)
        issues.extend(validation_issues)
        
        success = len([issue for issue in issues if issue.startswith("ERROR")]) == 0
        return success, issues
    
    def _validate_configuration_structure(self, config_dir: Path) -> List[str]:
        """Validate configuration file structure"""
        issues = []
        
        # Check required directories
        required_dirs = ["execution-plans", "dag-analysis", "component-metadata", "runtime"]
        for dir_name in required_dirs:
            dir_path = config_dir / dir_name
            if not dir_path.exists():
                issues.append(f"ERROR: Missing required directory: {dir_name}")
        
        # Check for configuration files
        if (config_dir / "execution-plans").exists():
            exec_files = list((config_dir / "execution-plans").glob("*_execution_plan.json"))
            if not exec_files:
                issues.append("WARNING: No execution plan files found")
        
        return issues


class ConfigurationUpgradeAssistant:
    """Assists with configuration upgrades and provides recommendations"""
    
    def __init__(self):
        self.logger = logging.getLogger("ConfigUpgradeAssistant")
        self.migrator = ConfigurationMigrator()
    
    def analyze_upgrade_impact(self, config_dir: Path, target_version: str) -> Dict[str, Any]:
        """Analyze the impact of upgrading to target version"""
        current_version = self.migrator.get_current_version(config_dir)
        if not current_version:
            return {"error": "Could not determine current version"}
        
        migration_path = self.migrator.get_migration_path(current_version, target_version)
        
        analysis = {
            "current_version": current_version,
            "target_version": target_version,
            "migration_steps": len(migration_path),
            "breaking_changes": False,
            "estimated_duration": "< 1 minute",
            "backup_recommended": True,
            "changes": []
        }
        
        for migration in migration_path:
            change_info = {
                "version": migration.to_version,
                "description": migration.description,
                "type": migration.migration_type.value,
                "config_types": migration.config_types,
                "required": migration.required
            }
            analysis["changes"].append(change_info)
            
            # Check for breaking changes
            version_info = next((v for v in self.migrator.version_history if v.version == migration.to_version), None)
            if version_info and version_info.breaking_changes:
                analysis["breaking_changes"] = True
                analysis["estimated_duration"] = "2-5 minutes"
        
        return analysis
    
    def generate_upgrade_report(self, config_dir: Path, target_version: str) -> str:
        """Generate detailed upgrade report"""
        analysis = self.analyze_upgrade_impact(config_dir, target_version)
        
        if "error" in analysis:
            return f"Error: {analysis['error']}"
        
        report = []
        report.append("Configuration Upgrade Analysis")
        report.append("=" * 40)
        report.append(f"Current Version: {analysis['current_version']}")
        report.append(f"Target Version:  {analysis['target_version']}")
        report.append(f"Migration Steps: {analysis['migration_steps']}")
        report.append(f"Estimated Time:  {analysis['estimated_duration']}")
        report.append("")
        
        if analysis["breaking_changes"]:
            report.append("⚠️  WARNING: This upgrade contains breaking changes!")
            report.append("")
        
        if analysis["changes"]:
            report.append("Changes to be applied:")
            for i, change in enumerate(analysis["changes"], 1):
                report.append(f"{i}. Version {change['version']}")
                report.append(f"   Description: {change['description']}")
                report.append(f"   Type: {change['type']}")
                report.append(f"   Affects: {', '.join(change['config_types'])}")
                report.append("")
        
        report.append("Recommendations:")
        if analysis["backup_recommended"]:
            report.append("• Create backup before proceeding")
        report.append("• Test upgrade in development environment first")
        report.append("• Review configuration after upgrade")
        
        return "\n".join(report)