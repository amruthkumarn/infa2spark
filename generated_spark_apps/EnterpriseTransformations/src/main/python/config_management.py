"""
Enterprise Configuration Management (Static Loading for Spark)
Provides configuration loading without hot-reloading capabilities
"""

import json
import yaml
import logging
from pathlib import Path
from typing import Dict, Any, Optional


class EnterpriseConfigurationManager:
    """Static configuration manager for Spark applications"""
    
    def __init__(self, mapping_name: str, config_dir: str = "config", environment: str = "default"):
        self.mapping_name = mapping_name
        self.config_dir = Path(config_dir)
        self.environment = environment
        self.logger = logging.getLogger("ConfigManager")
        
        # Validate config directory exists
        if not self.config_dir.exists():
            self.logger.warning(f"Configuration directory {self.config_dir} does not exist")
    
    def load_execution_plan(self) -> Optional[Dict[str, Any]]:
        """Load execution plan configuration"""
        plan_file = self.config_dir / "execution-plans" / f"{self.mapping_name.lower()}_execution_plan.json"
        return self._load_json_file(plan_file, "execution plan")
    
    def load_dag_analysis(self) -> Optional[Dict[str, Any]]:
        """Load DAG analysis configuration"""
        dag_file = self.config_dir / "dag-analysis" / f"{self.mapping_name.lower()}_dag_analysis.json"
        return self._load_json_file(dag_file, "DAG analysis")
    
    def load_component_metadata(self) -> Optional[Dict[str, Any]]:
        """Load component metadata configuration"""
        metadata_file = self.config_dir / "component-metadata" / f"{self.mapping_name.lower()}_components.json"
        return self._load_json_file(metadata_file, "component metadata")
    
    def load_memory_profile(self) -> Optional[Dict[str, Any]]:
        """Load memory profile configuration"""
        memory_file = self.config_dir / "runtime" / "memory-profiles.yaml"
        return self._load_yaml_file(memory_file, "memory profiles")
    
    def _load_json_file(self, file_path: Path, config_type: str) -> Optional[Dict[str, Any]]:
        """Load JSON configuration file"""
        try:
            if not file_path.exists():
                self.logger.warning(f"{config_type} file not found: {file_path}")
                return {}
            
            with open(file_path, 'r') as f:
                config = json.load(f)
                self.logger.debug(f"Loaded {config_type} from {file_path}")
                return config
                
        except Exception as e:
            self.logger.error(f"Error loading {config_type} from {file_path}: {e}")
            return {}
    
    def _load_yaml_file(self, file_path: Path, config_type: str) -> Optional[Dict[str, Any]]:
        """Load YAML configuration file"""
        try:
            if not file_path.exists():
                self.logger.warning(f"{config_type} file not found: {file_path}")
                return {}
            
            with open(file_path, 'r') as f:
                config = yaml.safe_load(f)
                self.logger.debug(f"Loaded {config_type} from {file_path}")
                return config
                
        except Exception as e:
            self.logger.error(f"Error loading {config_type} from {file_path}: {e}")
            return {}