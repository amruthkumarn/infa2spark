"""
Configuration management for the PoC
"""
import yaml
import os
from typing import Dict, Any, Optional
import logging

class ConfigManager:
    """Manages configuration files and settings"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = config_dir
        self.logger = logging.getLogger("ConfigManager")
        self._config_cache = {}
        
    def load_config(self, config_name: str) -> Dict[str, Any]:
        """Load configuration from YAML file"""
        if config_name in self._config_cache:
            return self._config_cache[config_name]
            
        config_file = os.path.join(self.config_dir, f"{config_name}.yaml")
        
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
                self._config_cache[config_name] = config
                self.logger.info(f"Loaded configuration from {config_file}")
                return config
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {config_file}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML file {config_file}: {str(e)}")
            raise
            
    def get_connections_config(self) -> Dict[str, Any]:
        """Get connections configuration"""
        return self.load_config("connections")
        
    def get_spark_config(self) -> Dict[str, Any]:
        """Get Spark configuration"""
        return self.load_config("spark_config")
        
    def get_sample_project_config(self) -> Dict[str, Any]:
        """Get sample project specific configuration"""
        return self.load_config("sample_project_config")
        
    def resolve_parameters(self, value: str, parameters: Dict[str, str]) -> str:
        """Resolve parameter placeholders in configuration values"""
        if not isinstance(value, str):
            return value
            
        resolved_value = value
        for param_name, param_value in parameters.items():
            placeholder = f"$${param_name}"
            if placeholder in resolved_value:
                resolved_value = resolved_value.replace(placeholder, param_value)
                
        # Handle system date placeholder
        if "$$SystemDate" in resolved_value:
            from datetime import datetime
            system_date = datetime.now().strftime("%Y-%m-%d")
            resolved_value = resolved_value.replace("$$SystemDate", system_date)
            
        return resolved_value
        
    def merge_configs(self, *configs: Dict[str, Any]) -> Dict[str, Any]:
        """Merge multiple configuration dictionaries"""
        merged = {}
        for config in configs:
            if config:
                merged.update(config)
        return merged