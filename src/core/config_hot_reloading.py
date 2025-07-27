"""
Configuration Hot-Reloading Framework for Phase 3
Implements file watching, configuration reload, and runtime updates
"""
import os
import json
import yaml
import time
import threading
from pathlib import Path
from typing import Dict, Any, Callable, Optional, Set
from dataclasses import dataclass
from datetime import datetime
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


@dataclass
class ConfigurationChange:
    """Configuration change event"""
    file_path: str
    change_type: str  # 'modified', 'created', 'deleted'
    timestamp: datetime
    config_type: str  # 'execution_plan', 'memory_profiles', etc.
    mapping_name: str


class ConfigurationChangeHandler(FileSystemEventHandler):
    """File system event handler for configuration changes"""
    
    def __init__(self, hot_reloader: 'ConfigurationHotReloader'):
        self.hot_reloader = hot_reloader
        self.logger = logging.getLogger("ConfigChangeHandler")
    
    def on_modified(self, event):
        if not event.is_directory:
            self._handle_file_change(event.src_path, 'modified')
    
    def on_created(self, event):
        if not event.is_directory:
            self._handle_file_change(event.src_path, 'created')
    
    def on_deleted(self, event):
        if not event.is_directory:
            self._handle_file_change(event.src_path, 'deleted')
    
    def _handle_file_change(self, file_path: str, change_type: str):
        """Handle configuration file change"""
        try:
            path = Path(file_path)
            
            # Only handle configuration files
            if not self._is_config_file(path):
                return
            
            config_type, mapping_name = self._parse_config_file_info(path)
            if not config_type or not mapping_name:
                return
            
            change = ConfigurationChange(
                file_path=file_path,
                change_type=change_type,
                timestamp=datetime.now(),
                config_type=config_type,
                mapping_name=mapping_name
            )
            
            self.hot_reloader._queue_configuration_change(change)
            
        except Exception as e:
            self.logger.error(f"Error handling file change {file_path}: {e}")
    
    def _is_config_file(self, path: Path) -> bool:
        """Check if file is a configuration file"""
        config_patterns = [
            "_execution_plan.json",
            "_dag_analysis.json", 
            "_components.json",
            "memory-profiles.yaml"
        ]
        return any(str(path).endswith(pattern) for pattern in config_patterns)
    
    def _parse_config_file_info(self, path: Path) -> tuple:
        """Parse configuration type and mapping name from file path"""
        try:
            filename = path.name
            
            if filename.endswith("_execution_plan.json"):
                mapping_name = filename.replace("_execution_plan.json", "")
                return "execution_plan", mapping_name
            elif filename.endswith("_dag_analysis.json"):
                mapping_name = filename.replace("_dag_analysis.json", "")
                return "dag_analysis", mapping_name
            elif filename.endswith("_components.json"):
                mapping_name = filename.replace("_components.json", "")
                return "component_metadata", mapping_name
            elif filename == "memory-profiles.yaml":
                return "memory_profiles", "global"
            
            return None, None
            
        except Exception:
            return None, None


class ConfigurationHotReloader:
    """Main configuration hot-reloading manager"""
    
    def __init__(self, config_dir: str, debounce_seconds: float = 1.0):
        self.config_dir = Path(config_dir)
        self.debounce_seconds = debounce_seconds
        self.logger = logging.getLogger("ConfigHotReloader")
        
        # File system watcher
        self.observer = Observer()
        self.event_handler = ConfigurationChangeHandler(self)
        
        # Change queue and processing
        self.change_queue = []
        self.queue_lock = threading.Lock()
        self.processing_thread = None
        self.stop_event = threading.Event()
        
        # Callbacks for configuration changes
        self.change_callbacks: Dict[str, Callable[[ConfigurationChange], None]] = {}
        
        # Configuration cache
        self.config_cache: Dict[str, Dict[str, Any]] = {}
        self.cache_lock = threading.Lock()
        
        # Debouncing support
        self.pending_changes: Dict[str, ConfigurationChange] = {}
        self.debounce_timers: Dict[str, threading.Timer] = {}
    
    def start(self):
        """Start the hot-reloading system"""
        try:
            self.logger.info(f"Starting configuration hot-reloading for directory: {self.config_dir}")
            
            # Start file system watcher
            self.observer.schedule(self.event_handler, str(self.config_dir), recursive=True)
            self.observer.start()
            
            # Start change processing thread
            self.processing_thread = threading.Thread(target=self._process_changes, daemon=True)
            self.processing_thread.start()
            
            self.logger.info("Configuration hot-reloading started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start configuration hot-reloading: {e}")
            raise
    
    def stop(self):
        """Stop the hot-reloading system"""
        try:
            self.logger.info("Stopping configuration hot-reloading")
            
            # Stop file system watcher
            self.observer.stop()
            self.observer.join()
            
            # Stop processing thread
            self.stop_event.set()
            if self.processing_thread and self.processing_thread.is_alive():
                self.processing_thread.join(timeout=5.0)
            
            # Cancel pending debounce timers
            for timer in self.debounce_timers.values():
                timer.cancel()
            self.debounce_timers.clear()
            
            self.logger.info("Configuration hot-reloading stopped")
            
        except Exception as e:
            self.logger.error(f"Error stopping configuration hot-reloading: {e}")
    
    def register_change_callback(self, config_type: str, callback: Callable[[ConfigurationChange], None]):
        """Register callback for configuration changes"""
        self.change_callbacks[config_type] = callback
        self.logger.info(f"Registered change callback for config type: {config_type}")
    
    def get_cached_config(self, config_type: str, mapping_name: str) -> Optional[Dict[str, Any]]:
        """Get cached configuration"""
        with self.cache_lock:
            cache_key = f"{config_type}:{mapping_name}"
            return self.config_cache.get(cache_key)
    
    def invalidate_cache(self, config_type: str, mapping_name: str):
        """Invalidate cached configuration"""
        with self.cache_lock:
            cache_key = f"{config_type}:{mapping_name}"
            if cache_key in self.config_cache:
                del self.config_cache[cache_key]
                self.logger.debug(f"Invalidated cache for {cache_key}")
    
    def preload_configurations(self, mapping_names: Set[str]):
        """Preload configurations into cache"""
        self.logger.info(f"Preloading configurations for {len(mapping_names)} mappings")
        
        for mapping_name in mapping_names:
            self._load_and_cache_configs(mapping_name)
    
    def _queue_configuration_change(self, change: ConfigurationChange):
        """Queue configuration change for processing"""
        with self.queue_lock:
            # Use debouncing to avoid excessive reloads
            change_key = f"{change.config_type}:{change.mapping_name}"
            
            # Cancel existing timer for this change
            if change_key in self.debounce_timers:
                self.debounce_timers[change_key].cancel()
            
            # Store the latest change
            self.pending_changes[change_key] = change
            
            # Set up debounce timer
            timer = threading.Timer(self.debounce_seconds, self._process_debounced_change, args=[change_key])
            self.debounce_timers[change_key] = timer
            timer.start()
    
    def _process_debounced_change(self, change_key: str):
        """Process a debounced configuration change"""
        with self.queue_lock:
            if change_key in self.pending_changes:
                change = self.pending_changes.pop(change_key)
                self.change_queue.append(change)
                
            if change_key in self.debounce_timers:
                del self.debounce_timers[change_key]
    
    def _process_changes(self):
        """Process configuration changes from the queue"""
        while not self.stop_event.is_set():
            try:
                changes_to_process = []
                
                # Get pending changes
                with self.queue_lock:
                    if self.change_queue:
                        changes_to_process = self.change_queue.copy()
                        self.change_queue.clear()
                
                # Process each change
                for change in changes_to_process:
                    self._handle_configuration_change(change)
                
                # Wait before checking again
                time.sleep(0.1)
                
            except Exception as e:
                self.logger.error(f"Error processing configuration changes: {e}")
                time.sleep(1.0)  # Back off on error
    
    def _handle_configuration_change(self, change: ConfigurationChange):
        """Handle a single configuration change"""
        try:
            self.logger.info(f"Processing configuration change: {change.config_type} for {change.mapping_name}")
            
            # Invalidate cache
            self.invalidate_cache(change.config_type, change.mapping_name)
            
            # Reload configuration if file still exists
            if change.change_type != 'deleted' and os.path.exists(change.file_path):
                self._reload_configuration(change.config_type, change.mapping_name, change.file_path)
            
            # Notify callbacks
            if change.config_type in self.change_callbacks:
                try:
                    self.change_callbacks[change.config_type](change)
                except Exception as e:
                    self.logger.error(f"Error in change callback for {change.config_type}: {e}")
            
        except Exception as e:
            self.logger.error(f"Error handling configuration change: {e}")
    
    def _reload_configuration(self, config_type: str, mapping_name: str, file_path: str):
        """Reload configuration from file"""
        try:
            path = Path(file_path)
            
            if path.suffix == ".json":
                with open(path, 'r') as f:
                    config_data = json.load(f)
            elif path.suffix == ".yaml":
                with open(path, 'r') as f:
                    config_data = yaml.safe_load(f)
            else:
                self.logger.warning(f"Unsupported configuration file format: {path.suffix}")
                return
            
            # Cache the reloaded configuration
            with self.cache_lock:
                cache_key = f"{config_type}:{mapping_name}"
                self.config_cache[cache_key] = config_data
            
            self.logger.info(f"Reloaded configuration: {config_type} for {mapping_name}")
            
        except Exception as e:
            self.logger.error(f"Error reloading configuration from {file_path}: {e}")
    
    def _load_and_cache_configs(self, mapping_name: str):
        """Load and cache all configurations for a mapping"""
        config_files = {
            "execution_plan": self.config_dir / "execution-plans" / f"{mapping_name}_execution_plan.json",
            "dag_analysis": self.config_dir / "dag-analysis" / f"{mapping_name}_dag_analysis.json",
            "component_metadata": self.config_dir / "component-metadata" / f"{mapping_name}_components.json",
            "memory_profiles": self.config_dir / "runtime" / "memory-profiles.yaml"
        }
        
        for config_type, file_path in config_files.items():
            if file_path.exists():
                self._reload_configuration(config_type, mapping_name, str(file_path))


class HotReloadAwareMappingConfigurationManager:
    """Enhanced configuration manager with hot-reloading support"""
    
    def __init__(self, mapping_name: str, config_dir: str = "config", 
                 environment: str = "default", enable_hot_reload: bool = True):
        self.mapping_name = mapping_name
        self.config_dir = Path(config_dir)
        self.environment = environment
        self.enable_hot_reload = enable_hot_reload
        self.logger = logging.getLogger(f"HotReloadConfigManager.{mapping_name}")
        
        # Hot reloader
        self.hot_reloader = None
        if enable_hot_reload:
            self.hot_reloader = ConfigurationHotReloader(str(self.config_dir))
            self._setup_hot_reload_callbacks()
        
        # Configuration cache with timestamps
        self._config_cache = {}
        self._cache_timestamps = {}
        
        # Original config manager for fallback
        from .config_externalization import MappingConfigurationManager
        self.fallback_manager = MappingConfigurationManager(mapping_name, config_dir, environment)
    
    def start_hot_reload(self):
        """Start hot-reloading if enabled"""
        if self.hot_reloader:
            self.hot_reloader.start()
            self.hot_reloader.preload_configurations({self.mapping_name})
    
    def stop_hot_reload(self):
        """Stop hot-reloading if enabled"""
        if self.hot_reloader:
            self.hot_reloader.stop()
    
    def load_execution_plan(self) -> Dict[str, Any]:
        """Load execution plan with hot-reload support"""
        if self.hot_reloader:
            cached_config = self.hot_reloader.get_cached_config("execution_plan", self.mapping_name)
            if cached_config:
                return cached_config
        
        # Fallback to original manager
        return self.fallback_manager.load_execution_plan()
    
    def load_dag_analysis(self) -> Dict[str, Any]:
        """Load DAG analysis with hot-reload support"""
        if self.hot_reloader:
            cached_config = self.hot_reloader.get_cached_config("dag_analysis", self.mapping_name)
            if cached_config:
                return cached_config
        
        # Fallback to original manager
        return self.fallback_manager.load_dag_analysis()
    
    def load_memory_profile(self, environment: str = None) -> Dict[str, Any]:
        """Load memory profile with hot-reload support"""
        env = environment or self.environment
        
        if self.hot_reloader:
            cached_config = self.hot_reloader.get_cached_config("memory_profiles", "global")
            if cached_config:
                # Apply environment overrides
                return self._apply_environment_overrides(cached_config, env)
        
        # Fallback to original manager
        return self.fallback_manager.load_memory_profile(env)
    
    def load_component_metadata(self) -> Dict[str, Any]:
        """Load component metadata with hot-reload support"""
        if self.hot_reloader:
            cached_config = self.hot_reloader.get_cached_config("component_metadata", self.mapping_name)
            if cached_config:
                return cached_config
        
        # Fallback to original manager
        return self.fallback_manager.load_component_metadata()
    
    def _setup_hot_reload_callbacks(self):
        """Setup callbacks for configuration changes"""
        def on_execution_plan_change(change: ConfigurationChange):
            if change.mapping_name == self.mapping_name:
                self.logger.info(f"Execution plan updated for {self.mapping_name}")
                self._notify_configuration_change("execution_plan", change)
        
        def on_memory_profiles_change(change: ConfigurationChange):
            self.logger.info("Memory profiles updated")
            self._notify_configuration_change("memory_profiles", change)
        
        def on_component_metadata_change(change: ConfigurationChange):
            if change.mapping_name == self.mapping_name:
                self.logger.info(f"Component metadata updated for {self.mapping_name}")
                self._notify_configuration_change("component_metadata", change)
        
        def on_dag_analysis_change(change: ConfigurationChange):
            if change.mapping_name == self.mapping_name:
                self.logger.info(f"DAG analysis updated for {self.mapping_name}")
                self._notify_configuration_change("dag_analysis", change)
        
        # Register callbacks
        self.hot_reloader.register_change_callback("execution_plan", on_execution_plan_change)
        self.hot_reloader.register_change_callback("memory_profiles", on_memory_profiles_change)
        self.hot_reloader.register_change_callback("component_metadata", on_component_metadata_change)
        self.hot_reloader.register_change_callback("dag_analysis", on_dag_analysis_change)
    
    def _notify_configuration_change(self, config_type: str, change: ConfigurationChange):
        """Notify about configuration changes (override for custom behavior)"""
        # Override this method in subclasses for custom change handling
        pass
    
    def _apply_environment_overrides(self, base_config: Dict, environment: str) -> Dict[str, Any]:
        """Apply environment-specific overrides (reuse from original manager)"""
        return self.fallback_manager._apply_environment_overrides(base_config, environment)
    
    def __enter__(self):
        """Context manager entry"""
        if self.enable_hot_reload:
            self.start_hot_reload()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.enable_hot_reload:
            self.stop_hot_reload()