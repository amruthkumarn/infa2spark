"""
Unit tests for configuration externalization framework
Tests the Phase 1 implementation of configuration management classes
"""
import unittest
import json
import yaml
import tempfile
import shutil
from pathlib import Path
from unittest.mock import patch, mock_open
import sys
import os

# Add the src directory to Python path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from core.config_externalization import MappingConfigurationManager, RuntimeConfigResolver
from core.config_file_generator import ConfigurationFileGenerator, ConfigurationValidator


class TestMappingConfigurationManager(unittest.TestCase):
    """Test cases for MappingConfigurationManager"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.config_dir = Path(self.temp_dir) / "conf"
        self.mapping_name = "test_mapping"
        
        # Create test directory structure
        (self.config_dir / "execution-plans").mkdir(parents=True, exist_ok=True)
        (self.config_dir / "dag-analysis").mkdir(parents=True, exist_ok=True)
        (self.config_dir / "component-metadata").mkdir(parents=True, exist_ok=True)
        (self.config_dir / "runtime").mkdir(parents=True, exist_ok=True)
        
        self.config_manager = MappingConfigurationManager(
            mapping_name=self.mapping_name,
            config_dir=str(self.config_dir),
            environment="testing"
        )
        
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir)
        
    def test_load_execution_plan_success(self):
        """Test successful execution plan loading"""
        test_plan = {
            "mapping_name": self.mapping_name,
            "execution_strategy": {
                "parallel_execution_enabled": True,
                "max_parallel_components": 4
            },
            "phases": [
                {
                    "phase_number": 1,
                    "components": ["test_component"]
                }
            ]
        }
        
        plan_file = self.config_dir / "execution-plans" / f"{self.mapping_name}_execution_plan.json"
        with open(plan_file, 'w') as f:
            json.dump(test_plan, f)
            
        loaded_plan = self.config_manager.load_execution_plan()
        self.assertEqual(loaded_plan["mapping_name"], self.mapping_name)
        self.assertTrue(loaded_plan["execution_strategy"]["parallel_execution_enabled"])
        
    def test_load_execution_plan_missing_file(self):
        """Test execution plan loading with missing file returns default"""
        loaded_plan = self.config_manager.load_execution_plan()
        self.assertEqual(loaded_plan["mapping_name"], self.mapping_name)
        self.assertIn("execution_strategy", loaded_plan)
        self.assertIn("phases", loaded_plan)
        
    def test_load_memory_profile_with_environment_scaling(self):
        """Test memory profile loading with environment scaling"""
        test_memory_config = {
            "memory_profiles": {
                "expression_transformation": {
                    "driver_memory": "2g",
                    "executor_memory": "4g",
                    "shuffle_partitions": 200
                }
            },
            "environments": {
                "testing": {
                    "global_scale_factor": 0.5,
                    "max_executors": 2
                }
            }
        }
        
        memory_file = self.config_dir / "runtime" / "memory-profiles.yaml"
        with open(memory_file, 'w') as f:
            yaml.dump(test_memory_config, f)
            
        loaded_config = self.config_manager.load_memory_profile()
        
        # Check that scaling was applied
        expression_profile = loaded_config["memory_profiles"]["expression_transformation"]
        self.assertEqual(expression_profile["driver_memory"], "1g")  # 2g * 0.5
        self.assertEqual(expression_profile["executor_memory"], "2g")  # 4g * 0.5
        self.assertEqual(expression_profile["shuffle_partitions"], 100)  # 200 * 0.5
        self.assertEqual(loaded_config["max_executors"], 2)
        
    def test_memory_value_scaling(self):
        """Test memory value scaling functionality"""
        # Test gigabyte scaling
        self.assertEqual(self.config_manager._scale_memory_value("4g", 0.5), "2g")
        self.assertEqual(self.config_manager._scale_memory_value("2g", 2.0), "4g")
        
        # Test megabyte scaling
        self.assertEqual(self.config_manager._scale_memory_value("512m", 2.0), "1024m")
        
        # Test edge cases
        self.assertEqual(self.config_manager._scale_memory_value("1g", 0.1), "1g")  # Minimum 1
        self.assertEqual(self.config_manager._scale_memory_value("invalid", 2.0), "invalid")
        
    def test_get_component_config(self):
        """Test component-specific configuration retrieval"""
        test_memory_config = {
            "memory_profiles": {
                "expression_transformation": {
                    "driver_memory": "1g",
                    "executor_memory": "2g"
                }
            },
            "component_overrides": {
                "test_component": {
                    "executor_memory": "4g",
                    "reason": "Large dataset processing"
                }
            },
            "environments": {
                "testing": {"global_scale_factor": 1.0}
            }
        }
        
        memory_file = self.config_dir / "runtime" / "memory-profiles.yaml"
        with open(memory_file, 'w') as f:
            yaml.dump(test_memory_config, f)
            
        component_config = self.config_manager.get_component_config(
            "test_component", "ExpressionTransformation"
        )
        
        # Should have base config plus overrides
        self.assertEqual(component_config["driver_memory"], "1g")
        self.assertEqual(component_config["executor_memory"], "4g")  # Override applied
        self.assertEqual(component_config["reason"], "Large dataset processing")


class TestRuntimeConfigResolver(unittest.TestCase):
    """Test cases for RuntimeConfigResolver"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.config_dir = Path(self.temp_dir) / "conf"
        self.mapping_name = "test_mapping"
        
        # Create test directory structure
        (self.config_dir / "execution-plans").mkdir(parents=True, exist_ok=True)
        (self.config_dir / "runtime").mkdir(parents=True, exist_ok=True)
        
        self.config_manager = MappingConfigurationManager(
            mapping_name=self.mapping_name,
            config_dir=str(self.config_dir),
            environment="production"
        )
        
        self.config_resolver = RuntimeConfigResolver(self.config_manager)
        
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir)
        
    def test_resolve_execution_strategy(self):
        """Test execution strategy resolution with environment overrides"""
        # Create test execution plan
        execution_plan = {
            "execution_strategy": {
                "parallel_execution_enabled": True,
                "max_parallel_components": 4
            }
        }
        
        plan_file = self.config_dir / "execution-plans" / f"{self.mapping_name}_execution_plan.json"
        with open(plan_file, 'w') as f:
            json.dump(execution_plan, f)
            
        # Create memory config with environment overrides
        memory_config = {
            "environments": {
                "production": {
                    "max_executors": 20,
                    "global_scale_factor": 2.0
                }
            }
        }
        
        memory_file = self.config_dir / "runtime" / "memory-profiles.yaml"
        with open(memory_file, 'w') as f:
            yaml.dump(memory_config, f)
            
        strategy = self.config_resolver.resolve_execution_strategy()
        
        self.assertTrue(strategy["parallel_execution_enabled"])
        self.assertEqual(strategy["max_executors"], 20)  # Environment override applied
        
    def test_resolve_monitoring_config(self):
        """Test monitoring configuration resolution"""
        memory_config = {
            "environments": {
                "production": {
                    "monitoring_enabled": True,
                    "metrics_namespace": "etl.production",
                    "log_level": "WARN",
                    "checkpoint_dir": "s3a://prod-checkpoints/"
                }
            }
        }
        
        memory_file = self.config_dir / "runtime" / "memory-profiles.yaml"
        with open(memory_file, 'w') as f:
            yaml.dump(memory_config, f)
            
        monitoring_config = self.config_resolver.resolve_monitoring_config()
        
        self.assertTrue(monitoring_config["monitoring_enabled"])
        self.assertEqual(monitoring_config["metrics_namespace"], "etl.production")
        self.assertEqual(monitoring_config["log_level"], "WARN")
        self.assertEqual(monitoring_config["checkpoint_dir"], "s3a://prod-checkpoints/")


class TestConfigurationFileGenerator(unittest.TestCase):
    """Test cases for ConfigurationFileGenerator"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.output_dir = Path(self.temp_dir)
        self.generator = ConfigurationFileGenerator(str(self.output_dir))
        
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir)
        
    def test_generate_execution_plan_config(self):
        """Test execution plan configuration file generation"""
        test_execution_plan = {
            "execution_strategy": {
                "parallel_execution_enabled": True
            },
            "phases": [
                {
                    "phase_number": 1,
                    "components": ["test_component"]
                }
            ]
        }
        
        mapping_name = "test_mapping"
        
        # Create directories first
        self.generator._create_config_directories()
        
        self.generator.generate_execution_plan_config(test_execution_plan, mapping_name)
        
        # Verify file was created
        config_file = self.output_dir / "conf" / "execution-plans" / f"{mapping_name}_execution_plan.json"
        self.assertTrue(config_file.exists())
        
        # Verify content
        with open(config_file, 'r') as f:
            generated_config = json.load(f)
            
        self.assertEqual(generated_config["mapping_name"], mapping_name)
        self.assertIn("metadata", generated_config)
        self.assertIn("optimization_hints", generated_config)
        self.assertTrue(generated_config["execution_strategy"]["parallel_execution_enabled"])
        
    def test_generate_memory_profiles_config(self):
        """Test memory profiles configuration file generation"""
        test_components = [
            {
                "name": "test_expression",
                "type": "ExpressionTransformation",
                "component_type": "transformation"
            },
            {
                "name": "test_aggregator", 
                "type": "AggregatorTransformation",
                "component_type": "transformation"
            }
        ]
        
        # Create directories first
        self.generator._create_config_directories()
        
        self.generator.generate_memory_profiles_config(test_components)
        
        # Verify file was created
        config_file = self.output_dir / "conf" / "runtime" / "memory-profiles.yaml"
        self.assertTrue(config_file.exists())
        
        # Verify content
        with open(config_file, 'r') as f:
            generated_config = yaml.safe_load(f)
            
        self.assertIn("memory_profiles", generated_config)
        self.assertIn("environments", generated_config)
        self.assertIn("expression_transformation", generated_config["memory_profiles"])
        self.assertIn("aggregator_transformation", generated_config["memory_profiles"])
        
        # Verify environment configurations
        environments = generated_config["environments"]
        self.assertIn("development", environments)
        self.assertIn("testing", environments) 
        self.assertIn("production", environments)
        
        # Verify scaling factors
        self.assertEqual(environments["development"]["global_scale_factor"], 0.5)
        self.assertEqual(environments["production"]["global_scale_factor"], 2.0)


class TestConfigurationValidator(unittest.TestCase):
    """Test cases for ConfigurationValidator"""
    
    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.validator = ConfigurationValidator()
        
    def tearDown(self):
        """Clean up test environment"""
        shutil.rmtree(self.temp_dir)
        
    def test_validate_execution_plan_valid(self):
        """Test validation of valid execution plan"""
        valid_config = {
            "mapping_name": "test_mapping",
            "execution_strategy": {
                "parallel_execution_enabled": True
            },
            "phases": [
                {
                    "phase_number": 1,
                    "components": ["test_component"]
                }
            ]
        }
        
        config_file = Path(self.temp_dir) / "valid_config.json"
        with open(config_file, 'w') as f:
            json.dump(valid_config, f)
            
        errors = self.validator.validate_execution_plan(config_file)
        self.assertEqual(len(errors), 0)
        
    def test_validate_execution_plan_missing_fields(self):
        """Test validation of execution plan with missing required fields"""
        invalid_config = {
            "mapping_name": "test_mapping"
            # Missing execution_strategy and phases
        }
        
        config_file = Path(self.temp_dir) / "invalid_config.json"
        with open(config_file, 'w') as f:
            json.dump(invalid_config, f)
            
        errors = self.validator.validate_execution_plan(config_file)
        self.assertGreater(len(errors), 0)
        self.assertTrue(any("execution_strategy" in error for error in errors))
        self.assertTrue(any("phases" in error for error in errors))
        
    def test_validate_execution_plan_invalid_json(self):
        """Test validation of invalid JSON file"""
        config_file = Path(self.temp_dir) / "invalid_json.json"
        with open(config_file, 'w') as f:
            f.write("{ invalid json content")
            
        errors = self.validator.validate_execution_plan(config_file)
        self.assertGreater(len(errors), 0)
        self.assertTrue(any("Invalid JSON format" in error for error in errors))


if __name__ == '__main__':
    # Create test suite
    test_suite = unittest.TestSuite()
    
    # Add test classes
    test_classes = [
        TestMappingConfigurationManager,
        TestRuntimeConfigResolver,
        TestConfigurationFileGenerator,
        TestConfigurationValidator
    ]
    
    for test_class in test_classes:
        tests = unittest.TestLoader().loadTestsFromTestCase(test_class)
        test_suite.addTests(tests)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(test_suite)
    
    # Print summary
    print(f"\nTest Summary:")
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    
    if result.failures:
        print("\nFailures:")
        for test, traceback in result.failures:
            print(f"  {test}: {traceback}")
            
    if result.errors:
        print("\nErrors:")
        for test, traceback in result.errors:
            print(f"  {test}: {traceback}")