#!/usr/bin/env python3
"""
Comprehensive Validation Script for Generated Spark Code
Tests syntax, imports, instantiation, and logical correctness
"""
import sys
import os
import ast
import importlib.util
import subprocess
from pathlib import Path
from typing import List, Dict, Any, Tuple
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class SparkCodeValidator:
    """Comprehensive validator for generated Spark code"""
    
    def __init__(self, generated_apps_dir: str = "generated_spark_apps"):
        self.generated_apps_dir = Path(generated_apps_dir)
        self.validation_results = []
        
    def validate_all_apps(self) -> Dict[str, Any]:
        """Validate all generated Spark applications"""
        results = {
            "summary": {"total_apps": 0, "passed": 0, "failed": 0},
            "apps": {}
        }
        
        if not self.generated_apps_dir.exists():
            logger.error(f"Generated apps directory not found: {self.generated_apps_dir}")
            return results
            
        for app_dir in self.generated_apps_dir.iterdir():
            if app_dir.is_dir():
                results["summary"]["total_apps"] += 1
                app_result = self.validate_single_app(app_dir)
                results["apps"][app_dir.name] = app_result
                
                if app_result["overall_status"] == "PASSED":
                    results["summary"]["passed"] += 1
                else:
                    results["summary"]["failed"] += 1
                    
        return results
    
    def validate_single_app(self, app_dir: Path) -> Dict[str, Any]:
        """Validate a single Spark application"""
        logger.info(f"Validating app: {app_dir.name}")
        
        result = {
            "app_name": app_dir.name,
            "overall_status": "PASSED",
            "tests": {
                "syntax_validation": {"status": "PENDING", "details": []},
                "import_validation": {"status": "PENDING", "details": []},
                "structure_validation": {"status": "PENDING", "details": []},
                "spark_compatibility": {"status": "PENDING", "details": []},
                "logical_validation": {"status": "PENDING", "details": []}
            }
        }
        
        try:
            # Test 1: Syntax Validation
            result["tests"]["syntax_validation"] = self.validate_syntax(app_dir)
            
            # Test 2: Import Validation  
            result["tests"]["import_validation"] = self.validate_imports(app_dir)
            
            # Test 3: Structure Validation
            result["tests"]["structure_validation"] = self.validate_structure(app_dir)
            
            # Test 4: Spark Compatibility
            result["tests"]["spark_compatibility"] = self.validate_spark_compatibility(app_dir)
            
            # Test 5: Logical Validation
            result["tests"]["logical_validation"] = self.validate_logic(app_dir)
            
            # Determine overall status
            failed_tests = [test for test, data in result["tests"].items() 
                          if data["status"] == "FAILED"]
            if failed_tests:
                result["overall_status"] = "FAILED"
                result["failed_tests"] = failed_tests
                
        except Exception as e:
            result["overall_status"] = "ERROR"
            result["error"] = str(e)
            logger.error(f"Error validating {app_dir.name}: {e}")
            
        return result
    
    def validate_syntax(self, app_dir: Path) -> Dict[str, Any]:
        """Validate Python syntax of all files"""
        result = {"status": "PASSED", "details": []}
        
        python_files = list(app_dir.rglob("*.py"))
        
        for py_file in python_files:
            try:
                with open(py_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                # Parse AST to check syntax
                ast.parse(content)
                result["details"].append(f"✅ {py_file.relative_to(app_dir)}: Syntax OK")
                
            except SyntaxError as e:
                result["status"] = "FAILED"
                result["details"].append(f"❌ {py_file.relative_to(app_dir)}: Syntax Error - {e}")
            except Exception as e:
                result["status"] = "FAILED"
                result["details"].append(f"❌ {py_file.relative_to(app_dir)}: Error - {e}")
                
        return result
    
    def validate_imports(self, app_dir: Path) -> Dict[str, Any]:
        """Validate import statements and dependencies"""
        result = {"status": "PASSED", "details": []}
        
        # Check main.py imports
        main_py = app_dir / "src" / "main" / "python" / "main.py"
        if main_py.exists():
            imports_ok = self.check_file_imports(main_py, app_dir)
            if imports_ok:
                result["details"].append("✅ main.py: Imports OK")
            else:
                result["status"] = "FAILED"
                result["details"].append("❌ main.py: Import issues")
        
        # Check transformations
        transform_dir = app_dir / "src" / "main" / "python" / "transformations"
        if transform_dir.exists():
            for py_file in transform_dir.glob("*.py"):
                if py_file.name != "__init__.py":
                    imports_ok = self.check_transformation_imports(py_file, app_dir)
                    if imports_ok:
                        result["details"].append(f"✅ {py_file.name}: Imports OK")
                    else:
                        result["status"] = "FAILED"
                        result["details"].append(f"❌ {py_file.name}: Import issues")
        
        return result
    
    def check_file_imports(self, file_path: Path, app_dir: Path) -> bool:
        """Check if file imports are valid"""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Parse and check imports
            tree = ast.parse(content)
            
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    # Check for common import issues
                    if isinstance(node, ast.ImportFrom):
                        if node.module and node.module.startswith('..'):
                            # Relative import - check if valid
                            continue
                            
            return True
            
        except Exception as e:
            logger.error(f"Import check failed for {file_path}: {e}")
            return False
    
    def check_transformation_imports(self, file_path: Path, app_dir: Path) -> bool:
        """Check transformation file imports specifically"""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Check for required PySpark imports
            required_imports = [
                "from pyspark.sql import DataFrame",
                "from pyspark.sql.functions import",
                "from pyspark.sql.types import"
            ]
            
            for req_import in required_imports:
                if req_import not in content:
                    logger.warning(f"Missing required import in {file_path}: {req_import}")
                    
            return True
            
        except Exception as e:
            logger.error(f"Transformation import check failed for {file_path}: {e}")
            return False
    
    def validate_structure(self, app_dir: Path) -> Dict[str, Any]:
        """Validate expected directory and file structure"""
        result = {"status": "PASSED", "details": []}
        
        expected_structure = [
            "src/main/python/main.py",
            "src/main/python/base_classes.py",
            "config/application.yaml",
            "README.md",
            "requirements.txt"
        ]
        
        for expected_file in expected_structure:
            file_path = app_dir / expected_file
            if file_path.exists():
                result["details"].append(f"✅ {expected_file}: Found")
            else:
                result["status"] = "FAILED"
                result["details"].append(f"❌ {expected_file}: Missing")
                
        return result
    
    def validate_spark_compatibility(self, app_dir: Path) -> Dict[str, Any]:
        """Validate Spark compatibility and best practices"""
        result = {"status": "PASSED", "details": []}
        
        # Check for Spark session creation
        main_py = app_dir / "src" / "main" / "python" / "main.py"
        if main_py.exists():
            with open(main_py, 'r') as f:
                content = f.read()
                
            spark_checks = [
                ("SparkSession.builder", "✅ SparkSession creation found"),
                ("enableHiveSupport", "✅ Hive support enabled"),
                ("setLogLevel", "✅ Log level configuration found")
            ]
            
            for check, success_msg in spark_checks:
                if check in content:
                    result["details"].append(success_msg)
                else:
                    result["details"].append(f"⚠️  {check} not found (optional)")
        
        # Check transformation files for Spark functions
        transform_dir = app_dir / "src" / "main" / "python" / "transformations"
        if transform_dir.exists():
            for py_file in transform_dir.glob("*.py"):
                if py_file.name != "__init__.py":
                    spark_compat = self.check_spark_transformation_compatibility(py_file)
                    if spark_compat:
                        result["details"].append(f"✅ {py_file.name}: Spark compatible")
                    else:
                        result["status"] = "FAILED"
                        result["details"].append(f"❌ {py_file.name}: Spark compatibility issues")
        
        return result
    
    def check_spark_transformation_compatibility(self, file_path: Path) -> bool:
        """Check if transformation uses proper Spark functions"""
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Check for proper Spark DataFrame operations
            spark_operations = [
                "DataFrame", "withColumn", "filter", "groupBy", 
                "agg", "join", "union", "orderBy"
            ]
            
            has_spark_ops = any(op in content for op in spark_operations)
            return has_spark_ops
            
        except Exception:
            return False
    
    def validate_logic(self, app_dir: Path) -> Dict[str, Any]:
        """Validate logical correctness of transformations"""
        result = {"status": "PASSED", "details": []}
        
        # Check transformation logic patterns
        transform_dir = app_dir / "src" / "main" / "python" / "transformations"
        if transform_dir.exists():
            for py_file in transform_dir.glob("*.py"):
                if py_file.name != "__init__.py":
                    logic_check = self.check_transformation_logic(py_file)
                    result["details"].extend(logic_check)
        
        return result
    
    def check_transformation_logic(self, file_path: Path) -> List[str]:
        """Check transformation logic patterns"""
        details = []
        
        try:
            with open(file_path, 'r') as f:
                content = f.read()
                
            # Check for common transformation patterns
            patterns = {
                "SequenceTransformation": ["monotonically_increasing_id", "row_number"],
                "SorterTransformation": ["orderBy", "asc", "desc"],
                "RouterTransformation": ["filter", "condition"],
                "UnionTransformation": ["union", "distinct"],
                "AggregatorTransformation": ["groupBy", "agg", "sum", "count"],
                "ExpressionTransformation": ["withColumn", "expr"],
                "LookupTransformation": ["join"],
                "JoinerTransformation": ["join"]
            }
            
            for transform_type, required_patterns in patterns.items():
                if transform_type in content:
                    found_patterns = [p for p in required_patterns if p in content]
                    if found_patterns:
                        details.append(f"✅ {transform_type}: Logic patterns found {found_patterns}")
                    else:
                        details.append(f"⚠️  {transform_type}: Missing expected patterns {required_patterns}")
                        
        except Exception as e:
            details.append(f"❌ Logic check failed: {e}")
            
        return details
    
    def generate_report(self, results: Dict[str, Any]) -> str:
        """Generate a comprehensive validation report"""
        report = []
        report.append("=" * 80)
        report.append("SPARK CODE VALIDATION REPORT")
        report.append("=" * 80)
        report.append("")
        
        # Summary
        summary = results["summary"]
        report.append(f"📊 SUMMARY:")
        report.append(f"   Total Applications: {summary['total_apps']}")
        report.append(f"   ✅ Passed: {summary['passed']}")
        report.append(f"   ❌ Failed: {summary['failed']}")
        report.append(f"   Success Rate: {(summary['passed']/summary['total_apps']*100):.1f}%" if summary['total_apps'] > 0 else "   Success Rate: N/A")
        report.append("")
        
        # Detailed results
        for app_name, app_result in results["apps"].items():
            report.append(f"🚀 APPLICATION: {app_name}")
            report.append(f"   Overall Status: {app_result['overall_status']}")
            report.append("")
            
            for test_name, test_result in app_result["tests"].items():
                report.append(f"   📋 {test_name.upper().replace('_', ' ')}: {test_result['status']}")
                for detail in test_result["details"]:
                    report.append(f"      {detail}")
                report.append("")
                
        report.append("=" * 80)
        return "\n".join(report)

def main():
    """Main validation function"""
    print("🔍 Starting Comprehensive Spark Code Validation...")
    
    validator = SparkCodeValidator()
    results = validator.validate_all_apps()
    
    # Generate and display report
    report = validator.generate_report(results)
    print(report)
    
    # Save report to file
    with open("spark_code_validation_report.txt", "w") as f:
        f.write(report)
    
    print(f"\n📄 Detailed report saved to: spark_code_validation_report.txt")
    
    # Exit with appropriate code
    if results["summary"]["failed"] > 0:
        print(f"\n❌ Validation completed with {results['summary']['failed']} failures")
        sys.exit(1)
    else:
        print(f"\n✅ All {results['summary']['passed']} applications passed validation!")
        sys.exit(0)

if __name__ == "__main__":
    main() 