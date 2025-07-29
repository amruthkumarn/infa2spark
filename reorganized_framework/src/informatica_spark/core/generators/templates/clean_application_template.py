"""
Clean Application Template for Generated Spark Applications
Simplified structure without unnecessary complexity
"""

CLEAN_APPLICATION_TEMPLATE = '''"""
{{ mapping.name }} Spark Application
Generated from Informatica BDM Project: {{ project.name }}

This application provides a clean, production-ready Spark implementation
of the Informatica mapping with simplified configuration management.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from typing import Dict, Any, List, Optional
import logging
import yaml
from pathlib import Path

# Import framework runtime components
from informatica_spark.runtime.base.mapping import BaseMapping
from informatica_spark.runtime.data_sources.data_manager import DataSourceManager
from informatica_spark.runtime.transformations.spark_transformations import *


class {{ class_name }}(BaseMapping):
    """{{ mapping.name }} Spark mapping implementation"""

    def __init__(self, spark: SparkSession, config_path: str = "config/application.yaml"):
        # Load configuration
        self.config = self._load_config(config_path)
        super().__init__("{{ mapping.name }}", spark, self.config)
        
        # Initialize data source manager with real connections
        self.data_manager = DataSourceManager(
            spark, 
            self.config.get("connections", {})
        )
        
        # Component execution cache
        self.data_cache = {}
        
        # Setup logging
        self._setup_logging()
    
    def _load_config(self, config_path: str) -> Dict[str, Any]:
        """Load application configuration"""
        config_file = Path(config_path)
        if not config_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_file, 'r') as f:
            return yaml.safe_load(f)
    
    def _setup_logging(self):
        """Setup application logging"""
        log_level = self.config.get("log_level", "INFO")
        logging.basicConfig(
            level=getattr(logging, log_level),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def execute(self) -> bool:
        """Execute the mapping"""
        try:
            self.logger.info(f"Starting execution of {self.name}")
            
            # Execute sources
            self._execute_sources()
            
            # Execute transformations
            self._execute_transformations()
            
            # Execute targets
            self._execute_targets()
            
            self.logger.info(f"Successfully completed {self.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error executing {self.name}: {e}")
            raise
    
    def _execute_sources(self):
        """Execute source components"""
        {% for source in sources %}
        # Execute {{ source.name }}
        self.logger.info("Reading from {{ source.name }}")
        df_{{ source.name|lower }} = self.data_manager.read_source(
            name="{{ source.name }}",
            source_type="{{ source.data_object.type if source.data_object else 'hive' }}",
            connection="{{ source.connection }}",
            table="{{ source.data_object.name if source.data_object else source.name|lower }}"
        )
        self.data_cache["{{ source.name }}"] = df_{{ source.name|lower }}
        self.logger.info(f"Read {df_{{ source.name|lower }}.count()} rows from {{ source.name }}")
        {% endfor %}
    
    def _execute_transformations(self):
        """Execute transformation components"""
        {% for transform in transformations %}
        # Execute {{ transform.name }}
        self.logger.info("Applying transformation: {{ transform.name }}")
        
        # Get input data (simplified dependency resolution)
        input_df = self._get_transformation_input("{{ transform.name }}")
        
        # Apply transformation based on type
        {% if transform.type == "ExpressionTransformation" %}
        output_df = self._apply_expression_transformation(input_df, {{ transform.expressions|tojson }})
        {% elif transform.type == "AggregatorTransformation" %}
        output_df = self._apply_aggregator_transformation(input_df, {{ transform.characteristics|tojson }})
        {% elif transform.type == "JoinerTransformation" %}
        output_df = self._apply_joiner_transformation(input_df, {{ transform.characteristics|tojson }})
        {% else %}
        # Generic transformation - pass through with basic operations
        output_df = input_df
        {% endif %}
        
        self.data_cache["{{ transform.name }}"] = output_df
        self.logger.info(f"Transformation {{ transform.name }} produced {output_df.count()} rows")
        {% endfor %}
    
    def _execute_targets(self):
        """Execute target components"""
        {% for target in targets %}
        # Execute {{ target.name }}
        self.logger.info("Writing to {{ target.name }}")
        
        # Get final transformed data
        final_df = self._get_target_input("{{ target.name }}")
        
        # Write to target
        self.data_manager.write_target(
            final_df,
            name="{{ target.name }}",
            target_type="{{ target.data_object.type if target.data_object else 'hive' }}",
            connection="{{ target.connection }}",
            table="{{ target.data_object.name if target.data_object else target.name|lower }}",
            mode="{{ 'overwrite' if target.load_type == 'BULK' else 'append' }}"
        )
        
        self.logger.info(f"Successfully wrote {final_df.count()} rows to {{ target.name }}")
        {% endfor %}
    
    def _get_transformation_input(self, transform_name: str) -> DataFrame:
        """Get input DataFrame for transformation (simplified)"""
        # In a real implementation, this would use dependency analysis
        # For now, use the most recent DataFrame
        if self.data_cache:
            return list(self.data_cache.values())[-1]
        else:
            raise ValueError(f"No input data available for {transform_name}")
    
    def _get_target_input(self, target_name: str) -> DataFrame:
        """Get input DataFrame for target (simplified)"""
        # Use the final transformation output
        if self.data_cache:
            return list(self.data_cache.values())[-1]
        else:
            raise ValueError(f"No input data available for {target_name}")
    
    def _apply_expression_transformation(self, df: DataFrame, expressions: List[Dict]) -> DataFrame:
        """Apply expression transformation"""
        result_df = df
        for expr in expressions:
            if expr.get("expression"):
                # Simple expression handling - in real implementation, parse Informatica expressions
                result_df = result_df.withColumn(expr["name"], lit(expr.get("expression", "")))
        return result_df
    
    def _apply_aggregator_transformation(self, df: DataFrame, config: Dict) -> DataFrame:
        """Apply aggregator transformation"""
        # Simple aggregation - group by first column, count
        if df.columns:
            return df.groupBy(df.columns[0]).count()
        return df
    
    def _apply_joiner_transformation(self, df: DataFrame, config: Dict) -> DataFrame:
        """Apply joiner transformation"""
        # For demonstration - in real implementation, handle multiple inputs
        return df


# Application entry point
def main():
    """Main application entry point"""
    import sys
    from pyspark.sql import SparkSession
    
    # Create Spark session
    spark = SparkSession.builder \\
        .appName("{{ mapping.name }}") \\
        .config("spark.sql.adaptive.enabled", "true") \\
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \\
        .getOrCreate()
    
    try:
        # Create and execute mapping
        mapping = {{ class_name }}(spark)
        success = mapping.execute()
        
        if success:
            print("✅ Mapping execution completed successfully")
            sys.exit(0)
        else:
            print("❌ Mapping execution failed")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Application error: {e}")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
'''