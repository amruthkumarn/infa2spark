"""
m_Complete_Transformation_Showcase Basic Implementation for Testing
Generated from Informatica BDM Project: Enterprise_Complete_Transformations
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from typing import Dict, Any, List, Optional
import logging

import sys
from pathlib import Path

# Add the parent directory to Python path for imports
sys.path.append(str(Path(__file__).parent.parent))

from base_classes import BaseMapping


class MCompleteTransformationShowcase(BaseMapping):
    """m_Complete_Transformation_Showcase mapping - simplified for testing"""

    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__("m_Complete_Transformation_Showcase", spark, config)
        self.logger.info("Initialized MCompleteTransformationShowcase mapping")

    def execute(self, input_df: DataFrame = None) -> DataFrame:
        """
        Execute the mapping transformation
        This is a simplified version for testing purposes
        """
        try:
            self.logger.info("Starting mapping execution")
            
            # If no input provided, create a simple test output
            if input_df is None:
                test_data = [
                    (1, "Test Customer 1", "Engineering", 75000),
                    (2, "Test Customer 2", "Finance", 65000),  
                    (3, "Test Customer 3", "Marketing", 55000)
                ]
                input_df = self.spark.createDataFrame(
                    test_data, 
                    ["id", "name", "department", "salary"]
                )
            
            # Simple transformation - add a calculated field
            result_df = input_df.withColumn(
                "annual_bonus", 
                col("salary") * 0.1
            ).withColumn(
                "processed_date", 
                current_timestamp()
            )
            
            self.logger.info(f"Mapping execution completed. Output rows: {result_df.count()}")
            return result_df
            
        except Exception as e:
            self.logger.error(f"Error in mapping execution: {str(e)}")
            raise
    
    def validate_schema(self, df: DataFrame) -> bool:
        """Validate input schema"""
        required_columns = ["id", "name"]
        actual_columns = df.columns
        
        for col in required_columns:
            if col not in actual_columns:
                self.logger.error(f"Missing required column: {col}")
                return False
        
        return True
    
    def get_execution_plan(self) -> List[Dict[str, Any]]:
        """Get execution plan for this mapping"""
        return [
            {
                "step": "input_validation",
                "description": "Validate input schema"
            },
            {
                "step": "transformation",
                "description": "Apply business transformations"
            },
            {
                "step": "output_generation", 
                "description": "Generate final output"
            }
        ]