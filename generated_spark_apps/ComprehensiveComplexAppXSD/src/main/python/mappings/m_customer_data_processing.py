"""
m_Customer_Data_Processing Mapping Implementation
Generated from Informatica BDM Project: Comprehensive_Complex_Project

Components:
- SRC_Customer_File (Source)
- EXP_Data_Cleansing (Expression)
- TGT_Customer_Warehouse (Target)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ..base_classes import BaseMapping, DataSourceManager
from ..transformations.generated_transformations import *


class MCustomerDataProcessing(BaseMapping):
    """m_Customer_Data_Processing mapping implementation"""

    def __init__(self, spark, config):
        super().__init__("m_Customer_Data_Processing", spark, config)
        self.data_source_manager = DataSourceManager(
            spark, config.get("connections", {})
        )

    def execute(self) -> bool:
        """Execute the m_Customer_Data_Processing mapping"""
        try:
            self.logger.info("Starting m_Customer_Data_Processing mapping execution")

            # Read source data
            src_customer_file_df = self._read_src_customer_file()
            # Apply transformations
            current_df = src_customer_file_df
            current_df = self._apply_exp_data_cleansing(current_df)
            # Write to targets
            self._write_to_tgt_customer_warehouse(current_df)
            self.logger.info("m_Customer_Data_Processing mapping executed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in m_Customer_Data_Processing mapping: {str(e)}")
            raise

    def _read_src_customer_file(self) -> DataFrame:
        """Read from SRC_Customer_File"""
        self.logger.info("Reading from SRC_Customer_File")
        return self.data_source_manager.read_source("SRC_Customer_File", "Source")

    def _apply_exp_data_cleansing(self, input_df: DataFrame) -> DataFrame:
        """Apply EXP_Data_Cleansing transformation"""
        self.logger.info("Applying EXP_Data_Cleansing transformation")

        # Use ExpressionTransformation class for reusability
        transformation = ExpressionTransformation(
            name="EXP_Data_Cleansing",
            expressions=self._get_exp_data_cleansing_expressions(),
            filters=self._get_exp_data_cleansing_filters(),
        )
        result_df = transformation.transform(input_df)

        # Apply data type casting based on port types (manual implementation for specific typing)
        # Cast FullName_OUT to string
        result_df = result_df.withColumn(
            "FullName_OUT", col("FullName_OUT").cast("string")
        )
        # Cast CleanEmail_OUT to string
        result_df = result_df.withColumn(
            "CleanEmail_OUT", col("CleanEmail_OUT").cast("string")
        )
        # Cast CleanPhone_OUT to string
        result_df = result_df.withColumn(
            "CleanPhone_OUT", col("CleanPhone_OUT").cast("string")
        )
        # Cast DataQualityScore_OUT to decimal
        result_df = result_df.withColumn(
            "DataQualityScore_OUT", col("DataQualityScore_OUT").cast("decimal")
        )

        return result_df

    def _get_exp_data_cleansing_expressions(self) -> dict:
        """Get expression transformation expressions"""
        return {
            "FullName_OUT": "concat(TRIM(UPPER(FirstName_IN)), ' ', TRIM(UPPER(LastName_IN)))",
            "CleanEmail_OUT": "CASE WHEN INSTR(LOWER(TRIM(Email_IN)), '@') > 0 THEN LOWER(TRIM(Email_IN)) ELSE NULL END",
            "CleanPhone_OUT": "REGEXP_REPLACE(Phone_IN, '[^0-9]', '')",
            "DataQualityScore_OUT": "(CASE WHEN FirstName_IN IS NOT NULL THEN 0.25 ELSE 0 END) + (CASE WHEN LastName_IN IS NOT NULL THEN 0.25 ELSE 0 END) + (CASE WHEN CleanEmail_OUT IS NOT NULL THEN 0.25 ELSE 0 END) + (CASE WHEN LENGTH(CleanPhone_OUT) >= 10 THEN 0.25 ELSE 0 END)",
        }

    def _get_exp_data_cleansing_filters(self) -> list:
        """Get expression transformation filters"""
        return [
            # Add your filter logic here
            # "column_name IS NOT NULL",
            # "amount > 0"
        ]

    def _write_to_tgt_customer_warehouse(self, output_df: DataFrame):
        """Write to TGT_Customer_Warehouse target"""
        self.logger.info("Writing to TGT_Customer_Warehouse target")

        # Add audit columns
        final_df = output_df.withColumn(
            "load_timestamp", current_timestamp()
        ).withColumn("load_date", current_date())

        self.data_source_manager.write_target(
            final_df, "TGT_Customer_Warehouse", "Target", mode="overwrite"
        )
