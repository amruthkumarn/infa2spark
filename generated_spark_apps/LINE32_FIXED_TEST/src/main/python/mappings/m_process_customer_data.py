"""
m_Process_Customer_Data Mapping Implementation
Generated from Informatica BDM Project: Complex_Production_Project

Components:
- SRC_Customer_File (Source)
- EXP_Standardize_Names (Expression)
- TGT_Customer_DW (Target)
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.types import *
from ..base_classes import BaseMapping, DataSourceManager
from ..transformations.generated_transformations import *


class MProcessCustomerData(BaseMapping):
    """m_Process_Customer_Data mapping implementation"""

    def __init__(self, spark, config):
        super().__init__("m_Process_Customer_Data", spark, config)
        self.data_source_manager = DataSourceManager(
            spark, config.get("connections", {})
        )

    def execute(self) -> bool:
        """Execute the m_Process_Customer_Data mapping"""
        try:
            self.logger.info("Starting m_Process_Customer_Data mapping execution")

            # Read source data
            src_customer_file_df = self._read_src_customer_file()
            # Apply transformations
            current_df = src_customer_file_df
            current_df = self._apply_exp_standardize_names(current_df)
            # Write to targets
            self._write_to_tgt_customer_dw(current_df)
            self.logger.info("m_Process_Customer_Data mapping executed successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error in m_Process_Customer_Data mapping: {str(e)}")
            raise

    def _read_src_customer_file(self) -> DataFrame:
        """Read from SRC_Customer_File"""
        self.logger.info("Reading from SRC_Customer_File")
        return self.data_source_manager.read_source("SRC_Customer_File", "Source")

    def _apply_exp_standardize_names(self, input_df: DataFrame) -> DataFrame:
        """Apply EXP_Standardize_Names transformation"""
        self.logger.info("Applying EXP_Standardize_Names transformation")

        # Expression transformation with field-level logic
        # Input fields validation
        input_fields = ["CustomerID_IN", "FirstName_IN", "LastName_IN"]
        missing_fields = [f for f in input_fields if f not in input_df.columns]
        if missing_fields:
            raise ValueError(f"Missing input fields: {missing_fields}")

        result_df = input_df

        # Apply field expressions based on ExpressionField definitions
        # Expression: FullName_OUT = FirstName_IN || ' ' || LastName_IN
        result_df = result_df.withColumn(
            "FullName_OUT", expr("concat(FirstName_IN, ' ', LastName_IN)")
        )

        # Apply data type casting based on port types
        # Cast FullName_OUT to string
        result_df = result_df.withColumn(
            "FullName_OUT", col("FullName_OUT").cast("string")
        )

        return result_df

    def _get_exp_standardize_names_expressions(self) -> dict:
        """Get expression transformation expressions"""
        return {
            "FullName_OUT": "concat(FirstName_IN, ' ', LastName_IN)",
        }

    def _get_exp_standardize_names_filters(self) -> list:
        """Get expression transformation filters"""
        return [
            # Add your filter logic here
            # "column_name IS NOT NULL",
            # "amount > 0"
        ]

    def _write_to_tgt_customer_dw(self, output_df: DataFrame):
        """Write to TGT_Customer_DW target"""
        self.logger.info("Writing to TGT_Customer_DW target")

        # Add audit columns
        final_df = output_df.withColumn(
            "load_timestamp", current_timestamp()
        ).withColumn("load_date", current_date())

        self.data_source_manager.write_target(
            final_df, "TGT_Customer_DW", "Target", mode="overwrite"
        )
