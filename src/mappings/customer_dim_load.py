"""
Customer Dimension Load Mapping Implementation
Converts: CUSTOMER_SOURCE -> LOOKUP_CUSTOMER -> SCD_LOGIC -> DIM_CUSTOMER
"""
from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from ..core.base_classes import BaseMapping
from ..transformations.base_transformation import LookupTransformation, JavaTransformation
from ..data_sources.data_source_manager import DataSourceManager

class CustomerDimLoad(BaseMapping):
    """Customer Dimension SCD Type 2 Load mapping implementation"""
    
    def __init__(self, spark_session, config):
        super().__init__("Customer_Dim_Load", spark_session, config)
        self.data_source_manager = DataSourceManager(spark_session, config.get('connections', {}))
        
    def execute(self) -> bool:
        """Execute the Customer Dimension Load mapping"""
        try:
            self.logger.info("Starting Customer Dimension Load mapping execution")
            
            # Step 1: Read from CUSTOMER_SOURCE (DB2)
            source_df = self._read_customer_source()
            
            # Step 2: Apply LOOKUP_CUSTOMER transformation
            lookup_result_df = self._apply_lookup_transformation(source_df)
            
            # Step 3: Apply SCD_LOGIC transformation
            scd_result_df = self._apply_scd_transformation(lookup_result_df)
            
            # Step 4: Write to DIM_CUSTOMER target (Hive)
            self._write_to_dimension_target(scd_result_df)
            
            self.logger.info("Customer Dimension Load mapping executed successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error in Customer Dimension Load mapping: {str(e)}")
            raise
            
    def _read_customer_source(self) -> DataFrame:
        """Read from CUSTOMER_SOURCE"""
        self.logger.info("Reading from CUSTOMER_SOURCE")
        return self.data_source_manager.read_source("CUSTOMER_SOURCE", "DB2")
        
    def _read_existing_dimension(self) -> DataFrame:
        """Read existing dimension data"""
        try:
            return self.data_source_manager.read_source("DIM_CUSTOMER", "HIVE")
        except Exception:
            # First load - no existing dimension
            self.logger.info("No existing dimension found - first load")
            return None
            
    def _apply_lookup_transformation(self, source_df: DataFrame) -> DataFrame:
        """Apply LOOKUP_CUSTOMER transformation"""
        self.logger.info("Applying LOOKUP_CUSTOMER transformation")
        
        # Read existing dimension for lookup
        existing_dim_df = self._read_existing_dimension()
        
        if existing_dim_df is None:
            # First load - no lookup needed, just add dimension keys
            return source_df.withColumn("customer_key", monotonically_increasing_id())
            
        # Perform lookup to find existing customers
        join_conditions = ["source_df.customer_id = existing_dim_df.customer_id"]
        
        lookup_transformation = LookupTransformation(
            name="LOOKUP_CUSTOMER",
            join_conditions=join_conditions,
            join_type="left"
        )
        
        # Select only current records for lookup
        current_dim_df = existing_dim_df.filter(col("is_current") == True) \
                                       .select("customer_id", "customer_key", "customer_name", 
                                             "city", "status", "email")
        
        result_df = lookup_transformation.transform(source_df, current_dim_df)
        
        return result_df
        
    def _apply_scd_transformation(self, input_df: DataFrame) -> DataFrame:
        """Apply SCD_LOGIC transformation"""
        self.logger.info("Applying SCD_LOGIC transformation")
        
        # Read existing dimension data
        existing_dim_df = self._read_existing_dimension()
        
        scd_transformation = JavaTransformation(
            name="SCD_LOGIC",
            logic_type="scd_type2"
        )
        
        if existing_dim_df is None:
            # First load
            result_df = scd_transformation.transform(input_df, None)
        else:
            # Incremental load - implement SCD Type 2
            result_df = self._implement_scd_type2_logic(input_df, existing_dim_df)
            
        return result_df
        
    def _implement_scd_type2_logic(self, source_df: DataFrame, existing_df: DataFrame) -> DataFrame:
        """Implement SCD Type 2 logic manually"""
        self.logger.info("Implementing SCD Type 2 logic")
        
        # Get current records from existing dimension
        current_existing = existing_df.filter(col("is_current") == True)
        
        # Join source with existing to identify changes
        joined_df = source_df.alias("src").join(
            current_existing.alias("dim"),
            col("src.customer_id") == col("dim.customer_id"),
            "left"
        )
        
        # Identify new customers (not in dimension)
        new_customers = joined_df.filter(col("dim.customer_id").isNull()) \
                                .select("src.*") \
                                .withColumn("customer_key", monotonically_increasing_id() + 100000) \
                                .withColumn("effective_date", current_date()) \
                                .withColumn("expiry_date", lit(None).cast("date")) \
                                .withColumn("is_current", lit(True)) \
                                .withColumn("record_version", lit(1))
        
        # Identify changed customers
        changed_customers = joined_df.filter(
            (col("dim.customer_id").isNotNull()) &
            ((col("src.customer_name") != col("dim.customer_name")) |
             (col("src.city") != col("dim.city")) |
             (col("src.status") != col("dim.status")) |
             (col("src.email") != col("dim.email")))
        )
        
        # Create new versions for changed customers
        new_versions = changed_customers.select("src.*") \
                                       .withColumn("customer_key", monotonically_increasing_id() + 200000) \
                                       .withColumn("effective_date", current_date()) \
                                       .withColumn("expiry_date", lit(None).cast("date")) \
                                       .withColumn("is_current", lit(True)) \
                                       .withColumn("record_version", lit(2))
        
        # Expire old versions for changed customers
        expired_customers = existing_df.join(
            changed_customers.select("src.customer_id").alias("changed"),
            existing_df.customer_id == col("changed.customer_id"),
            "inner"
        ).filter(col("is_current") == True) \
         .withColumn("expiry_date", current_date()) \
         .withColumn("is_current", lit(False))
        
        # Keep unchanged current records
        unchanged_customers = existing_df.join(
            source_df.select("customer_id").alias("src"),
            existing_df.customer_id == col("src.customer_id"),
            "left"
        ).filter(
            (col("src.customer_id").isNull()) |  # Customers not in source
            (col("is_current") == False)  # Historical records
        )
        
        # Union all records
        result_df = new_customers.unionByName(new_versions, allowMissingColumns=True) \
                                .unionByName(expired_customers, allowMissingColumns=True) \
                                .unionByName(unchanged_customers, allowMissingColumns=True)
        
        return result_df
        
    def _write_to_dimension_target(self, output_df: DataFrame):
        """Write to DIM_CUSTOMER target"""
        self.logger.info("Writing to DIM_CUSTOMER target")
        
        # Add audit columns
        final_df = output_df.withColumn("load_date", current_date()) \
                           .withColumn("load_timestamp", current_timestamp()) \
                           .withColumn("source_system", lit("CUSTOMER_SYSTEM"))
        
        self.data_source_manager.write_target(final_df, "DIM_CUSTOMER", "HIVE", mode="overwrite")