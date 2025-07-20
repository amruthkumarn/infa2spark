"""
Mock data generator for PoC testing
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
import random
from datetime import datetime, timedelta

class MockDataGenerator:
    """Generates mock data for testing purposes"""
    
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        
    def generate_mock_data(self, source_name: str) -> DataFrame:
        """Generate mock data based on source name"""
        source_lower = source_name.lower()
        
        if "sales" in source_lower:
            return self._generate_sales_data()
        elif "customer" in source_lower:
            return self._generate_customer_data()
        elif "order" in source_lower:
            return self._generate_order_data()
        elif "product" in source_lower:
            return self._generate_product_data()
        elif "fact" in source_lower:
            return self._generate_fact_data()
        else:
            return self._generate_generic_data()
            
    def _generate_sales_data(self) -> DataFrame:
        """Generate sales source data"""
        data = []
        regions = ['North', 'South', 'East', 'West']
        products = ['Product_A', 'Product_B', 'Product_C', 'Product_D']
        
        for i in range(1000):
            data.append((
                f"SALE_{i:06d}",
                random.choice(regions),
                random.choice(products),
                round(random.uniform(100.0, 1000.0), 2),
                (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
                f"CUST_{random.randint(1, 100):06d}"
            ))
            
        schema = StructType([
            StructField("sale_id", StringType(), True),
            StructField("region", StringType(), True),
            StructField("product", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("sale_date", StringType(), True),
            StructField("customer_id", StringType(), True)
        ])
        
        return self.spark.createDataFrame(data, schema)
        
    def _generate_customer_data(self) -> DataFrame:
        """Generate customer source data"""
        data = []
        cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
        statuses = ['Active', 'Inactive', 'Pending']
        
        for i in range(100):
            data.append((
                f"CUST_{i:06d}",
                f"Customer_{i}",
                random.choice(cities),
                random.choice(statuses),
                (datetime.now() - timedelta(days=random.randint(30, 365))).strftime('%Y-%m-%d'),
                f"customer_{i}@email.com"
            ))
            
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("customer_name", StringType(), True),
            StructField("city", StringType(), True),
            StructField("status", StringType(), True),
            StructField("created_date", StringType(), True),
            StructField("email", StringType(), True)
        ])
        
        return self.spark.createDataFrame(data, schema)
        
    def _generate_order_data(self) -> DataFrame:
        """Generate order source data"""
        data = []
        order_statuses = ['Completed', 'Pending', 'Cancelled']
        
        for i in range(500):
            data.append((
                f"ORD_{i:06d}",
                f"CUST_{random.randint(1, 100):06d}",
                random.choice(order_statuses),
                round(random.uniform(50.0, 500.0), 2),
                (datetime.now() - timedelta(days=random.randint(0, 60))).strftime('%Y-%m-%d')
            ))
            
        schema = StructType([
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("order_status", StringType(), True),
            StructField("order_amount", DoubleType(), True),
            StructField("order_date", StringType(), True)
        ])
        
        return self.spark.createDataFrame(data, schema)
        
    def _generate_product_data(self) -> DataFrame:
        """Generate product source data"""
        data = []
        categories = ['Electronics', 'Clothing', 'Books', 'Home']
        
        for i in range(50):
            data.append((
                f"PROD_{i:06d}",
                f"Product_{i}",
                random.choice(categories),
                round(random.uniform(10.0, 200.0), 2)
            ))
            
        schema = StructType([
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("unit_price", DoubleType(), True)
        ])
        
        return self.spark.createDataFrame(data, schema)
        
    def _generate_fact_data(self) -> DataFrame:
        """Generate fact table data"""
        data = []
        
        for i in range(800):
            data.append((
                f"FACT_{i:06d}",
                f"CUST_{random.randint(1, 100):06d}",
                f"PROD_{random.randint(1, 50):06d}",
                random.randint(1, 10),
                round(random.uniform(100.0, 1000.0), 2),
                (datetime.now() - timedelta(days=random.randint(0, 90))).strftime('%Y-%m-%d')
            ))
            
        schema = StructType([
            StructField("fact_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("total_amount", DoubleType(), True),
            StructField("transaction_date", StringType(), True)
        ])
        
        return self.spark.createDataFrame(data, schema)
        
    def _generate_generic_data(self) -> DataFrame:
        """Generate generic data"""
        data = [
            ("1", "Sample Data 1", "2023-01-01"),
            ("2", "Sample Data 2", "2023-01-02"),
            ("3", "Sample Data 3", "2023-01-03")
        ]
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("date", StringType(), True)
        ])
        
        return self.spark.createDataFrame(data, schema)