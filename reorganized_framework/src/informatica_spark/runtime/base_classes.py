"""
Base classes for Informatica to PySpark conversion framework
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
import logging

class BaseInformaticaObject(ABC):
    """Base class for all Informatica objects"""
    
    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self.properties = {}
        
    def set_property(self, key: str, value: Any):
        self.properties[key] = value
        
    def get_property(self, key: str, default: Any = None):
        return self.properties.get(key, default)

class BaseMapping(BaseInformaticaObject):
    """Base class for all mapping objects"""
    
    def __init__(self, name: str, spark_session: SparkSession, config: Dict[str, Any]):
        super().__init__(name)
        self.spark = spark_session
        self.config = config
        self.sources = []
        self.transformations = []
        self.targets = []
        self.logger = logging.getLogger(f"Mapping.{name}")
        
    @abstractmethod
    def execute(self) -> bool:
        """Execute the mapping logic"""
        pass
        
    def add_source(self, source):
        self.sources.append(source)
        
    def add_transformation(self, transformation):
        self.transformations.append(transformation)
        
    def add_target(self, target):
        self.targets.append(target)

class BaseTransformation(BaseInformaticaObject):
    """Base class for all transformation objects"""
    
    def __init__(self, name: str, transformation_type: str):
        super().__init__(name)
        self.transformation_type = transformation_type
        self.input_ports = []
        self.output_ports = []
        
    @abstractmethod
    def transform(self, input_df: DataFrame) -> DataFrame:
        """Apply transformation to input DataFrame"""
        pass

class BaseWorkflow(BaseInformaticaObject):
    """Base class for workflow objects"""
    
    def __init__(self, name: str, spark_session: SparkSession, config: Dict[str, Any]):
        super().__init__(name)
        self.spark = spark_session
        self.config = config
        self.tasks = []
        self.links = []
        self.logger = logging.getLogger(f"Workflow.{name}")
        
    @abstractmethod
    def execute(self) -> bool:
        """Execute the workflow"""
        pass
        
    def add_task(self, task):
        self.tasks.append(task)
        
    def add_link(self, link):
        self.links.append(link)

class Task:
    """Represents a workflow task"""
    
    def __init__(self, name: str, task_type: str, mapping_name: str = None):
        self.name = name
        self.task_type = task_type
        self.mapping_name = mapping_name
        self.properties = {}
        self.status = "PENDING"
        
    def set_status(self, status: str):
        self.status = status

class WorkflowLink:
    """Represents a link between workflow tasks"""
    
    def __init__(self, from_task: str, to_task: str, condition: str = "SUCCESS"):
        self.from_task = from_task
        self.to_task = to_task
        self.condition = condition

class Connection:
    """Represents a data connection"""
    
    def __init__(self, name: str, connection_type: str, host: str, port: int, **kwargs):
        self.name = name
        self.connection_type = connection_type
        self.host = host
        self.port = port
        self.properties = kwargs
        
    def get_connection_string(self) -> str:
        """Get connection string based on type"""
        if self.connection_type.upper() == "DB2":
            return f"jdbc:db2://{self.host}:{self.port}/{self.properties.get('database')}"
        elif self.connection_type.upper() == "ORACLE":
            return f"jdbc:oracle:thin:@{self.host}:{self.port}:{self.properties.get('service')}"
        elif self.connection_type.upper() == "HDFS":
            return f"hdfs://{self.host}:{self.port}"
        elif self.connection_type.upper() == "HIVE":
            return f"jdbc:hive2://{self.host}:{self.port}"
        else:
            return f"{self.connection_type.lower()}://{self.host}:{self.port}"

class Project:
    """Represents an Informatica project"""
    
    def __init__(self, name: str, version: str):
        self.name = name
        self.version = version
        self.description = ""
        self.folders = {}
        self.connections = {}
        self.parameters = {}
        self.mappings = {}
        self.workflows = {}
        self.applications = {}
        
    def add_folder(self, folder_name: str, folder_type: str):
        if folder_type not in self.folders:
            self.folders[folder_type] = {}
        self.folders[folder_type][folder_name] = []
        
    def add_connection(self, connection: Connection):
        self.connections[connection.name] = connection
        
    def add_parameter(self, name: str, value: str):
        self.parameters[name] = value
        
    def add_mapping(self, mapping):
        self.mappings[mapping.name] = mapping
        
    def add_workflow(self, workflow):
        self.workflows[workflow.name] = workflow


class BaseWorkflow(BaseInformaticaObject):
    """Base class for all workflow objects"""
    
    def __init__(self, name: str, spark_session: SparkSession, config: Dict[str, Any]):
        super().__init__(name)
        self.spark = spark_session
        self.config = config
        self.tasks = []
        self.logger = logging.getLogger(f"Workflow.{name}")
        
    @abstractmethod
    def execute(self) -> bool:
        """Execute the workflow"""
        pass


class BaseTransformation(BaseInformaticaObject):
    """Base class for all transformation objects"""
    
    def __init__(self, name: str):
        super().__init__(name)
        self.input_ports = []
        self.output_ports = []
        
    @abstractmethod
    def transform(self, input_df: DataFrame) -> DataFrame:
        """Apply transformation to input DataFrame"""
        pass


class DataSourceManager:
    """Manages data source connections and operations"""
    
    def __init__(self, spark: SparkSession, connections_config: Dict[str, Any]):
        self.spark = spark
        self.connections = connections_config
        self.logger = logging.getLogger("DataSourceManager")
        
    def read_source(self, source_name: str, source_type: str, **kwargs) -> DataFrame:
        """Read data from source"""
        self.logger.info(f"Reading from source: {source_name} (type: {source_type})")
        
        if source_type.upper() == "HDFS":
            return self._read_hdfs_source(source_name, **kwargs)
        elif source_type.upper() == "HIVE":
            return self._read_hive_source(source_name, **kwargs)
        elif source_type.upper() in ["DB2", "ORACLE"]:
            return self._read_jdbc_source(source_name, source_type, **kwargs)
        else:
            # Generate mock data for unknown sources
            return self._generate_mock_data(source_name)
            
    def write_target(self, df: DataFrame, target_name: str, target_type: str, **kwargs):
        """Write data to target"""
        self.logger.info(f"Writing to target: {target_name} (type: {target_type})")
        
        mode = kwargs.get('mode', 'overwrite')
        
        if target_type.upper() == "HIVE":
            self._write_hive_target(df, target_name, mode)
        elif target_type.upper() == "HDFS":
            self._write_hdfs_target(df, target_name, mode)
        else:
            self.logger.warning(f"Unknown target type: {target_type}, writing as parquet")
            df.write.mode(mode).parquet(f"output/{target_name}")
    
    def _read_hive_source(self, source_name: str, **kwargs) -> DataFrame:
        """Read from Hive source"""
        return self.spark.sql(f"SELECT * FROM {source_name}")
    
    def _read_hdfs_source(self, source_name: str, **kwargs) -> DataFrame:
        """Read from HDFS source"""
        path = kwargs.get('path', f"input/{source_name}")
        return self.spark.read.parquet(path)
    
    def _read_jdbc_source(self, source_name: str, source_type: str, **kwargs) -> DataFrame:
        """Read from JDBC source"""
        # This would use real JDBC connection in production
        return self._generate_mock_data(source_name)
    
    def _write_hive_target(self, df: DataFrame, target_name: str, mode: str):
        """Write to Hive target"""
        df.write.mode(mode).saveAsTable(target_name)
    
    def _write_hdfs_target(self, df: DataFrame, target_name: str, mode: str):
        """Write to HDFS target"""
        df.write.mode(mode).parquet(f"output/{target_name}")
    
    def _generate_mock_data(self, source_name: str) -> DataFrame:
        """Generate mock data for testing"""
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True)
        ])
        data = [(1, f"{source_name}_row1", "value1"), (2, f"{source_name}_row2", "value2")]
        return self.spark.createDataFrame(data, schema)
        
    def get_connection(self, name: str) -> Optional[Connection]:
        return self.connections.get(name)