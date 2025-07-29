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
        
    def get_connection(self, name: str) -> Optional[Connection]:
        return self.connections.get(name)