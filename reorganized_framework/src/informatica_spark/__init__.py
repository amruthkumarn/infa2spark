"""
Informatica Spark Framework

A comprehensive framework for converting Informatica BDM projects 
into production-ready PySpark applications.
"""

__version__ = "2.0.0"
__author__ = "Informatica Spark Team"

# Main exports
from .core.generators.spark_generator import SparkCodeGenerator
from .core.parsers.xml_parser import InformaticaXMLParser  
from .core.models.base_classes import Project, Connection, Task

__all__ = [
    "SparkCodeGenerator",
    "InformaticaXMLParser", 
    "Project",
    "Connection", 
    "Task",
]