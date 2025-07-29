"""Core framework functionality."""

from .generators.spark_generator import SparkCodeGenerator
from .parsers.xml_parser import InformaticaXMLParser
from .models.base_classes import Project, Connection, Task

__all__ = ["SparkCodeGenerator", "InformaticaXMLParser", "Project", "Connection", "Task"]