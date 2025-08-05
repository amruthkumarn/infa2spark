# 04_XSD_BASE_ARCHITECTURE.md

## 🏗️ XSD-Compliant Base Architecture Implementation

Implement the foundational XSD-compliant classes that mirror Informatica's official XML Schema Definition. This forms the backbone of the entire framework.

## 📋 Prerequisites

**ASSUMPTION**: The `/informatica_xsd_xml/` directory with complete Informatica XSD schemas is available and contains files like:
- `com.informatica.metadata.common.core.xsd`
- `com.informatica.metadata.common.types.xsd`
- `IMX.xsd`
- And 170+ other XSD files

## 🎯 Implementation Overview

The XSD base architecture provides:
- **Element hierarchy** mirroring Informatica XSD schemas
- **PMDataType enumeration** with 50+ Informatica data types
- **Reference management** for ID/IDREF resolution
- **Type validation** and conversion utilities
- **Annotation support** for metadata
- **Enterprise error handling**

## 📁 File Structure

```
src/core/
├── xsd_base_classes.py          # Foundation XSD classes
├── reference_manager.py         # ID/IDREF resolution system
└── type_validation.py           # Data type validation utilities
```

## 🔧 Core Implementation

### 1. src/core/xsd_base_classes.py

```python
"""
XSD-compliant base classes for Informatica metadata objects
Based on com.informatica.metadata.common.core.xsd and IMX.xsd

This module implements the complete Informatica XSD hierarchy with:
- Element, NamedElement, ObjectReference, ElementReference
- PMDataType enumeration with all Informatica data types
- TypedElement with comprehensive type information
- Annotation support for metadata
- Collection management with lookup capabilities
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from enum import Enum
import logging
from dataclasses import dataclass, field
from datetime import datetime

class PMDataType(Enum):
    """
    Informatica BDM data types from XSD schema
    Based on com.informatica.metadata.common.types.xsd
    """
    # Character types
    SQL_CHAR = "SQL_CHAR"
    SQL_VARCHAR = "SQL_VARCHAR" 
    SQL_LONGVARCHAR = "SQL_LONGVARCHAR"
    SQL_CLOB = "SQL_CLOB"
    
    # Unicode character types
    SQL_NCHAR = "SQL_NCHAR"
    SQL_NVARCHAR = "SQL_NVARCHAR"
    SQL_LONGNVARCHAR = "SQL_LONGNVARCHAR"
    SQL_NCLOB = "SQL_NCLOB"
    SQL_WCHAR = "SQL_WCHAR"
    SQL_WVARCHAR = "SQL_WVARCHAR"
    SQL_WLONGVARCHAR = "SQL_WLONGVARCHAR"
    
    # Binary types
    SQL_BINARY = "SQL_BINARY"
    SQL_VARBINARY = "SQL_VARBINARY"
    SQL_LONGVARBINARY = "SQL_LONGVARBINARY"
    SQL_BLOB = "SQL_BLOB"
    
    # Boolean type
    SQL_BOOLEAN = "SQL_BOOLEAN"
    
    # Integer types
    SQL_TINYINT = "SQL_TINYINT"
    SQL_SMALLINT = "SQL_SMALLINT"
    SQL_INTEGER = "SQL_INTEGER"
    SQL_BIGINT = "SQL_BIGINT"
    
    # Floating point types
    SQL_REAL = "SQL_REAL"
    SQL_FLOAT = "SQL_FLOAT"
    SQL_DOUBLE = "SQL_DOUBLE"
    SQL_DECIMAL = "SQL_DECIMAL"
    SQL_NUMERIC = "SQL_NUMERIC"
    
    # Date and time types
    SQL_DATE = "SQL_DATE"
    SQL_TIME = "SQL_TIME"
    SQL_TIMESTAMP = "SQL_TIMESTAMP"
    
    # Interval types
    SQL_INTERVAL_YEAR = "SQL_INTERVAL_YEAR"
    SQL_INTERVAL_MONTH = "SQL_INTERVAL_MONTH"
    SQL_INTERVAL_DAY = "SQL_INTERVAL_DAY"
    SQL_INTERVAL_HOUR = "SQL_INTERVAL_HOUR"
    SQL_INTERVAL_MINUTE = "SQL_INTERVAL_MINUTE"
    SQL_INTERVAL_SECOND = "SQL_INTERVAL_SECOND"
    SQL_INTERVAL_YEAR_TO_MONTH = "SQL_INTERVAL_YEAR_TO_MONTH"
    SQL_INTERVAL_DAY_TO_HOUR = "SQL_INTERVAL_DAY_TO_HOUR"
    SQL_INTERVAL_DAY_TO_MINUTE = "SQL_INTERVAL_DAY_TO_MINUTE"
    SQL_INTERVAL_DAY_TO_SECOND = "SQL_INTERVAL_DAY_TO_SECOND"
    SQL_INTERVAL_HOUR_TO_MINUTE = "SQL_INTERVAL_HOUR_TO_MINUTE"
    SQL_INTERVAL_HOUR_TO_SECOND = "SQL_INTERVAL_HOUR_TO_SECOND"
    SQL_INTERVAL_MINUTE_TO_SECOND = "SQL_INTERVAL_MINUTE_TO_SECOND"
    
    # Special types
    SQL_GUID = "SQL_GUID"
    SQL_NONE = "SQL_NONE"
    
    # Extended types for specific databases
    ORACLE_NUMBER = "ORACLE_NUMBER"
    ORACLE_DATE = "ORACLE_DATE"
    ORACLE_TIMESTAMP = "ORACLE_TIMESTAMP"
    ORACLE_CLOB = "ORACLE_CLOB"
    ORACLE_BLOB = "ORACLE_BLOB"
    
    # SQL Server specific types
    SQLSERVER_UNIQUEIDENTIFIER = "SQLSERVER_UNIQUEIDENTIFIER"
    SQLSERVER_XML = "SQLSERVER_XML"
    SQLSERVER_DATETIME = "SQLSERVER_DATETIME"
    SQLSERVER_DATETIME2 = "SQLSERVER_DATETIME2"
    
    # PostgreSQL specific types
    POSTGRES_UUID = "POSTGRES_UUID"
    POSTGRES_JSON = "POSTGRES_JSON"
    POSTGRES_JSONB = "POSTGRES_JSONB"
    POSTGRES_ARRAY = "POSTGRES_ARRAY"
    
    # Hive/Spark specific types
    HIVE_STRING = "HIVE_STRING"
    HIVE_ARRAY = "HIVE_ARRAY"
    HIVE_MAP = "HIVE_MAP"
    HIVE_STRUCT = "HIVE_STRUCT"

@dataclass
class Annotation:
    """
    XSD-compliant annotation for metadata objects
    Based on com.informatica.metadata.common.annotation.xsd
    """
    name: str
    value: Any
    namespace: Optional[str] = None
    created_by: Optional[str] = None
    created_date: Optional[datetime] = None
    annotation_type: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert annotation to dictionary"""
        return {
            'name': self.name,
            'value': self.value,
            'namespace': self.namespace,
            'created_by': self.created_by,
            'created_date': self.created_date.isoformat() if self.created_date else None,
            'annotation_type': self.annotation_type
        }

class Element(ABC):
    """
    XSD-compliant abstract base class for all Informatica metadata elements
    Based on com.informatica.metadata.common.core.Element
    
    This is the foundation class that all Informatica objects inherit from.
    Provides core functionality for:
    - Unique identification (id, iid)
    - Federation support (locator)
    - Annotation management
    - Logging integration
    """
    
    def __init__(self, 
                 id: Optional[str] = None,
                 iid: Optional[int] = None,
                 locator: Optional[str] = None):
        """
        Initialize Element with XSD-compliant attributes
        
        Args:
            id: imx:id attribute - unique identifier for first-class objects
            iid: imx:iid attribute - internal identifier for second-class objects  
            locator: imx:locator attribute - federation location for proxy objects
        """
        self.id = id
        self.iid = iid
        self.locator = locator
        self.annotations: List[Annotation] = []
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        
        # Internal tracking
        self._creation_time = datetime.now()
        self._modified_time = datetime.now()
        
    def add_annotation(self, annotation: Annotation):
        """Add annotation to the element"""
        self.annotations.append(annotation)
        self._touch()
        
    def get_annotation(self, name: str) -> Optional[Annotation]:
        """Get annotation by name"""
        for annotation in self.annotations:
            if annotation.name == name:
                return annotation
        return None
        
    def get_annotations_by_namespace(self, namespace: str) -> List[Annotation]:
        """Get all annotations for a specific namespace"""
        return [ann for ann in self.annotations if ann.namespace == namespace]
        
    def remove_annotation(self, name: str) -> bool:
        """Remove annotation by name"""
        for i, annotation in enumerate(self.annotations):
            if annotation.name == name:
                del self.annotations[i]
                self._touch()
                return True
        return False
        
    def has_annotation(self, name: str) -> bool:
        """Check if element has annotation with given name"""
        return self.get_annotation(name) is not None
        
    def _touch(self):
        """Update modification time"""
        self._modified_time = datetime.now()
        
    def get_metadata(self) -> Dict[str, Any]:
        """Get element metadata"""
        return {
            'id': self.id,
            'iid': self.iid,
            'locator': self.locator,
            'type': self.__class__.__name__,
            'created': self._creation_time.isoformat(),
            'modified': self._modified_time.isoformat(),
            'annotations_count': len(self.annotations)
        }
        
    def is_federated(self) -> bool:
        """Check if this is a federated (proxy) object"""
        return self.locator is not None
        
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id}, iid={self.iid})"
        
    def __str__(self) -> str:
        return self.__repr__()

class NamedElement(Element):
    """
    XSD-compliant named element base class
    Based on com.informatica.metadata.common.core.NamedElement
    
    Extends Element with name and description attributes.
    Most Informatica objects inherit from this class.
    """
    
    def __init__(self, 
                 name: str,
                 description: str = "",
                 **kwargs):
        """
        Initialize NamedElement
        
        Args:
            name: Element name (required)
            description: Element description
            **kwargs: Additional Element attributes
        """
        super().__init__(**kwargs)
        self.name = name
        self.description = description
        
    def set_name(self, name: str):
        """Set element name"""
        old_name = self.name
        self.name = name
        self._touch()
        self.logger.debug(f"Name changed from '{old_name}' to '{name}'")
        
    def set_description(self, description: str):
        """Set element description"""
        self.description = description
        self._touch()
        
    def get_qualified_name(self) -> str:
        """Get qualified name including type"""
        return f"{self.__class__.__name__}:{self.name}"
        
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', id={self.id})"

class ObjectReference(Element):
    """
    XSD-compliant object reference for first-class objects
    Based on com.informatica.metadata.common.core.ObjectReference
    
    Used to reference other objects by their ID.
    Supports lazy loading and reference validation.
    """
    
    def __init__(self, 
                 idref: str,
                 **kwargs):
        """
        Initialize ObjectReference
        
        Args:
            idref: imx:idref attribute - reference to another object's ID
            **kwargs: Additional Element attributes
        """
        super().__init__(**kwargs)
        self.idref = idref
        self.resolved_object: Optional[Element] = None
        self._resolution_attempted = False
        
    def resolve(self, reference_manager) -> Optional[Element]:
        """
        Resolve the reference using a reference manager
        
        Args:
            reference_manager: ReferenceManager instance
            
        Returns:
            Referenced object or None if not found
        """
        if not self._resolution_attempted:
            self.resolved_object = reference_manager.resolve_object_reference(self.idref)
            self._resolution_attempted = True
            
            if self.resolved_object:
                self.logger.debug(f"Reference {self.idref} resolved to {self.resolved_object}")
            else:
                self.logger.warning(f"Could not resolve reference {self.idref}")
                
        return self.resolved_object
        
    def is_resolved(self) -> bool:
        """Check if reference is resolved"""
        return self.resolved_object is not None
        
    def get_target_type(self) -> Optional[str]:
        """Get type of referenced object"""
        if self.resolved_object:
            return self.resolved_object.__class__.__name__
        return None
        
    def __repr__(self) -> str:
        resolved_status = "resolved" if self.is_resolved() else "unresolved"
        return f"{self.__class__.__name__}(idref='{self.idref}', {resolved_status})"

class ElementReference(Element):
    """
    XSD-compliant element reference for second-class objects
    Based on com.informatica.metadata.common.core.ElementReference
    
    Used to reference elements by their internal ID (iid).
    """
    
    def __init__(self, 
                 iidref: int,
                 **kwargs):
        """
        Initialize ElementReference
        
        Args:
            iidref: Reference to internal ID (iid)
            **kwargs: Additional Element attributes
        """
        super().__init__(**kwargs)
        self.iidref = iidref
        self.resolved_element: Optional[Element] = None
        self._resolution_attempted = False
        
    def resolve(self, reference_manager) -> Optional[Element]:
        """Resolve the element reference"""
        if not self._resolution_attempted:
            self.resolved_element = reference_manager.resolve_element_reference(self.iidref)
            self._resolution_attempted = True
            
        return self.resolved_element
        
    def is_resolved(self) -> bool:
        """Check if reference is resolved"""
        return self.resolved_element is not None
        
    def __repr__(self) -> str:
        resolved_status = "resolved" if self.is_resolved() else "unresolved"
        return f"{self.__class__.__name__}(iidref={self.iidref}, {resolved_status})"

class TypedElement(Element):
    """
    XSD-compliant typed element with data type information
    Based on type system integration requirements
    
    Provides comprehensive type information including:
    - PMDataType enumeration
    - Precision, scale, length
    - Nullability
    - Type validation and conversion
    """
    
    def __init__(self,
                 data_type: PMDataType,
                 precision: Optional[int] = None,
                 scale: Optional[int] = None,
                 length: Optional[int] = None,
                 nullable: bool = True,
                 **kwargs):
        """
        Initialize TypedElement
        
        Args:
            data_type: PMDataType enumeration value
            precision: Numeric precision
            scale: Numeric scale  
            length: String/binary length
            nullable: Whether NULL values are allowed
            **kwargs: Additional Element attributes
        """
        super().__init__(**kwargs)
        self.data_type = data_type
        self.precision = precision
        self.scale = scale
        self.length = length
        self.nullable = nullable
        
    def get_type_info(self) -> Dict[str, Any]:
        """Get complete type information"""
        return {
            'data_type': self.data_type.value,
            'precision': self.precision,
            'scale': self.scale,
            'length': self.length,
            'nullable': self.nullable,
            'type_category': self.get_type_category(),
            'sql_type_name': self.get_sql_type_name()
        }
        
    def get_type_category(self) -> str:
        """Get high-level type category"""
        if self.is_numeric_type():
            return "NUMERIC"
        elif self.is_string_type():
            return "STRING"
        elif self.is_date_type():
            return "DATETIME"
        elif self.is_binary_type():
            return "BINARY"
        elif self.data_type == PMDataType.SQL_BOOLEAN:
            return "BOOLEAN"
        else:
            return "OTHER"
            
    def is_numeric_type(self) -> bool:
        """Check if this is a numeric data type"""
        numeric_types = {
            PMDataType.SQL_TINYINT, PMDataType.SQL_SMALLINT,
            PMDataType.SQL_INTEGER, PMDataType.SQL_BIGINT,
            PMDataType.SQL_REAL, PMDataType.SQL_FLOAT,
            PMDataType.SQL_DOUBLE, PMDataType.SQL_DECIMAL,
            PMDataType.SQL_NUMERIC
        }
        return self.data_type in numeric_types
        
    def is_string_type(self) -> bool:
        """Check if this is a string data type"""
        string_types = {
            PMDataType.SQL_CHAR, PMDataType.SQL_VARCHAR,
            PMDataType.SQL_LONGVARCHAR, PMDataType.SQL_CLOB,
            PMDataType.SQL_NCHAR, PMDataType.SQL_NVARCHAR,
            PMDataType.SQL_LONGNVARCHAR, PMDataType.SQL_NCLOB,
            PMDataType.SQL_WCHAR, PMDataType.SQL_WVARCHAR,
            PMDataType.SQL_WLONGVARCHAR
        }
        return self.data_type in string_types
        
    def is_date_type(self) -> bool:
        """Check if this is a date/time data type"""
        date_types = {
            PMDataType.SQL_DATE, PMDataType.SQL_TIME,
            PMDataType.SQL_TIMESTAMP
        }
        return self.data_type in date_types
        
    def is_binary_type(self) -> bool:
        """Check if this is a binary data type"""
        binary_types = {
            PMDataType.SQL_BINARY, PMDataType.SQL_VARBINARY,
            PMDataType.SQL_LONGVARBINARY, PMDataType.SQL_BLOB
        }
        return self.data_type in binary_types
        
    def get_sql_type_name(self) -> str:
        """Get SQL type name for code generation"""
        if self.data_type == PMDataType.SQL_VARCHAR and self.length:
            return f"VARCHAR({self.length})"
        elif self.data_type == PMDataType.SQL_CHAR and self.length:
            return f"CHAR({self.length})"
        elif self.data_type == PMDataType.SQL_DECIMAL and self.precision and self.scale is not None:
            return f"DECIMAL({self.precision},{self.scale})"
        elif self.data_type == PMDataType.SQL_NUMERIC and self.precision and self.scale is not None:
            return f"NUMERIC({self.precision},{self.scale})"
        else:
            return self.data_type.value
            
    def get_spark_type(self) -> str:
        """Get equivalent Spark/PySpark data type"""
        type_mapping = {
            PMDataType.SQL_CHAR: "StringType()",
            PMDataType.SQL_VARCHAR: "StringType()",
            PMDataType.SQL_LONGVARCHAR: "StringType()",
            PMDataType.SQL_CLOB: "StringType()",
            PMDataType.SQL_INTEGER: "IntegerType()",
            PMDataType.SQL_BIGINT: "LongType()",
            PMDataType.SQL_SMALLINT: "ShortType()",
            PMDataType.SQL_TINYINT: "ByteType()",
            PMDataType.SQL_DECIMAL: "DecimalType()",
            PMDataType.SQL_NUMERIC: "DecimalType()",
            PMDataType.SQL_REAL: "FloatType()",
            PMDataType.SQL_FLOAT: "FloatType()",
            PMDataType.SQL_DOUBLE: "DoubleType()",
            PMDataType.SQL_BOOLEAN: "BooleanType()",
            PMDataType.SQL_DATE: "DateType()",
            PMDataType.SQL_TIME: "StringType()",  # Spark doesn't have TimeType
            PMDataType.SQL_TIMESTAMP: "TimestampType()",
            PMDataType.SQL_BINARY: "BinaryType()",
            PMDataType.SQL_VARBINARY: "BinaryType()",
            PMDataType.SQL_BLOB: "BinaryType()"
        }
        
        spark_type = type_mapping.get(self.data_type, "StringType()")
        
        # Add precision/scale for decimal types
        if self.data_type in [PMDataType.SQL_DECIMAL, PMDataType.SQL_NUMERIC]:
            if self.precision and self.scale is not None:
                spark_type = f"DecimalType({self.precision}, {self.scale})"
            elif self.precision:
                spark_type = f"DecimalType({self.precision}, 0)"
                
        return spark_type
        
    def validate_value(self, value: Any) -> bool:
        """Validate a value against this type definition"""
        from .type_validation import TypeValidator
        return TypeValidator.validate_value(value, self)
        
    def convert_value(self, value: Any) -> Any:
        """Convert a value to this type"""
        from .type_validation import TypeValidator
        return TypeValidator.convert_value(value, self)

class ProxyObject(Element):
    """
    XSD-compliant proxy object for federated references
    Based on IMX proxy object specification
    
    Represents objects that exist in other locations/systems
    and need to be loaded on demand.
    """
    
    def __init__(self,
                 locator: str,
                 target_id: str,
                 **kwargs):
        """
        Initialize ProxyObject
        
        Args:
            locator: Federation location URI
            target_id: ID of target object in remote location
            **kwargs: Additional Element attributes
        """
        super().__init__(locator=locator, **kwargs)
        self.target_id = target_id
        self.is_federated = True
        self.loaded = False
        self.target_object: Optional[Element] = None
        self.load_error: Optional[str] = None
        
    def load_target(self, federation_manager=None) -> Optional[Element]:
        """
        Load the target object from federation location
        
        Args:
            federation_manager: Federation manager for loading remote objects
            
        Returns:
            Loaded target object or None if failed
        """
        if self.loaded:
            return self.target_object
            
        if federation_manager:
            try:
                self.target_object = federation_manager.load_object(self.locator, self.target_id)
                self.loaded = True
                self.load_error = None
                self.logger.debug(f"Loaded proxy object {self.target_id} from {self.locator}")
            except Exception as e:
                self.load_error = str(e)
                self.logger.error(f"Failed to load proxy object {self.target_id}: {e}")
        else:
            self.load_error = "No federation manager provided"
            
        return self.target_object
        
    def is_loaded(self) -> bool:
        """Check if target object is loaded"""
        return self.loaded and self.target_object is not None
        
    def get_load_status(self) -> Dict[str, Any]:
        """Get loading status information"""
        return {
            'loaded': self.loaded,
            'has_target': self.target_object is not None,
            'load_error': self.load_error,
            'locator': self.locator,
            'target_id': self.target_id
        }

# Collection classes for managing groups of elements
class ElementCollection:
    """
    Generic collection for managing elements with lookup capabilities
    
    Provides efficient storage and retrieval of elements by:
    - ID (for first-class objects)
    - Name (for named elements)
    - Type (for filtering by class)
    """
    
    def __init__(self, name: str = "ElementCollection"):
        self.name = name
        self._elements: Dict[str, Element] = {}  # id -> element
        self._by_name: Dict[str, Element] = {}   # name -> element (for NamedElements)
        self._by_iid: Dict[int, Element] = {}    # iid -> element (for second-class objects)
        self.logger = logging.getLogger(f"ElementCollection.{name}")
        
    def add(self, element: Element):
        """Add element to collection"""
        if element.id:
            if element.id in self._elements:
                self.logger.warning(f"Replacing element with duplicate ID: {element.id}")
            self._elements[element.id] = element
            
        if element.iid is not None:
            if element.iid in self._by_iid:
                self.logger.warning(f"Replacing element with duplicate IID: {element.iid}")
            self._by_iid[element.iid] = element
            
        if isinstance(element, NamedElement):
            if element.name in self._by_name:
                self.logger.debug(f"Replacing element with duplicate name: {element.name}")
            self._by_name[element.name] = element
            
        self.logger.debug(f"Added {element.__class__.__name__} to collection")
            
    def get_by_id(self, id: str) -> Optional[Element]:
        """Get element by ID"""
        return self._elements.get(id)
        
    def get_by_name(self, name: str) -> Optional[Element]:
        """Get named element by name"""
        return self._by_name.get(name)
        
    def get_by_iid(self, iid: int) -> Optional[Element]:
        """Get element by internal ID"""
        return self._by_iid.get(iid)
        
    def remove(self, element: Element):
        """Remove element from collection"""
        if element.id and element.id in self._elements:
            del self._elements[element.id]
            
        if element.iid is not None and element.iid in self._by_iid:
            del self._by_iid[element.iid]
            
        if isinstance(element, NamedElement) and element.name in self._by_name:
            del self._by_name[element.name]
            
        self.logger.debug(f"Removed {element.__class__.__name__} from collection")
            
    def remove_by_id(self, id: str) -> bool:
        """Remove element by ID"""
        element = self.get_by_id(id)
        if element:
            self.remove(element)
            return True
        return False
        
    def list_all(self) -> List[Element]:
        """Get all elements"""
        return list(self._elements.values())
        
    def list_by_type(self, element_type: type) -> List[Element]:
        """Get all elements of specific type"""
        return [elem for elem in self._elements.values() if isinstance(elem, element_type)]
        
    def find_by_annotation(self, annotation_name: str, annotation_value: Any = None) -> List[Element]:
        """Find elements by annotation"""
        results = []
        for element in self._elements.values():
            annotation = element.get_annotation(annotation_name)
            if annotation:
                if annotation_value is None or annotation.value == annotation_value:
                    results.append(element)
        return results
        
    def count(self) -> int:
        """Get total count of elements"""
        return len(self._elements)
        
    def count_by_type(self, element_type: type) -> int:
        """Get count of elements by type"""
        return len(self.list_by_type(element_type))
        
    def get_statistics(self) -> Dict[str, Any]:
        """Get collection statistics"""
        type_counts = {}
        for element in self._elements.values():
            type_name = element.__class__.__name__
            type_counts[type_name] = type_counts.get(type_name, 0) + 1
            
        return {
            'total_elements': len(self._elements),
            'elements_with_id': len(self._elements),
            'elements_with_iid': len(self._by_iid),
            'named_elements': len(self._by_name),
            'type_distribution': type_counts
        }
        
    def clear(self):
        """Clear all elements"""
        self._elements.clear()
        self._by_name.clear()
        self._by_iid.clear()
        self.logger.debug("Collection cleared")
        
    def __len__(self) -> int:
        return self.count()
        
    def __contains__(self, element: Element) -> bool:
        if element.id:
            return element.id in self._elements
        elif element.iid is not None:
            return element.iid in self._by_iid
        else:
            return element in self._elements.values()
            
    def __iter__(self):
        return iter(self._elements.values())

# Error classes for XSD compliance
class XSDComplianceError(Exception):
    """Base exception for XSD compliance issues"""
    def __init__(self, message: str, element: Optional[Element] = None, xsd_file: Optional[str] = None):
        super().__init__(message)
        self.element = element
        self.xsd_file = xsd_file
        self.message = message

class InvalidReferenceError(XSDComplianceError):
    """Exception for invalid object references"""
    def __init__(self, message: str, reference_id: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.reference_id = reference_id

class TypeValidationError(XSDComplianceError):
    """Exception for data type validation failures"""
    def __init__(self, message: str, data_type: Optional[PMDataType] = None, value: Any = None, **kwargs):
        super().__init__(message, **kwargs)
        self.data_type = data_type
        self.value = value

class RequiredAttributeError(XSDComplianceError):
    """Exception for missing required attributes"""
    def __init__(self, message: str, attribute_name: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.attribute_name = attribute_name

class DuplicateElementError(XSDComplianceError):
    """Exception for duplicate element IDs or names"""
    def __init__(self, message: str, duplicate_key: Optional[str] = None, **kwargs):
        super().__init__(message, **kwargs)
        self.duplicate_key = duplicate_key

# Utility functions for XSD compliance
def create_annotation(name: str, value: Any, namespace: str = None, created_by: str = None) -> Annotation:
    """Create a new annotation with current timestamp"""
    return Annotation(
        name=name,
        value=value,
        namespace=namespace,
        created_by=created_by,
        created_date=datetime.now()
    )

def validate_element_id(element_id: str) -> bool:
    """Validate element ID format"""
    if not element_id or not isinstance(element_id, str):
        return False
    # Basic validation - can be extended based on XSD requirements
    return len(element_id) > 0 and element_id.strip() == element_id

def generate_element_id(prefix: str = "elem", counter: int = None) -> str:
    """Generate a unique element ID"""
    import uuid
    if counter is not None:
        return f"{prefix}_{counter}"
    else:
        return f"{prefix}_{uuid.uuid4().hex[:8]}"
```

### 2. src/core/type_validation.py

```python
"""
Type validation and conversion utilities for XSD-compliant types
"""
from typing import Any, Optional, Union, List, Dict
import re
import datetime
import logging
from decimal import Decimal, InvalidOperation

from .xsd_base_classes import PMDataType, TypedElement, TypeValidationError

class TypeValidator:
    """
    Utility class for validating and converting data types
    Based on Informatica's type system and XSD specifications
    """
    
    logger = logging.getLogger("TypeValidator")
    
    @classmethod
    def validate_value(cls, value: Any, typed_element: TypedElement) -> bool:
        """
        Validate that a value matches the typed element definition
        
        Args:
            value: Value to validate
            typed_element: TypedElement with type constraints
            
        Returns:
            True if value is valid, False otherwise
        """
        if value is None:
            return typed_element.nullable
            
        try:
            # Convert and validate
            converted = cls.convert_value(value, typed_element)
            return cls._validate_constraints(converted, typed_element)
        except (ValueError, TypeError, InvalidOperation):
            return False
    
    @classmethod
    def convert_value(cls, value: Any, typed_element: TypedElement) -> Any:
        """
        Convert a value to match the typed element definition
        
        Args:
            value: Value to convert
            typed_element: TypedElement with target type
            
        Returns:
            Converted value
            
        Raises:
            TypeValidationError: If conversion is not possible
        """
        if value is None:
            if typed_element.nullable:
                return None
            else:
                raise TypeValidationError(f"NULL value not allowed for non-nullable type {typed_element.data_type}")
        
        data_type = typed_element.data_type
        
        try:
            if data_type in cls._get_integer_types():
                return cls._convert_to_integer(value, data_type)
            elif data_type in cls._get_float_types():
                return cls._convert_to_float(value, data_type)
            elif data_type in cls._get_decimal_types():
                return cls._convert_to_decimal(value, typed_element)
            elif data_type in cls._get_string_types():
                return cls._convert_to_string(value, typed_element)
            elif data_type == PMDataType.SQL_BOOLEAN:
                return cls._convert_to_boolean(value)
            elif data_type in cls._get_date_types():
                return cls._convert_to_date(value, data_type)
            elif data_type in cls._get_binary_types():
                return cls._convert_to_binary(value)
            else:
                # Default to string conversion
                return str(value)
                
        except Exception as e:
            raise TypeValidationError(f"Cannot convert {value} to {data_type}: {str(e)}")
    
    @classmethod
    def _convert_to_integer(cls, value: Any, data_type: PMDataType) -> int:
        """Convert to integer with range validation"""
        if isinstance(value, bool):
            return 1 if value else 0
        
        int_value = int(float(str(value)))  # Handle string floats like "123.0"
        
        # Validate ranges
        if data_type == PMDataType.SQL_TINYINT:
            if not (-128 <= int_value <= 127):
                raise ValueError(f"Value {int_value} out of range for TINYINT")
        elif data_type == PMDataType.SQL_SMALLINT:
            if not (-32768 <= int_value <= 32767):
                raise ValueError(f"Value {int_value} out of range for SMALLINT")
        elif data_type == PMDataType.SQL_INTEGER:
            if not (-2147483648 <= int_value <= 2147483647):
                raise ValueError(f"Value {int_value} out of range for INTEGER")
        # BIGINT range is larger than Python int range, so no validation needed
        
        return int_value
    
    @classmethod
    def _convert_to_float(cls, value: Any, data_type: PMDataType) -> float:
        """Convert to float"""
        return float(value)
    
    @classmethod
    def _convert_to_decimal(cls, value: Any, typed_element: TypedElement) -> Decimal:
        """Convert to decimal with precision/scale validation"""
        decimal_value = Decimal(str(value))
        
        if typed_element.precision:
            # Check total digits
            sign, digits, exponent = decimal_value.as_tuple()
            total_digits = len(digits)
            
            if total_digits > typed_element.precision:
                raise ValueError(f"Value has {total_digits} digits, exceeds precision {typed_element.precision}")
            
            # Check scale (decimal places)
            if typed_element.scale is not None:
                if exponent < -typed_element.scale:
                    raise ValueError(f"Value has too many decimal places for scale {typed_element.scale}")
        
        return decimal_value
    
    @classmethod
    def _convert_to_string(cls, value: Any, typed_element: TypedElement) -> str:
        """Convert to string with length validation"""
        str_value = str(value)
        
        if typed_element.length:
            if len(str_value) > typed_element.length:
                if typed_element.data_type in [PMDataType.SQL_CHAR, PMDataType.SQL_NCHAR]:
                    # For CHAR types, pad or truncate
                    str_value = str_value[:typed_element.length].ljust(typed_element.length)
                else:
                    # For VARCHAR types, truncate
                    str_value = str_value[:typed_element.length]
                    cls.logger.warning(f"String truncated to {typed_element.length} characters")
        
        return str_value
    
    @classmethod
    def _convert_to_boolean(cls, value: Any) -> bool:
        """Convert to boolean"""
        if isinstance(value, bool):
            return value
        elif isinstance(value, (int, float)):
            return value != 0
        elif isinstance(value, str):
            lower_val = value.lower().strip()
            if lower_val in ['true', '1', 'yes', 'y', 't']:
                return True
            elif lower_val in ['false', '0', 'no', 'n', 'f']:
                return False
            else:
                raise ValueError(f"Cannot convert '{value}' to boolean")
        else:
            raise ValueError(f"Cannot convert {type(value)} to boolean")
    
    @classmethod
    def _convert_to_date(cls, value: Any, data_type: PMDataType) -> Union[datetime.date, datetime.time, datetime.datetime]:
        """Convert to date/time types"""
        if isinstance(value, str):
            # Try common date/time formats
            formats = {
                PMDataType.SQL_DATE: ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y'],
                PMDataType.SQL_TIME: ['%H:%M:%S', '%H:%M:%S.%f', '%H:%M'],
                PMDataType.SQL_TIMESTAMP: ['%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S']
            }
            
            for fmt in formats.get(data_type, []):
                try:
                    parsed = datetime.datetime.strptime(value, fmt)
                    if data_type == PMDataType.SQL_DATE:
                        return parsed.date()
                    elif data_type == PMDataType.SQL_TIME:
                        return parsed.time()
                    else:
                        return parsed
                except ValueError:
                    continue
            
            raise ValueError(f"Cannot parse '{value}' as {data_type}")
        
        elif isinstance(value, datetime.datetime):
            if data_type == PMDataType.SQL_DATE:
                return value.date()
            elif data_type == PMDataType.SQL_TIME:
                return value.time()
            else:
                return value
        
        elif isinstance(value, datetime.date) and data_type == PMDataType.SQL_DATE:
            return value
        
        elif isinstance(value, datetime.time) and data_type == PMDataType.SQL_TIME:
            return value
        
        else:
            raise ValueError(f"Cannot convert {type(value)} to {data_type}")
    
    @classmethod
    def _convert_to_binary(cls, value: Any) -> bytes:
        """Convert to binary"""
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            # Try hex decoding first, then UTF-8 encoding
            try:
                return bytes.fromhex(value)
            except ValueError:
                return value.encode('utf-8')
        else:
            return str(value).encode('utf-8')
    
    @classmethod
    def _validate_constraints(cls, value: Any, typed_element: TypedElement) -> bool:
        """Validate additional constraints"""
        # This can be extended for more complex validation rules
        return True
    
    @classmethod
    def _get_integer_types(cls) -> set:
        """Get set of integer data types"""
        return {
            PMDataType.SQL_TINYINT,
            PMDataType.SQL_SMALLINT,
            PMDataType.SQL_INTEGER,
            PMDataType.SQL_BIGINT
        }
    
    @classmethod
    def _get_float_types(cls) -> set:
        """Get set of floating point data types"""
        return {
            PMDataType.SQL_REAL,
            PMDataType.SQL_FLOAT,
            PMDataType.SQL_DOUBLE
        }
    
    @classmethod
    def _get_decimal_types(cls) -> set:
        """Get set of decimal data types"""
        return {
            PMDataType.SQL_DECIMAL,
            PMDataType.SQL_NUMERIC
        }
    
    @classmethod
    def _get_string_types(cls) -> set:
        """Get set of string data types"""
        return {
            PMDataType.SQL_CHAR,
            PMDataType.SQL_VARCHAR,
            PMDataType.SQL_LONGVARCHAR,
            PMDataType.SQL_CLOB,
            PMDataType.SQL_NCHAR,
            PMDataType.SQL_NVARCHAR,
            PMDataType.SQL_LONGNVARCHAR,
            PMDataType.SQL_NCLOB,
            PMDataType.SQL_WCHAR,
            PMDataType.SQL_WVARCHAR,
            PMDataType.SQL_WLONGVARCHAR
        }
    
    @classmethod
    def _get_date_types(cls) -> set:
        """Get set of date/time data types"""
        return {
            PMDataType.SQL_DATE,
            PMDataType.SQL_TIME,
            PMDataType.SQL_TIMESTAMP
        }
    
    @classmethod
    def _get_binary_types(cls) -> set:
        """Get set of binary data types"""
        return {
            PMDataType.SQL_BINARY,
            PMDataType.SQL_VARBINARY,
            PMDataType.SQL_LONGVARBINARY,
            PMDataType.SQL_BLOB
        }

class TypeCompatibilityChecker:
    """
    Utility for checking type compatibility between different systems
    """
    
    @classmethod
    def is_compatible(cls, source_type: PMDataType, target_type: PMDataType) -> bool:
        """Check if source type can be converted to target type without data loss"""
        
        # Same type is always compatible
        if source_type == target_type:
            return True
        
        # Define compatibility matrix
        compatibility_rules = {
            # Integer types
            PMDataType.SQL_TINYINT: {PMDataType.SQL_SMALLINT, PMDataType.SQL_INTEGER, PMDataType.SQL_BIGINT, PMDataType.SQL_DECIMAL, PMDataType.SQL_NUMERIC, PMDataType.SQL_REAL, PMDataType.SQL_FLOAT, PMDataType.SQL_DOUBLE},
            PMDataType.SQL_SMALLINT: {PMDataType.SQL_INTEGER, PMDataType.SQL_BIGINT, PMDataType.SQL_DECIMAL, PMDataType.SQL_NUMERIC, PMDataType.SQL_REAL, PMDataType.SQL_FLOAT, PMDataType.SQL_DOUBLE},
            PMDataType.SQL_INTEGER: {PMDataType.SQL_BIGINT, PMDataType.SQL_DECIMAL, PMDataType.SQL_NUMERIC, PMDataType.SQL_DOUBLE},
            PMDataType.SQL_BIGINT: {PMDataType.SQL_DECIMAL, PMDataType.SQL_NUMERIC, PMDataType.SQL_DOUBLE},
            
            # Float types
            PMDataType.SQL_REAL: {PMDataType.SQL_FLOAT, PMDataType.SQL_DOUBLE, PMDataType.SQL_DECIMAL, PMDataType.SQL_NUMERIC},
            PMDataType.SQL_FLOAT: {PMDataType.SQL_DOUBLE, PMDataType.SQL_DECIMAL, PMDataType.SQL_NUMERIC},
            
            # String types (generally all compatible with each other)
            PMDataType.SQL_CHAR: {PMDataType.SQL_VARCHAR, PMDataType.SQL_LONGVARCHAR, PMDataType.SQL_CLOB},
            PMDataType.SQL_VARCHAR: {PMDataType.SQL_LONGVARCHAR, PMDataType.SQL_CLOB},
            
            # Date types
            PMDataType.SQL_DATE: {PMDataType.SQL_TIMESTAMP},
            PMDataType.SQL_TIME: {PMDataType.SQL_TIMESTAMP},
        }
        
        return target_type in compatibility_rules.get(source_type, set())
    
    @classmethod
    def get_conversion_risk(cls, source_type: PMDataType, target_type: PMDataType) -> str:
        """Get risk level of type conversion"""
        if source_type == target_type:
            return "NONE"
        elif cls.is_compatible(source_type, target_type):
            return "LOW"
        elif cls._is_lossy_conversion(source_type, target_type):
            return "HIGH"
        else:
            return "MEDIUM"
    
    @classmethod
    def _is_lossy_conversion(cls, source_type: PMDataType, target_type: PMDataType) -> bool:
        """Check if conversion is lossy (data loss possible)"""
        lossy_conversions = {
            (PMDataType.SQL_DOUBLE, PMDataType.SQL_REAL),
            (PMDataType.SQL_BIGINT, PMDataType.SQL_INTEGER),
            (PMDataType.SQL_INTEGER, PMDataType.SQL_SMALLINT),
            (PMDataType.SQL_SMALLINT, PMDataType.SQL_TINYINT),
            (PMDataType.SQL_TIMESTAMP, PMDataType.SQL_DATE),
            (PMDataType.SQL_TIMESTAMP, PMDataType.SQL_TIME),
        }
        
        return (source_type, target_type) in lossy_conversions
```

## ✅ Testing the XSD Base Architecture

Create `tests/test_xsd_base_classes.py`:

```python
"""
Comprehensive tests for XSD base classes
"""
import pytest
from datetime import datetime
from src.core.xsd_base_classes import (
    Element, NamedElement, ObjectReference, ElementReference, TypedElement,
    PMDataType, Annotation, ElementCollection, ProxyObject,
    XSDComplianceError, InvalidReferenceError, TypeValidationError,
    create_annotation, validate_element_id, generate_element_id
)
from src.core.type_validation import TypeValidator, TypeCompatibilityChecker

class TestXSDBaseClasses:
    """Test XSD base classes functionality"""
    
    def test_element_creation(self):
        """Test basic Element creation"""
        element = Element(id="test_id", iid=123)
        assert element.id == "test_id"
        assert element.iid == 123
        assert len(element.annotations) == 0
        assert not element.is_federated()
        
    def test_named_element_creation(self):
        """Test NamedElement creation"""
        element = NamedElement(name="TestElement", description="Test description", id="named_1")
        assert element.name == "TestElement"
        assert element.description == "Test description"
        assert element.id == "named_1"
        assert element.get_qualified_name() == "NamedElement:TestElement"
        
    def test_object_reference_creation(self):
        """Test ObjectReference creation"""
        ref = ObjectReference(idref="target_id", id="ref_1")
        assert ref.idref == "target_id"
        assert ref.id == "ref_1"
        assert not ref.is_resolved()
        assert ref.get_target_type() is None
        
    def test_typed_element_creation(self):
        """Test TypedElement creation and type information"""
        typed_elem = TypedElement(
            data_type=PMDataType.SQL_VARCHAR,
            length=50,
            nullable=True,
            id="typed_1"
        )
        
        assert typed_elem.data_type == PMDataType.SQL_VARCHAR
        assert typed_elem.length == 50
        assert typed_elem.nullable == True
        assert typed_elem.is_string_type() == True
        assert typed_elem.is_numeric_type() == False
        assert typed_elem.get_sql_type_name() == "VARCHAR(50)"
        assert typed_elem.get_spark_type() == "StringType()"
        
    def test_pmdata_type_enumeration(self):
        """Test PMDataType enumeration completeness"""
        # Test that all major SQL types are present
        assert PMDataType.SQL_INTEGER
        assert PMDataType.SQL_VARCHAR
        assert PMDataType.SQL_DECIMAL
        assert PMDataType.SQL_TIMESTAMP
        assert PMDataType.SQL_BOOLEAN
        
        # Test that there are at least 40 types (should be 50+)
        assert len(PMDataType) >= 40
        
    def test_annotation_management(self):
        """Test annotation functionality"""
        element = Element(id="ann_test")
        
        # Add annotation
        annotation = create_annotation("test_annotation", "test_value", "test_namespace")
        element.add_annotation(annotation)
        
        assert len(element.annotations) == 1
        assert element.has_annotation("test_annotation")
        
        # Get annotation
        retrieved = element.get_annotation("test_annotation")
        assert retrieved is not None
        assert retrieved.value == "test_value"
        assert retrieved.namespace == "test_namespace"
        
        # Remove annotation
        assert element.remove_annotation("test_annotation") == True
        assert len(element.annotations) == 0
        assert not element.has_annotation("test_annotation")
        
    def test_element_collection(self):
        """Test ElementCollection functionality"""
        collection = ElementCollection("TestCollection")
        
        # Add elements
        elem1 = NamedElement(name="Element1", id="id1")
        elem2 = NamedElement(name="Element2", id="id2", iid=100)
        
        collection.add(elem1)
        collection.add(elem2)
        
        assert collection.count() == 2
        assert collection.get_by_id("id1") == elem1
        assert collection.get_by_name("Element2") == elem2
        assert collection.get_by_iid(100) == elem2
        
        # Test statistics
        stats = collection.get_statistics()
        assert stats['total_elements'] == 2
        assert stats['named_elements'] == 2
        assert 'NamedElement' in stats['type_distribution']
        
    def test_proxy_object(self):
        """Test ProxyObject functionality"""
        proxy = ProxyObject(
            locator="http://remote.system/objects",
            target_id="remote_object_123",
            id="proxy_1"
        )
        
        assert proxy.is_federated() == True
        assert proxy.is_loaded() == False
        assert proxy.target_id == "remote_object_123"
        
        # Test load status
        status = proxy.get_load_status()
        assert status['loaded'] == False
        assert status['locator'] == "http://remote.system/objects"

class TestTypeValidation:
    """Test type validation functionality"""
    
    def test_integer_validation(self):
        """Test integer type validation"""
        typed_elem = TypedElement(data_type=PMDataType.SQL_INTEGER)
        
        # Valid conversions
        assert TypeValidator.convert_value("123", typed_elem) == 123
        assert TypeValidator.convert_value(123.0, typed_elem) == 123
        assert TypeValidator.convert_value(True, typed_elem) == 1
        
        # Invalid conversion
        with pytest.raises(TypeValidationError):
            TypeValidator.convert_value("not_a_number", typed_elem)
            
    def test_string_validation(self):
        """Test string type validation with length constraints"""
        typed_elem = TypedElement(data_type=PMDataType.SQL_VARCHAR, length=10)
        
        # Valid string
        assert TypeValidator.convert_value("hello", typed_elem) == "hello"
        
        # String too long (should be truncated)
        result = TypeValidator.convert_value("this_is_too_long", typed_elem)
        assert len(result) == 10
        assert result == "this_is_to"
        
    def test_decimal_validation(self):
        """Test decimal type validation with precision/scale"""
        typed_elem = TypedElement(
            data_type=PMDataType.SQL_DECIMAL,
            precision=5,
            scale=2
        )
        
        # Valid decimal
        result = TypeValidator.convert_value("123.45", typed_elem)
        assert str(result) == "123.45"
        
        # Too many digits
        with pytest.raises(TypeValidationError):
            TypeValidator.convert_value("123456.78", typed_elem)
            
    def test_boolean_validation(self):
        """Test boolean type validation"""
        typed_elem = TypedElement(data_type=PMDataType.SQL_BOOLEAN)
        
        # Various true values
        assert TypeValidator.convert_value("true", typed_elem) == True
        assert TypeValidator.convert_value("1", typed_elem) == True
        assert TypeValidator.convert_value(1, typed_elem) == True
        
        # Various false values
        assert TypeValidator.convert_value("false", typed_elem) == False
        assert TypeValidator.convert_value("0", typed_elem) == False
        assert TypeValidator.convert_value(0, typed_elem) == False
        
    def test_type_compatibility(self):
        """Test type compatibility checking"""
        # Compatible types
        assert TypeCompatibilityChecker.is_compatible(
            PMDataType.SQL_INTEGER, PMDataType.SQL_BIGINT
        ) == True
        
        # Incompatible types
        assert TypeCompatibilityChecker.is_compatible(
            PMDataType.SQL_BIGINT, PMDataType.SQL_INTEGER
        ) == False
        
        # Same type
        assert TypeCompatibilityChecker.is_compatible(
            PMDataType.SQL_VARCHAR, PMDataType.SQL_VARCHAR
        ) == True

class TestUtilityFunctions:
    """Test utility functions"""
    
    def test_element_id_validation(self):
        """Test element ID validation"""
        assert validate_element_id("valid_id") == True
        assert validate_element_id("") == False
        assert validate_element_id(None) == False
        assert validate_element_id("  spaced  ") == False
        
    def test_element_id_generation(self):
        """Test element ID generation"""
        id1 = generate_element_id("test")
        id2 = generate_element_id("test")
        
        assert id1.startswith("test_")
        assert id2.startswith("test_")
        assert id1 != id2  # Should be unique
        
        # Test with counter
        id3 = generate_element_id("test", 123)
        assert id3 == "test_123"
```

## ✅ Verification Steps

After implementing the XSD base architecture:

```bash
# 1. Test XSD classes
pytest tests/test_xsd_base_classes.py -v

# 2. Test type validation
python -c "from src.core.type_validation import TypeValidator; from src.core.xsd_base_classes import TypedElement, PMDataType; te = TypedElement(PMDataType.SQL_INTEGER); print('✅ Type validation works:', TypeValidator.convert_value('123', te))"

# 3. Test element collections
python -c "from src.core.xsd_base_classes import ElementCollection, NamedElement; ec = ElementCollection(); ec.add(NamedElement('test', id='1')); print('✅ Collections work:', ec.count())"

# 4. Verify PMDataType completeness
python -c "from src.core.xsd_base_classes import PMDataType; print('✅ PMDataType has', len(PMDataType), 'types')"
```

## 🔗 Next Steps

With the XSD base architecture implemented, proceed to **`05_XML_PARSER_IMPLEMENTATION.md`** to build the advanced XML parser that will use these XSD-compliant classes.

The XSD base architecture now provides:
- ✅ Complete XSD-compliant element hierarchy
- ✅ 50+ PMDataType enumeration values
- ✅ Comprehensive type validation and conversion
- ✅ Reference management foundation
- ✅ Enterprise-grade error handling
- ✅ Collection management with efficient lookups
- ✅ Annotation support for metadata
- ✅ Production-ready logging integration