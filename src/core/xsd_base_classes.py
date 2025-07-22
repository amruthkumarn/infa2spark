"""
XSD-compliant base classes for Informatica metadata objects
Based on com.informatica.metadata.common.core.xsd and IMX.xsd
"""
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Any, Union
from enum import Enum
import logging
from dataclasses import dataclass, field
from datetime import datetime

class PMDataType(Enum):
    """Informatica BDM data types from XSD schema"""
    SQL_CHAR = "SQL_CHAR"
    SQL_VARCHAR = "SQL_VARCHAR"
    SQL_LONGVARCHAR = "SQL_LONGVARCHAR"
    SQL_CLOB = "SQL_CLOB"
    SQL_NCHAR = "SQL_NCHAR"
    SQL_NVARCHAR = "SQL_NVARCHAR"
    SQL_LONGNVARCHAR = "SQL_LONGNVARCHAR"
    SQL_NCLOB = "SQL_NCLOB"
    SQL_BINARY = "SQL_BINARY"
    SQL_VARBINARY = "SQL_VARBINARY"
    SQL_LONGVARBINARY = "SQL_LONGVARBINARY"
    SQL_BLOB = "SQL_BLOB"
    SQL_BOOLEAN = "SQL_BOOLEAN"
    SQL_TINYINT = "SQL_TINYINT"
    SQL_SMALLINT = "SQL_SMALLINT"
    SQL_INTEGER = "SQL_INTEGER"
    SQL_BIGINT = "SQL_BIGINT"
    SQL_REAL = "SQL_REAL"
    SQL_FLOAT = "SQL_FLOAT"
    SQL_DOUBLE = "SQL_DOUBLE"
    SQL_DECIMAL = "SQL_DECIMAL"
    SQL_NUMERIC = "SQL_NUMERIC"
    SQL_DATE = "SQL_DATE"
    SQL_TIME = "SQL_TIME"
    SQL_TIMESTAMP = "SQL_TIMESTAMP"
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
    SQL_GUID = "SQL_GUID"
    SQL_WCHAR = "SQL_WCHAR"
    SQL_WVARCHAR = "SQL_WVARCHAR"
    SQL_WLONGVARCHAR = "SQL_WLONGVARCHAR"
    SQL_NONE = "SQL_NONE"

@dataclass
class Annotation:
    """XSD-compliant annotation for metadata objects"""
    name: str
    value: Any
    namespace: Optional[str] = None
    created_by: Optional[str] = None
    created_date: Optional[datetime] = None

class Element(ABC):
    """
    XSD-compliant abstract base class for all Informatica metadata elements
    Based on com.informatica.metadata.common.core.Element
    """
    
    def __init__(self, 
                 id: Optional[str] = None,
                 iid: Optional[int] = None,
                 locator: Optional[str] = None):
        self.id = id  # imx:id attribute - unique identifier
        self.iid = iid  # imx:iid attribute - internal identifier for second-class objects
        self.locator = locator  # imx:locator attribute - federation location for proxy objects
        self.annotations: List[Annotation] = []  # imx:annotations element
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        
    def add_annotation(self, annotation: Annotation):
        """Add annotation to the element"""
        self.annotations.append(annotation)
        
    def get_annotation(self, name: str) -> Optional[Annotation]:
        """Get annotation by name"""
        for annotation in self.annotations:
            if annotation.name == name:
                return annotation
        return None
        
    def remove_annotation(self, name: str) -> bool:
        """Remove annotation by name"""
        for i, annotation in enumerate(self.annotations):
            if annotation.name == name:
                del self.annotations[i]
                return True
        return False
        
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(id={self.id}, iid={self.iid})"

class NamedElement(Element):
    """
    XSD-compliant named element base class
    Based on com.informatica.metadata.common.core.NamedElement
    """
    
    def __init__(self, 
                 name: str,
                 description: str = "",
                 **kwargs):
        super().__init__(**kwargs)
        self.name = name
        self.description = description
        
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name='{self.name}', id={self.id})"

class ObjectReference(Element):
    """
    XSD-compliant object reference for first-class objects
    Based on com.informatica.metadata.common.core.ObjectReference
    """
    
    def __init__(self, 
                 idref: str,
                 **kwargs):
        super().__init__(**kwargs)
        self.idref = idref  # imx:idref attribute - reference to another object
        self.resolved_object: Optional[Element] = None  # Resolved reference
        
    def is_resolved(self) -> bool:
        """Check if reference is resolved"""
        return self.resolved_object is not None
        
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(idref='{self.idref}', resolved={self.is_resolved()})"

class ElementReference(Element):
    """
    XSD-compliant element reference for second-class objects
    Based on com.informatica.metadata.common.core.ElementReference
    """
    
    def __init__(self, 
                 iidref: int,
                 **kwargs):
        super().__init__(**kwargs)
        self.iidref = iidref  # Reference to internal ID
        self.resolved_element: Optional[Element] = None
        
    def is_resolved(self) -> bool:
        """Check if reference is resolved"""
        return self.resolved_element is not None

class TypedElement(Element):
    """
    XSD-compliant typed element with data type information
    Based on type system integration requirements
    """
    
    def __init__(self,
                 data_type: PMDataType,
                 precision: Optional[int] = None,
                 scale: Optional[int] = None,
                 length: Optional[int] = None,
                 nullable: bool = True,
                 **kwargs):
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
            'nullable': self.nullable
        }
        
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
            PMDataType.SQL_LONGNVARCHAR, PMDataType.SQL_NCLOB
        }
        return self.data_type in string_types
        
    def is_date_type(self) -> bool:
        """Check if this is a date/time data type"""
        date_types = {
            PMDataType.SQL_DATE, PMDataType.SQL_TIME,
            PMDataType.SQL_TIMESTAMP
        }
        return self.data_type in date_types

class ProxyObject(Element):
    """
    XSD-compliant proxy object for federated references
    Based on IMX proxy object specification
    """
    
    def __init__(self,
                 locator: str,
                 target_id: str,
                 **kwargs):
        super().__init__(locator=locator, **kwargs)
        self.target_id = target_id
        self.is_federated = True
        self.loaded = False
        self.target_object: Optional[Element] = None
        
    def load_target(self) -> Optional[Element]:
        """Load the target object from federation location"""
        # Implementation would depend on federation mechanism
        # For now, return None (not implemented)
        return None
        
    def is_loaded(self) -> bool:
        """Check if target object is loaded"""
        return self.loaded and self.target_object is not None

# Collection classes for managing groups of elements
class ElementCollection:
    """Generic collection for managing elements with lookup capabilities"""
    
    def __init__(self):
        self._elements: Dict[str, Element] = {}  # id -> element
        self._by_name: Dict[str, Element] = {}   # name -> element (for NamedElements)
        
    def add(self, element: Element):
        """Add element to collection"""
        if element.id:
            self._elements[element.id] = element
            
        if isinstance(element, NamedElement):
            self._by_name[element.name] = element
            
    def get_by_id(self, id: str) -> Optional[Element]:
        """Get element by ID"""
        return self._elements.get(id)
        
    def get_by_name(self, name: str) -> Optional[Element]:
        """Get named element by name"""
        return self._by_name.get(name)
        
    def remove(self, element: Element):
        """Remove element from collection"""
        if element.id and element.id in self._elements:
            del self._elements[element.id]
            
        if isinstance(element, NamedElement) and element.name in self._by_name:
            del self._by_name[element.name]
            
    def list_all(self) -> List[Element]:
        """Get all elements"""
        return list(self._elements.values())
        
    def list_by_type(self, element_type: type) -> List[Element]:
        """Get all elements of specific type"""
        return [elem for elem in self._elements.values() if isinstance(elem, element_type)]
        
    def count(self) -> int:
        """Get total count of elements"""
        return len(self._elements)
        
    def clear(self):
        """Clear all elements"""
        self._elements.clear()
        self._by_name.clear()

# Type validation utilities
class TypeValidator:
    """Utility class for validating and converting data types"""
    
    @staticmethod
    def validate_data_type(value: Any, expected_type: PMDataType) -> bool:
        """Validate that a value matches the expected data type"""
        if value is None:
            return True  # NULL values are generally allowed
            
        try:
            if expected_type in [PMDataType.SQL_INTEGER, PMDataType.SQL_BIGINT, 
                               PMDataType.SQL_SMALLINT, PMDataType.SQL_TINYINT]:
                int(value)
                return True
            elif expected_type in [PMDataType.SQL_DECIMAL, PMDataType.SQL_NUMERIC,
                                 PMDataType.SQL_REAL, PMDataType.SQL_FLOAT, PMDataType.SQL_DOUBLE]:
                float(value)
                return True
            elif expected_type in [PMDataType.SQL_CHAR, PMDataType.SQL_VARCHAR,
                                 PMDataType.SQL_LONGVARCHAR, PMDataType.SQL_CLOB]:
                str(value)
                return True
            elif expected_type == PMDataType.SQL_BOOLEAN:
                return isinstance(value, bool) or str(value).lower() in ['true', 'false', '1', '0']
            else:
                # For other types, assume valid for now
                return True
        except (ValueError, TypeError):
            return False
            
    @staticmethod
    def convert_value(value: Any, target_type: PMDataType) -> Any:
        """Convert value to target data type"""
        if value is None:
            return None
            
        if target_type in [PMDataType.SQL_INTEGER, PMDataType.SQL_BIGINT,
                          PMDataType.SQL_SMALLINT, PMDataType.SQL_TINYINT]:
            return int(value)
        elif target_type in [PMDataType.SQL_DECIMAL, PMDataType.SQL_NUMERIC,
                           PMDataType.SQL_REAL, PMDataType.SQL_FLOAT, PMDataType.SQL_DOUBLE]:
            return float(value)
        elif target_type in [PMDataType.SQL_CHAR, PMDataType.SQL_VARCHAR,
                           PMDataType.SQL_LONGVARCHAR, PMDataType.SQL_CLOB]:
            return str(value)
        elif target_type == PMDataType.SQL_BOOLEAN:
            if isinstance(value, bool):
                return value
            return str(value).lower() in ['true', '1', 'yes']
        else:
            return value

# Error classes for XSD compliance
class XSDComplianceError(Exception):
    """Base exception for XSD compliance issues"""
    pass

class InvalidReferenceError(XSDComplianceError):
    """Exception for invalid object references"""
    pass

class TypeValidationError(XSDComplianceError):
    """Exception for data type validation failures"""
    pass

class RequiredAttributeError(XSDComplianceError):
    """Exception for missing required attributes"""
    pass