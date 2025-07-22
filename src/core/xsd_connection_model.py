"""
XSD-compliant connection model classes
Based on com.informatica.metadata.common.connectinfo.xsd
"""
from .xsd_base_classes import NamedElement

class XSDConnection(NamedElement):
    """XSD-compliant Connection class"""
    
    def __init__(self, name: str, connection_type: str = "UNKNOWN", **kwargs):
        super().__init__(name=name, **kwargs)
        self.connection_type = connection_type