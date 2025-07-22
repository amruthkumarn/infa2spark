"""
XSD-compliant legacy model classes for backward compatibility
Based on legacy Informatica formats and PowerCenter imports to BDM
"""
from .xsd_base_classes import NamedElement

class XSDTLoaderMapping(NamedElement):
    """XSD-compliant legacy TLoaderMapping class for PowerCenter imports"""
    pass

class XSDTWidgetInstance(NamedElement):
    """XSD-compliant legacy TWidgetInstance class for PowerCenter imports"""
    
    def __init__(self, name: str, widget_ref: str = None, **kwargs):
        super().__init__(name=name, **kwargs)
        self.widget_ref = widget_ref

class XSDTRepConnection(NamedElement):
    """XSD-compliant legacy TRepConnection class for PowerCenter imports"""
    
    def __init__(self, connection_name: str, connection_type: str = "UNKNOWN", **kwargs):
        super().__init__(name=connection_name, **kwargs)
        self.connection_name = connection_name
        self.connection_type = connection_type