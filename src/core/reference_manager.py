"""
Reference management system for ID/IDREF resolution
Handles XSD-compliant object references and cross-references
"""
from typing import Dict, List, Optional, Set, Any, Callable
from collections import defaultdict
import logging
from dataclasses import dataclass
from enum import Enum

from .xsd_base_classes import Element, ObjectReference, ElementReference, XSDComplianceError, InvalidReferenceError

class ReferenceType(Enum):
    """Types of references supported"""
    ID_REF = "idref"  # Standard ID reference
    IID_REF = "iidref"  # Internal ID reference
    LOCATOR = "locator"  # Federation locator reference

@dataclass
class PendingReference:
    """Information about an unresolved reference"""
    referring_object: Element
    reference_attribute: str  # The attribute name that holds the reference
    reference_value: str  # The ID/IID being referenced
    reference_type: ReferenceType
    callback: Optional[Callable] = None  # Optional callback when resolved

class ReferenceManager:
    """
    Manages ID/IDREF resolution for XSD-compliant objects
    
    This class handles:
    - Registration of objects with IDs
    - Resolution of IDREF/IIDREF references
    - Validation of reference integrity
    - Circular reference detection
    - Proxy object management
    """
    
    def __init__(self):
        # Object storage by ID
        self._objects_by_id: Dict[str, Element] = {}
        self._objects_by_iid: Dict[int, Element] = {}
        
        # Pending references waiting for resolution
        self._pending_id_refs: Dict[str, List[PendingReference]] = defaultdict(list)
        self._pending_iid_refs: Dict[int, List[PendingReference]] = defaultdict(list)
        
        # Reference tracking for validation
        self._reference_graph: Dict[str, Set[str]] = defaultdict(set)  # from_id -> {to_id}
        self._reverse_references: Dict[str, Set[str]] = defaultdict(set)  # to_id -> {from_id}
        
        # Statistics and debugging
        self._registration_count = 0
        self._resolution_count = 0
        self._validation_errors: List[str] = []
        
        self.logger = logging.getLogger("ReferenceManager")
        
    def register_object(self, obj: Element) -> bool:
        """
        Register an object with the reference manager
        
        Args:
            obj: Element to register
            
        Returns:
            bool: True if successfully registered, False if ID conflict
        """
        registered = False
        
        # Register by ID if present
        if obj.id:
            if obj.id in self._objects_by_id:
                existing = self._objects_by_id[obj.id]
                if existing is not obj:  # Different object with same ID
                    self.logger.error(f"ID conflict: {obj.id} already registered for {existing}")
                    return False
            else:
                self._objects_by_id[obj.id] = obj
                self.logger.debug(f"Registered object with ID: {obj.id}")
                registered = True
                
                # Resolve any pending references to this ID
                self._resolve_pending_id_references(obj.id, obj)
                
        # Register by IID if present
        if obj.iid is not None:
            if obj.iid in self._objects_by_iid:
                existing = self._objects_by_iid[obj.iid]
                if existing is not obj:  # Different object with same IID
                    self.logger.error(f"IID conflict: {obj.iid} already registered for {existing}")
                    return False
            else:
                self._objects_by_iid[obj.iid] = obj
                self.logger.debug(f"Registered object with IID: {obj.iid}")
                registered = True
                
                # Resolve any pending references to this IID
                self._resolve_pending_iid_references(obj.iid, obj)
                
        if registered:
            self._registration_count += 1
            
        return registered
        
    def resolve_id_reference(self, idref: str) -> Optional[Element]:
        """
        Resolve an ID reference to an object
        
        Args:
            idref: The ID to resolve
            
        Returns:
            Element if found, None otherwise
        """
        obj = self._objects_by_id.get(idref)
        if obj:
            self._resolution_count += 1
            self.logger.debug(f"Resolved ID reference: {idref} -> {obj}")
        return obj
        
    def resolve_iid_reference(self, iidref: int) -> Optional[Element]:
        """
        Resolve an IID reference to an object
        
        Args:
            iidref: The internal ID to resolve
            
        Returns:
            Element if found, None otherwise
        """
        obj = self._objects_by_iid.get(iidref)
        if obj:
            self._resolution_count += 1
            self.logger.debug(f"Resolved IID reference: {iidref} -> {obj}")
        return obj
        
    def add_pending_reference(self, 
                             referring_obj: Element,
                             attribute_name: str,
                             reference_value: str,
                             reference_type: ReferenceType,
                             callback: Optional[Callable] = None) -> bool:
        """
        Add a pending reference that will be resolved later
        
        Args:
            referring_obj: Object that contains the reference
            attribute_name: Name of the attribute containing the reference
            reference_value: The ID/IID value being referenced
            reference_type: Type of reference (ID_REF or IID_REF)
            callback: Optional callback function when resolved
            
        Returns:
            bool: True if added, False if immediately resolved
        """
        # Try immediate resolution first
        if reference_type == ReferenceType.ID_REF:
            target_obj = self.resolve_id_reference(reference_value)
            if target_obj:
                self._apply_reference_resolution(referring_obj, attribute_name, target_obj, callback)
                return False  # Immediately resolved
                
            # Add to pending
            pending_ref = PendingReference(
                referring_object=referring_obj,
                reference_attribute=attribute_name,
                reference_value=reference_value,
                reference_type=reference_type,
                callback=callback
            )
            self._pending_id_refs[reference_value].append(pending_ref)
            
        elif reference_type == ReferenceType.IID_REF:
            try:
                iid_value = int(reference_value)
                target_obj = self.resolve_iid_reference(iid_value)
                if target_obj:
                    self._apply_reference_resolution(referring_obj, attribute_name, target_obj, callback)
                    return False  # Immediately resolved
                    
                # Add to pending
                pending_ref = PendingReference(
                    referring_object=referring_obj,
                    reference_attribute=attribute_name,
                    reference_value=reference_value,
                    reference_type=reference_type,
                    callback=callback
                )
                self._pending_iid_refs[iid_value].append(pending_ref)
                
            except ValueError:
                self.logger.error(f"Invalid IID reference value: {reference_value}")
                return False
                
        # Track reference in graph (for circular reference detection)
        if referring_obj.id and reference_type == ReferenceType.ID_REF:
            self._reference_graph[referring_obj.id].add(reference_value)
            self._reverse_references[reference_value].add(referring_obj.id)
            
        self.logger.debug(f"Added pending {reference_type.value}: {referring_obj} -> {reference_value}")
        return True
        
    def _resolve_pending_id_references(self, target_id: str, target_obj: Element):
        """Resolve all pending ID references to a newly registered object"""
        if target_id in self._pending_id_refs:
            pending_refs = self._pending_id_refs[target_id]
            self.logger.debug(f"Resolving {len(pending_refs)} pending references to ID: {target_id}")
            
            for pending_ref in pending_refs:
                self._apply_reference_resolution(
                    pending_ref.referring_object,
                    pending_ref.reference_attribute,
                    target_obj,
                    pending_ref.callback
                )
                
            # Clear pending references
            del self._pending_id_refs[target_id]
            
    def _resolve_pending_iid_references(self, target_iid: int, target_obj: Element):
        """Resolve all pending IID references to a newly registered object"""
        if target_iid in self._pending_iid_refs:
            pending_refs = self._pending_iid_refs[target_iid]
            self.logger.debug(f"Resolving {len(pending_refs)} pending references to IID: {target_iid}")
            
            for pending_ref in pending_refs:
                self._apply_reference_resolution(
                    pending_ref.referring_object,
                    pending_ref.reference_attribute,
                    target_obj,
                    pending_ref.callback
                )
                
            # Clear pending references
            del self._pending_iid_refs[target_iid]
            
    def _apply_reference_resolution(self,
                                   referring_obj: Element,
                                   attribute_name: str,
                                   target_obj: Element,
                                   callback: Optional[Callable]):
        """Apply the resolution by setting the target object"""
        try:
            # Handle ObjectReference and ElementReference objects specially
            if isinstance(referring_obj, ObjectReference):
                referring_obj.resolved_object = target_obj
            elif isinstance(referring_obj, ElementReference):
                referring_obj.resolved_element = target_obj
            else:
                # Set the attribute directly on the object
                if hasattr(referring_obj, attribute_name):
                    setattr(referring_obj, attribute_name, target_obj)
                else:
                    # Store in a resolved_references dictionary if attribute doesn't exist
                    if not hasattr(referring_obj, '_resolved_references'):
                        referring_obj._resolved_references = {}
                    referring_obj._resolved_references[attribute_name] = target_obj
                    
            # Call callback if provided
            if callback:
                callback(referring_obj, target_obj)
                
            self.logger.debug(f"Applied reference resolution: {referring_obj} -> {target_obj}")
            
        except Exception as e:
            self.logger.error(f"Error applying reference resolution: {e}")
            
    def validate_references(self) -> List[str]:
        """
        Validate all references and return list of validation errors
        
        Returns:
            List of error messages
        """
        errors = []
        
        # Check for unresolved references
        total_pending_ids = sum(len(refs) for refs in self._pending_id_refs.values())
        total_pending_iids = sum(len(refs) for refs in self._pending_iid_refs.values())
        
        if total_pending_ids > 0:
            errors.append(f"Found {total_pending_ids} unresolved ID references")
            for ref_id, pending_refs in self._pending_id_refs.items():
                errors.append(f"  Unresolved ID: {ref_id} ({len(pending_refs)} references)")
                
        if total_pending_iids > 0:
            errors.append(f"Found {total_pending_iids} unresolved IID references")
            for ref_iid, pending_refs in self._pending_iid_refs.items():
                errors.append(f"  Unresolved IID: {ref_iid} ({len(pending_refs)} references)")
                
        # Check for circular references
        circular_refs = self._detect_circular_references()
        if circular_refs:
            errors.append(f"Found {len(circular_refs)} circular reference chains:")
            for chain in circular_refs:
                errors.append(f"  Circular: {' -> '.join(chain)}")
                
        self._validation_errors = errors
        return errors
        
    def _detect_circular_references(self) -> List[List[str]]:
        """Detect circular references in the object graph"""
        circular_chains = []
        visited = set()
        path = []
        
        def dfs(node_id: str):
            if node_id in path:
                # Found cycle
                cycle_start = path.index(node_id)
                circular_chains.append(path[cycle_start:] + [node_id])
                return
                
            if node_id in visited:
                return
                
            visited.add(node_id)
            path.append(node_id)
            
            # Visit all referenced objects
            for ref_id in self._reference_graph.get(node_id, set()):
                dfs(ref_id)
                
            path.pop()
            
        # Check all objects for cycles
        for obj_id in self._objects_by_id.keys():
            if obj_id not in visited:
                dfs(obj_id)
                
        return circular_chains
        
    def get_reference_statistics(self) -> Dict[str, Any]:
        """Get statistics about reference management"""
        total_pending = (sum(len(refs) for refs in self._pending_id_refs.values()) +
                        sum(len(refs) for refs in self._pending_iid_refs.values()))
        
        return {
            'registered_objects': len(self._objects_by_id),
            'registered_iid_objects': len(self._objects_by_iid),
            'total_registrations': self._registration_count,
            'total_resolutions': self._resolution_count,
            'pending_id_references': len(self._pending_id_refs),
            'pending_iid_references': len(self._pending_iid_refs),
            'total_pending': total_pending,
            'reference_graph_nodes': len(self._reference_graph),
            'validation_errors': len(self._validation_errors)
        }
        
    def clear(self):
        """Clear all registered objects and references"""
        self._objects_by_id.clear()
        self._objects_by_iid.clear()
        self._pending_id_refs.clear()
        self._pending_iid_refs.clear()
        self._reference_graph.clear()
        self._reverse_references.clear()
        self._registration_count = 0
        self._resolution_count = 0
        self._validation_errors.clear()
        self.logger.info("Reference manager cleared")
        
    def get_object_dependencies(self, obj_id: str) -> Set[str]:
        """Get all objects that this object depends on (references)"""
        return self._reference_graph.get(obj_id, set()).copy()
        
    def get_object_dependents(self, obj_id: str) -> Set[str]:
        """Get all objects that depend on this object (are referenced by)"""
        return self._reverse_references.get(obj_id, set()).copy()
        
    def export_reference_graph(self) -> Dict[str, List[str]]:
        """Export the reference graph for external analysis"""
        return {k: list(v) for k, v in self._reference_graph.items()}

# Convenience functions for working with references
def create_object_reference(idref: str, **kwargs) -> ObjectReference:
    """Create an ObjectReference with the given idref"""
    return ObjectReference(idref=idref, **kwargs)

def create_element_reference(iidref: int, **kwargs) -> ElementReference:
    """Create an ElementReference with the given iidref"""
    return ElementReference(iidref=iidref, **kwargs)

def auto_resolve_references(obj: Element, ref_manager: ReferenceManager):
    """
    Automatically scan an object for reference attributes and register them
    
    This function inspects an object for attributes ending in '_ref', '_idref', 
    or '_iidref' and automatically registers them with the reference manager.
    """
    for attr_name in dir(obj):
        if attr_name.startswith('_'):
            continue
            
        attr_value = getattr(obj, attr_name, None)
        if not attr_value:
            continue
            
        # Handle different reference attribute patterns
        if attr_name.endswith('_idref') or attr_name.endswith('_ref'):
            if isinstance(attr_value, str):
                ref_manager.add_pending_reference(
                    obj, attr_name, attr_value, ReferenceType.ID_REF
                )
        elif attr_name.endswith('_iidref'):
            if isinstance(attr_value, (int, str)):
                ref_manager.add_pending_reference(
                    obj, attr_name, str(attr_value), ReferenceType.IID_REF
                )
        elif attr_name.endswith('_idrefs'):  # Multiple references
            if isinstance(attr_value, (list, tuple)):
                for ref_val in attr_value:
                    if isinstance(ref_val, str):
                        ref_manager.add_pending_reference(
                            obj, attr_name, ref_val, ReferenceType.ID_REF
                        )