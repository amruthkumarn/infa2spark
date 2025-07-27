"""
Mapping DAG Processor for Informatica to PySpark Converter
Implements topological sorting and dependency analysis for mapping transformations
"""
import logging
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict, deque


class MappingDAGProcessor:
    """Process mapping DAG with topological sorting and dependency analysis for transformations"""
    
    def __init__(self):
        self.logger = logging.getLogger("MappingDAGProcessor")
    
    def process_mapping_dag(self, mapping_info: Dict) -> Dict:
        """
        Process mapping information and create transformation DAG with execution order
        
        Args:
            mapping_info: Parsed mapping information from XML
            
        Returns:
            Enhanced mapping info with execution order and parallel groups
        """
        self.logger.info(f"Processing transformation DAG for mapping: {mapping_info.get('name', 'Unknown')}")
        
        # Extract components (sources, transformations, targets)
        components = mapping_info.get('components', [])
        
        # Build dependency graph from port connections
        dependency_graph = self._build_transformation_dependency_graph(components)
        
        # Perform topological sorting
        execution_order = self._topological_sort(dependency_graph)
        
        # Identify parallel execution groups
        parallel_groups = self._identify_parallel_groups(dependency_graph, execution_order)
        
        # Calculate execution levels
        execution_levels = self._calculate_execution_levels(dependency_graph)
        
        # Validate DAG (no cycles)
        is_valid_dag = self._validate_dag(dependency_graph)
        
        # Categorize components by type for execution strategy
        sources, transformations, targets = self._categorize_components(components, dependency_graph)
        
        # Convert sets to lists for JSON serialization
        serializable_graph = {}
        for comp_name, comp_data in dependency_graph.items():
            serializable_graph[comp_name] = {
                'dependencies': list(comp_data['dependencies']),
                'dependents': list(comp_data['dependents']),
                'component_info': comp_data['component_info']
            }
        
        # Enhance mapping info with DAG analysis
        enhanced_mapping = mapping_info.copy()
        enhanced_mapping.update({
            'dag_analysis': {
                'dependency_graph': serializable_graph,
                'execution_order': execution_order,
                'parallel_groups': parallel_groups,
                'execution_levels': execution_levels,
                'is_valid_dag': is_valid_dag,
                'sources': sources,
                'transformations': transformations,
                'targets': targets,
                'total_components': len(components),
                'port_connections': self._extract_port_connections(components)
            }
        })
        
        self.logger.info(f"Mapping DAG analysis complete: {len(execution_order)} components, "
                        f"{len(parallel_groups)} parallel groups, "
                        f"{len(execution_levels)} execution levels")
        
        return enhanced_mapping
    
    def _build_transformation_dependency_graph(self, components: List[Dict]) -> Dict[str, Dict]:
        """
        Build dependency graph from transformation components and port connections
        
        Returns:
            Dict mapping component_name -> {'dependencies': [names], 'dependents': [names], 'component_info': info}
        """
        graph = defaultdict(lambda: {'dependencies': set(), 'dependents': set(), 'component_info': None})
        
        # Add all components to graph
        for component in components:
            comp_name = component.get('name', '')
            if comp_name:
                graph[comp_name]['component_info'] = component
                
        # Build dependencies from port connections
        port_connections = self._extract_port_connections(components)
        
        for connection in port_connections:
            from_comp = connection.get('from_component', '')
            to_comp = connection.get('to_component', '')
            
            if from_comp and to_comp and from_comp != to_comp:
                graph[to_comp]['dependencies'].add(from_comp)
                graph[from_comp]['dependents'].add(to_comp)
                
                self.logger.debug(f"Added transformation dependency: {from_comp} -> {to_comp}")
        
        return dict(graph)
    
    def _extract_port_connections(self, components: List[Dict]) -> List[Dict]:
        """
        Extract port-to-port connections from component definitions
        
        Returns:
            List of connection dictionaries with from_component, to_component, from_port, to_port
        """
        connections = []
        port_to_component = {}  # Map port_id -> component_name
        
        # First pass: Build port to component mapping
        for component in components:
            comp_name = component.get('name', '')
            
            # Extract output ports and map them to this component
            for port in component.get('ports', []):
                port_name = port.get('name', '')
                direction = port.get('direction', '').upper()
                
                if direction == 'OUTPUT' and port_name:
                    port_id = f"{comp_name}_{port_name}"
                    port_to_component[port_id] = comp_name
        
        # Second pass: Find connections via fromPort references
        for component in components:
            comp_name = component.get('name', '')
            
            for port in component.get('ports', []):
                direction = port.get('direction', '').upper()
                from_port = port.get('fromPort', '')
                
                if direction == 'INPUT' and from_port:
                    # Find which component this fromPort belongs to
                    from_comp = self._find_component_by_port_id(from_port, components)
                    
                    if from_comp and from_comp != comp_name:
                        connection = {
                            'from_component': from_comp,
                            'to_component': comp_name,
                            'from_port': from_port,
                            'to_port': port.get('name', ''),
                            'data_type': port.get('type', ''),
                            'nullable': port.get('nullable', True)
                        }
                        connections.append(connection)
        
        self.logger.debug(f"Extracted {len(connections)} port connections")
        return connections
    
    def _find_component_by_port_id(self, port_id: str, components: List[Dict]) -> Optional[str]:
        """
        Find which component owns a specific port ID using dynamic pattern matching
        
        This method is designed to work with any XML by using multiple fallback strategies:
        1. Direct port name matching
        2. Dynamic abbreviation extraction and matching
        3. Pattern-based component name matching
        4. Fuzzy matching based on port naming conventions
        """
        
        # Strategy 1: Direct exact match - check if any component has this exact port
        for component in components:
            comp_name = component.get('name', '')
            
            # Check both regular ports and source output ports
            for port in component.get('ports', []):
                if port.get('name') == port_id:
                    return comp_name
            
            # For source components, also check outputPorts (XML structure difference)
            for port in component.get('outputPorts', []):
                if port.get('name') == port_id:
                    return comp_name
        
        # Strategy 2: Handle PORT_ prefix patterns dynamically
        if port_id.startswith('PORT_'):
            parts = port_id.split('_')
            if len(parts) >= 2:
                # Extract potential component abbreviation (second part)
                potential_abbrev = parts[1]
                
                # Strategy 2a: Build dynamic abbreviation map from actual component names
                abbrev_candidates = self._build_dynamic_abbreviation_map(components)
                
                if potential_abbrev in abbrev_candidates:
                    return abbrev_candidates[potential_abbrev]
                
                # Strategy 2b: Try to match abbreviation with component names containing that pattern
                for component in components:
                    comp_name = component.get('name', '')
                    # Check if component name contains the abbreviation pattern
                    if potential_abbrev.upper() in comp_name.upper():
                        return comp_name
                
                # Strategy 2c: Try to match with transformation type prefixes
                for component in components:
                    comp_name = component.get('name', '')
                    comp_type = component.get('type', '').upper()
                    
                    # Match by transformation type abbreviation (EXP->Expression, AGG->Aggregator, etc.)
                    if self._matches_transformation_type(potential_abbrev, comp_type):
                        # If multiple matches, prefer the one with similar naming pattern
                        if self._has_similar_naming_pattern(potential_abbrev, comp_name):
                            return comp_name
        
        # Strategy 3: Pattern-based matching for component names in port IDs
        for component in components:
            comp_name = component.get('name', '')
            
            # Extract potential component identifier from component name
            comp_identifier = self._extract_component_identifier(comp_name)
            
            if comp_identifier and comp_identifier.upper() in port_id.upper():
                return comp_name
        
        # Strategy 4: Fuzzy matching - try to find best match based on string similarity
        best_match = self._find_best_fuzzy_match(port_id, components)
        if best_match:
            return best_match
        
        self.logger.warning(f"Could not find component for port ID: {port_id}")
        return None
    
    def _build_dynamic_abbreviation_map(self, components: List[Dict]) -> Dict[str, str]:
        """
        Build a dynamic abbreviation map from actual component names in the XML
        This replaces hardcoded mappings with dynamic extraction
        """
        abbrev_map = {}
        
        for component in components:
            comp_name = component.get('name', '')
            if not comp_name:
                continue
                
            # Strategy 1: Extract abbreviation from name patterns like "EXP_Data_Standardization"
            if '_' in comp_name:
                potential_abbrev = comp_name.split('_')[0]
                if len(potential_abbrev) >= 2 and len(potential_abbrev) <= 5:
                    abbrev_map[potential_abbrev.upper()] = comp_name
            
            # Strategy 2: Extract from transformation type patterns (more generic)
            comp_type = component.get('type', '')
            comp_category = component.get('component_type', '').lower()
            
            if comp_type or comp_category:
                # Dynamic type abbreviation extraction
                type_upper = comp_type.upper() if comp_type else ''
                
                # Extract abbreviation based on component category first
                if comp_category == 'source':
                    # Handle multiple sources with numeric suffixes or unique identifiers
                    if 'SRC' not in abbrev_map:
                        abbrev_map['SRC'] = comp_name
                    # Also map by component-specific identifier if available
                    comp_id = self._extract_component_identifier(comp_name)
                    if comp_id and comp_id != 'SRC':
                        abbrev_map[comp_id.upper()] = comp_name
                elif comp_category == 'target':
                    if 'TGT' not in abbrev_map:
                        abbrev_map['TGT'] = comp_name
                    comp_id = self._extract_component_identifier(comp_name)
                    if comp_id and comp_id != 'TGT':
                        abbrev_map[comp_id.upper()] = comp_name
                elif comp_category == 'transformation':
                    # Try to extract transformation type abbreviation
                    if 'EXPRESSION' in type_upper:
                        abbrev_map['EXP'] = comp_name
                    elif 'AGGREGATOR' in type_upper:
                        abbrev_map['AGG'] = comp_name
                    elif 'LOOKUP' in type_upper:
                        abbrev_map['LKP'] = comp_name
                    elif 'JOINER' in type_upper:
                        abbrev_map['JNR'] = comp_name
                    elif 'SEQUENCE' in type_upper:
                        abbrev_map['SEQ'] = comp_name
                    elif 'SORTER' in type_upper or 'SORT' in type_upper:
                        abbrev_map['SRT'] = comp_name
                    elif 'ROUTER' in type_upper:
                        abbrev_map['RTR'] = comp_name
                    elif 'UNION' in type_upper:
                        abbrev_map['UNI'] = comp_name
                    elif 'JAVA' in type_upper or 'SCD' in type_upper:
                        abbrev_map['SCD'] = comp_name
                    # Add more generic patterns
                    elif 'FILTER' in type_upper:
                        abbrev_map['FLT'] = comp_name
                    elif 'NORMALIZER' in type_upper:
                        abbrev_map['NRM'] = comp_name
                    elif 'RANK' in type_upper:
                        abbrev_map['RNK'] = comp_name
            
            # Strategy 3: Extract from component name keywords
            name_upper = comp_name.upper()
            if 'EXPRESSION' in name_upper or 'EXP_' in name_upper:
                abbrev_map['EXP'] = comp_name
            elif 'AGGREGATOR' in name_upper or 'AGG_' in name_upper:
                abbrev_map['AGG'] = comp_name
            elif 'LOOKUP' in name_upper or 'LKP_' in name_upper:
                abbrev_map['LKP'] = comp_name
            elif 'JOINER' in name_upper or 'JNR_' in name_upper:
                abbrev_map['JNR'] = comp_name
            elif 'SEQUENCE' in name_upper or 'SEQ_' in name_upper:
                abbrev_map['SEQ'] = comp_name
            elif 'SORTER' in name_upper or 'SRT_' in name_upper:
                abbrev_map['SRT'] = comp_name
            elif 'ROUTER' in name_upper or 'RTR_' in name_upper:
                abbrev_map['RTR'] = comp_name
            elif 'UNION' in name_upper or 'UNI_' in name_upper:
                abbrev_map['UNI'] = comp_name
            elif 'SCD' in name_upper:
                abbrev_map['SCD'] = comp_name
            elif 'SOURCE' in name_upper or 'SRC_' in name_upper:
                abbrev_map['SRC'] = comp_name
            elif 'TARGET' in name_upper or 'TGT_' in name_upper:
                abbrev_map['TGT'] = comp_name
        
        self.logger.debug(f"Built dynamic abbreviation map: {abbrev_map}")
        return abbrev_map
    
    def _matches_transformation_type(self, abbrev: str, comp_type: str) -> bool:
        """Check if abbreviation matches transformation type"""
        abbrev_upper = abbrev.upper()
        type_upper = comp_type.upper()
        
        type_mappings = {
            'EXP': ['EXPRESSION'],
            'AGG': ['AGGREGATOR'],
            'LKP': ['LOOKUP'],
            'JNR': ['JOINER'],
            'SEQ': ['SEQUENCE'],
            'SRT': ['SORTER', 'SORT'],
            'RTR': ['ROUTER'],
            'UNI': ['UNION'],
            'SCD': ['JAVA', 'SCD'],
            'SRC': ['SOURCE'],
            'TGT': ['TARGET']
        }
        
        if abbrev_upper in type_mappings:
            return any(pattern in type_upper for pattern in type_mappings[abbrev_upper])
        
        return False
    
    def _has_similar_naming_pattern(self, abbrev: str, comp_name: str) -> bool:
        """Check if component name has similar pattern to abbreviation"""
        return abbrev.upper() in comp_name.upper()
    
    def _extract_component_identifier(self, comp_name: str) -> Optional[str]:
        """Extract a component identifier from component name"""
        if not comp_name:
            return None
            
        # Try to extract meaningful identifier patterns
        name_upper = comp_name.upper()
        
        # Pattern 1: Extract prefix before underscore
        if '_' in comp_name:
            prefix = comp_name.split('_')[0]
            if len(prefix) >= 2 and len(prefix) <= 5:
                return prefix
        
        # Pattern 2: Extract transformation type keywords
        keywords = ['EXP', 'AGG', 'LKP', 'JNR', 'SEQ', 'SRT', 'RTR', 'UNI', 'SCD', 'SRC', 'TGT']
        for keyword in keywords:
            if keyword in name_upper:
                return keyword
        
        # Pattern 3: Extract first few characters if no clear pattern
        if len(comp_name) >= 3:
            return comp_name[:3]
        
        return None
    
    def _find_best_fuzzy_match(self, port_id: str, components: List[Dict]) -> Optional[str]:
        """Find best fuzzy match for port ID using string similarity"""
        best_match = None
        best_score = 0.0
        port_upper = port_id.upper()
        
        for component in components:
            comp_name = component.get('name', '')
            if not comp_name:
                continue
                
            # Calculate similarity score based on various factors
            score = 0.0
            comp_upper = comp_name.upper()
            
            # Factor 1: Direct substring match
            if comp_upper in port_upper or any(part in port_upper for part in comp_upper.split('_')):
                score += 0.5
            
            # Factor 2: Common characters
            common_chars = set(comp_upper) & set(port_upper)
            if common_chars:
                score += len(common_chars) / max(len(comp_upper), len(port_upper)) * 0.3
            
            # Factor 3: Prefix matching
            comp_prefix = comp_name.split('_')[0] if '_' in comp_name else comp_name[:3]
            if comp_prefix.upper() in port_upper:
                score += 0.4
            
            if score > best_score and score > 0.3:  # Minimum threshold
                best_score = score
                best_match = comp_name
        
        if best_match:
            self.logger.debug(f"Fuzzy match for {port_id}: {best_match} (score: {best_score:.2f})")
        
        return best_match
    
    def _topological_sort(self, graph: Dict[str, Dict]) -> List[str]:
        """
        Perform topological sorting using Kahn's algorithm
        
        Returns:
            List of component names in execution order
        """
        # Calculate in-degrees
        in_degree = {comp_name: len(graph[comp_name]['dependencies']) for comp_name in graph}
        
        # Find components with no dependencies (sources)
        queue = deque([comp_name for comp_name, degree in in_degree.items() if degree == 0])
        execution_order = []
        
        self.logger.debug(f"Starting topological sort with {len(queue)} source components")
        
        while queue:
            current_comp = queue.popleft()
            execution_order.append(current_comp)
            
            # Update in-degrees for dependent components
            for dependent_comp in graph[current_comp]['dependents']:
                in_degree[dependent_comp] -= 1
                
                if in_degree[dependent_comp] == 0:
                    queue.append(dependent_comp)
                    
            self.logger.debug(f"Processed component: {current_comp}, remaining queue: {len(queue)}")
        
        # Check for cycles (if not all components are processed)
        if len(execution_order) != len(graph):
            unprocessed = set(graph.keys()) - set(execution_order)
            self.logger.warning(f"Possible cycle detected in mapping. Unprocessed components: {unprocessed}")
            
        return execution_order
    
    def _identify_parallel_groups(self, graph: Dict[str, Dict], execution_order: List[str]) -> List[List[str]]:
        """
        Identify groups of transformations that can execute in parallel based on dependency levels
        
        Returns:
            List of parallel groups, where each group is a list of component names
        """
        # Calculate dependency levels for each component
        levels = {}
        visited = set()
        
        def calculate_level(comp_name: str) -> int:
            if comp_name in visited:
                return levels.get(comp_name, 0)
            
            visited.add(comp_name)
            dependencies = graph[comp_name]['dependencies']
            
            if not dependencies:
                levels[comp_name] = 0
                return 0
            
            max_dep_level = max(calculate_level(dep) for dep in dependencies)
            levels[comp_name] = max_dep_level + 1
            return levels[comp_name]
        
        # Calculate levels for all components
        for comp_name in execution_order:
            calculate_level(comp_name)
        
        # Group components by dependency level
        level_groups = {}
        for comp_name in execution_order:
            level = levels[comp_name]
            if level not in level_groups:
                level_groups[level] = []
            level_groups[level].append(comp_name)
        
        # Convert to list of groups ordered by level
        parallel_groups = []
        for level in sorted(level_groups.keys()):
            parallel_groups.append(level_groups[level])
            
        self.logger.debug(f"Identified {len(parallel_groups)} parallel transformation groups across {len(level_groups)} dependency levels")
        self.logger.debug(f"Level breakdown: {[(level, len(comps)) for level, comps in enumerate(parallel_groups)]}")
        
        return parallel_groups
    
    def _calculate_execution_levels(self, graph: Dict[str, Dict]) -> Dict[str, int]:
        """
        Calculate execution level for each component (longest path from source)
        
        Returns:
            Dict mapping component_name -> execution_level
        """
        levels = {}
        
        # Find starting components (sources - no dependencies)
        start_components = [comp_name for comp_name, info in graph.items() 
                           if len(info['dependencies']) == 0]
        
        # BFS to calculate levels
        queue = deque([(comp_name, 0) for comp_name in start_components])
        
        while queue:
            comp_name, level = queue.popleft()
            
            # Update level if we found a longer path
            if comp_name not in levels or level > levels[comp_name]:
                levels[comp_name] = level
                
                # Add dependent components to queue with incremented level
                for dependent in graph[comp_name]['dependents']:
                    queue.append((dependent, level + 1))
        
        self.logger.debug(f"Calculated execution levels for {len(levels)} components")
        return levels
    
    def _validate_dag(self, graph: Dict[str, Dict]) -> bool:
        """
        Validate that the graph is a valid DAG (no cycles)
        
        Returns:
            True if valid DAG, False if cycles detected
        """
        # Use DFS with color coding: 0=white, 1=gray, 2=black
        color = {comp_name: 0 for comp_name in graph}
        
        def dfs(comp_name: str) -> bool:
            if color[comp_name] == 1:  # Gray = cycle detected
                return False
            if color[comp_name] == 2:  # Black = already processed
                return True
                
            color[comp_name] = 1  # Mark as gray
            
            # Visit all dependents
            for dependent in graph[comp_name]['dependents']:
                if not dfs(dependent):
                    return False
                    
            color[comp_name] = 2  # Mark as black
            return True
        
        # Check all nodes
        for comp_name in graph:
            if color[comp_name] == 0:
                if not dfs(comp_name):
                    self.logger.error(f"Cycle detected in mapping DAG involving component: {comp_name}")
                    return False
                    
        self.logger.info("Mapping DAG validation successful - no cycles detected")
        return True
    
    def _categorize_components(self, components: List[Dict], graph: Dict[str, Dict]) -> Tuple[List[str], List[str], List[str]]:
        """
        Categorize components into sources, transformations, and targets
        
        Returns:
            Tuple of (sources, transformations, targets) as lists of component names
        """
        sources = []
        transformations = []
        targets = []
        
        for component in components:
            comp_name = component.get('name', '')
            comp_type = component.get('component_type', '').lower()
            
            if comp_type == 'source':
                sources.append(comp_name)
            elif comp_type == 'target':
                targets.append(comp_name)
            else:
                transformations.append(comp_name)
        
        self.logger.debug(f"Categorized components: {len(sources)} sources, "
                         f"{len(transformations)} transformations, {len(targets)} targets")
        
        return sources, transformations, targets
    
    def generate_execution_plan(self, enhanced_mapping: Dict) -> Dict:
        """
        Generate detailed execution plan for mapping with transformation scheduling
        
        Returns:
            Execution plan with detailed transformation scheduling information
        """
        dag_analysis = enhanced_mapping.get('dag_analysis', {})
        parallel_groups = dag_analysis.get('parallel_groups', [])
        sources = dag_analysis.get('sources', [])
        transformations = dag_analysis.get('transformations', [])
        targets = dag_analysis.get('targets', [])
        
        execution_plan = {
            'mapping_name': enhanced_mapping.get('name', 'Unknown'),
            'total_phases': len(parallel_groups),
            'estimated_duration': 0,
            'phases': [],
            'data_flow_strategy': 'port_based',
            'parallel_execution_enabled': True
        }
        
        for i, group in enumerate(parallel_groups):
            phase = {
                'phase_number': i + 1,
                'parallel_components': len(group),
                'components': [],
                'can_run_parallel': len(group) > 1,
                'estimated_phase_duration': 0,
                'phase_type': self._determine_phase_type(group, sources, transformations, targets)
            }
            
            for comp_name in group:
                comp_info = dag_analysis['dependency_graph'][comp_name]['component_info']
                if comp_info is None:
                    self.logger.warning(f"Component info is None for component: {comp_name}")
                    comp_info = {'name': comp_name, 'type': 'Unknown'}
                
                comp_plan = {
                    'component_name': comp_info.get('name', comp_name),
                    'component_type': comp_info.get('component_type', 'transformation'),
                    'transformation_type': comp_info.get('type', 'Unknown'),
                    'dependencies': list(dag_analysis['dependency_graph'][comp_name]['dependencies']),
                    'estimated_duration': self._estimate_component_duration(comp_info),
                    'memory_requirements': self._estimate_memory_requirements(comp_info),
                    'parallel_eligible': self._is_parallel_eligible(comp_info)
                }
                phase['components'].append(comp_plan)
                
                # Phase duration is max of parallel components
                phase['estimated_phase_duration'] = max(
                    phase['estimated_phase_duration'], 
                    comp_plan['estimated_duration']
                )
            
            execution_plan['phases'].append(phase)
            execution_plan['estimated_duration'] += phase['estimated_phase_duration']
        
        self.logger.info(f"Generated mapping execution plan: {execution_plan['total_phases']} phases, "
                        f"estimated duration: {execution_plan['estimated_duration']} seconds")
        
        return execution_plan
    
    def _determine_phase_type(self, group: List[str], sources: List[str], 
                             transformations: List[str], targets: List[str]) -> str:
        """Determine the type of execution phase"""
        if all(comp in sources for comp in group):
            return 'source_phase'
        elif all(comp in targets for comp in group):
            return 'target_phase'
        elif all(comp in transformations for comp in group):
            return 'transformation_phase'
        else:
            return 'mixed_phase'
    
    def _estimate_component_duration(self, comp_info: Dict) -> int:
        """Estimate component duration in seconds based on transformation type"""
        comp_type = comp_info.get('type', '').lower()
        
        duration_map = {
            'source': 5,
            'target': 8,
            'expression': 3,
            'aggregator': 15,  # GroupBy operations
            'lookup': 10,      # Join operations
            'joiner': 12,      # Multi-source joins
            'sequence': 2,     # Row number generation
            'sorter': 8,       # OrderBy operations
            'router': 4,       # Filtering operations
            'union': 3,        # DataFrame unions
            'java': 20,        # Custom logic (SCD, etc.)
            'expressionetransformation': 3,
            'aggregatortransformation': 15,
            'lookuptransformation': 10,
            'joinertransformation': 12,
            'sequencetransformation': 2,
            'sortertransformation': 8,
            'routertransformation': 4,
            'uniontransformation': 3,
            'javatransformation': 20
        }
        
        return duration_map.get(comp_type, 5)  # Default 5 seconds
    
    def _estimate_memory_requirements(self, comp_info: Dict) -> Dict:
        """Estimate memory requirements for component"""
        comp_type = comp_info.get('type', '').lower()
        
        if 'aggregator' in comp_type:
            return {'executor_memory': '4g', 'driver_memory': '2g', 'shuffle_partitions': 400}
        elif 'joiner' in comp_type or 'lookup' in comp_type:
            return {'executor_memory': '3g', 'driver_memory': '2g', 'shuffle_partitions': 200}
        elif 'sorter' in comp_type:
            return {'executor_memory': '2g', 'driver_memory': '1g', 'shuffle_partitions': 200}
        else:
            return {'executor_memory': '1g', 'driver_memory': '1g', 'shuffle_partitions': 100}
    
    def _is_parallel_eligible(self, comp_info: Dict) -> bool:
        """Determine if component can be executed in parallel with others"""
        comp_type = comp_info.get('type', '').lower()
        
        # Some transformations are inherently sequential
        sequential_types = ['sequence', 'sequencetransformation']
        
        return comp_type not in sequential_types