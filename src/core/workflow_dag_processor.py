"""
Workflow DAG Processor for Informatica to PySpark Converter
Implements topological sorting and dependency analysis for workflow tasks
"""
import logging
from typing import Dict, List, Set, Tuple, Optional
from collections import defaultdict, deque


class WorkflowDAGProcessor:
    """Process workflow DAG with topological sorting and dependency analysis"""
    
    def __init__(self):
        self.logger = logging.getLogger("WorkflowDAGProcessor")
    
    def process_workflow_dag(self, workflow_info: Dict) -> Dict:
        """
        Process workflow information and create DAG with execution order
        
        Args:
            workflow_info: Parsed workflow information from XML
            
        Returns:
            Enhanced workflow info with execution order and parallel groups
        """
        self.logger.info(f"Processing DAG for workflow: {workflow_info.get('name', 'Unknown')}")
        
        # Build dependency graph from sequence flows
        dependency_graph = self._build_dependency_graph(workflow_info)
        
        # Perform topological sorting
        execution_order = self._topological_sort(dependency_graph)
        
        # Identify parallel execution groups
        parallel_groups = self._identify_parallel_groups(dependency_graph, execution_order)
        
        # Calculate execution levels
        execution_levels = self._calculate_execution_levels(dependency_graph)
        
        # Validate DAG (no cycles)
        is_valid_dag = self._validate_dag(dependency_graph)
        
        # Convert sets to lists for JSON serialization
        serializable_graph = {}
        for task_id, task_data in dependency_graph.items():
            serializable_graph[task_id] = {
                'dependencies': list(task_data['dependencies']),
                'dependents': list(task_data['dependents']),
                'task_info': task_data['task_info']
            }
        
        # Enhance workflow info with DAG analysis
        enhanced_workflow = workflow_info.copy()
        enhanced_workflow.update({
            'dag_analysis': {
                'dependency_graph': serializable_graph,
                'execution_order': execution_order,
                'parallel_groups': parallel_groups,
                'execution_levels': execution_levels,
                'is_valid_dag': is_valid_dag,
                'total_tasks': len(workflow_info.get('tasks', [])),
                'total_dependencies': len(workflow_info.get('sequence_flows', []))
            }
        })
        
        self.logger.info(f"DAG analysis complete: {len(execution_order)} tasks, "
                        f"{len(parallel_groups)} parallel groups, "
                        f"{len(execution_levels)} execution levels")
        
        return enhanced_workflow
    
    def _build_dependency_graph(self, workflow_info: Dict) -> Dict[str, Dict]:
        """
        Build dependency graph from workflow tasks and sequence flows
        
        Returns:
            Dict mapping task_id -> {'dependencies': [task_ids], 'dependents': [task_ids], 'task_info': task_info}
        """
        graph = defaultdict(lambda: {'dependencies': set(), 'dependents': set(), 'task_info': None})
        
        # Add all tasks to graph
        tasks = workflow_info.get('tasks', [])
        for task in tasks:
            task_id = task.get('id', task.get('name', ''))
            if task_id:  # Only add if we have a valid task_id
                graph[task_id]['task_info'] = task
            
        # Add dependencies from sequence flows
        sequence_flows = workflow_info.get('sequence_flows', [])
        for flow in sequence_flows:
            from_task = flow.get('from_instance', '')
            to_task = flow.get('to_instance', '')
            condition = flow.get('condition', 'SUCCEEDED')
            
            # Only process SUCCEEDED conditions for normal execution flow
            if condition.upper() == 'SUCCEEDED' and from_task and to_task:
                graph[to_task]['dependencies'].add(from_task)
                graph[from_task]['dependents'].add(to_task)
                
                self.logger.debug(f"Added dependency: {from_task} -> {to_task}")
        
        # Also process legacy links if sequence flows are not available
        if not sequence_flows:
            self._process_legacy_links(workflow_info, graph)
            
        return dict(graph)
    
    def _process_legacy_links(self, workflow_info: Dict, graph: Dict):
        """Process legacy TaskLink elements for backward compatibility"""
        tasks = workflow_info.get('tasks', [])
        for task in tasks:
            task_id = task.get('id', task.get('name', ''))
            
            # Process incoming flows from task info
            for flow in task.get('incoming_flows', []):
                from_task = flow.get('from_instance', '')
                condition = flow.get('condition', 'SUCCEEDED')
                
                if condition.upper() == 'SUCCEEDED' and from_task:
                    graph[task_id]['dependencies'].add(from_task)
                    graph[from_task]['dependents'].add(task_id)
    
    def _topological_sort(self, graph: Dict[str, Dict]) -> List[str]:
        """
        Perform topological sorting using Kahn's algorithm
        
        Returns:
            List of task IDs in execution order
        """
        # Calculate in-degrees
        in_degree = {task_id: len(graph[task_id]['dependencies']) for task_id in graph}
        
        # Find tasks with no dependencies (starting points)
        queue = deque([task_id for task_id, degree in in_degree.items() if degree == 0])
        execution_order = []
        
        self.logger.debug(f"Starting topological sort with {len(queue)} initial tasks")
        
        while queue:
            current_task = queue.popleft()
            execution_order.append(current_task)
            
            # Update in-degrees for dependent tasks
            for dependent_task in graph[current_task]['dependents']:
                in_degree[dependent_task] -= 1
                
                if in_degree[dependent_task] == 0:
                    queue.append(dependent_task)
                    
            self.logger.debug(f"Processed task: {current_task}, remaining queue: {len(queue)}")
        
        # Check for cycles (if not all tasks are processed)
        if len(execution_order) != len(graph):
            unprocessed = set(graph.keys()) - set(execution_order)
            self.logger.warning(f"Possible cycle detected. Unprocessed tasks: {unprocessed}")
            
        return execution_order
    
    def _identify_parallel_groups(self, graph: Dict[str, Dict], execution_order: List[str]) -> List[List[str]]:
        """
        Identify groups of tasks that can execute in parallel
        
        Returns:
            List of parallel groups, where each group is a list of task IDs
        """
        parallel_groups = []
        processed = set()
        
        for i, task_id in enumerate(execution_order):
            if task_id in processed:
                continue
                
            # Find all tasks at the same dependency level that can run in parallel
            current_group = [task_id]
            processed.add(task_id)
            
            # Check remaining tasks in execution order
            for j in range(i + 1, len(execution_order)):
                other_task = execution_order[j]
                if other_task in processed:
                    continue
                
                # Can run in parallel if no dependency relationship exists
                if (task_id not in graph[other_task]['dependencies'] and 
                    other_task not in graph[task_id]['dependencies']):
                    
                    # Also check that all dependencies are already processed
                    other_deps = graph[other_task]['dependencies']
                    if all(dep in processed or dep in current_group for dep in other_deps):
                        current_group.append(other_task)
                        processed.add(other_task)
            
            parallel_groups.append(current_group)
            
        self.logger.debug(f"Identified {len(parallel_groups)} parallel groups")
        return parallel_groups
    
    def _calculate_execution_levels(self, graph: Dict[str, Dict]) -> Dict[str, int]:
        """
        Calculate execution level for each task (longest path from start)
        
        Returns:
            Dict mapping task_id -> execution_level
        """
        levels = {}
        
        # Find starting tasks (no dependencies)
        start_tasks = [task_id for task_id, info in graph.items() 
                      if len(info['dependencies']) == 0]
        
        # BFS to calculate levels
        queue = deque([(task_id, 0) for task_id in start_tasks])
        
        while queue:
            task_id, level = queue.popleft()
            
            # Update level if we found a longer path
            if task_id not in levels or level > levels[task_id]:
                levels[task_id] = level
                
                # Add dependent tasks to queue with incremented level
                for dependent in graph[task_id]['dependents']:
                    queue.append((dependent, level + 1))
        
        self.logger.debug(f"Calculated execution levels for {len(levels)} tasks")
        return levels
    
    def _validate_dag(self, graph: Dict[str, Dict]) -> bool:
        """
        Validate that the graph is a valid DAG (no cycles)
        
        Returns:
            True if valid DAG, False if cycles detected
        """
        # Use DFS with color coding: 0=white, 1=gray, 2=black
        color = {task_id: 0 for task_id in graph}
        
        def dfs(task_id: str) -> bool:
            if color[task_id] == 1:  # Gray = cycle detected
                return False
            if color[task_id] == 2:  # Black = already processed
                return True
                
            color[task_id] = 1  # Mark as gray
            
            # Visit all dependents
            for dependent in graph[task_id]['dependents']:
                if not dfs(dependent):
                    return False
                    
            color[task_id] = 2  # Mark as black
            return True
        
        # Check all nodes
        for task_id in graph:
            if color[task_id] == 0:
                if not dfs(task_id):
                    self.logger.error(f"Cycle detected in workflow DAG involving task: {task_id}")
                    return False
                    
        self.logger.info("Workflow DAG validation successful - no cycles detected")
        return True
    
    def generate_execution_plan(self, enhanced_workflow: Dict) -> Dict:
        """
        Generate detailed execution plan with timing and resource considerations
        
        Returns:
            Execution plan with detailed scheduling information
        """
        dag_analysis = enhanced_workflow.get('dag_analysis', {})
        parallel_groups = dag_analysis.get('parallel_groups', [])
        
        execution_plan = {
            'workflow_name': enhanced_workflow.get('name', 'Unknown'),
            'total_phases': len(parallel_groups),
            'estimated_duration': 0,
            'phases': []
        }
        
        for i, group in enumerate(parallel_groups):
            phase = {
                'phase_number': i + 1,
                'parallel_tasks': len(group),
                'tasks': [],
                'can_run_parallel': len(group) > 1,
                'estimated_phase_duration': 0
            }
            
            for task_id in group:
                task_info = dag_analysis['dependency_graph'][task_id]['task_info']
                if task_info is None:
                    self.logger.warning(f"Task info is None for task_id: {task_id}")
                    task_info = {'name': task_id, 'type': 'Unknown'}
                
                task_plan = {
                    'task_id': task_id,
                    'task_name': task_info.get('name', task_id),
                    'task_type': task_info.get('type', 'Unknown'),
                    'dependencies': list(dag_analysis['dependency_graph'][task_id]['dependencies']),
                    'estimated_duration': self._estimate_task_duration(task_info),
                    'resource_requirements': self._estimate_resource_requirements(task_info)
                }
                phase['tasks'].append(task_plan)
                
                # Phase duration is max of parallel tasks
                phase['estimated_phase_duration'] = max(
                    phase['estimated_phase_duration'], 
                    task_plan['estimated_duration']
                )
            
            execution_plan['phases'].append(phase)
            execution_plan['estimated_duration'] += phase['estimated_phase_duration']
        
        self.logger.info(f"Generated execution plan: {execution_plan['total_phases']} phases, "
                        f"estimated duration: {execution_plan['estimated_duration']} minutes")
        
        return execution_plan
    
    def _estimate_task_duration(self, task_info: Dict) -> int:
        """Estimate task duration in minutes based on task type"""
        task_type = task_info.get('type', '').lower()
        
        duration_map = {
            'starttask': 1,
            'endtask': 1,
            'commandtask': 5,
            'sessiontask': 30,  # Mapping execution
            'emailtask': 2,
            'decisiontask': 1,
            'assignmenttask': 1
        }
        
        return duration_map.get(task_type, 10)  # Default 10 minutes
    
    def _estimate_resource_requirements(self, task_info: Dict) -> Dict:
        """Estimate resource requirements for task"""
        task_type = task_info.get('type', '').lower()
        
        if task_type == 'sessiontask':
            return {'cpu_cores': 4, 'memory_gb': 8, 'disk_gb': 10}
        elif task_type == 'commandtask':
            return {'cpu_cores': 1, 'memory_gb': 2, 'disk_gb': 1}
        else:
            return {'cpu_cores': 1, 'memory_gb': 1, 'disk_gb': 0.5}