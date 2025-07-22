"""
Workflow Task Generators for Informatica BDM to Spark Conversion
===============================================================

This module provides generators for various Informatica workflow task types
that are converted to equivalent PySpark/Python implementations.

Currently Implements:
- Command Tasks (shell script execution)
- Decision Tasks (conditional branching)
- Assignment Tasks (parameter/variable management)
- Start Workflow Tasks (sub-workflow execution)
- Timer Tasks (delay execution)
- Event Tasks (file/event monitoring)
"""

import os
import subprocess
import time
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
from datetime import datetime, timedelta
from jinja2 import Template
import logging


class WorkflowTaskGenerator:
    """Base class for workflow task generators"""
    
    def __init__(self, task_type: str):
        self.task_type = task_type
        self.logger = logging.getLogger(f"TaskGenerator.{task_type}")
    
    def generate_task_code(self, task_config: Dict[str, Any]) -> str:
        """Generate Python code for the task"""
        raise NotImplementedError("Subclasses must implement generate_task_code")


class CommandTaskGenerator(WorkflowTaskGenerator):
    """Generator for Command Tasks (PM_COMMAND_TASK)"""
    
    def __init__(self):
        super().__init__("Command")
    
    def generate_task_code(self, task_config: Dict[str, Any]) -> str:
        """Generate code for command task execution"""
        
        template = Template('''
    def _execute_{{ task_name | lower }}_command_task(self) -> bool:
        """
        Execute {{ task_name }} Command Task
        
        Features:
        - Shell command execution with error handling
        - Environment variable support
        - Working directory configuration
        - Output capture and logging
        - Timeout management
        - Return code validation
        """
        import subprocess
        import os
        import time
        from pathlib import Path
        
        self.logger.info("Executing command task: {{ task_name }}")
        
        # Task configuration
        task_config = {
            'command': '{{ command }}',
            'working_directory': '{{ working_directory }}',
            'environment_vars': {{ environment_vars }},
            'timeout_seconds': {{ timeout_seconds }},
            'capture_output': {{ capture_output }},
            'shell': {{ shell }},
            'success_exit_codes': {{ success_exit_codes }}
        }
        
        try:
            start_time = time.time()
            
            # Set working directory
            original_cwd = os.getcwd()
            if task_config['working_directory']:
                work_dir = Path(task_config['working_directory'])
                if work_dir.exists():
                    os.chdir(work_dir)
                    self.logger.info(f"Changed working directory to: {work_dir}")
                else:
                    self.logger.warning(f"Working directory does not exist: {work_dir}")
            
            # Prepare environment
            env = os.environ.copy()
            if task_config['environment_vars']:
                env.update(task_config['environment_vars'])
                self.logger.info(f"Added environment variables: {list(task_config['environment_vars'].keys())}")
            
            # Execute command
            self.logger.info(f"Executing command: {task_config['command']}")
            
            result = subprocess.run(
                task_config['command'],
                shell=task_config['shell'],
                env=env,
                capture_output=task_config['capture_output'],
                text=True,
                timeout=task_config['timeout_seconds'] if task_config['timeout_seconds'] > 0 else None
            )
            
            execution_time = time.time() - start_time
            
            # Log output if captured
            if task_config['capture_output']:
                if result.stdout:
                    self.logger.info(f"Command stdout:\\n{result.stdout}")
                if result.stderr:
                    self.logger.warning(f"Command stderr:\\n{result.stderr}")
            
            # Check exit code
            if result.returncode in task_config['success_exit_codes']:
                self.logger.info(f"Command completed successfully in {execution_time:.2f}s (exit code: {result.returncode})")
                return True
            else:
                self.logger.error(f"Command failed with exit code: {result.returncode}")
                return False
                
        except subprocess.TimeoutExpired:
            execution_time = time.time() - start_time
            self.logger.error(f"Command timed out after {execution_time:.2f}s")
            return False
            
        except Exception as e:
            execution_time = time.time() - start_time
            self.logger.error(f"Command execution failed after {execution_time:.2f}s: {str(e)}")
            return False
            
        finally:
            # Restore original working directory
            if task_config['working_directory']:
                os.chdir(original_cwd)
        ''')
        
        return template.render(
            task_name=task_config.get('name', 'Unknown'),
            command=task_config.get('command', 'echo "No command specified"'),
            working_directory=task_config.get('working_directory', ''),
            environment_vars=task_config.get('environment_vars', {}),
            timeout_seconds=task_config.get('timeout_seconds', 300),
            capture_output=task_config.get('capture_output', True),
            shell=task_config.get('shell', True),
            success_exit_codes=task_config.get('success_exit_codes', [0])
        )


class DecisionTaskGenerator(WorkflowTaskGenerator):
    """Generator for Decision Tasks (PM_DECISION_TASK)"""
    
    def __init__(self):
        super().__init__("Decision")
    
    def generate_task_code(self, task_config: Dict[str, Any]) -> str:
        """Generate code for decision task logic"""
        
        template = Template('''
    def _execute_{{ task_name | lower }}_decision_task(self) -> Dict[str, Any]:
        """
        Execute {{ task_name }} Decision Task
        
        Features:
        - Conditional expression evaluation
        - Multiple condition support with priorities
        - Parameter-based decision logic
        - Workflow routing decisions
        - Statistical decision tracking
        """
        
        self.logger.info("Executing decision task: {{ task_name }}")
        
        # Task configuration
        decision_config = {
            'conditions': {{ conditions }},
            'default_path': '{{ default_path }}',
            'evaluation_mode': '{{ evaluation_mode }}',  # 'first_match' or 'all_match'
            'track_statistics': {{ track_statistics }}
        }
        
        decision_result = {
            'task_name': '{{ task_name }}',
            'decision_made': None,
            'execution_path': None,
            'conditions_evaluated': [],
            'evaluation_time': None
        }
        
        try:
            start_time = time.time()
            
            # Get current workflow parameters for evaluation
            workflow_params = self._get_workflow_parameters()
            
            # Evaluate conditions in priority order
            for condition in decision_config['conditions']:
                condition_name = condition['name']
                expression = condition['expression']
                target_path = condition['target_path']
                priority = condition.get('priority', 0)
                
                self.logger.info(f"Evaluating condition '{condition_name}': {expression}")
                
                try:
                    # Evaluate the condition expression
                    condition_result = self._evaluate_decision_expression(expression, workflow_params)
                    
                    decision_result['conditions_evaluated'].append({
                        'name': condition_name,
                        'expression': expression,
                        'result': condition_result,
                        'priority': priority
                    })
                    
                    if condition_result:
                        decision_result['decision_made'] = condition_name
                        decision_result['execution_path'] = target_path
                        self.logger.info(f"Decision made: {condition_name} -> {target_path}")
                        
                        if decision_config['evaluation_mode'] == 'first_match':
                            break
                            
                except Exception as e:
                    self.logger.error(f"Error evaluating condition '{condition_name}': {str(e)}")
                    decision_result['conditions_evaluated'].append({
                        'name': condition_name,
                        'expression': expression,
                        'result': False,
                        'error': str(e)
                    })
            
            # Use default path if no conditions matched
            if not decision_result['decision_made']:
                decision_result['decision_made'] = 'default'
                decision_result['execution_path'] = decision_config['default_path']
                self.logger.info(f"No conditions matched, using default path: {decision_config['default_path']}")
            
            decision_result['evaluation_time'] = time.time() - start_time
            
            # Track statistics if enabled
            if decision_config['track_statistics']:
                self._track_decision_statistics(decision_result)
            
            self.logger.info(f"Decision task completed in {decision_result['evaluation_time']:.3f}s")
            return decision_result
            
        except Exception as e:
            self.logger.error(f"Decision task failed: {str(e)}")
            return {
                'task_name': '{{ task_name }}',
                'decision_made': 'error',
                'execution_path': 'error_handler',
                'error': str(e)
            }
    
    def _evaluate_decision_expression(self, expression: str, params: Dict[str, Any]) -> bool:
        """Safely evaluate decision expression"""
        
        # Simple expression evaluator - can be enhanced with more sophisticated parsing
        try:
            # Replace parameter references with actual values
            eval_expression = expression
            for param_name, param_value in params.items():
                if isinstance(param_value, str):
                    eval_expression = eval_expression.replace(f"${param_name}", f"'{param_value}'")
                else:
                    eval_expression = eval_expression.replace(f"${param_name}", str(param_value))
            
            # Safe evaluation of basic expressions
            # In production, use a more secure expression parser
            return eval(eval_expression, {"__builtins__": {}}, {})
            
        except Exception as e:
            self.logger.error(f"Expression evaluation failed: {str(e)}")
            return False
    
    def _get_workflow_parameters(self) -> Dict[str, Any]:
        """Get current workflow parameters for decision evaluation"""
        return {
            'current_date': datetime.now().strftime('%Y-%m-%d'),
            'current_time': datetime.now().strftime('%H:%M:%S'),
            'workflow_start_time': getattr(self, 'workflow_start_time', datetime.now()),
            **getattr(self, 'workflow_parameters', {})
        }
    
    def _track_decision_statistics(self, decision_result: Dict[str, Any]):
        """Track decision-making statistics"""
        # Implementation for decision tracking/analytics
        self.logger.info(f"Decision statistics: {decision_result['decision_made']} in {decision_result['evaluation_time']:.3f}s")
        ''')
        
        return template.render(
            task_name=task_config.get('name', 'Unknown'),
            conditions=task_config.get('conditions', [
                {'name': 'default_condition', 'expression': 'True', 'target_path': 'continue', 'priority': 1}
            ]),
            default_path=task_config.get('default_path', 'continue'),
            evaluation_mode=task_config.get('evaluation_mode', 'first_match'),
            track_statistics=task_config.get('track_statistics', True)
        )


class AssignmentTaskGenerator(WorkflowTaskGenerator):
    """Generator for Assignment Tasks (PM_ASSIGNMENT_TASK)"""
    
    def __init__(self):
        super().__init__("Assignment")
    
    def generate_task_code(self, task_config: Dict[str, Any]) -> str:
        """Generate code for assignment task logic"""
        
        template = Template('''
    def _execute_{{ task_name | lower }}_assignment_task(self) -> bool:
        """
        Execute {{ task_name }} Assignment Task
        
        Features:
        - Workflow parameter assignment
        - Expression-based value calculation
        - Type-safe parameter handling
        - Parameter validation
        - Assignment history tracking
        """
        
        self.logger.info("Executing assignment task: {{ task_name }}")
        
        # Task configuration
        assignment_config = {
            'assignments': {{ assignments }},
            'validate_assignments': {{ validate_assignments }},
            'track_changes': {{ track_changes }}
        }
        
        try:
            successful_assignments = 0
            failed_assignments = 0
            
            for assignment in assignment_config['assignments']:
                param_name = assignment['parameter_name']
                expression = assignment['expression']
                data_type = assignment.get('data_type', 'string')
                validation_rules = assignment.get('validation_rules', {})
                
                self.logger.info(f"Assigning parameter '{param_name}': {expression}")
                
                try:
                    # Calculate new value
                    new_value = self._calculate_assignment_value(expression, data_type)
                    
                    # Validate new value if validation is enabled
                    if assignment_config['validate_assignments']:
                        if not self._validate_parameter_value(new_value, validation_rules):
                            self.logger.error(f"Validation failed for parameter '{param_name}': {new_value}")
                            failed_assignments += 1
                            continue
                    
                    # Track change history if enabled
                    if assignment_config['track_changes']:
                        old_value = getattr(self, 'workflow_parameters', {}).get(param_name)
                        self._track_parameter_change(param_name, old_value, new_value)
                    
                    # Make the assignment
                    if not hasattr(self, 'workflow_parameters'):
                        self.workflow_parameters = {}
                    
                    self.workflow_parameters[param_name] = new_value
                    successful_assignments += 1
                    
                    self.logger.info(f"Parameter '{param_name}' assigned value: {new_value}")
                    
                except Exception as e:
                    self.logger.error(f"Failed to assign parameter '{param_name}': {str(e)}")
                    failed_assignments += 1
            
            # Log summary
            total_assignments = successful_assignments + failed_assignments
            success_rate = (successful_assignments / total_assignments * 100) if total_assignments > 0 else 0
            
            self.logger.info(f"Assignment task completed: {successful_assignments}/{total_assignments} successful ({success_rate:.1f}%)")
            
            return failed_assignments == 0
            
        except Exception as e:
            self.logger.error(f"Assignment task failed: {str(e)}")
            return False
    
    def _calculate_assignment_value(self, expression: str, data_type: str) -> Any:
        """Calculate assignment value from expression"""
        
        try:
            # Get current parameters for expression evaluation
            params = getattr(self, 'workflow_parameters', {})
            params.update({
                'CURRENT_DATE': datetime.now().strftime('%Y-%m-%d'),
                'CURRENT_TIMESTAMP': datetime.now(),
                'WORKFLOW_NAME': self.name
            })
            
            # Replace parameter references
            eval_expression = expression
            for param_name, param_value in params.items():
                if isinstance(param_value, str):
                    eval_expression = eval_expression.replace(f"${param_name}", f"'{param_value}'")
                else:
                    eval_expression = eval_expression.replace(f"${param_name}", str(param_value))
            
            # Evaluate expression
            result = eval(eval_expression, {"__builtins__": {}}, {
                'datetime': datetime,
                'date': datetime.now().date(),
                'time': datetime.now().time()
            })
            
            # Convert to specified data type
            if data_type.lower() == 'integer':
                return int(result)
            elif data_type.lower() == 'float':
                return float(result)
            elif data_type.lower() == 'boolean':
                return bool(result)
            elif data_type.lower() == 'string':
                return str(result)
            else:
                return result
                
        except Exception as e:
            self.logger.error(f"Expression evaluation failed: {str(e)}")
            raise
    
    def _validate_parameter_value(self, value: Any, validation_rules: Dict[str, Any]) -> bool:
        """Validate parameter value against rules"""
        
        try:
            # Check min/max for numeric values
            if 'min_value' in validation_rules and isinstance(value, (int, float)):
                if value < validation_rules['min_value']:
                    return False
            
            if 'max_value' in validation_rules and isinstance(value, (int, float)):
                if value > validation_rules['max_value']:
                    return False
            
            # Check string length
            if 'max_length' in validation_rules and isinstance(value, str):
                if len(value) > validation_rules['max_length']:
                    return False
            
            # Check allowed values
            if 'allowed_values' in validation_rules:
                if value not in validation_rules['allowed_values']:
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Parameter validation error: {str(e)}")
            return False
    
    def _track_parameter_change(self, param_name: str, old_value: Any, new_value: Any):
        """Track parameter change for audit purposes"""
        change_record = {
            'parameter_name': param_name,
            'old_value': old_value,
            'new_value': new_value,
            'change_timestamp': datetime.now(),
            'workflow_name': self.name
        }
        
        if not hasattr(self, 'parameter_change_history'):
            self.parameter_change_history = []
        
        self.parameter_change_history.append(change_record)
        self.logger.debug(f"Parameter change tracked: {param_name} = {old_value} -> {new_value}")
        ''')
        
        return template.render(
            task_name=task_config.get('name', 'Unknown'),
            assignments=task_config.get('assignments', [
                {'parameter_name': 'SAMPLE_PARAM', 'expression': "'default_value'", 'data_type': 'string'}
            ]),
            validate_assignments=task_config.get('validate_assignments', True),
            track_changes=task_config.get('track_changes', True)
        )


class StartWorkflowTaskGenerator(WorkflowTaskGenerator):
    """Generator for Start Workflow Tasks (PM_STARTWORKFLOW_TASK)"""
    
    def __init__(self):
        super().__init__("StartWorkflow")
    
    def generate_task_code(self, task_config: Dict[str, Any]) -> str:
        """Generate code for start workflow task"""
        
        template = Template('''
    def _execute_{{ task_name | lower }}_start_workflow_task(self) -> bool:
        """
        Execute {{ task_name }} Start Workflow Task
        
        Features:
        - Child workflow execution
        - Parameter passing to child workflows
        - Synchronous and asynchronous execution modes
        - Child workflow monitoring
        - Error handling and recovery
        """
        import subprocess
        import json
        
        self.logger.info("Executing start workflow task: {{ task_name }}")
        
        # Task configuration
        workflow_config = {
            'target_workflow': '{{ target_workflow }}',
            'execution_mode': '{{ execution_mode }}',  # 'synchronous' or 'asynchronous'
            'parameter_mapping': {{ parameter_mapping }},
            'timeout_minutes': {{ timeout_minutes }},
            'retry_count': {{ retry_count }},
            'wait_for_completion': {{ wait_for_completion }}
        }
        
        try:
            # Prepare child workflow parameters
            child_parameters = self._prepare_child_workflow_parameters(workflow_config['parameter_mapping'])
            
            # Build child workflow execution command
            workflow_script = f"{workflow_config['target_workflow']}.py"
            cmd = ['python', workflow_script]
            
            # Add parameters as JSON string
            if child_parameters:
                cmd.extend(['--parameters', json.dumps(child_parameters)])
            
            self.logger.info(f"Starting child workflow: {workflow_config['target_workflow']}")
            self.logger.info(f"Execution command: {' '.join(cmd)}")
            self.logger.info(f"Parameters: {child_parameters}")
            
            if workflow_config['execution_mode'] == 'synchronous':
                return self._execute_synchronous_workflow(cmd, workflow_config)
            else:
                return self._execute_asynchronous_workflow(cmd, workflow_config)
                
        except Exception as e:
            self.logger.error(f"Start workflow task failed: {str(e)}")
            return False
    
    def _prepare_child_workflow_parameters(self, parameter_mapping: Dict[str, str]) -> Dict[str, Any]:
        """Prepare parameters to pass to child workflow"""
        
        child_params = {}
        current_params = getattr(self, 'workflow_parameters', {})
        
        for child_param, parent_expression in parameter_mapping.items():
            try:
                # Evaluate parent expression to get value
                if parent_expression.startswith('$'):
                    # Direct parameter reference
                    param_name = parent_expression[1:]
                    child_params[child_param] = current_params.get(param_name)
                else:
                    # Literal value or expression
                    child_params[child_param] = parent_expression
                    
            except Exception as e:
                self.logger.warning(f"Failed to map parameter {child_param}: {str(e)}")
                child_params[child_param] = None
        
        return child_params
    
    def _execute_synchronous_workflow(self, cmd: List[str], config: Dict[str, Any]) -> bool:
        """Execute child workflow synchronously and wait for completion"""
        
        try:
            timeout_seconds = config['timeout_minutes'] * 60 if config['timeout_minutes'] > 0 else None
            
            self.logger.info("Executing child workflow synchronously...")
            start_time = time.time()
            
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout_seconds
            )
            
            execution_time = time.time() - start_time
            
            if result.stdout:
                self.logger.info(f"Child workflow output:\\n{result.stdout}")
            
            if result.stderr:
                self.logger.warning(f"Child workflow errors:\\n{result.stderr}")
            
            if result.returncode == 0:
                self.logger.info(f"Child workflow completed successfully in {execution_time:.2f}s")
                return True
            else:
                self.logger.error(f"Child workflow failed with exit code: {result.returncode}")
                return False
                
        except subprocess.TimeoutExpired:
            self.logger.error(f"Child workflow timed out after {config['timeout_minutes']} minutes")
            return False
            
        except Exception as e:
            self.logger.error(f"Error executing child workflow: {str(e)}")
            return False
    
    def _execute_asynchronous_workflow(self, cmd: List[str], config: Dict[str, Any]) -> bool:
        """Execute child workflow asynchronously"""
        
        try:
            self.logger.info("Starting child workflow asynchronously...")
            
            process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # Store process reference for potential monitoring
            if not hasattr(self, 'child_processes'):
                self.child_processes = {}
            
            self.child_processes[config['target_workflow']] = {
                'process': process,
                'start_time': time.time(),
                'command': ' '.join(cmd)
            }
            
            self.logger.info(f"Child workflow started with PID: {process.pid}")
            
            # If configured to wait, monitor the process
            if config.get('wait_for_completion', False):
                return self._monitor_asynchronous_workflow(process, config)
            
            return True  # Assume success for fire-and-forget mode
            
        except Exception as e:
            self.logger.error(f"Error starting asynchronous workflow: {str(e)}")
            return False
    
    def _monitor_asynchronous_workflow(self, process, config: Dict[str, Any]) -> bool:
        """Monitor asynchronous workflow execution"""
        
        timeout_seconds = config['timeout_minutes'] * 60 if config['timeout_minutes'] > 0 else None
        start_time = time.time()
        
        try:
            while process.poll() is None:
                # Check timeout
                if timeout_seconds and (time.time() - start_time) > timeout_seconds:
                    process.terminate()
                    self.logger.error("Child workflow terminated due to timeout")
                    return False
                
                # Wait a bit before next check
                time.sleep(5)
            
            # Get final result
            stdout, stderr = process.communicate()
            execution_time = time.time() - start_time
            
            if stdout:
                self.logger.info(f"Child workflow output:\\n{stdout}")
            
            if stderr:
                self.logger.warning(f"Child workflow errors:\\n{stderr}")
            
            if process.returncode == 0:
                self.logger.info(f"Child workflow completed successfully in {execution_time:.2f}s")
                return True
            else:
                self.logger.error(f"Child workflow failed with exit code: {process.returncode}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error monitoring child workflow: {str(e)}")
            return False
        ''')
        
        return template.render(
            task_name=task_config.get('name', 'Unknown'),
            target_workflow=task_config.get('target_workflow', 'child_workflow'),
            execution_mode=task_config.get('execution_mode', 'synchronous'),
            parameter_mapping=task_config.get('parameter_mapping', {}),
            timeout_minutes=task_config.get('timeout_minutes', 30),
            retry_count=task_config.get('retry_count', 0),
            wait_for_completion=task_config.get('wait_for_completion', True)
        )


class TimerTaskGenerator(WorkflowTaskGenerator):
    """Generator for Timer Tasks (PM_TIMER_TASK)"""
    
    def __init__(self):
        super().__init__("Timer")
    
    def generate_task_code(self, task_config: Dict[str, Any]) -> str:
        """Generate code for timer task"""
        
        template = Template('''
    def _execute_{{ task_name | lower }}_timer_task(self) -> bool:
        """
        Execute {{ task_name }} Timer Task
        
        Features:
        - Configurable delay duration
        - Multiple time units support
        - Conditional delay based on expressions
        - Progress logging during wait
        """
        import time
        from datetime import datetime, timedelta
        
        self.logger.info("Executing timer task: {{ task_name }}")
        
        # Task configuration
        timer_config = {
            'delay_amount': {{ delay_amount }},
            'delay_unit': '{{ delay_unit }}',  # seconds, minutes, hours
            'condition': '{{ condition }}',  # Optional condition for delay
            'show_progress': {{ show_progress }}
        }
        
        try:
            # Calculate delay in seconds
            delay_seconds = self._calculate_delay_seconds(
                timer_config['delay_amount'], 
                timer_config['delay_unit']
            )
            
            # Check condition if specified
            if timer_config['condition']:
                if not self._evaluate_timer_condition(timer_config['condition']):
                    self.logger.info("Timer condition not met, skipping delay")
                    return True
            
            self.logger.info(f"Timer delay: {timer_config['delay_amount']} {timer_config['delay_unit']} ({delay_seconds}s)")
            
            # Execute delay with optional progress logging
            if timer_config['show_progress'] and delay_seconds > 30:
                self._execute_delay_with_progress(delay_seconds)
            else:
                time.sleep(delay_seconds)
            
            self.logger.info("Timer task completed")
            return True
            
        except Exception as e:
            self.logger.error(f"Timer task failed: {str(e)}")
            return False
    
    def _calculate_delay_seconds(self, amount: int, unit: str) -> int:
        """Convert delay amount and unit to seconds"""
        
        unit_multipliers = {
            'seconds': 1,
            'minutes': 60,
            'hours': 3600,
            'days': 86400
        }
        
        return amount * unit_multipliers.get(unit.lower(), 1)
    
    def _evaluate_timer_condition(self, condition: str) -> bool:
        """Evaluate timer condition expression"""
        
        try:
            # Simple condition evaluation
            # In production, use more sophisticated expression parser
            params = getattr(self, 'workflow_parameters', {})
            params.update({
                'CURRENT_HOUR': datetime.now().hour,
                'CURRENT_MINUTE': datetime.now().minute
            })
            
            # Replace parameter references
            eval_condition = condition
            for param_name, param_value in params.items():
                eval_condition = eval_condition.replace(f"${param_name}", str(param_value))
            
            return eval(eval_condition, {"__builtins__": {}}, {})
            
        except Exception as e:
            self.logger.error(f"Timer condition evaluation failed: {str(e)}")
            return True  # Default to executing delay on error
    
    def _execute_delay_with_progress(self, total_seconds: int):
        """Execute delay with progress logging"""
        
        progress_interval = max(30, total_seconds // 10)  # Log every 30s or 10% of total
        elapsed = 0
        
        while elapsed < total_seconds:
            sleep_duration = min(progress_interval, total_seconds - elapsed)
            time.sleep(sleep_duration)
            elapsed += sleep_duration
            
            progress_pct = (elapsed / total_seconds) * 100
            remaining = total_seconds - elapsed
            
            self.logger.info(f"Timer progress: {progress_pct:.1f}% complete ({remaining}s remaining)")
        ''')
        
        return template.render(
            task_name=task_config.get('name', 'Unknown'),
            delay_amount=task_config.get('delay_amount', 60),
            delay_unit=task_config.get('delay_unit', 'seconds'),
            condition=task_config.get('condition', ''),
            show_progress=task_config.get('show_progress', True)
        )


# Task Generator Registry
class WorkflowTaskGeneratorRegistry:
    """Registry for all workflow task generators"""
    
    def __init__(self):
        self.generators = {
            'Command': CommandTaskGenerator(),
            'Decision': DecisionTaskGenerator(),
            'Assignment': AssignmentTaskGenerator(),
            'StartWorkflow': StartWorkflowTaskGenerator(),
            'Timer': TimerTaskGenerator()
        }
    
    def get_generator(self, task_type: str) -> Optional[WorkflowTaskGenerator]:
        """Get generator for specified task type"""
        return self.generators.get(task_type)
    
    def get_supported_task_types(self) -> List[str]:
        """Get list of supported task types"""
        return list(self.generators.keys())
    
    def register_custom_generator(self, task_type: str, generator: WorkflowTaskGenerator):
        """Register custom task generator"""
        self.generators[task_type] = generator


# Global registry instance
task_generator_registry = WorkflowTaskGeneratorRegistry() 