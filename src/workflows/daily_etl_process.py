"""
Daily ETL Process Workflow Implementation
Orchestrates the execution of mappings in the correct order
"""
from src.core.base_classes import BaseWorkflow, Task, WorkflowLink
from src.mappings.sales_staging import SalesStaging
from src.mappings.customer_dim_load import CustomerDimLoad
from src.utils.notifications import NotificationManager
import time

class DailyETLProcess(BaseWorkflow):
    """Daily ETL Process workflow implementation"""
    
    def __init__(self, spark_session, config):
        super().__init__("Daily_ETL_Process", spark_session, config)
        self.notification_manager = NotificationManager(config.get('notifications', {}))
        
        # Initialize mapping classes
        self.mapping_classes = {
            "T1_Sales_Staging": SalesStaging,
            "T2_Customer_Dim": CustomerDimLoad,
            # TODO: Add other mappings when implemented
            # "T3_Order_Fact": FactOrderLoad,
            # "T4_Aggregates": AggregateReports
        }
        
        # Task execution order based on dependencies
        self.execution_order = [
            "T1_Sales_Staging",
            "T2_Customer_Dim",
            # "T3_Order_Fact",
            # "T4_Aggregates",
            "T5_Send_Notification"
        ]
        
    def execute(self) -> bool:
        """Execute the complete ETL workflow"""
        try:
            self.logger.info("Starting Daily ETL Process workflow")
            start_time = time.time()
            
            # Execute tasks in order
            for task_name in self.execution_order:
                success = self._execute_task(task_name)
                if not success:
                    self.logger.error(f"Task {task_name} failed. Stopping workflow.")
                    self._handle_workflow_failure(task_name)
                    return False
                    
            # Calculate execution time
            execution_time = time.time() - start_time
            self.logger.info(f"Daily ETL Process completed successfully in {execution_time:.2f} seconds")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error in Daily ETL Process workflow: {str(e)}")
            self._handle_workflow_failure("UNKNOWN")
            raise
            
    def _execute_task(self, task_name: str) -> bool:
        """Execute a single task"""
        try:
            self.logger.info(f"Executing task: {task_name}")
            task_start_time = time.time()
            
            if task_name in self.mapping_classes:
                # Execute mapping task
                mapping_class = self.mapping_classes[task_name]
                mapping = mapping_class(self.spark, self.config)
                success = mapping.execute()
                
            elif task_name == "T5_Send_Notification":
                # Execute notification task
                success = self._send_success_notification()
                
            else:
                self.logger.warning(f"Task {task_name} not implemented yet")
                success = True  # Skip unimplemented tasks for PoC
                
            task_execution_time = time.time() - task_start_time
            
            if success:
                self.logger.info(f"Task {task_name} completed successfully in {task_execution_time:.2f} seconds")
            else:
                self.logger.error(f"Task {task_name} failed")
                
            return success
            
        except Exception as e:
            self.logger.error(f"Error executing task {task_name}: {str(e)}")
            return False
            
    def _send_success_notification(self) -> bool:
        """Send workflow completion notification"""
        try:
            subject = "ETL Process Completed Successfully"
            message = f"""
            Daily ETL Process has completed successfully.
            
            Executed Tasks:
            - Sales Staging: ✓
            - Customer Dimension Load: ✓
            
            Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            recipients = ["etl-team@company.com"]  # From XML configuration
            
            return self.notification_manager.send_email(recipients, subject, message)
            
        except Exception as e:
            self.logger.error(f"Error sending success notification: {str(e)}")
            return False
            
    def _handle_workflow_failure(self, failed_task: str):
        """Handle workflow failure"""
        try:
            subject = "ETL Process Failed"
            message = f"""
            Daily ETL Process has failed at task: {failed_task}
            
            Please check the logs for more details.
            
            Timestamp: {time.strftime('%Y-%m-%d %H:%M:%S')}
            """
            
            recipients = ["etl-team@company.com"]
            
            self.notification_manager.send_email(recipients, subject, message)
            
        except Exception as e:
            self.logger.error(f"Error sending failure notification: {str(e)}")
            
    def get_task_dependencies(self) -> dict:
        """Get task dependencies from XML configuration"""
        return {
            "T1_Sales_Staging": [],
            "T2_Customer_Dim": ["T1_Sales_Staging"],
            "T3_Order_Fact": ["T2_Customer_Dim"],
            "T4_Aggregates": ["T3_Order_Fact"],
            "T5_Send_Notification": ["T4_Aggregates"]
        }
        
    def validate_workflow(self) -> bool:
        """Validate workflow configuration"""
        dependencies = self.get_task_dependencies()
        
        # Check for circular dependencies
        for task, deps in dependencies.items():
            if self._has_circular_dependency(task, deps, dependencies):
                self.logger.error(f"Circular dependency detected for task: {task}")
                return False
                
        return True
        
    def _has_circular_dependency(self, task: str, dependencies: list, all_deps: dict, visited: set = None) -> bool:
        """Check for circular dependencies recursively"""
        if visited is None:
            visited = set()
            
        if task in visited:
            return True
            
        visited.add(task)
        
        for dep in dependencies:
            if dep in all_deps:
                if self._has_circular_dependency(dep, all_deps[dep], all_deps, visited.copy()):
                    return True
                    
        return False