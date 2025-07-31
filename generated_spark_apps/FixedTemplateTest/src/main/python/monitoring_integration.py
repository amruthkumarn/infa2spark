"""
Monitoring and Metrics Integration for Phase 3
Implements comprehensive monitoring, metrics collection, and alerting
"""
import time
import json
import threading
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import logging
from collections import defaultdict, deque
import statistics


class MetricType(Enum):
    """Types of metrics"""
    COUNTER = "counter"
    GAUGE = "gauge" 
    HISTOGRAM = "histogram"
    TIMER = "timer"


class AlertSeverity(Enum):
    """Alert severity levels"""
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


@dataclass
class Metric:
    """Metric data point"""
    name: str
    value: float
    metric_type: MetricType
    tags: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class Alert:
    """Alert definition"""
    name: str
    severity: AlertSeverity
    condition: str
    message: str
    threshold: float
    tags: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)


class MetricsCollector:
    """Collects and aggregates metrics"""
    
    def __init__(self, flush_interval: int = 60):
        self.metrics: List[Metric] = []
        self.metrics_lock = threading.Lock()
        self.flush_interval = flush_interval
        self.logger = logging.getLogger("MetricsCollector")
        
        # Metric aggregation storage
        self.counters: Dict[str, float] = defaultdict(float)
        self.gauges: Dict[str, float] = {}
        self.histograms: Dict[str, List[float]] = defaultdict(list)
        self.timers: Dict[str, List[float]] = defaultdict(list)
        
        # Background flushing
        self.flush_thread = None
        self.stop_event = threading.Event()
        
    def start(self):
        """Start metrics collection"""
        self.flush_thread = threading.Thread(target=self._flush_loop, daemon=True)
        self.flush_thread.start()
        self.logger.info("Metrics collector started")
    
    def stop(self):
        """Stop metrics collection"""
        self.stop_event.set()
        if self.flush_thread:
            self.flush_thread.join(timeout=5.0)
        self.logger.info("Metrics collector stopped")
    
    def counter(self, name: str, value: float = 1.0, tags: Dict[str, str] = None):
        """Record counter metric"""
        tags = tags or {}
        metric_key = self._get_metric_key(name, tags)
        
        with self.metrics_lock:
            self.counters[metric_key] += value
            
        self._record_metric(Metric(name, value, MetricType.COUNTER, tags))
    
    def gauge(self, name: str, value: float, tags: Dict[str, str] = None):
        """Record gauge metric"""
        tags = tags or {}
        metric_key = self._get_metric_key(name, tags)
        
        with self.metrics_lock:
            self.gauges[metric_key] = value
            
        self._record_metric(Metric(name, value, MetricType.GAUGE, tags))
    
    def histogram(self, name: str, value: float, tags: Dict[str, str] = None):
        """Record histogram metric"""
        tags = tags or {}
        metric_key = self._get_metric_key(name, tags)
        
        with self.metrics_lock:
            self.histograms[metric_key].append(value)
            
        self._record_metric(Metric(name, value, MetricType.HISTOGRAM, tags))
    
    def timer(self, name: str, tags: Dict[str, str] = None):
        """Create timer context manager"""
        return TimerContext(self, name, tags or {})
    
    def time_function(self, name: str, tags: Dict[str, str] = None):
        """Decorator to time function execution"""
        def decorator(func):
            def wrapper(*args, **kwargs):
                with self.timer(name, tags):
                    return func(*args, **kwargs)
            return wrapper
        return decorator
    
    def get_metric_summary(self) -> Dict[str, Any]:
        """Get summary of all metrics"""
        with self.metrics_lock:
            summary = {
                "counters": dict(self.counters),
                "gauges": dict(self.gauges),
                "histograms": {},
                "timers": {}
            }
            
            # Calculate histogram statistics
            for name, values in self.histograms.items():
                if values:
                    summary["histograms"][name] = {
                        "count": len(values),
                        "sum": sum(values),
                        "min": min(values),
                        "max": max(values),
                        "mean": statistics.mean(values),
                        "median": statistics.median(values)
                    }
                    if len(values) > 1:
                        summary["histograms"][name]["std_dev"] = statistics.stdev(values)
            
            # Calculate timer statistics
            for name, values in self.timers.items():
                if values:
                    summary["timers"][name] = {
                        "count": len(values),
                        "total_time": sum(values),
                        "min_time": min(values),
                        "max_time": max(values),
                        "avg_time": statistics.mean(values),
                        "median_time": statistics.median(values)
                    }
                    if len(values) > 1:
                        summary["timers"][name]["std_dev"] = statistics.stdev(values)
        
        return summary
    
    def _record_metric(self, metric: Metric):
        """Record metric for flushing"""
        with self.metrics_lock:
            self.metrics.append(metric)
    
    def _get_metric_key(self, name: str, tags: Dict[str, str]) -> str:
        """Generate unique key for metric with tags"""
        tag_str = ",".join(f"{k}={v}" for k, v in sorted(tags.items()))
        return f"{name}:{tag_str}" if tag_str else name
    
    def _flush_loop(self):
        """Background thread for flushing metrics"""
        while not self.stop_event.wait(self.flush_interval):
            try:
                self._flush_metrics()
            except Exception as e:
                self.logger.error(f"Error flushing metrics: {e}")
    
    def _flush_metrics(self):
        """Flush accumulated metrics"""
        with self.metrics_lock:
            if self.metrics:
                self.logger.debug(f"Flushing {len(self.metrics)} metrics")
                # In a real implementation, you would send these to your metrics backend
                # For now, we just clear them
                self.metrics.clear()


class TimerContext:
    """Context manager for timing operations"""
    
    def __init__(self, collector: MetricsCollector, name: str, tags: Dict[str, str]):
        self.collector = collector
        self.name = name
        self.tags = tags
        self.start_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.start_time:
            duration = time.time() - self.start_time
            metric_key = self.collector._get_metric_key(self.name, self.tags)
            
            with self.collector.metrics_lock:
                self.collector.timers[metric_key].append(duration)
            
            self.collector._record_metric(Metric(self.name, duration, MetricType.TIMER, self.tags))


class AlertManager:
    """Manages alerts and notifications"""
    
    def __init__(self, metrics_collector: MetricsCollector):
        self.metrics_collector = metrics_collector
        self.alert_rules: Dict[str, Dict[str, Any]] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_history: deque = deque(maxlen=1000)
        self.alert_callbacks: List[Callable[[Alert], None]] = []
        self.logger = logging.getLogger("AlertManager")
        
        # Alert checking
        self.check_interval = 30  # seconds
        self.check_thread = None
        self.stop_event = threading.Event()
    
    def start(self):
        """Start alert monitoring"""
        self.check_thread = threading.Thread(target=self._alert_check_loop, daemon=True)
        self.check_thread.start()
        self.logger.info("Alert manager started")
    
    def stop(self):
        """Stop alert monitoring"""
        self.stop_event.set()
        if self.check_thread:
            self.check_thread.join(timeout=5.0)
        self.logger.info("Alert manager stopped")
    
    def add_alert_rule(self, name: str, metric_name: str, condition: str, 
                      threshold: float, severity: AlertSeverity, message: str):
        """Add alert rule"""
        self.alert_rules[name] = {
            "metric_name": metric_name,
            "condition": condition,  # "greater_than", "less_than", "equals"
            "threshold": threshold,
            "severity": severity,
            "message": message
        }
        self.logger.info(f"Added alert rule: {name}")
    
    def add_alert_callback(self, callback: Callable[[Alert], None]):
        """Add callback for alert notifications"""
        self.alert_callbacks.append(callback)
    
    def get_active_alerts(self) -> List[Alert]:
        """Get currently active alerts"""
        return list(self.active_alerts.values())
    
    def get_alert_history(self) -> List[Alert]:
        """Get alert history"""
        return list(self.alert_history)
    
    def _alert_check_loop(self):
        """Background thread for checking alerts"""
        while not self.stop_event.wait(self.check_interval):
            try:
                self._check_alert_rules()
            except Exception as e:
                self.logger.error(f"Error checking alerts: {e}")
    
    def _check_alert_rules(self):
        """Check all alert rules against current metrics"""
        metric_summary = self.metrics_collector.get_metric_summary()
        
        for rule_name, rule in self.alert_rules.items():
            try:
                self._evaluate_alert_rule(rule_name, rule, metric_summary)
            except Exception as e:
                self.logger.error(f"Error evaluating alert rule {rule_name}: {e}")
    
    def _evaluate_alert_rule(self, rule_name: str, rule: Dict[str, Any], metrics: Dict[str, Any]):
        """Evaluate a single alert rule"""
        metric_name = rule["metric_name"]
        condition = rule["condition"]
        threshold = rule["threshold"]
        severity = rule["severity"]
        message = rule["message"]
        
        # Find metric value
        metric_value = None
        
        # Check in different metric types
        for metric_type in ["counters", "gauges"]:
            if metric_type in metrics and metric_name in metrics[metric_type]:
                metric_value = metrics[metric_type][metric_name]
                break
        
        # Check in histograms and timers (use mean)
        if metric_value is None:
            for metric_type in ["histograms", "timers"]:
                if metric_type in metrics and metric_name in metrics[metric_type]:
                    if "mean" in metrics[metric_type][metric_name]:
                        metric_value = metrics[metric_type][metric_name]["mean"]
                    break
        
        if metric_value is None:
            return  # Metric not found
        
        # Evaluate condition
        alert_triggered = False
        if condition == "greater_than":
            alert_triggered = metric_value > threshold
        elif condition == "less_than":
            alert_triggered = metric_value < threshold
        elif condition == "equals":
            alert_triggered = abs(metric_value - threshold) < 0.001
        
        # Handle alert state
        if alert_triggered:
            if rule_name not in self.active_alerts:
                # New alert
                alert = Alert(
                    name=rule_name,
                    severity=severity,
                    condition=f"{metric_name} {condition} {threshold} (current: {metric_value})",
                    message=message.format(metric_value=metric_value, threshold=threshold),
                    threshold=threshold,
                    tags={"metric_name": metric_name, "metric_value": str(metric_value)}
                )
                
                self.active_alerts[rule_name] = alert
                self.alert_history.append(alert)
                self._notify_alert(alert)
                
                self.logger.warning(f"Alert triggered: {rule_name}")
        else:
            if rule_name in self.active_alerts:
                # Alert resolved
                resolved_alert = self.active_alerts.pop(rule_name)
                self.logger.info(f"Alert resolved: {rule_name}")
    
    def _notify_alert(self, alert: Alert):
        """Notify alert callbacks"""
        for callback in self.alert_callbacks:
            try:
                callback(alert)
            except Exception as e:
                self.logger.error(f"Error in alert callback: {e}")


class ConfigurationMonitor:
    """Monitors configuration-related metrics and performance"""
    
    def __init__(self, metrics_collector: MetricsCollector, alert_manager: AlertManager):
        self.metrics_collector = metrics_collector
        self.alert_manager = alert_manager
        self.logger = logging.getLogger("ConfigurationMonitor")
        
        # Setup default alert rules
        self._setup_default_alerts()
    
    def _setup_default_alerts(self):
        """Setup default configuration monitoring alerts"""
        # Configuration load time alerts
        self.alert_manager.add_alert_rule(
            name="config_load_time_high",
            metric_name="config.load_time",
            condition="greater_than",
            threshold=1.0,  # 1 second
            severity=AlertSeverity.WARNING,
            message="Configuration load time is high: {metric_value:.2f}s (threshold: {threshold}s)"
        )
        
        # Memory usage alerts
        self.alert_manager.add_alert_rule(
            name="memory_usage_critical",
            metric_name="memory.usage_percent",
            condition="greater_than",
            threshold=90.0,  # 90%
            severity=AlertSeverity.CRITICAL,
            message="Memory usage is critical: {metric_value:.1f}% (threshold: {threshold}%)"
        )
        
        # Configuration validation errors
        self.alert_manager.add_alert_rule(
            name="config_validation_errors",
            metric_name="config.validation_errors",
            condition="greater_than",
            threshold=0,
            severity=AlertSeverity.WARNING,
            message="Configuration validation errors detected: {metric_value} errors"
        )
    
    def record_config_load_time(self, config_type: str, load_time: float, mapping_name: str = None):
        """Record configuration load time"""
        tags = {"config_type": config_type}
        if mapping_name:
            tags["mapping_name"] = mapping_name
        
        self.metrics_collector.histogram("config.load_time", load_time, tags)
        self.metrics_collector.gauge("config.last_load_time", load_time, tags)
    
    def record_config_validation_result(self, config_type: str, errors: int, warnings: int, mapping_name: str = None):
        """Record configuration validation results"""
        tags = {"config_type": config_type}
        if mapping_name:
            tags["mapping_name"] = mapping_name
        
        self.metrics_collector.gauge("config.validation_errors", errors, tags)
        self.metrics_collector.gauge("config.validation_warnings", warnings, tags)
        self.metrics_collector.counter("config.validations_total", 1, tags)
    
    def record_config_hot_reload(self, config_type: str, success: bool, mapping_name: str = None):
        """Record configuration hot reload event"""
        tags = {"config_type": config_type, "success": str(success)}
        if mapping_name:
            tags["mapping_name"] = mapping_name
        
        self.metrics_collector.counter("config.hot_reloads_total", 1, tags)
    
    def record_mapping_execution_metrics(self, mapping_name: str, phase_number: int, 
                                       component_count: int, duration: float, success: bool):
        """Record mapping execution metrics"""
        tags = {
            "mapping_name": mapping_name,
            "phase_number": str(phase_number),
            "success": str(success)
        }
        
        self.metrics_collector.counter("mapping.executions_total", 1, tags)
        self.metrics_collector.histogram("mapping.execution_duration", duration, tags)
        self.metrics_collector.gauge("mapping.component_count", component_count, tags)
        
        if success:
            self.metrics_collector.counter("mapping.successful_executions", 1, tags)
        else:
            self.metrics_collector.counter("mapping.failed_executions", 1, tags)
    
    def record_component_execution_metrics(self, mapping_name: str, component_name: str, 
                                         component_type: str, duration: float, success: bool):
        """Record component execution metrics"""
        tags = {
            "mapping_name": mapping_name,
            "component_name": component_name,
            "component_type": component_type,
            "success": str(success)
        }
        
        self.metrics_collector.counter("component.executions_total", 1, tags)
        self.metrics_collector.histogram("component.execution_duration", duration, tags)
        
        if success:
            self.metrics_collector.counter("component.successful_executions", 1, tags)
        else:
            self.metrics_collector.counter("component.failed_executions", 1, tags)


class MonitoringIntegration:
    """Main monitoring integration class"""
    
    def __init__(self, enable_metrics: bool = True, enable_alerts: bool = True):
        self.enable_metrics = enable_metrics
        self.enable_alerts = enable_alerts
        self.logger = logging.getLogger("MonitoringIntegration")
        
        # Initialize components
        self.metrics_collector = MetricsCollector() if enable_metrics else None
        self.alert_manager = AlertManager(self.metrics_collector) if enable_alerts and self.metrics_collector else None
        self.config_monitor = ConfigurationMonitor(self.metrics_collector, self.alert_manager) if self.metrics_collector else None
        
        # Alert notification handlers
        if self.alert_manager:
            self.alert_manager.add_alert_callback(self._default_alert_handler)
    
    def start(self):
        """Start monitoring"""
        if self.metrics_collector:
            self.metrics_collector.start()
        if self.alert_manager:
            self.alert_manager.start()
        self.logger.info("Monitoring integration started")
    
    def stop(self):
        """Stop monitoring"""
        if self.alert_manager:
            self.alert_manager.stop()
        if self.metrics_collector:
            self.metrics_collector.stop()
        self.logger.info("Monitoring integration stopped")
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get comprehensive metrics summary"""
        if not self.metrics_collector:
            return {}
        
        return self.metrics_collector.get_metric_summary()
    
    def get_monitoring_status(self) -> Dict[str, Any]:
        """Get monitoring system status"""
        status = {
            "monitoring_enabled": self.enable_metrics,
            "alerts_enabled": self.enable_alerts,
            "timestamp": datetime.now().isoformat()
        }
        
        if self.alert_manager:
            active_alerts = self.alert_manager.get_active_alerts()
            status["active_alerts"] = len(active_alerts)
            status["alerts"] = [
                {
                    "name": alert.name,
                    "severity": alert.severity.value,
                    "message": alert.message,
                    "timestamp": alert.timestamp.isoformat()
                }
                for alert in active_alerts
            ]
        
        if self.metrics_collector:
            metrics = self.get_metrics_summary()
            status["metrics_summary"] = {
                "counters_count": len(metrics.get("counters", {})),
                "gauges_count": len(metrics.get("gauges", {})),
                "histograms_count": len(metrics.get("histograms", {})),
                "timers_count": len(metrics.get("timers", {}))
            }
        
        return status
    
    def _default_alert_handler(self, alert: Alert):
        """Default alert notification handler"""
        level = logging.CRITICAL if alert.severity == AlertSeverity.CRITICAL else logging.WARNING
        self.logger.log(level, f"ALERT [{alert.severity.value.upper()}] {alert.name}: {alert.message}")
    
    def __enter__(self):
        """Context manager entry"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.stop()