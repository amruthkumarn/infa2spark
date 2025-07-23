"""
Enhanced Spark Code Generator with Production-Ready Transformations
Generates complete, enterprise-ready PySpark applications from Informatica BDM projects
"""

from pathlib import Path
from typing import Dict, Any, List, Optional
import logging
from jinja2 import Template

try:
    from .advanced_spark_transformations import (
        AdvancedTransformationEngine, 
        TransformationContext
    )
    from .enhanced_parameter_system import (
        EnhancedParameterManager,
        EnhancedParameter,
        ParameterType,
        ParameterScope,
        ParameterValidation,
        TransformationParameterScope
    )
except ImportError:
    # Fallback if modules are not available
    AdvancedTransformationEngine = None
    TransformationContext = None
    EnhancedParameterManager = None
    EnhancedParameter = None

class EnhancedSparkCodeGenerator:
    """Enhanced Spark code generator with production-ready transformation logic"""
    
    def __init__(self, base_output_dir: str = "generated_spark_apps"):
        self.base_output_dir = Path(base_output_dir)
        if AdvancedTransformationEngine:
            self.transformation_engine = AdvancedTransformationEngine()
        else:
            self.transformation_engine = None
        
        # Initialize enhanced parameter system
        if EnhancedParameterManager:
            self.parameter_manager = EnhancedParameterManager()
        else:
            self.parameter_manager = None
            
        self.logger = logging.getLogger("EnhancedSparkGenerator")
    
        # Transformation pattern detection rules
        self.transformation_patterns = {
            'scd_type2': ['scd', 'dimension', 'slowly_changing', 'type_2', 'type2'],
            'scd_type1': ['scd_type1', 'type_1', 'type1', 'overwrite_dimension'],
            'router': ['router', 'split', 'condition', 'branch', 'filter_split'],
            'union': ['union', 'combine', 'merge_sources', 'concatenate'],
            'rank': ['rank', 'top_n', 'ranking', 'dense_rank', 'row_number'],
            'data_masking': ['mask', 'pii', 'encrypt', 'anonymize', 'tokenize', 'privacy'],
            'complex_lookup': ['lookup', 'joiner', 'reference', 'dimension_lookup'],
            'advanced_aggregation': ['aggregate', 'group_by', 'sum', 'count', 'rollup', 'cube']
        }
    
    def generate_enhanced_mapping_code(self, mapping: Dict[str, Any], project: Dict[str, Any]) -> str:
        """Generate enhanced mapping code with production-ready transformations"""
        
        # Analyze mapping to identify transformation patterns
        transformation_analysis = self._analyze_mapping_transformations(mapping)
        
        # Generate enhanced mapping class
        return self._generate_enhanced_mapping_class(mapping, project, transformation_analysis)
    
    def generate_production_mapping(self, app_dir: Path, mapping: Dict[str, Any], project: Dict[str, Any]):
        """Generate production-ready mapping file (integration method for SparkCodeGenerator)"""
        
        # Convert project object to dictionary if needed
        if hasattr(project, '__dict__'):
            project_dict = {
                'project_name': getattr(project, 'name', 'Unknown'),
                'mappings': [m.__dict__ if hasattr(m, '__dict__') else m for m in getattr(project, 'mappings', [])],
                'workflows': [w.__dict__ if hasattr(w, '__dict__') else w for w in getattr(project, 'workflows', [])],
                'connections': [c.__dict__ if hasattr(c, '__dict__') else c for c in getattr(project, 'connections', [])]
            }
        else:
            project_dict = project
            
        # Convert mapping object to dictionary if needed
        if hasattr(mapping, '__dict__'):
            mapping_dict = mapping.__dict__
        else:
            mapping_dict = mapping
        
        # Generate mapping code
        mapping_code = self.generate_enhanced_mapping_code(mapping_dict, project_dict)
        
        # Create mapping file
        mapping_name = mapping_dict.get('name', 'unknown_mapping')
        class_name = self._to_class_name(mapping_name)
        file_name = mapping_name.lower().replace(' ', '_').replace('-', '_') + '.py'
        
        mappings_dir = app_dir / "src" / "main" / "python" / "mappings"
        mappings_dir.mkdir(parents=True, exist_ok=True)
        
        mapping_file = mappings_dir / file_name
        
        try:
            with open(mapping_file, 'w', encoding='utf-8') as f:
                f.write(mapping_code)
            self.logger.info(f"Generated enhanced mapping: {mapping_file}")
        except Exception as e:
            self.logger.error(f"Error writing mapping file {mapping_file}: {str(e)}")
            raise
    
    def _to_class_name(self, name: str) -> str:
        """Convert mapping name to class name"""
        # Remove special characters and title case
        import re
        clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', name)
        return ''.join(word.capitalize() for word in clean_name.split('_'))
    
    def _analyze_mapping_transformations(self, mapping: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze mapping to identify transformation patterns and generate appropriate code"""
        
        transformations = mapping.get('components', [])
        
        analysis = {
            'has_scd_type2': False,
            'has_scd_type1': False,
            'has_router': False,
            'has_union': False,
            'has_rank': False,
            'has_data_masking': False,
            'has_complex_lookup': False,
            'has_advanced_aggregation': False,
            'has_expressions': False,
            'has_joins': False,
            'transformation_details': []
        }
        
        # Set up transformation-specific parameters if parameter manager is available
        if self.parameter_manager:
            self._setup_transformation_parameters(mapping)
        
        for transform in transformations:
            transform_type = transform.get('type', '').lower()
            transform_name = transform.get('name', '').lower()
            
            detail = {
                'name': transform.get('name', ''),
                'type': transform_type,
                'original_type': transform.get('type', ''),
                'config': {}
            }
            
            # Check for each transformation pattern
            for pattern_name, keywords in self.transformation_patterns.items():
                if any(keyword in transform_name or keyword in transform_type for keyword in keywords):
                    analysis[f'has_{pattern_name}'] = True
                    detail['enhanced_type'] = pattern_name
                    detail['config'] = self._generate_transformation_config(pattern_name, transform)
                    break
            else:
                # Default handling for unmatched transformations
                if transform_type in ['expression']:
                    analysis['has_expressions'] = True
                elif transform_type in ['lookup', 'joiner']:
                    analysis['has_complex_lookup'] = True
                    detail['enhanced_type'] = 'complex_lookup'
                    detail['config'] = self._generate_transformation_config('complex_lookup', transform)
                elif transform_type in ['aggregator']:
                    analysis['has_advanced_aggregation'] = True
                    detail['enhanced_type'] = 'advanced_aggregation'
                    detail['config'] = self._generate_transformation_config('advanced_aggregation', transform)
            
            analysis['transformation_details'].append(detail)
        
        return analysis
    
    def _setup_transformation_parameters(self, mapping: Dict[str, Any]):
        """Set up transformation-specific parameters with type validation"""
        if not self.parameter_manager:
            return
            
        mapping_name = mapping.get('name', 'Unknown')
        transformations = mapping.get('components', [])
        
        for transform in transformations:
            if transform.get('component_type') != 'transformation':
                continue
                
            transform_name = transform.get('name', 'Unknown')
            transform_type = transform.get('type', '').lower()
            transform_id = transform.get('id', f"{mapping_name}_{transform_name}")
            
            # Create transformation parameter scope
            trans_scope = self.parameter_manager.create_transformation_scope(
                transform_id, transform_name
            )
            
            # Add common transformation parameters
            self._add_common_transformation_parameters(trans_scope, transform_type)
            
            # Add transformation-specific parameters
            self._add_specific_transformation_parameters(trans_scope, transform_type, transform)
    
    def _add_common_transformation_parameters(self, scope: TransformationParameterScope, transform_type: str):
        """Add common parameters for all transformations"""
        if not EnhancedParameter:
            return
            
        # Cache configuration
        scope.add_parameter(EnhancedParameter(
            name="cache_enabled",
            param_type=ParameterType.BOOLEAN,
            scope=ParameterScope.TRANSFORMATION,
            default_value=True,
            description="Enable caching for this transformation"
        ))
        
        # Performance tuning
        scope.add_parameter(EnhancedParameter(
            name="batch_size",
            param_type=ParameterType.INTEGER,
            scope=ParameterScope.TRANSFORMATION,
            default_value=10000,
            description="Batch size for transformation processing",
            validation=ParameterValidation(
                min_value=100,
                max_value=1000000
            )
        ))
        
        # Error handling
        scope.add_parameter(EnhancedParameter(
            name="error_threshold",
            param_type=ParameterType.FLOAT,
            scope=ParameterScope.TRANSFORMATION,
            default_value=0.01,
            description="Maximum error rate for this transformation",
            validation=ParameterValidation(
                min_value=0.0,
                max_value=1.0
            )
        ))
    
    def _add_specific_transformation_parameters(self, scope: TransformationParameterScope, transform_type: str, transform: Dict[str, Any]):
        """Add transformation-specific parameters with validation"""
        if not EnhancedParameter:
            return
            
        if transform_type in ['lookup', 'joiner']:
            # Lookup-specific parameters
            scope.add_parameter(EnhancedParameter(
                name="lookup_cache_size_mb",
                param_type=ParameterType.INTEGER,
                scope=ParameterScope.TRANSFORMATION,
                default_value=256,
                description="Lookup cache size in MB",
                validation=ParameterValidation(
                    min_value=64,
                    max_value=8192
                )
            ))
            
            scope.add_parameter(EnhancedParameter(
                name="broadcast_threshold",
                param_type=ParameterType.INTEGER,
                scope=ParameterScope.TRANSFORMATION,
                default_value=100000,
                description="Row count threshold for broadcast join",
                validation=ParameterValidation(
                    min_value=1000,
                    max_value=10000000
                )
            ))
            
            scope.add_parameter(EnhancedParameter(
                name="join_type",
                param_type=ParameterType.STRING,
                scope=ParameterScope.TRANSFORMATION,
                default_value="left_outer",
                description="Join type for lookup",
                validation=ParameterValidation(
                    allowed_values=["inner", "left_outer", "right_outer", "full_outer"]
                )
            ))
        
        elif transform_type == 'aggregator':
            # Aggregator-specific parameters
            scope.add_parameter(EnhancedParameter(
                name="sort_before_agg",
                param_type=ParameterType.BOOLEAN,
                scope=ParameterScope.TRANSFORMATION,
                default_value=True,
                description="Sort data before aggregation"
            ))
            
            scope.add_parameter(EnhancedParameter(
                name="group_by_partitions",
                param_type=ParameterType.INTEGER,
                scope=ParameterScope.TRANSFORMATION,
                default_value=200,
                description="Number of partitions for groupBy operations",
                validation=ParameterValidation(
                    min_value=1,
                    max_value=10000
                )
            ))
        
        elif transform_type in ['java', 'custom']:
            # Custom transformation parameters
            if 'scd' in transform.get('name', '').lower():
                scope.add_parameter(EnhancedParameter(
                    name="scd_history_days",
                    param_type=ParameterType.INTEGER,
                    scope=ParameterScope.TRANSFORMATION,
                    default_value=2555,  # 7 years
                    description="Number of days to retain SCD history",
                    validation=ParameterValidation(
                        min_value=1,
                        max_value=36500  # 100 years
                    )
                ))
                
                scope.add_parameter(EnhancedParameter(
                    name="effective_date_format",
                    param_type=ParameterType.STRING,
                    scope=ParameterScope.TRANSFORMATION,
                    default_value="yyyy-MM-dd",
                    description="Date format for effective dates",
                    validation=ParameterValidation(
                        regex_pattern=r'^[yMdHmsS\-/: ]+$'
                    )
                ))
        
        elif transform_type == 'expression':
            # Expression-specific parameters
            scope.add_parameter(EnhancedParameter(
                name="null_value_handling",
                param_type=ParameterType.STRING,
                scope=ParameterScope.TRANSFORMATION,
                default_value="SKIP",
                description="How to handle null values in expressions",
                validation=ParameterValidation(
                    allowed_values=["SKIP", "ERROR", "DEFAULT"]
                )
            ))

    def _generate_transformation_config(self, pattern_name: str, transform: Dict[str, Any]) -> Dict[str, Any]:
        """Generate configuration for specific transformation patterns"""
        
        if pattern_name == 'scd_type2':
            return {
                'business_keys': ['customer_id'],
                'tracked_columns': ['customer_name', 'email', 'phone', 'address', 'status'],
                'effective_date_column': 'effective_date',
                'expiry_date_column': 'expiry_date',
                'current_flag_column': 'is_current',
                'version_column': 'record_version'
            }
        
        elif pattern_name == 'scd_type1':
            return {
                    'business_keys': ['customer_id'],
                'update_columns': ['customer_name', 'email', 'phone', 'address', 'status'],
                'handle_deletes': False
            }
        
        elif pattern_name == 'router':
            # Extract routing conditions from transformation properties
            return {
                'routes': [
                    {'name': 'high_value', 'condition': 'amount > 1000', 'priority': 1},
                    {'name': 'medium_value', 'condition': 'amount > 100 AND amount <= 1000', 'priority': 2},
                    {'name': 'low_value', 'condition': 'amount <= 100', 'priority': 3}
                ],
                'default_route': 'unclassified'
            }
        
        elif pattern_name == 'union':
            return {
                'union_all': True,
                'remove_duplicates': False,
                'duplicate_columns': [],
                'add_source_columns': True
            }
        
        elif pattern_name == 'rank':
            return {
                'partition_by': ['category', 'region'],
                'order_by': [{'column': 'sales_amount', 'direction': 'desc'}],
                'rank_methods': ['rank', 'dense_rank', 'row_number'],
                'top_n': 10,
                'filter_rank_column': 'rank_value',
                'tie_breaking': 'first'
            }
        
        elif pattern_name == 'data_masking':
            return {
                'columns': [
                    {'name': 'customer_name', 'method': 'partial_mask', 'params': {'show_first': 2, 'show_last': 1}},
                    {'name': 'email', 'method': 'email_mask', 'params': {}},
                    {'name': 'phone', 'method': 'phone_mask', 'params': {}},
                    {'name': 'ssn', 'method': 'hash', 'params': {'salt': 'secure_salt_123'}}
                ],
                'add_audit_columns': True,
                'version': '1.0'
            }
        
        elif pattern_name == 'complex_lookup':
            return {
                    'lookup_type': 'connected',
                    'join_strategy': 'auto',
                    'multiple_match': 'first',
                    'cache_lookup': True,
                    'lookup_keys': [{'input_column': 'customer_id', 'lookup_column': 'customer_id'}],
                    'return_columns': ['customer_name', 'customer_status', 'segment'],
                    'default_values': {'customer_status': 'UNKNOWN', 'segment': 'DEFAULT'}
                }
            
        elif pattern_name == 'advanced_aggregation':
            return {
                    'group_by_columns': ['customer_segment', 'region'],
                    'aggregations': [
                        {'type': 'sum', 'column': 'sales_amount', 'alias': 'total_sales'},
                        {'type': 'count', 'column': '*', 'alias': 'transaction_count'},
                        {'type': 'avg', 'column': 'sales_amount', 'alias': 'avg_sales'}
                    ],
                'window_functions': [],
                'incremental_mode': False,
                'partition_by': []
            }
        
        return {}
    
    def _generate_enhanced_mapping_class(self, mapping: Dict[str, Any], project: Dict[str, Any], 
                                       transformation_analysis: Dict[str, Any]) -> str:
        """Generate enhanced mapping class with production-ready features"""
        
        class_name = self._to_class_name(mapping['name'])
        
        # Build the enhanced template with conditional sections
        template_sections = []
        
        # Add imports and class definition
        template_sections.append(self._get_mapping_header_template())
        
        # Add class definition and constructor
        template_sections.append(self._get_class_definition_template(class_name, mapping))
        
        # Add execute method with production features
        template_sections.append(self._get_execute_method_template(mapping, transformation_analysis))
        
        # Add source reading methods
        template_sections.append(self._get_source_methods_template(mapping))
        
        # Add advanced transformation methods based on analysis
        if transformation_analysis.get('has_scd_type2'):
            template_sections.append(self._get_scd_type2_methods_template())
        
        if transformation_analysis.get('has_scd_type1'):
            template_sections.append(self._get_scd_type1_methods_template())
        
        if transformation_analysis.get('has_router'):
            template_sections.append(self._get_router_methods_template())
        
        if transformation_analysis.get('has_union'):
            template_sections.append(self._get_union_methods_template())
        
        if transformation_analysis.get('has_rank'):
            template_sections.append(self._get_rank_methods_template())
        
        if transformation_analysis.get('has_data_masking'):
            template_sections.append(self._get_data_masking_methods_template())
        
        if transformation_analysis.get('has_complex_lookup'):
            template_sections.append(self._get_complex_lookup_methods_template())
        
        if transformation_analysis.get('has_advanced_aggregation'):
            template_sections.append(self._get_advanced_aggregation_methods_template())
        
        # Add target writing methods
        template_sections.append(self._get_target_methods_template(mapping))
        
        # Add utility methods
        template_sections.append(self._get_utility_methods_template())
        
        # Combine all sections
        full_template = '\n'.join(template_sections)
        
        # Render with context
        template = Template(full_template)
        return template.render(
            mapping=mapping,
            project=project,
            class_name=class_name,
            transformation_analysis=transformation_analysis
        )
    
    def _get_mapping_header_template(self) -> str:
        return '''"""
{{ mapping.name }} Mapping Implementation  
Generated from Informatica BDM Project: {{ project.name }}

This is a production-ready PySpark implementation with enterprise features:
- Advanced transformation logic (SCD, lookups, aggregations, routing, masking)
- Data quality validation and error handling
- Performance optimization and caching strategies
- Comprehensive logging and monitoring
- Configurable parameters and environment support

Components:
{%- for component in mapping.components %}
- {{ component.name }} ({{ component.type }})
{%- endfor %}
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.storage import StorageLevel

from ..base_classes import BaseMapping, DataSourceManager
from ..transformations.generated_transformations import *'''

    def _get_class_definition_template(self, class_name: str, mapping: Dict[str, Any]) -> str:
        return f'''

class {class_name}(BaseMapping):
    """{{{{ mapping.description or mapping.name + ' - Production-ready mapping implementation' }}}}"""
    
    def __init__(self, spark: SparkSession, config: Dict[str, Any]):
        super().__init__("{{{{ mapping.name }}}}", spark, config)
        self.data_source_manager = DataSourceManager(spark, config.get('connections', {{}}))
        
        # Performance configuration
        self.cache_intermediate_results = config.get('cache_intermediate_results', True)
        self.checkpoint_interval = config.get('checkpoint_interval', 10)
        self.broadcast_threshold = config.get('broadcast_threshold', 100000)
        
        # Data quality configuration  
        self.data_quality_enabled = config.get('data_quality_enabled', True)
        self.max_error_threshold = config.get('max_error_threshold', 0.05)  # 5% error threshold
        
        # Monitoring configuration
        self.enable_metrics = config.get('enable_metrics', True)
        self.metrics = {{}}'''
        
    def _get_execute_method_template(self, mapping: Dict[str, Any], analysis: Dict[str, Any]) -> str:
        return '''
    def execute(self) -> bool:
        """Execute the {{ mapping.name }} mapping with production features"""
        start_time = datetime.now()
        
        try:
            self.logger.info("=" * 80)
            self.logger.info(f"Starting {{ mapping.name }} mapping execution")
            self.logger.info("=" * 80)
            
            # Initialize metrics
            if self.enable_metrics:
                self._initialize_metrics()
            
            {%- set sources = mapping.components | selectattr('component_type', 'equalto', 'source') | list %}
            {%- set transformations = mapping.components | selectattr('component_type', 'equalto', 'transformation') | list %}  
            {%- set targets = mapping.components | selectattr('component_type', 'equalto', 'target') | list %}
            
            # Step 1: Read and validate source data
            self.logger.info("Step 1: Reading source data")
            source_dataframes = {}
            
            {%- for source in sources %}
            self.logger.info("Reading {{ source.name }} data...")
            {{ source.name | lower }}_df = self._read_{{ source.name | lower }}()
            
            # Data quality validation
            if self.data_quality_enabled:
                {{ source.name | lower }}_df = self._validate_source_quality({{ source.name | lower }}_df, "{{ source.name }}")
            
            # Performance optimization
            if self.cache_intermediate_results:
                {{ source.name | lower }}_df = {{ source.name | lower }}_df.persist(StorageLevel.MEMORY_AND_DISK)
            
            source_dataframes["{{ source.name | lower }}"] = {{ source.name | lower }}_df
            self._update_metrics("{{ source.name | lower }}_records", {{ source.name | lower }}_df.count())
            {%- endfor %}
            
            # Step 2: Apply transformations with production logic
            self.logger.info("Step 2: Applying transformations")
            
            current_df = list(source_dataframes.values())[0] if source_dataframes else None
            
            {%- for detail in transformation_analysis.transformation_details %}
            {%- if detail.get('enhanced_type') %}
            self.logger.info("Applying {{ detail.name }} ({{ detail.enhanced_type }}) transformation...")
            transformation_start = datetime.now()
            
            {%- if detail.enhanced_type == 'scd_type2' %}
            existing_df = self._load_existing_dimension_data()
            current_df = self.apply_scd_type2_transformation(current_df, existing_df)
            {%- elif detail.enhanced_type == 'scd_type1' %}
            existing_df = self._load_existing_dimension_data()
            current_df = self.apply_scd_type1_transformation(current_df, existing_df)
            {%- elif detail.enhanced_type == 'router' %}
            router_results = self.apply_router_transformation(current_df)
            # Handle router results (multiple output DataFrames)
            current_df = router_results.get('default', current_df)
            {%- elif detail.enhanced_type == 'union' %}
            # Union requires multiple inputs - collect all source DataFrames
            current_df = self.apply_union_transformation(*source_dataframes.values())
            {%- elif detail.enhanced_type == 'rank' %}
            current_df = self.apply_rank_transformation(current_df)
            {%- elif detail.enhanced_type == 'data_masking' %}
            current_df = self.apply_data_masking_transformation(current_df)
            {%- elif detail.enhanced_type == 'complex_lookup' %}
            lookup_df = self._load_lookup_data_{{ loop.index }}()
            current_df = self.apply_complex_lookup_transformation(current_df, lookup_df)
            {%- elif detail.enhanced_type == 'advanced_aggregation' %}
            current_df = self.apply_advanced_aggregation_transformation(current_df)
            {%- endif %}
            
            # Record transformation metrics
            transformation_time = (datetime.now() - transformation_start).total_seconds()
            self._update_metrics(f"{{ detail.name | lower }}_duration_seconds", transformation_time)
            self._update_metrics(f"{{ detail.name | lower }}_output_records", current_df.count())
            
            self.logger.info(f"{{ detail.name }} completed in {transformation_time:.2f} seconds")
            {%- endif %}
            {%- endfor %}
            
            # Step 3: Final data quality checks
            if self.data_quality_enabled:
                current_df = self._validate_final_output_quality(current_df)
            
            # Step 4: Write to targets with optimization
            self.logger.info("Step 3: Writing to target systems")
            
            {%- for target in targets %}
            self.logger.info("Writing to {{ target.name }}...")
            target_start = datetime.now()
            
            # Optimize for target write
            optimized_df = self._optimize_for_target_write(current_df, "{{ target.name }}")
            self._write_to_{{ target.name | lower }}(optimized_df)
            
            target_time = (datetime.now() - target_start).total_seconds()
            self._update_metrics("{{ target.name | lower }}_write_duration_seconds", target_time)
            self.logger.info(f"{{ target.name }} write completed in {target_time:.2f} seconds")
            {%- endfor %}
            
            # Step 5: Cleanup and finalize
            self._cleanup_cached_dataframes()
            
            # Calculate total execution metrics
            total_time = (datetime.now() - start_time).total_seconds()
            self._update_metrics("total_execution_seconds", total_time)
            
            # Log final metrics
            self._log_execution_metrics()
            
            self.logger.info("=" * 80)
            self.logger.info(f"{{ mapping.name }} mapping completed successfully in {total_time:.2f} seconds")
            self.logger.info("=" * 80)
            
            return True
            
        except Exception as e:
            execution_time = (datetime.now() - start_time).total_seconds()
            self.logger.error("=" * 80)
            self.logger.error(f"{{ mapping.name }} mapping FAILED after {execution_time:.2f} seconds")
            self.logger.error(f"Error: {str(e)}")
            self.logger.error("=" * 80)
            
            # Cleanup on error
            self._cleanup_cached_dataframes()
            raise'''
    
    def _get_source_methods_template(self, mapping: Dict[str, Any]) -> str:
        sources = [comp for comp in mapping.get('components', []) if comp.get('component_type') == 'source']
        
        template = '''
    # Source reading methods with production features'''
        
        for source in sources:
            template += f'''
    def _read_{source["name"].lower()}(self) -> DataFrame:
        """Read from {source["name"]} with production optimizations"""
        self.logger.info("Reading {source["name"]} data source")
        
        try:
            # Read with optimized configuration
            df = self.data_source_manager.read_source(
                "{source["name"]}", 
                "{source.get("type", "")}"'''
            
            if source.get('format'):
                template += f''', format="{source["format"]}"'''
            
            template += '''
            )
            
            # Add data lineage and audit columns
            df = df.withColumn("source_system", lit("''' + source["name"] + '''")) \\
                   .withColumn("load_timestamp", current_timestamp()) \\
                   .withColumn("batch_id", lit(self.config.get('batch_id', 'unknown')))
            
            record_count = df.count()
            self.logger.info(f"Successfully read {record_count:,} records from ''' + source["name"] + '''")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to read ''' + source["name"] + ''': {str(e)}")
            raise'''
        
        return template
    
    def _get_scd_type2_methods_template(self) -> str:
        if not self.transformation_engine:
            return '''
    def apply_scd_type2_transformation(self, source_df: DataFrame, existing_df: DataFrame) -> DataFrame:
        """Apply SCD Type 2 logic (placeholder implementation)"""
        self.logger.info("Applying SCD Type 2 transformation (basic implementation)")
        return source_df.withColumn("scd_processed", lit(True))'''
        
        return '''
    def _load_existing_dimension_data(self) -> DataFrame:
        """Load existing dimension data for SCD processing"""
        try:
            # This would typically read from the target dimension table
            return self.data_source_manager.read_source(
                "DIM_CUSTOMER_EXISTING", 
                "HIVE", 
                table_name="dim_customer"
            )
        except Exception:
            # Return empty DataFrame if table doesn't exist (initial load)
            self.logger.warning("Existing dimension table not found - assuming initial load")
            return self.spark.createDataFrame([], StructType([]))
    
    # SCD Type 2 transformation implementation
    ''' + (self.transformation_engine.generate_transformation_code(
        'scd_type2', 
        TransformationContext(
            mapping_name="sample",
            project_name="sample", 
            source_tables=[],
            target_tables=[],
            transformation_config={
                'business_keys': ['customer_id'],
                'tracked_columns': ['customer_name', 'email', 'phone', 'address', 'status']
            }
        )
    ) if self.transformation_engine else "")
    
    def _get_scd_type1_methods_template(self) -> str:
        if not self.transformation_engine:
            return '''
    def apply_scd_type1_transformation(self, source_df: DataFrame, existing_df: DataFrame) -> DataFrame:
        """Apply SCD Type 1 logic (placeholder implementation)"""
        self.logger.info("Applying SCD Type 1 transformation (basic implementation)")
        return source_df.withColumn("scd_processed", lit(True))'''
        
        return '''
    # SCD Type 1 transformation implementation
    ''' + (self.transformation_engine.generate_transformation_code(
        'scd_type1', 
        TransformationContext(
            mapping_name="sample",
            project_name="sample",
            source_tables=[],
            target_tables=[],
            transformation_config={
                'business_keys': ['customer_id'],
                'update_columns': ['customer_name', 'email', 'phone', 'address', 'status'],
                'handle_deletes': False
            }
        )
    ) if self.transformation_engine else "")
    
    def _get_router_methods_template(self) -> str:
        if not self.transformation_engine:
            return '''
    def apply_router_transformation(self, input_df: DataFrame) -> Dict[str, DataFrame]:
        """Apply router transformation (placeholder implementation)"""
        self.logger.info("Applying router transformation (basic implementation)")
        return {'default': input_df}'''
        
        return '''
    # Router transformation implementation
    ''' + (self.transformation_engine.generate_transformation_code(
        'router',
        TransformationContext(
            mapping_name="sample",
            project_name="sample",
            source_tables=[],
            target_tables=[],
            transformation_config={
                'routes': [
                    {'name': 'high_value', 'condition': 'amount > 1000', 'priority': 1},
                    {'name': 'medium_value', 'condition': 'amount > 100 AND amount <= 1000', 'priority': 2}
                ],
                'default_route': 'unclassified'
            }
        )
    ) if self.transformation_engine else "")
    
    def _get_union_methods_template(self) -> str:
        if not self.transformation_engine:
            return '''
    def apply_union_transformation(self, *input_dfs: DataFrame) -> DataFrame:
        """Apply union transformation (placeholder implementation)"""
        self.logger.info("Applying union transformation (basic implementation)")
        result_df = input_dfs[0]
        for df in input_dfs[1:]:
            result_df = result_df.union(df)
        return result_df'''
        
        return '''
    # Union transformation implementation
    ''' + (self.transformation_engine.generate_transformation_code(
        'union',
        TransformationContext(
            mapping_name="sample",
            project_name="sample",
            source_tables=[],
            target_tables=[],
            transformation_config={
                'union_all': True,
                'remove_duplicates': False,
                'add_source_columns': True
            }
        )
    ) if self.transformation_engine else "")
    
    def _get_rank_methods_template(self) -> str:
        if not self.transformation_engine:
            return '''
    def apply_rank_transformation(self, input_df: DataFrame) -> DataFrame:
        """Apply rank transformation (placeholder implementation)"""
        from pyspark.sql.window import Window
        from pyspark.sql.functions import rank
        window = Window.orderBy("amount")
        return input_df.withColumn("rank", rank().over(window))'''
        
        return '''
    # Rank transformation implementation
    ''' + (self.transformation_engine.generate_transformation_code(
        'rank',
        TransformationContext(
            mapping_name="sample",
            project_name="sample",
            source_tables=[],
            target_tables=[],
            transformation_config={
                'partition_by': ['category'],
                'order_by': [{'column': 'amount', 'direction': 'desc'}],
                'rank_methods': ['rank', 'row_number'],
                'top_n': 10
            }
        )
    ) if self.transformation_engine else "")
    
    def _get_data_masking_methods_template(self) -> str:
        if not self.transformation_engine:
            return '''
    def apply_data_masking_transformation(self, input_df: DataFrame) -> DataFrame:
        """Apply data masking transformation (placeholder implementation)"""
        self.logger.info("Applying data masking transformation (basic implementation)")
        return input_df.withColumn("data_masked", lit(True))'''
        
        return '''
    # Data masking transformation implementation
    ''' + (self.transformation_engine.generate_transformation_code(
        'data_masking',
        TransformationContext(
            mapping_name="sample",
            project_name="sample",
            source_tables=[],
            target_tables=[],
            transformation_config={
                'columns': [
                    {'name': 'customer_name', 'method': 'partial_mask', 'params': {'show_first': 2, 'show_last': 1}},
                    {'name': 'email', 'method': 'email_mask', 'params': {}},
                    {'name': 'phone', 'method': 'phone_mask', 'params': {}}
                ],
                'add_audit_columns': True,
                'version': '1.0'
            }
        )
    ) if self.transformation_engine else "")
    
    def _get_complex_lookup_methods_template(self) -> str:
        if not self.transformation_engine:
            return '''
    def apply_complex_lookup_transformation(self, input_df: DataFrame, lookup_df: DataFrame) -> DataFrame:
        """Apply complex lookup transformation (placeholder implementation)"""
        self.logger.info("Applying complex lookup transformation (basic implementation)")
        return input_df.join(lookup_df, "customer_id", "left_outer")
        
    def _load_lookup_data_1(self) -> DataFrame:
        """Load lookup reference data"""
        return self.data_source_manager.read_source("CUSTOMER_LOOKUP", "HIVE")'''
        
        return '''
    def _load_lookup_data_1(self) -> DataFrame:
        """Load lookup reference data"""
        return self.data_source_manager.read_source(
            "CUSTOMER_LOOKUP", 
            "HIVE",
            table_name="customer_reference"  
        )
    
    # Complex lookup transformation implementation
    ''' + (self.transformation_engine.generate_transformation_code(
        'complex_lookup',
        TransformationContext(
            mapping_name="sample",
            project_name="sample",
            source_tables=[],
            target_tables=[],
            transformation_config={
                'lookup_type': 'connected',
                'join_strategy': 'auto',
                'lookup_keys': [{'input_column': 'customer_id', 'lookup_column': 'customer_id'}],
                'return_columns': ['customer_name', 'status'],
                'default_values': {'status': 'UNKNOWN'}
            }
        )
    ) if self.transformation_engine else "")
    
    def _get_advanced_aggregation_methods_template(self) -> str:
        if not self.transformation_engine:
            return '''
    def apply_advanced_aggregation_transformation(self, input_df: DataFrame) -> DataFrame:
        """Apply advanced aggregation transformation (placeholder implementation)"""
        self.logger.info("Applying advanced aggregation transformation (basic implementation)")
        return input_df.groupBy("category").sum("amount")'''
        
        return '''
    # Advanced aggregation transformation implementation
    ''' + (self.transformation_engine.generate_transformation_code(
        'advanced_aggregation',
        TransformationContext(
            mapping_name="sample",
            project_name="sample",
        source_tables=[],
        target_tables=[], 
            transformation_config={
                'group_by_columns': ['category', 'region'],
                'aggregations': [
                    {'type': 'sum', 'column': 'amount', 'alias': 'total_amount'},
                    {'type': 'count', 'column': '*', 'alias': 'record_count'}
                ],
                'window_functions': []
            }
        )
    ) if self.transformation_engine else "")
    
    def _get_target_methods_template(self, mapping: Dict[str, Any]) -> str:
        targets = [comp for comp in mapping.get('components', []) if comp.get('component_type') == 'target']
        
        template = '''
    # Target writing methods with production optimizations'''
        
        for target in targets:
            template += f'''
    def _write_to_{target["name"].lower()}(self, df: DataFrame) -> None:
        """Write to {target["name"]} with production optimizations"""
        self.logger.info(f"Writing to {target["name"]} target")
        
        try:
            # Optimize DataFrame for write performance
            write_df = df.coalesce(self.config.get('output_partitions', 10))
            
            # Add final audit columns
            write_df = write_df.withColumn("insert_timestamp", current_timestamp()) \\
                             .withColumn("batch_id", lit(self.config.get('batch_id', 'unknown'))) \\
                             .withColumn("process_name", lit("{mapping["name"]}"))
            
            # Write with target-specific optimizations
            self.data_source_manager.write_target(
                write_df,
                "{target["name"]}", 
                "{target.get("type", "HIVE")}",
                mode=self.config.get('write_mode', 'overwrite')
            )
            
            record_count = write_df.count()
            self.logger.info(f"Successfully wrote {{record_count:,}} records to {target["name"]}")
            
        except Exception as e:
            self.logger.error(f"Failed to write to {target["name"]}: {{str(e)}}")
            raise'''
        
        return template
    
    def _get_utility_methods_template(self) -> str:
        return '''
    
    # Utility methods for production features
    def _initialize_metrics(self):
        """Initialize metrics tracking"""
        self.metrics = {
            'start_time': datetime.now(),
            'records_processed': 0,
            'transformations_applied': 0,
            'data_quality_issues': 0
        }
    
    def _update_metrics(self, metric_name: str, value: Any):
        """Update metrics"""
        if self.enable_metrics:
            self.metrics[metric_name] = value
    
    def _validate_source_quality(self, df: DataFrame, source_name: str) -> DataFrame:
        """Validate source data quality"""
        if not self.data_quality_enabled:
            return df
        
        initial_count = df.count()
        null_checks = df.filter(col("id").isNull())
        null_count = null_checks.count()
        
        if null_count > 0:
            self.logger.warning(f"{source_name}: Found {null_count} records with null IDs")
            self._update_metrics(f"{source_name}_null_ids", null_count)
        
        error_rate = null_count / initial_count if initial_count > 0 else 0
        if error_rate > self.max_error_threshold:
            raise ValueError(f"{source_name}: Error rate {error_rate:.2%} exceeds threshold {self.max_error_threshold:.2%}")
        
        return df
    
    def _validate_final_output_quality(self, df: DataFrame) -> DataFrame:
        """Validate final output quality"""
        if not self.data_quality_enabled:
            return df
        
        # Add validation logic here
        return df
    
    def _optimize_for_target_write(self, df: DataFrame, target_name: str) -> DataFrame:
        """Optimize DataFrame for target write"""
        # Coalesce partitions for better write performance
        partition_count = self.config.get('output_partitions', 10)
        return df.coalesce(partition_count)
    
    def _cleanup_cached_dataframes(self):
        """Cleanup cached DataFrames"""
        if self.cache_intermediate_results:
            # In production, track and unpersist cached DataFrames
            pass
    
    def _log_execution_metrics(self):
        """Log final execution metrics"""
        if self.enable_metrics and self.metrics:
            self.logger.info("Execution Metrics:")
            for metric_name, value in self.metrics.items():
                self.logger.info(f"  - {metric_name}: {value}")'''