from core.spark_code_generator import SparkCodeGenerator

# Set up logging to see detailed output from the parser
# Force the XMLParser logger to DEBUG level for detailed tracing
logging.basicConfig(level=logging.INFO, format='%(levelname)s:%(name)s:%(message)s')
logging.getLogger("XMLParser").setLevel(logging.DEBUG)

print('🧪 Running Generation with FORCED DEBUG LOGGING')
print('=' * 60) 