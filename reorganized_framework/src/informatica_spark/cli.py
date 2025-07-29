"""
Command Line Interface for Informatica Spark Framework
"""

import argparse
import sys
from pathlib import Path
from .core.generators.spark_generator import SparkGenerator


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="Informatica Spark Framework - Convert BDM projects to Spark applications"
    )
    
    parser.add_argument(
        "input_xml",
        help="Path to Informatica project XML file"
    )
    
    parser.add_argument(
        "output_dir", 
        help="Output directory for generated Spark application"
    )
    
    parser.add_argument(
        "--project-name",
        default="GeneratedSparkProject",
        help="Name of the generated project (default: GeneratedSparkProject)"
    )
    
    parser.add_argument(
        "--enterprise-features",
        action="store_true",
        help="Enable enterprise features (monitoring, validation, etc.)"
    )
    
    parser.add_argument(
        "--config-externalization", 
        action="store_true",
        help="Enable configuration externalization"
    )
    
    parser.add_argument(
        "--version",
        action="version",
        version="%(prog)s 2.0.0"
    )
    
    args = parser.parse_args()
    
    # Validate input file exists
    input_path = Path(args.input_xml)
    if not input_path.exists():
        print(f"Error: Input file '{input_path}' does not exist", file=sys.stderr)
        sys.exit(1)
    
    # Create output directory if it doesn't exist
    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    try:
        # Initialize generator
        generator = SparkGenerator(
            output_base_dir=str(output_path),
            enable_config_externalization=args.config_externalization,
            enterprise_features=args.enterprise_features
        )
        
        # Generate application
        print(f"Generating Spark application from {input_path}...")
        app_path = generator.generate_spark_application(
            str(input_path),
            args.project_name
        )
        
        print(f"✅ Successfully generated Spark application at: {app_path}")
        
    except Exception as e:
        print(f"Error generating application: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()