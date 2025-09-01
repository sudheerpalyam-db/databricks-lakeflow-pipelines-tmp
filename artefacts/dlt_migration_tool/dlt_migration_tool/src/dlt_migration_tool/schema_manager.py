#!/usr/bin/env python3
"""
Command-line interface for generating and updating schema files for DLT Framework.
"""

import argparse
import logging
import sys
import os
import json
from pathlib import Path
from typing import List, Optional, Dict, Any, Union

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.dlt_migration_tool.utils.schema_utils import SchemaGenerator, generate_schema_script
from src.dlt_migration_tool.utils.logging_utils import LoggingUtils


class SchemaManager:
    """
    Manager for generating and updating schema files for DLT Framework.
    """
    
    def __init__(self, logger: logging.Logger):
        """
        Initialize the schema manager.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger
    
    def generate_schemas(self, 
                        table_names: List[str], 
                        output_dir: Union[str, Path],
                        prefix: str = "maximo_",
                        suffix: str = "_schema.json") -> Dict[str, Path]:
        """
        Generate schema files for a list of tables.
        
        Args:
            table_names: List of table names
            output_dir: Directory to save the schema files
            prefix: Prefix for the schema file names
            suffix: Suffix for the schema file names
            
        Returns:
            Dictionary mapping table names to schema file paths
        """
        output_dir = Path(output_dir)
        os.makedirs(output_dir, exist_ok=True)
        
        result = {}
        for table_name in table_names:
            # Extract the simple table name (without catalog and schema)
            simple_name = table_name.split('.')[-1]
            
            # Generate the schema file path
            schema_file = output_dir / f"{prefix}{simple_name}{suffix}"
            
            # Generate an empty schema
            schema = SchemaGenerator.generate_empty_schema()
            
            # Save the schema
            SchemaGenerator.save_schema(schema, schema_file)
            
            result[table_name] = schema_file
            
            # Generate script for extracting schema
            script_file = output_dir / f"extract_schema_{simple_name}.py"
            script = generate_schema_script(table_name, str(schema_file))
            
            with open(script_file, 'w') as f:
                f.write(script)
            
            self.logger.info(f"Generated script: {script_file}")
        
        return result
    
    def update_schema_references(self, 
                               config_dir: Path, 
                               schema_dir: Path) -> Dict[str, List[str]]:
        """
        Update schema references in DLT Framework configurations.
        
        Args:
            config_dir: Directory containing DLT Framework configurations
            schema_dir: Directory containing schema files
            
        Returns:
            Dictionary mapping schema files to the configurations that reference them
        """
        result = {}
        
        # Find all JSON files in the config directory
        for config_file in config_dir.glob("**/*.json"):
            try:
                # Read the configuration file
                with open(config_file, 'r') as f:
                    config = json.load(f)
                
                # Check if the configuration has a targetDetails.schemaPath field
                schema_path = config.get("targetDetails", {}).get("schemaPath")
                if not schema_path:
                    continue
                
                # Get the schema file name
                schema_file_name = os.path.basename(schema_path)
                schema_file = schema_dir / schema_file_name
                
                # Check if the schema file exists
                if not schema_file.exists():
                    self.logger.warning(f"Schema file not found: {schema_file}")
                    continue
                
                # Add the configuration file to the result
                if str(schema_file) not in result:
                    result[str(schema_file)] = []
                result[str(schema_file)].append(str(config_file))
                
                self.logger.info(f"Found schema reference: {config_file} -> {schema_file}")
            except Exception as e:
                self.logger.error(f"Error processing {config_file}: {str(e)}")
        
        return result
    
    def extract_schema_from_table(self, 
                                table_name: str, 
                                output_file: Union[str, Path]) -> None:
        """
        Extract schema from a table and save it to a file.
        
        This method should be run in a Databricks environment with access to the table.
        
        Args:
            table_name: Fully qualified table name (e.g., "edp_bronze_dev.maximo.asset")
            output_file: Path to save the schema
        """
        try:
            # This code will only work in a Databricks environment
            from pyspark.sql import SparkSession
            
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.table(table_name)
            schema_json = df.schema.json()
            schema_dict = json.loads(schema_json)
            
            # Save the schema to a file
            output_file = Path(output_file)
            os.makedirs(output_file.parent, exist_ok=True)
            
            with open(output_file, 'w') as f:
                json.dump(schema_dict, f, indent=2)
            
            self.logger.info(f"Extracted schema from table {table_name} and saved to {output_file}")
        except Exception as e:
            self.logger.error(f"Error extracting schema from table {table_name}: {str(e)}")


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Args:
        args: Command-line arguments (default: sys.argv[1:])
        
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Generate and update schema files for DLT Framework"
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Command to run")
    
    # Generate command
    generate_parser = subparsers.add_parser("generate", help="Generate schema files")
    generate_parser.add_argument(
        "-t", "--table",
        required=True,
        help="Table name or path to a file containing table names"
    )
    generate_parser.add_argument(
        "-o", "--output",
        required=True,
        help="Output directory path"
    )
    generate_parser.add_argument(
        "-p", "--prefix",
        default="maximo_",
        help="Prefix for schema file names (default: maximo_)"
    )
    generate_parser.add_argument(
        "-s", "--suffix",
        default="_schema.json",
        help="Suffix for schema file names (default: _schema.json)"
    )
    
    # Update command
    update_parser = subparsers.add_parser("update", help="Update schema references")
    update_parser.add_argument(
        "-s", "--schema-dir",
        required=True,
        help="Directory containing schema files"
    )
    update_parser.add_argument(
        "-c", "--config-dir",
        required=True,
        help="Directory containing DLT Framework configurations"
    )
    
    # Extract command
    extract_parser = subparsers.add_parser("extract", help="Extract schema from a table")
    extract_parser.add_argument(
        "-t", "--table",
        required=True,
        help="Table name"
    )
    extract_parser.add_argument(
        "-o", "--output",
        required=True,
        help="Output file path"
    )
    
    # Common options
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output"
    )
    parser.add_argument(
        "--log-file",
        help="Path to log file"
    )
    
    return parser.parse_args(args)


def main(args: Optional[List[str]] = None) -> int:
    """
    Main entry point for the Schema Manager.
    
    Args:
        args: Command-line arguments (default: sys.argv[1:])
        
    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    # Parse arguments
    parsed_args = parse_args(args)
    
    # Set up logging
    log_level = logging.DEBUG if parsed_args.verbose else logging.INFO
    logger = LoggingUtils.setup_logger(
        "schema_manager",
        level=log_level,
        file_path=parsed_args.log_file
    )
    
    logger.info(f"Starting Schema Manager: {parsed_args.command}")
    
    try:
        # Create schema manager
        manager = SchemaManager(logger)
        
        if parsed_args.command == "generate":
            # Determine if the input is a file or a table name
            table_names = []
            if os.path.isfile(parsed_args.table):
                # Read table names from file
                with open(parsed_args.table, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line and not line.startswith('#'):
                            table_names.append(line)
                logger.info(f"Read {len(table_names)} table names from {parsed_args.table}")
            else:
                # Single table name
                table_names = [parsed_args.table]
            
            # Create output directory
            output_dir = Path(parsed_args.output)
            os.makedirs(output_dir, exist_ok=True)
            
            # Generate schema files
            result = manager.generate_schemas(
                table_names,
                output_dir,
                prefix=parsed_args.prefix,
                suffix=parsed_args.suffix
            )
            
            # Print results
            logger.info(f"Generated {len(result)} schema files:")
            for table_name, schema_file in result.items():
                logger.info(f"  {table_name} -> {schema_file}")
            
        elif parsed_args.command == "update":
            # Convert paths to Path objects
            schema_dir = Path(parsed_args.schema_dir)
            config_dir = Path(parsed_args.config_dir)
            
            # Check if the directories exist
            if not schema_dir.exists():
                logger.error(f"Schema directory does not exist: {schema_dir}")
                return 1
            
            if not config_dir.exists():
                logger.error(f"Config directory does not exist: {config_dir}")
                return 1
            
            # Update schema references
            result = manager.update_schema_references(config_dir, schema_dir)
            
            # Print results
            logger.info(f"Found {len(result)} schema references:")
            for schema_file, config_files in result.items():
                logger.info(f"  {schema_file} -> {len(config_files)} configurations")
            
        elif parsed_args.command == "extract":
            # Extract schema from table
            manager.extract_schema_from_table(parsed_args.table, parsed_args.output)
            
        else:
            logger.error(f"Unknown command: {parsed_args.command}")
            return 1
        
        logger.info(f"Schema Manager {parsed_args.command} completed successfully")
        return 0
    
    except Exception as e:
        logger.exception(f"Error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
