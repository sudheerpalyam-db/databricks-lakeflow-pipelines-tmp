#!/usr/bin/env python3
"""
Command-line interface for generating schema files.
"""

import argparse
import logging
import sys
import os
import json
from pathlib import Path
from typing import List, Optional, Dict, Any

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.dlt_migration_tool.utils.schema_utils import SchemaGenerator, generate_schema_script
from src.dlt_migration_tool.utils.logging_utils import LoggingUtils


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Args:
        args: Command-line arguments (default: sys.argv[1:])
        
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Generate schema files for DLT Framework"
    )
    
    # Input/output options
    parser.add_argument(
        "-t", "--table",
        required=True,
        help="Table name or path to a file containing table names"
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Output directory path"
    )
    parser.add_argument(
        "-p", "--prefix",
        default="maximo_",
        help="Prefix for schema file names (default: maximo_)"
    )
    parser.add_argument(
        "-s", "--suffix",
        default="_schema.json",
        help="Suffix for schema file names (default: _schema.json)"
    )
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
    Main entry point for the Schema Generator.
    
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
        "schema_generator",
        level=log_level,
        file_path=parsed_args.log_file
    )
    
    logger.info("Starting Schema Generator")
    logger.info(f"Table: {parsed_args.table}")
    logger.info(f"Output: {parsed_args.output}")
    
    try:
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
        result = SchemaGenerator.generate_schema_files(
            table_names,
            output_dir,
            prefix=parsed_args.prefix,
            suffix=parsed_args.suffix
        )
        
        # Print results
        logger.info(f"Generated {len(result)} schema files:")
        for table_name, schema_file in result.items():
            logger.info(f"  {table_name} -> {schema_file}")
        
        # Generate script for each table
        for table_name in table_names:
            simple_name = table_name.split('.')[-1]
            script_file = output_dir / f"extract_schema_{simple_name}.py"
            schema_file = output_dir / f"{parsed_args.prefix}{simple_name}{parsed_args.suffix}"
            
            script = generate_schema_script(table_name, str(schema_file))
            
            with open(script_file, 'w') as f:
                f.write(script)
            
            logger.info(f"Generated script: {script_file}")
        
        logger.info("Schema Generator completed successfully")
        return 0
    
    except Exception as e:
        logger.exception(f"Error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
