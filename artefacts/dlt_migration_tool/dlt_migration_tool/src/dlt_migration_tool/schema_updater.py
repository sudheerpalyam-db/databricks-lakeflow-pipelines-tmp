#!/usr/bin/env python3
"""
Command-line interface for updating schema files in DLT Framework configurations.
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

from src.dlt_migration_tool.utils.schema_utils import SchemaGenerator
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
        description="Update schema files in DLT Framework configurations"
    )
    
    # Input/output options
    parser.add_argument(
        "-s", "--schema-dir",
        required=True,
        help="Directory containing schema files"
    )
    parser.add_argument(
        "-c", "--config-dir",
        required=True,
        help="Directory containing DLT Framework configurations"
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


def update_schema_references(config_dir: Path, schema_dir: Path, logger: logging.Logger) -> Dict[str, List[str]]:
    """
    Update schema references in DLT Framework configurations.
    
    Args:
        config_dir: Directory containing DLT Framework configurations
        schema_dir: Directory containing schema files
        logger: Logger instance
        
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
                logger.warning(f"Schema file not found: {schema_file}")
                continue
            
            # Add the configuration file to the result
            if str(schema_file) not in result:
                result[str(schema_file)] = []
            result[str(schema_file)].append(str(config_file))
            
            logger.info(f"Found schema reference: {config_file} -> {schema_file}")
        except Exception as e:
            logger.error(f"Error processing {config_file}: {str(e)}")
    
    return result


def main(args: Optional[List[str]] = None) -> int:
    """
    Main entry point for the Schema Updater.
    
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
        "schema_updater",
        level=log_level,
        file_path=parsed_args.log_file
    )
    
    logger.info("Starting Schema Updater")
    logger.info(f"Schema Directory: {parsed_args.schema_dir}")
    logger.info(f"Config Directory: {parsed_args.config_dir}")
    
    try:
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
        result = update_schema_references(config_dir, schema_dir, logger)
        
        # Print results
        logger.info(f"Found {len(result)} schema references:")
        for schema_file, config_files in result.items():
            logger.info(f"  {schema_file} -> {len(config_files)} configurations")
        
        logger.info("Schema Updater completed successfully")
        return 0
    
    except Exception as e:
        logger.exception(f"Error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
