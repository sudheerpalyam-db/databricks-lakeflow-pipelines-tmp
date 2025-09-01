#!/usr/bin/env python3
"""
Command-line interface for the DLT Migration Tool.
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

from src.dlt_migration_tool.core.transformer import MigrationProcessor
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
        description="Migrate Databricks DLT-meta JSON configurations to DLT Framework dataflowspec format"
    )
    
    # Input/output options
    parser.add_argument(
        "-i", "--input",
        required=True,
        help="Input file or directory path"
    )
    parser.add_argument(
        "-o", "--output",
        required=True,
        help="Output directory path"
    )
    
    # Processing options
    parser.add_argument(
        "-r", "--recursive",
        action="store_true",
        help="Process subdirectories recursively (when input is a directory)"
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


def organize_output_files(output_dir: Path, logger: logging.Logger) -> None:
    """
    Organize output files into bronze and silver directories.
    
    Args:
        output_dir: Directory containing output files
        logger: Logger instance
    """
    logger.info("Organizing output files into bronze and silver directories")
    
    # Create bronze and silver directories
    bronze_dir = output_dir / "bronze_maximo" / "dataflowspec"
    silver_dir = output_dir / "silver_maximo" / "dataflowspec"
    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(silver_dir, exist_ok=True)
    
    # Create schema directories
    bronze_schema_dir = output_dir / "bronze_maximo" / "schemas"
    silver_schema_dir = output_dir / "silver_maximo" / "schemas"
    os.makedirs(bronze_schema_dir, exist_ok=True)
    os.makedirs(silver_schema_dir, exist_ok=True)
    
    # Process all JSON files in the output directory
    for file_path in output_dir.glob("*.json"):
        try:
            # Read the JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Extract dataFlowGroup and table name
            dataflow_group = data.get("dataFlowGroup", "")
            dataflow_id = data.get("dataFlowId", "")
            table_name = data.get("targetDetails", {}).get("table", "")
            
            # Skip if no dataflow ID
            if not dataflow_id:
                continue
            
            # Determine target directory based on dataFlowGroup
            if "silver" in dataflow_group:
                target_dir = silver_dir
                schema_dir = silver_schema_dir
                
                # Update silver-specific fields
                if "sourceDetails" in data:
                    # Remove path and readerOptions
                    if "path" in data["sourceDetails"]:
                        del data["sourceDetails"]["path"]
                    if "readerOptions" in data["sourceDetails"]:
                        del data["sourceDetails"]["readerOptions"]
                    
                    # Set database and cdfEnabled
                    data["sourceDetails"]["database"] = "edp_bronze_dev.maximo_lakeflow"
                    data["sourceDetails"]["cdfEnabled"] = True
                
                # Update sourceType
                data["sourceType"] = "delta"
                
                # Update sourceViewName if present
                if "sourceViewName" not in data and table_name:
                    # Use the same view name format as bronze level
                    data["sourceViewName"] = f"v_{table_name}"
                
                # Convert cdcApplyChanges to cdcSettings
                if "cdcApplyChanges" in data:
                    data["cdcSettings"] = data["cdcApplyChanges"]
                    del data["cdcApplyChanges"]
                    
                    # Add ignore_null_updates if not present
                    if "ignore_null_updates" not in data["cdcSettings"]:
                        data["cdcSettings"]["ignore_null_updates"] = False
                
                # Update schema path
                if "targetDetails" in data and "schemaPath" in data["targetDetails"]:
                    data["targetDetails"]["schemaPath"] = f"maximo_{table_name}_schema.json"
            else:
                target_dir = bronze_dir
                schema_dir = bronze_schema_dir
                
                # Add bronze-specific fields
                # Add dataQualityExpectationsEnabled if not present
                if "dataQualityExpectationsEnabled" not in data:
                    data["dataQualityExpectationsEnabled"] = True
                
                # Add dataQualityExpectationsPath if not present
                if "dataQualityExpectationsPath" not in data and table_name:
                    data["dataQualityExpectationsPath"] = f"./maximo_{table_name}_dqe.json"
                
                # Add quarantineMode if not present
                if "quarantineMode" not in data:
                    data["quarantineMode"] = "table"
                
                # Add quarantineTargetDetails if not present
                if "quarantineTargetDetails" not in data:
                    data["quarantineTargetDetails"] = {
                        "targetFormat": "delta"
                    }
                
                # Remove fields that shouldn't be in bronze level
                if "cdcSettings" in data:
                    del data["cdcSettings"]
                
                # Add sourceViewName if not present
                if "sourceViewName" not in data and table_name:
                    data["sourceViewName"] = f"v_{table_name}"
                
                # Remove database and table from sourceDetails for bronze level
                if "sourceDetails" in data:
                    if "database" in data["sourceDetails"]:
                        del data["sourceDetails"]["database"]
                    if "table" in data["sourceDetails"]:
                        del data["sourceDetails"]["table"]
            
            # Remove path from targetDetails if it's "Ignore" or "ignore"
            if "targetDetails" in data and "path" in data["targetDetails"]:
                path_value = data["targetDetails"]["path"]
                if path_value == "Ignore" or path_value == "ignore":
                    del data["targetDetails"]["path"]
            
            # Create empty schema file
            if table_name:
                schema_file_name = f"maximo_{table_name}_schema.json"
                
                schema_file = schema_dir / schema_file_name
                if not schema_file.exists():
                    with open(schema_file, 'w') as f:
                        json.dump({"type": "struct", "fields": []}, f, indent=2)
            
            # Use the correct filename with _main suffix
            if table_name:
                target_file = target_dir / f"maximo_{table_name}_main.json"
            else:
                # Fallback to dataflow_id if table name is not available
                target_file = target_dir / f"{dataflow_id}_main.json"
            
            # Reorder fields to match the reference format for better readability
            ordered_data = {}
            field_order = [
                "dataFlowId", "dataFlowGroup", "dataFlowType", "sourceSystem", "sourceType", 
                "sourceViewName", "sourceDetails", "mode", "targetFormat", "targetDetails", 
                "dataQualityExpectationsEnabled", "dataQualityExpectationsPath", 
                "quarantineMode", "quarantineTargetDetails", "cdcSettings"
            ]
            
            # Add fields in the specified order if they exist
            for field in field_order:
                if field in data:
                    ordered_data[field] = data[field]
            
            # Add any remaining fields that weren't in the order list
            for field in data:
                if field not in ordered_data:
                    ordered_data[field] = data[field]
            
            # Write the updated data to the target file
            with open(target_file, 'w') as f:
                json.dump(ordered_data, f, indent=2)
            
            logger.info(f"Organized {file_path.name} to {target_file}")
            
            # Delete the original file
            os.remove(file_path)
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")


def main(args: Optional[List[str]] = None) -> int:
    """
    Main entry point for the DLT Migration Tool.
    
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
        "dlt_migration_tool",
        level=log_level,
        file_path=parsed_args.log_file
    )
    
    logger.info("Starting DLT Migration Tool")
    logger.info(f"Input: {parsed_args.input}")
    logger.info(f"Output: {parsed_args.output}")
    
    try:
        # Initialize processor
        processor = MigrationProcessor(verbose=parsed_args.verbose)
        
        # Process input
        input_path = Path(parsed_args.input)
        output_path = Path(parsed_args.output)
        
        if input_path.is_file():
            logger.info("Processing single file")
            output_files = processor.process_file(input_path, output_path)
            logger.info(f"Generated {len(output_files)} output files")
        elif input_path.is_dir():
            logger.info("Processing directory")
            results = processor.process_directory(
                input_path,
                output_path,
                recursive=parsed_args.recursive
            )
            
            # Count total output files
            total_outputs = sum(len(outputs) for outputs in results.values())
            logger.info(f"Processed {len(results)} input files, generated {total_outputs} output files")
        else:
            logger.error(f"Input path does not exist: {input_path}")
            return 1
        
        # Organize output files
        organize_output_files(output_path, logger)
        
        logger.info("DLT Migration Tool completed successfully")
        return 0
    
    except Exception as e:
        logger.exception(f"Error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
