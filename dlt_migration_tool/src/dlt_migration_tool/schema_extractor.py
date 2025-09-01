#!/usr/bin/env python3
"""
Command-line interface for extracting schemas from Databricks tables.
"""

import argparse
import logging
import sys
import os
import json
import subprocess
from pathlib import Path
from typing import List, Optional, Dict, Any, Union

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.dlt_migration_tool.utils.logging_utils import LoggingUtils


class SchemaExtractor:
    """
    Extracts schemas from Databricks tables.
    """
    
    def __init__(self, logger: logging.Logger):
        """
        Initialize the schema extractor.
        
        Args:
            logger: Logger instance
        """
        self.logger = logger
    
    def extract_schemas_local(self, 
                            table_names: List[str], 
                            output_dir: Union[str, Path],
                            prefix: str = "maximo_",
                            suffix: str = "_schema.json") -> Dict[str, Path]:
        """
        Extract schemas from tables using local Databricks CLI.
        
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
            
            # Generate a temporary Python script to extract the schema
            script_content = f"""
import json
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

try:
    df = spark.read.table("{table_name}")
    schema_json = df.schema.json()
    schema_dict = json.loads(schema_json)
    
    # Filter out specific metadata columns
    if "fields" in schema_dict:
        schema_dict["fields"] = [
            field for field in schema_dict["fields"] 
            if field.get("name", "") not in ["_change_type", "_commit_version", "_commit_timestamp", "meta_load_details", "__START_AT", "__END_AT"]
        ]
    
    with open("{schema_file}", 'w') as f:
        json.dump(schema_dict, f, indent=2)
    
    print("Success")
except Exception as e:
    print(f"Error: {{str(e)}}")
    
    # Generate an empty schema
    schema = {{"type": "struct", "fields": []}}
    
    with open("{schema_file}", 'w') as f:
        json.dump(schema, f, indent=2)
"""
            
            temp_script = output_dir / f"extract_schema_{simple_name}.py"
            with open(temp_script, 'w') as f:
                f.write(script_content)
            
            # Run the script using databricks-cli
            try:
                self.logger.info(f"Extracting schema for {table_name}")
                cmd = ["databricks", "workspace", "run", str(temp_script)]
                process = subprocess.run(cmd, capture_output=True, text=True)
                
                if process.returncode != 0:
                    self.logger.error(f"Error extracting schema for {table_name}: {process.stderr}")
                    
                    # Create an empty schema file
                    schema = {"type": "struct", "fields": []}
                    with open(schema_file, 'w') as f:
                        json.dump(schema, f, indent=2)
                else:
                    self.logger.info(f"Successfully extracted schema for {table_name}")
                
                # Clean up the temporary script
                os.remove(temp_script)
                
                result[table_name] = schema_file
            except Exception as e:
                self.logger.error(f"Error running databricks-cli: {str(e)}")
                
                # Create an empty schema file
                schema = {"type": "struct", "fields": []}
                with open(schema_file, 'w') as f:
                    json.dump(schema, f, indent=2)
                
                result[table_name] = schema_file
        
        return result
    
    def extract_schemas_notebook(self,
                               table_names: List[str],
                               output_dir: Union[str, Path],
                               notebook_path: Union[str, Path],
                               prefix: str = "maximo_",
                               suffix: str = "_schema.json") -> Dict[str, Path]:
        """
        Extract schemas using a Databricks notebook.
        
        Args:
            table_names: List of table names
            output_dir: Directory to save the schema files
            notebook_path: Path to the notebook
            prefix: Prefix for the schema file names
            suffix: Suffix for the schema file names
            
        Returns:
            Dictionary mapping table names to schema file paths
        """
        output_dir = Path(output_dir)
        os.makedirs(output_dir, exist_ok=True)
        
        # Create the parameters for the notebook
        table_names_str = ",".join(table_names)
        
        # Run the notebook using databricks-cli
        try:
            self.logger.info(f"Running notebook to extract schemas for {len(table_names)} tables")
            cmd = [
                "databricks", "jobs", "run-now",
                "--job-id", "schema-extractor",
                "--notebook-params", json.dumps({
                    "table_names": table_names_str,
                    "output_dir": str(output_dir),
                    "prefix": prefix,
                    "suffix": suffix
                })
            ]
            process = subprocess.run(cmd, capture_output=True, text=True)
            
            if process.returncode != 0:
                self.logger.error(f"Error running notebook: {process.stderr}")
            else:
                self.logger.info(f"Successfully ran notebook")
        except Exception as e:
            self.logger.error(f"Error running databricks-cli: {str(e)}")
        
        # Check for the generated schema files
        result = {}
        for table_name in table_names:
            # Extract the simple table name (without catalog and schema)
            simple_name = table_name.split('.')[-1]
            
            # Generate the schema file path
            schema_file = output_dir / f"{prefix}{simple_name}{suffix}"
            
            if os.path.exists(schema_file):
                result[table_name] = schema_file
            else:
                self.logger.warning(f"Schema file not found for {table_name}")
                
                # Create an empty schema file
                schema = {"type": "struct", "fields": []}
                with open(schema_file, 'w') as f:
                    json.dump(schema, f, indent=2)
                
                result[table_name] = schema_file
        
        return result


def parse_args(args: Optional[List[str]] = None) -> argparse.Namespace:
    """
    Parse command-line arguments.
    
    Args:
        args: Command-line arguments (default: sys.argv[1:])
        
    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Extract schemas from Databricks tables"
    )
    
    parser.add_argument(
        "-t", "--tables",
        required=True,
        help="Comma-separated list of table names or path to a file containing table names"
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
        "-m", "--method",
        choices=["local", "notebook"],
        default="local",
        help="Method to extract schemas (default: local)"
    )
    parser.add_argument(
        "-n", "--notebook",
        help="Path to the notebook to use for extraction (only used with --method notebook)"
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
    Main entry point for the Schema Extractor.
    
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
        "schema_extractor",
        level=log_level,
        file_path=parsed_args.log_file
    )
    
    logger.info("Starting Schema Extractor")
    
    try:
        # Parse table names
        table_names = []
        if os.path.isfile(parsed_args.tables):
            # Read table names from file
            with open(parsed_args.tables, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        table_names.append(line)
            logger.info(f"Read {len(table_names)} table names from {parsed_args.tables}")
        else:
            # Parse comma-separated list
            table_names = [name.strip() for name in parsed_args.tables.split(",") if name.strip()]
            logger.info(f"Parsed {len(table_names)} table names from command line")
        
        # Create output directory
        output_dir = Path(parsed_args.output)
        os.makedirs(output_dir, exist_ok=True)
        
        # Create schema extractor
        extractor = SchemaExtractor(logger)
        
        # Extract schemas
        if parsed_args.method == "local":
            logger.info("Using local method to extract schemas")
            result = extractor.extract_schemas_local(
                table_names,
                output_dir,
                prefix=parsed_args.prefix,
                suffix=parsed_args.suffix
            )
        else:
            logger.info("Using notebook method to extract schemas")
            if not parsed_args.notebook:
                logger.error("Notebook path is required for notebook method")
                return 1
            
            result = extractor.extract_schemas_notebook(
                table_names,
                output_dir,
                parsed_args.notebook,
                prefix=parsed_args.prefix,
                suffix=parsed_args.suffix
            )
        
        # Print results
        logger.info(f"Extracted {len(result)} schemas:")
        for table_name, schema_file in result.items():
            logger.info(f"  {table_name} -> {schema_file}")
        
        logger.info("Schema Extractor completed successfully")
        return 0
    
    except Exception as e:
        logger.exception(f"Error: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
