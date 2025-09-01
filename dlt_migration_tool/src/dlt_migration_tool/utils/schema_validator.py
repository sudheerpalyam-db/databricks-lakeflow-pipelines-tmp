"""
Schema validation module for DLT Migration Tool.

This module provides functionality to validate generated dataflowspec JSON files
against the JSON Schema definitions from the Databricks LakeFlow Framework.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Union, Any
import jsonschema

logger = logging.getLogger(__name__)

class SchemaValidator:
    """
    Validates dataflowspec JSON files against JSON Schema definitions.
    """
    
    def __init__(self, schemas_dir: Optional[str] = None):
        """
        Initialize the SchemaValidator.
        
        Args:
            schemas_dir: Directory containing JSON Schema definition files.
                         If None, uses the default schemas directory in the package.
        """
        if schemas_dir is None:
            # Use the default schemas directory in the package
            current_dir = os.path.dirname(os.path.abspath(__file__))
            # Go up two levels to the src directory
            parent_dir = os.path.dirname(os.path.dirname(current_dir))
            schemas_dir = os.path.join(parent_dir, "dlt_migration_tool", "schemas")
        
        self.schemas_dir = schemas_dir
        self.schemas = {}
        self._load_schemas()
    
    def _load_schemas(self) -> None:
        """
        Load all JSON Schema definition files from the schemas directory.
        """
        try:
            schema_files = [f for f in os.listdir(self.schemas_dir) if f.endswith('.json')]
            for schema_file in schema_files:
                schema_path = os.path.join(self.schemas_dir, schema_file)
                with open(schema_path, 'r') as f:
                    schema = json.load(f)
                    self.schemas[schema_file] = schema
            
            logger.info(f"Loaded {len(self.schemas)} JSON Schema definition files")
        except Exception as e:
            logger.error(f"Error loading JSON Schema definition files: {str(e)}")
            raise
    
    def validate_file(self, file_path: str) -> Dict[str, Any]:
        """
        Validate a dataflowspec JSON file against the appropriate JSON Schema.
        
        Args:
            file_path: Path to the dataflowspec JSON file
            
        Returns:
            Dictionary with validation results
        """
        try:
            # Load the dataflowspec JSON
            with open(file_path, 'r') as f:
                dataflowspec = json.load(f)
            
            # Determine the appropriate schema based on the dataflowspec
            schema_file = self._determine_schema(dataflowspec)
            
            if schema_file not in self.schemas:
                return {
                    "valid": False,
                    "file": file_path,
                    "error": f"No appropriate schema found for the dataflowspec"
                }
            
            # Validate the dataflowspec against the schema
            jsonschema.validate(dataflowspec, self.schemas[schema_file])
            
            return {
                "valid": True,
                "file": file_path,
                "schema": schema_file
            }
        except jsonschema.exceptions.ValidationError as e:
            return {
                "valid": False,
                "file": file_path,
                "error": str(e),
                "schema": schema_file if 'schema_file' in locals() else None
            }
        except Exception as e:
            return {
                "valid": False,
                "file": file_path,
                "error": str(e)
            }
    
    def _determine_schema(self, dataflowspec: Dict[str, Any]) -> str:
        """
        Determine the appropriate schema file for a dataflowspec.
        
        Args:
            dataflowspec: The dataflowspec JSON object
            
        Returns:
            Name of the appropriate schema file
        """
        # Check if it's a standard dataflowspec
        if "dataFlowType" in dataflowspec and dataflowspec["dataFlowType"] == "standard":
            return "spec_standard.json"
        
        # Check if it's a flow dataflowspec
        if "dataFlowType" in dataflowspec and dataflowspec["dataFlowType"] == "flow":
            return "spec_flows.json"
        
        # Check if it's a materialized view dataflowspec
        if "dataFlowType" in dataflowspec and dataflowspec["dataFlowType"] == "materialized_view":
            return "spec_materialized_views.json"
        
        # Default to standard schema
        return "spec_standard.json"
    
    def validate_directory(self, directory_path: str, recursive: bool = False) -> Dict[str, Any]:
        """
        Validate all dataflowspec JSON files in a directory.
        
        Args:
            directory_path: Path to the directory containing dataflowspec JSON files
            recursive: Whether to search for files recursively
            
        Returns:
            Dictionary with validation results
        """
        results = {
            "valid": 0,
            "invalid": 0,
            "files": []
        }
        
        # Find all JSON files in the directory
        if recursive:
            json_files = []
            for root, _, files in os.walk(directory_path):
                for file in files:
                    if file.endswith('.json'):
                        json_files.append(os.path.join(root, file))
        else:
            json_files = [os.path.join(directory_path, f) for f in os.listdir(directory_path) 
                         if f.endswith('.json') and os.path.isfile(os.path.join(directory_path, f))]
        
        # Validate each file
        for file_path in json_files:
            result = self.validate_file(file_path)
            results["files"].append(result)
            
            if result["valid"]:
                results["valid"] += 1
            else:
                results["invalid"] += 1
        
        return results
    
    def validate_and_report(self, path: str, recursive: bool = False) -> None:
        """
        Validate dataflowspec JSON files and print a report.
        
        Args:
            path: Path to a file or directory to validate
            recursive: Whether to search for files recursively (only applies if path is a directory)
        """
        if os.path.isfile(path):
            result = self.validate_file(path)
            if result["valid"]:
                logger.info(f"VALID: {path} is valid according to schema {result.get('schema', 'unknown')}")
            else:
                logger.error(f"ERROR: {path} is invalid: {result.get('error', 'unknown error')}")
        elif os.path.isdir(path):
            results = self.validate_directory(path, recursive)
            logger.info(f"Validation results for {path}:")
            logger.info(f"- Valid files: {results['valid']}")
            logger.info(f"- Invalid files: {results['invalid']}")
            
            if results["invalid"] > 0:
                logger.info("Invalid files:")
                for result in results["files"]:
                    if not result["valid"]:
                        logger.error(f"ERROR: {result['file']}: {result.get('error', 'unknown error')}")
        else:
            logger.error(f"Path not found: {path}")


def validate_dataflowspec(file_path: str, schemas_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Validate a dataflowspec JSON file against the appropriate JSON Schema.
    
    Args:
        file_path: Path to the dataflowspec JSON file
        schemas_dir: Directory containing JSON Schema definition files
        
    Returns:
        Dictionary with validation results
    """
    validator = SchemaValidator(schemas_dir)
    return validator.validate_file(file_path)


def validate_directory(directory_path: str, recursive: bool = False, schemas_dir: Optional[str] = None) -> Dict[str, Any]:
    """
    Validate all dataflowspec JSON files in a directory.
    
    Args:
        directory_path: Path to the directory containing dataflowspec JSON files
        recursive: Whether to search for files recursively
        schemas_dir: Directory containing JSON Schema definition files
        
    Returns:
        Dictionary with validation results
    """
    validator = SchemaValidator(schemas_dir)
    return validator.validate_directory(directory_path, recursive)


if __name__ == "__main__":
    import argparse
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Validate dataflowspec JSON files against JSON Schema definitions')
    parser.add_argument('path', help='Path to a file or directory to validate')
    parser.add_argument('--recursive', '-r', action='store_true', help='Search for files recursively')
    parser.add_argument('--schemas-dir', help='Directory containing JSON Schema definition files')
    args = parser.parse_args()
    
    # Validate the specified path
    validator = SchemaValidator(args.schemas_dir)
    validator.validate_and_report(args.path, args.recursive)
