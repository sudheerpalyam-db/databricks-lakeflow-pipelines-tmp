"""
Utility for generating schema files from existing delta tables.
"""

import json
import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional, List, Union

logger = logging.getLogger(__name__)

class SchemaGenerator:
    """
    Utility for generating schema files from existing delta tables.
    """
    
    @staticmethod
    def generate_empty_schema() -> Dict[str, Any]:
        """
        Generate an empty schema structure.
        
        Returns:
            Empty schema dictionary
        """
        return {
            "type": "struct",
            "fields": []
        }
    
    @staticmethod
    def save_schema(schema: Dict[str, Any], file_path: Union[str, Path]) -> None:
        """
        Save schema to a file.
        
        Args:
            schema: Schema dictionary
            file_path: Path to save the schema
        """
        file_path = Path(file_path)
        os.makedirs(file_path.parent, exist_ok=True)
        
        with open(file_path, 'w') as f:
            json.dump(schema, f, indent=2)
        
        logger.info(f"Saved schema to {file_path}")
    
    @staticmethod
    def load_schema(file_path: Union[str, Path]) -> Dict[str, Any]:
        """
        Load schema from a file.
        
        Args:
            file_path: Path to the schema file
            
        Returns:
            Schema dictionary
        """
        file_path = Path(file_path)
        
        if not file_path.exists():
            logger.warning(f"Schema file not found: {file_path}")
            return SchemaGenerator.generate_empty_schema()
        
        with open(file_path, 'r') as f:
            schema = json.load(f)
        
        logger.info(f"Loaded schema from {file_path}")
        return schema
    
    @staticmethod
    def extract_schema_from_spark(table_name: str) -> Dict[str, Any]:
        """
        Extract schema from a Spark table.
        
        This method should be run in a Databricks environment with access to the table.
        
        Args:
            table_name: Fully qualified table name (e.g., "edp_bronze_dev.maximo.asset")
            
        Returns:
            Schema dictionary
        """
        try:
            # This code will only work in a Databricks environment
            from pyspark.sql import SparkSession
            
            spark = SparkSession.builder.getOrCreate()
            df = spark.read.table(table_name)
            schema_json = df.schema.json()
            schema_dict = json.loads(schema_json)
            
            # Filter out specific metadata columns
            if "fields" in schema_dict:
                schema_dict["fields"] = [
                    field for field in schema_dict["fields"] 
                    if field.get("name", "") not in ["_change_type", "_commit_version", "_commit_timestamp", "meta_load_details", "__START_AT", "__END_AT"]
                ]
            
            logger.info(f"Extracted schema from table {table_name}")
            return schema_dict
        except Exception as e:
            logger.error(f"Error extracting schema from table {table_name}: {str(e)}")
            return SchemaGenerator.generate_empty_schema()
    
    @staticmethod
    def extract_schema_from_databricks_api(table_name: str, 
                                          workspace_url: str, 
                                          token: str) -> Dict[str, Any]:
        """
        Extract schema from a Databricks table using the REST API.
        
        Args:
            table_name: Fully qualified table name (e.g., "edp_bronze_dev.maximo.asset")
            workspace_url: Databricks workspace URL
            token: Databricks API token
            
        Returns:
            Schema dictionary
        """
        try:
            import requests
            
            # Parse the table name
            parts = table_name.split('.')
            if len(parts) != 3:
                raise ValueError(f"Invalid table name format: {table_name}. Expected format: catalog.schema.table")
            
            catalog, schema, table = parts
            
            # Construct the API URL
            api_url = f"{workspace_url}/api/2.0/unity-catalog/tables/{catalog}.{schema}.{table}"
            
            # Set up headers
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            # Make the API request
            response = requests.get(api_url, headers=headers)
            response.raise_for_status()
            
            # Extract the schema from the response
            table_info = response.json()
            columns = table_info.get("columns", [])
            
            # Convert to the expected schema format
            schema = {
                "type": "struct",
                "fields": []
            }
            
            for column in columns:
                field = {
                    "name": column["name"],
                    "type": column["type_text"],
                    "nullable": column.get("nullable", True),
                    "metadata": column.get("metadata", {})
                }
                schema["fields"].append(field)
            
            logger.info(f"Extracted schema from table {table_name} using Databricks API")
            return schema
        except Exception as e:
            logger.error(f"Error extracting schema from table {table_name} using Databricks API: {str(e)}")
            return SchemaGenerator.generate_empty_schema()
    
    @staticmethod
    def generate_schema_files(table_names: List[str], 
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
        
        return result


def generate_schema_script(table_name: str, output_path: str) -> str:
    """
    Generate a Python script to extract schema from a table.
    
    Args:
        table_name: Fully qualified table name (e.g., "edp_bronze_dev.maximo.asset")
        output_path: Path to save the schema
        
    Returns:
        Python script as a string
    """
    script = f'''
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# Create a Spark session
spark = SparkSession.builder.getOrCreate()

# Read the table
df = spark.read.table("{table_name}")

# Get the schema
schema = df.schema
schema_json = schema.json()

# Convert to the expected format
schema_dict = json.loads(schema_json)

# Filter out specific metadata columns
if "fields" in schema_dict:
    schema_dict["fields"] = [
        field for field in schema_dict["fields"] 
        if field.get("name", "") not in ["_change_type", "_commit_version", "_commit_timestamp", "meta_load_details", "__START_AT", "__END_AT"]
    ]

# Save the schema to a file
with open("{output_path}", "w") as f:
    json.dump(schema_dict, f, indent=2)

print(f"Schema saved to {output_path}")

# Print field details for reference
print("\\nField details:")
for field in schema.fields:
    if field.name not in ["_change_type", "_commit_version", "_commit_timestamp", "meta_load_details", "__START_AT", "__END_AT"]:
        field_dict = {{
            "name": field.name,
            "type": field.dataType.simpleString(),
            "nullable": field.nullable,
            "metadata": field.metadata
        }}
        print(json.dumps(field_dict, indent=2), ",")
'''
    return script
