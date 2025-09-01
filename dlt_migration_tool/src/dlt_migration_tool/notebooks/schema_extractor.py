#!/usr/bin/env python3
"""
Schema extractor notebook for Databricks.

This notebook extracts schemas from existing tables in Databricks and saves them to files.
It can be run directly in a Databricks notebook environment.
"""

# Databricks notebook source
# MAGIC %md
# MAGIC # Schema Extractor for DLT Migration Tool
# MAGIC 
# MAGIC This notebook extracts schemas from existing tables in Databricks and saves them to files.
# MAGIC It can be used to generate schema files for use with the DLT Migration Tool.
# MAGIC 
# MAGIC ## Parameters
# MAGIC 
# MAGIC - `table_names`: List of fully qualified table names to extract schemas from
# MAGIC - `output_dir`: Directory to save the schema files
# MAGIC - `prefix`: Prefix for schema file names (default: "maximo_")
# MAGIC - `suffix`: Suffix for schema file names (default: "_schema.json")

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

# COMMAND ----------

# DBTITLE 1,Parameters
# Define parameters with default values
dbutils.widgets.text("table_names", "edp_bronze_dev.maximo.asset,edp_bronze_dev.maximo.a_locations", "Table Names (comma-separated)")
dbutils.widgets.text("output_dir", "/dbfs/FileStore/dlt_migration_tool/schemas", "Output Directory")
dbutils.widgets.text("prefix", "maximo_", "Schema File Prefix")
dbutils.widgets.text("suffix", "_schema.json", "Schema File Suffix")

# Get parameter values
table_names_str = dbutils.widgets.get("table_names")
output_dir = dbutils.widgets.get("output_dir")
prefix = dbutils.widgets.get("prefix")
suffix = dbutils.widgets.get("suffix")

# Parse table names
table_names = [name.strip() for name in table_names_str.split(",") if name.strip()]

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# COMMAND ----------

# DBTITLE 1,Extract Schemas
def extract_schema(table_name, output_dir, prefix="maximo_", suffix="_schema.json"):
    """
    Extract schema from a table and save it to a file.
    
    Args:
        table_name: Fully qualified table name (e.g., "edp_bronze_dev.maximo.asset")
        output_dir: Directory to save the schema file
        prefix: Prefix for the schema file name
        suffix: Suffix for the schema file name
    
    Returns:
        Path to the schema file
    """
    # Extract the simple table name (without catalog and schema)
    simple_name = table_name.split('.')[-1]
    
    # Generate the schema file path
    schema_file = os.path.join(output_dir, f"{prefix}{simple_name}{suffix}")
    
    try:
        # Read the table
        df = spark.read.table(table_name)
        
        # Get the schema
        schema_json = df.schema.json()
        schema_dict = json.loads(schema_json)
        
        # Filter out specific metadata columns
        if "fields" in schema_dict:
            schema_dict["fields"] = [
                field for field in schema_dict["fields"] 
                if field.get("name", "") not in ["_change_type", "_commit_version", "_commit_timestamp", "meta_load_details", "__START_AT", "__END_AT"]
            ]
        
        # Save the schema to a file
        with open(schema_file, 'w') as f:
            json.dump(schema_dict, f, indent=2)
        
        print(f"Extracted schema from {table_name} and saved to {schema_file}")
        return schema_file
    except Exception as e:
        print(f"Error extracting schema from {table_name}: {str(e)}")
        
        # Generate an empty schema
        schema = {"type": "struct", "fields": []}
        
        # Save the schema to a file
        with open(schema_file, 'w') as f:
            json.dump(schema, f, indent=2)
        
        print(f"Generated empty schema file: {schema_file}")
        return schema_file

# Extract schemas for all tables
schema_files = []
for table in table_names:
    schema_file = extract_schema(table, output_dir, prefix, suffix)
    schema_files.append(schema_file)

print(f"Generated {len(schema_files)} schema files")

# COMMAND ----------

# DBTITLE 1,Validate Schemas
def validate_schema(schema_file):
    """
    Validate a schema file.
    
    Args:
        schema_file: Path to the schema file
    
    Returns:
        True if the schema is valid, False otherwise
    """
    try:
        # Read the schema file
        with open(schema_file, 'r') as f:
            schema_dict = json.load(f)
        
        # Convert to StructType
        schema = StructType.fromJson(schema_dict)
        
        # Check if the schema has fields
        if len(schema.fields) == 0:
            print(f"Warning: Schema {schema_file} has no fields")
            return False
        
        print(f"Schema {schema_file} is valid with {len(schema.fields)} fields")
        return True
    except Exception as e:
        print(f"Error validating schema {schema_file}: {str(e)}")
        return False

# Validate all schema files
valid_schemas = []
invalid_schemas = []
for schema_file in schema_files:
    if validate_schema(schema_file):
        valid_schemas.append(schema_file)
    else:
        invalid_schemas.append(schema_file)

print(f"Valid schemas: {len(valid_schemas)}")
print(f"Invalid schemas: {len(invalid_schemas)}")

# COMMAND ----------

# DBTITLE 1,Summary
print("Schema Extractor completed")
print(f"Processed {len(table_names)} tables")
print(f"Generated {len(schema_files)} schema files")
print(f"Valid schemas: {len(valid_schemas)}")
print(f"Invalid schemas: {len(invalid_schemas)}")

# Display the output directory structure
def print_directory_structure(path, prefix=""):
    """Print the directory structure."""
    if not os.path.exists(path):
        return
    
    files = os.listdir(path)
    for i, file in enumerate(sorted(files)):
        is_last = i == len(files) - 1
        print(f"{prefix}{'└── ' if is_last else '├── '}{file}")
        full_path = os.path.join(path, file)
        if os.path.isdir(full_path):
            print_directory_structure(full_path, prefix + ('    ' if is_last else '│   '))

print("\nOutput Directory Structure:")
print(output_dir)
print_directory_structure(output_dir)

# COMMAND ----------
