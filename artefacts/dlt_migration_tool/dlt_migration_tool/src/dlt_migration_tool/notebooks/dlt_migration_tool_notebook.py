# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Migration Tool - Notebook Version
# MAGIC 
# MAGIC This notebook provides a simplified interface for the DLT Migration Tool, allowing you to:
# MAGIC 
# MAGIC 1. Convert legacy DLT-meta configurations to the new DLT Framework format
# MAGIC 2. Extract schemas from existing tables
# MAGIC 3. Update schema references in the generated configurations
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC - The DLT Migration Tool package must be installed
# MAGIC - Access to the source and target tables

# COMMAND ----------

# MAGIC %pip install -U /dbfs/FileStore/jars/dlt_migration_tool-0.1.0-py3-none-any.whl

# COMMAND ----------

# DBTITLE 1,Import Libraries
import os
import json
from pathlib import Path
from pyspark.sql import SparkSession

# Import the DLT Migration Tool
from dlt_migration_tool.core.transformer import MigrationProcessor
from dlt_migration_tool.utils.schema_utils import SchemaGenerator

# COMMAND ----------

# DBTITLE 1,Configuration
# Input and output paths
input_path = "/dbfs/FileStore/dlt_migration_tool/input"
output_path = "/dbfs/FileStore/dlt_migration_tool/output"

# Tables to extract schemas from
tables = [
    "edp_bronze_dev.maximo.asset",
    "edp_bronze_dev.maximo.a_locations",
    "edp_bronze_dev.maximo.a_pm"
]

# Schema paths
schema_dir = os.path.join(output_path, "schemas")

# Create directories if they don't exist
os.makedirs(input_path, exist_ok=True)
os.makedirs(output_path, exist_ok=True)
os.makedirs(schema_dir, exist_ok=True)

# COMMAND ----------

# DBTITLE 1,Extract Schemas from Tables
def extract_schema(table_name, output_dir):
    """
    Extract schema from a table and save it to a file.
    
    Args:
        table_name: Fully qualified table name (e.g., "edp_bronze_dev.maximo.asset")
        output_dir: Directory to save the schema file
    
    Returns:
        Path to the schema file
    """
    # Extract the simple table name (without catalog and schema)
    simple_name = table_name.split('.')[-1]
    
    # Generate the schema file path
    schema_file = os.path.join(output_dir, f"maximo_{simple_name}_schema.json")
    
    try:
        # Read the table
        df = spark.read.table(table_name)
        
        # Get the schema
        schema_json = df.schema.json()
        schema_dict = json.loads(schema_json)
        
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
for table in tables:
    schema_file = extract_schema(table, schema_dir)
    schema_files.append(schema_file)

print(f"Generated {len(schema_files)} schema files")

# COMMAND ----------

# DBTITLE 1,Process Input Files
def process_files(input_dir, output_dir):
    """
    Process all JSON files in the input directory.
    
    Args:
        input_dir: Directory containing input files
        output_dir: Directory to save output files
    
    Returns:
        List of output files
    """
    # Initialize processor
    processor = MigrationProcessor(verbose=True)
    
    # Find all JSON files in the input directory
    input_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith('.json')]
    
    if not input_files:
        print(f"No JSON files found in {input_dir}")
        return []
    
    print(f"Found {len(input_files)} JSON files to process")
    
    # Process each file
    output_files = []
    for input_file in input_files:
        try:
            print(f"Processing file: {input_file}")
            result = processor.process_file(input_file, output_dir)
            output_files.extend(result)
            print(f"Generated {len(result)} output files")
        except Exception as e:
            print(f"Error processing {input_file}: {str(e)}")
    
    return output_files

# Process all files in the input directory
output_files = process_files(input_path, output_path)
print(f"Generated {len(output_files)} output files")

# COMMAND ----------

# DBTITLE 1,Organize Output Files
def organize_output_files(output_dir):
    """
    Organize output files into bronze and silver directories.
    
    Args:
        output_dir: Directory containing output files
    
    Returns:
        Dictionary mapping directories to the files they contain
    """
    # Create bronze and silver directories
    bronze_dir = os.path.join(output_dir, "bronze_maximo", "dataflowspec")
    silver_dir = os.path.join(output_dir, "silver_maximo", "dataflowspec")
    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(silver_dir, exist_ok=True)
    
    # Create schema directories
    bronze_schema_dir = os.path.join(output_dir, "bronze_maximo", "schemas")
    silver_schema_dir = os.path.join(output_dir, "silver_maximo", "schemas")
    os.makedirs(bronze_schema_dir, exist_ok=True)
    os.makedirs(silver_schema_dir, exist_ok=True)
    
    # Find all JSON files in the output directory
    result = {"bronze": [], "silver": []}
    
    for file_path in [f for f in os.listdir(output_dir) if f.endswith('.json')]:
        full_path = os.path.join(output_dir, file_path)
        try:
            # Read the JSON file
            with open(full_path, 'r') as f:
                data = json.load(f)
            
            # Extract dataFlowGroup
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
                
                # Add sourceViewName if not present
                if "sourceViewName" not in data and table_name:
                    # Use the same view name format as bronze level
                    data["sourceViewName"] = f"v_{table_name}"
                
                # Update schema path
                if "targetDetails" in data and "schemaPath" in data["targetDetails"]:
                    data["targetDetails"]["schemaPath"] = f"maximo_{table_name}_schema.json"
                
                result["silver"].append(dataflow_id)
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
                
                result["bronze"].append(dataflow_id)
            
            # Remove path from targetDetails if it's "Ignore" or "ignore"
            if "targetDetails" in data and "path" in data["targetDetails"]:
                path_value = data["targetDetails"]["path"]
                if path_value == "Ignore" or path_value == "ignore":
                    del data["targetDetails"]["path"]
            
            # Create empty schema file
            if table_name:
                schema_file_name = f"maximo_{table_name}_schema.json"
                
                # Copy schema from the extracted schemas
                source_schema_file = os.path.join(schema_dir, schema_file_name)
                target_schema_file = os.path.join(schema_dir, schema_file_name)
                
                if os.path.exists(source_schema_file):
                    with open(source_schema_file, 'r') as f:
                        schema_data = json.load(f)
                    
                    with open(target_schema_file, 'w') as f:
                        json.dump(schema_data, f, indent=2)
                else:
                    # Create empty schema
                    with open(target_schema_file, 'w') as f:
                        json.dump({"type": "struct", "fields": []}, f, indent=2)
            
            # Use the correct filename with _main suffix
            if table_name:
                target_file = os.path.join(target_dir, f"maximo_{table_name}_main.json")
            else:
                # Fallback to dataflow_id if table name is not available
                target_file = os.path.join(target_dir, f"{dataflow_id}_main.json")
            
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
            
            print(f"Organized {file_path} to {target_file}")
            
            # Delete the original file
            os.remove(full_path)
            
        except Exception as e:
            print(f"Error processing {file_path}: {str(e)}")
    
    return result

# Organize output files
organized_files = organize_output_files(output_path)
print(f"Organized {len(organized_files['bronze'])} bronze files and {len(organized_files['silver'])} silver files")

# COMMAND ----------

# DBTITLE 1,Summary
print("DLT Migration Tool Notebook completed successfully")
print(f"Input directory: {input_path}")
print(f"Output directory: {output_path}")
print(f"Generated {len(output_files)} output files")
print(f"Organized {len(organized_files['bronze'])} bronze files and {len(organized_files['silver'])} silver files")
print(f"Generated {len(schema_files)} schema files")

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
print(output_path)
print_directory_structure(output_path)

# COMMAND ----------
