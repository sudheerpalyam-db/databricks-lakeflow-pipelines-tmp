# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Migration Tool - Notebook Version (Unity Catalog Volume) for Workday
# MAGIC 
# MAGIC This notebook provides a complete workflow for the DLT Migration Tool for Workday data, allowing you to:
# MAGIC 
# MAGIC 1. Extract CDC logic from existing Workday PySpark files in Unity Catalog volume
# MAGIC 2. Extract schemas from existing tables
# MAGIC 3. Generate DLT Framework specifications
# MAGIC 4. Organize output files into appropriate directories
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC - Access to the source and target tables
# MAGIC - Source code repository mounted in Databricks workspace
# MAGIC - Unity Catalog volume created for storing input/output files
# MAGIC - Workday PySpark files uploaded to Unity Catalog volume

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Modules
# MAGIC 
# MAGIC This will import all the necessary modules directly from the source code.

# COMMAND ----------

# MAGIC %run ./imports

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Workday Parser Module
# MAGIC 
# MAGIC This will import the module for parsing Workday PySpark files.

# COMMAND ----------

import sys
import os
import json
import re
from pathlib import Path

# Add the parent directory to the Python path using os.getcwd() instead of __file__
current_dir = os.getcwd()
# Go up two levels from notebooks directory to reach the src directory
module_path = os.path.abspath(os.path.join(current_dir, "../.."))
if module_path not in sys.path:
    sys.path.append(module_path)

# Import the workday_parser module
from dlt_migration_tool.utils.workday_parser import (
    find_workday_volume_files,
    parse_workday_volume_file,
    generate_bronze_config,
    generate_silver_config,
    read_volume_file,
    extract_variable_value
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC 
# MAGIC Set up configuration parameters for the migration tool.

# COMMAND ----------

# Add widgets for configuration
dbutils.widgets.dropdown("environment", "dev", ["dev", "test", "prod"])
dbutils.widgets.text("catalog", "utilities_dev")
dbutils.widgets.text("schema", "erp_remediation")
dbutils.widgets.text("volume", "dlt_migration_tool")
dbutils.widgets.text("source_system", "workday")
dbutils.widgets.text("tables", "account_sets,bank_accounts") # Comma-separated list of tables to process
dbutils.widgets.text("source_volume_path", "/Volumes/utilities_dev/erp_remediation/dlt_migration_tool/source/")

# Add widgets for bronze and silver catalogs/schemas from databricks.yml
dbutils.widgets.text("bronze_catalog", "edp_bronze_dev")
dbutils.widgets.text("bronze_schema", "workday")
dbutils.widgets.text("bronze_target_schema", "workday_lakeflow")
dbutils.widgets.text("silver_catalog", "edp_silver_dev")
dbutils.widgets.text("silver_schema", "workday_ods_lakeflow")

# Add widget for S3 bucket based on environment
dbutils.widgets.text("s3_bucket", "s3-staging-workday-dev-apse2-058264204922")

# Get widget values
environment = dbutils.widgets.get("environment")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_name = dbutils.widgets.get("volume")
source_system = dbutils.widgets.get("source_system")
tables_input = dbutils.widgets.get("tables")
source_volume_path = dbutils.widgets.get("source_volume_path")
s3_bucket = dbutils.widgets.get("s3_bucket")

# Set S3 bucket based on environment if not explicitly provided
if not s3_bucket:
    if environment == "dev":
        s3_bucket = "s3-staging-workday-dev-apse2-058264204922"
    elif environment == "test":
        s3_bucket = "s3-staging-workday-test-apse2-058264204922"
    elif environment == "prod":
        s3_bucket = "s3-staging-workday-prod-apse2-058264204922"
    else:
        s3_bucket = "s3-staging-workday-dev-apse2-058264204922"  # Default to dev

# Get bronze and silver catalog/schema values
bronze_catalog = dbutils.widgets.get("bronze_catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
bronze_target_schema = dbutils.widgets.get("bronze_target_schema")
silver_catalog = dbutils.widgets.get("silver_catalog")
silver_schema = dbutils.widgets.get("silver_schema")

# Parse tables input (comma-separated list)
tables_to_process = [table.strip() for table in tables_input.split(",") if table.strip()]

print(f"Environment: {environment}")
print(f"Source System: {source_system}")
print(f"Tables to process: {', '.join(tables_to_process)}")
print(f"Source volume path: {source_volume_path}")
print(f"Bronze source: {bronze_catalog}.{bronze_schema}")
print(f"Bronze target: {bronze_catalog}.{bronze_target_schema}")
print(f"Silver target: {silver_catalog}.{silver_schema}")

# Unity Catalog configuration
UC_CATALOG = catalog
UC_SCHEMA = schema
VOLUME_NAME = volume_name

# Paths within the volume
INPUT_DIR = "input"
OUTPUT_DIR = f"output/{source_system}_{environment}"
SCHEMA_DIR = f"schemas/{source_system}_{environment}"

# Full volume path
VOLUME_PATH = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{VOLUME_NAME}"

# Full paths
input_path = f"{VOLUME_PATH}/{INPUT_DIR}"
output_path = f"{VOLUME_PATH}/{OUTPUT_DIR}"
schema_dir = f"{VOLUME_PATH}/{SCHEMA_DIR}"

# Create directories if they don't exist using Databricks utilities
dbutils.fs.mkdirs(input_path)
dbutils.fs.mkdirs(output_path)
dbutils.fs.mkdirs(schema_dir)

print(f"Input path: {input_path}")
print(f"Output path: {output_path}")
print(f"Schema directory: {schema_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract CDC Logic from Workday PySpark Files in Volume

# COMMAND ----------

def find_reference_files_in_volume(tables, volume_path):
    """
    Find reference PySpark files for the specified tables in Unity Catalog volume.
    
    Args:
        tables (list): List of table names to find
        volume_path (str): Volume directory to search
        
    Returns:
        dict: Dictionary mapping table names to file paths
    """
    reference_files = {}
    
    # Check if volume path exists
    try:
        dbutils.fs.ls(volume_path)
    except Exception as e:
        print(f"Error accessing volume path {volume_path}: {str(e)}")
        return reference_files
    
    # Find all Workday PySpark files in the volume directory
    workday_files = find_workday_volume_files(volume_path)
    print(f"Found {len(workday_files)} Workday PySpark files in {volume_path}")
    
    # First, try to match files based on bronze_table_name variable in the file content
    for table in tables:
        found = False
        for file_path in workday_files:
            try:
                # Read file content
                content = read_volume_file(file_path)
                if not content:
                    continue
                
                # Extract bronze_table_name
                bronze_table_name = extract_variable_value(content, "bronze_table_name")
                if bronze_table_name and bronze_table_name.lower() == table.lower():
                    reference_files[table] = file_path
                    found = True
                    print(f"Found file for {table} by bronze_table_name match: {file_path}")
                    break
            except Exception as e:
                print(f"Error reading file {file_path}: {str(e)}")
        
        # If no match found by bronze_table_name, try filename matching
        if not found:
            # Try to find an exact match by filename
            for file_path in workday_files:
                file_name = os.path.basename(file_path)
                file_name_without_ext = os.path.splitext(file_name)[0].lower()
                
                if file_name_without_ext.lower() == table.lower():
                    reference_files[table] = file_path
                    found = True
                    print(f"Found file for {table} by exact filename match: {file_path}")
                    break
            
            # If still no match, try partial filename match
            if not found:
                for file_path in workday_files:
                    file_name = os.path.basename(file_path)
                    file_name_without_ext = os.path.splitext(file_name)[0].lower()
                    
                    if table.lower() in file_name_without_ext.lower():
                        reference_files[table] = file_path
                        print(f"Found file for {table} by partial filename match: {file_path}")
                        break
    
    return reference_files

# Find reference files for the tables to process
reference_files = find_reference_files_in_volume(tables_to_process, source_volume_path)
print(f"Found reference files for {len(reference_files)} tables:")
for table, file_path in reference_files.items():
    print(f"- {table}: {file_path}")

# Parse reference files to extract CDC logic
table_info_dict = {}
for table, file_path in reference_files.items():
    table_info = parse_workday_volume_file(file_path)
    if table_info:
        table_info_dict[table] = table_info
        print(f"Extracted CDC logic for {table}")
    else:
        print(f"Failed to extract CDC logic for {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Schemas from Tables

# COMMAND ----------

# Generate tables to extract schemas from based on user input
tables = []
for table_name in tables_to_process:
    tables.append(f"{bronze_catalog}.{bronze_schema}.{table_name}")

if not tables:
    raise ValueError("No tables specified. Please provide at least one table in the 'tables' widget.")

# Create bronze and silver schema directories in the output path
bronze_schema_dir = f"{output_path}/bronze_{source_system}/schemas"
silver_schema_dir = f"{output_path}/silver_{source_system}/schemas"
dbutils.fs.mkdirs(bronze_schema_dir)
dbutils.fs.mkdirs(silver_schema_dir)

def extract_schema(table_name, output_dir):
    """
    Extract schema from a table and save it to a file.
    
    Args:
        table_name: Fully qualified table name (e.g., "edp_bronze_dev.workday.account_sets")
        output_dir: Directory to save the schema file
    
    Returns:
        Path to the schema file
    """
    # Extract the simple table name (without catalog and schema)
    simple_name = table_name.split('.')[-1]
    
    # Generate the schema file paths - save directly to both bronze and silver schema directories
    schema_file = f"{output_dir}/{source_system}_{simple_name}_schema.json"
    bronze_schema_file = f"{bronze_schema_dir}/{source_system}_{simple_name}_schema.json"
    silver_schema_file = f"{silver_schema_dir}/{source_system}_{simple_name}_schema.json"
    
    try:
        print(f"Attempting to extract schema from table: {table_name}")
        
        # First check if the table exists
        try:
            # Check if the table exists using spark.catalog
            table_exists = False
            for db in spark.catalog.listDatabases():
                if db.name == table_name.split('.')[0]:
                    for tbl in spark.catalog.listTables(db.name):
                        if f"{db.name}.{tbl.name}" == '.'.join(table_name.split('.')[:2]):
                            table_exists = True
                            break
            
            if not table_exists:
                print(f"WARNING: Table {table_name} does not exist in the catalog")
                raise ValueError(f"Table {table_name} does not exist")
        except Exception as e:
            print(f"Error checking table existence: {str(e)}")
            # Continue anyway as the table might still be accessible
        
        # Try to read the table
        print(f"Reading table: {table_name}")
        df = spark.read.table(table_name)
        
        # Check if the dataframe is empty
        row_count = df.count()
        print(f"Table {table_name} has {row_count} rows")
        
        # Get the schema
        schema_json = df.schema.json()
        schema_dict = json.loads(schema_json)
        
        # Print schema information for debugging
        print(f"Schema for {table_name} has {len(schema_dict.get('fields', []))} fields")
        
        # Save the schema to all required locations
        dbutils.fs.put(schema_file, json.dumps(schema_dict, indent=2), overwrite=True)
        dbutils.fs.put(bronze_schema_file, json.dumps(schema_dict, indent=2), overwrite=True)
        dbutils.fs.put(silver_schema_file, json.dumps(schema_dict, indent=2), overwrite=True)
        
        # Verify the files were written correctly
        try:
            written_content = dbutils.fs.head(schema_file)
            written_schema = json.loads(written_content)
            print(f"Successfully wrote schema with {len(written_schema.get('fields', []))} fields to:")
            print(f"- {schema_file}")
            print(f"- {bronze_schema_file}")
            print(f"- {silver_schema_file}")
        except Exception as e:
            print(f"WARNING: Could not verify written schema files: {str(e)}")
        
        return schema_file
    except Exception as e:
        print(f"ERROR extracting schema from {table_name}: {str(e)}")
        print("Stack trace:")
        import traceback
        traceback.print_exc()
        
        # Try an alternative approach for schema extraction
        try:
            print(f"Attempting alternative schema extraction for {table_name}")
            # Try to get schema using SQL approach
            schema_df = spark.sql(f"DESCRIBE TABLE {table_name}")
            
            if schema_df.count() > 0:
                print(f"Found {schema_df.count()} columns using SQL DESCRIBE")
                
                # Convert the SQL schema format to the expected JSON format
                fields = []
                for row in schema_df.collect():
                    if row['col_name'] != '' and not row['col_name'].startswith('#'):
                        field = {
                            "name": row['col_name'],
                            "type": row['data_type'],
                            "nullable": True,
                            "metadata": {}
                        }
                        fields.append(field)
                
                schema = {"type": "struct", "fields": fields}
                
                # Save the schema to all required locations
                dbutils.fs.put(schema_file, json.dumps(schema, indent=2), overwrite=True)
                dbutils.fs.put(bronze_schema_file, json.dumps(schema, indent=2), overwrite=True)
                dbutils.fs.put(silver_schema_file, json.dumps(schema, indent=2), overwrite=True)
                print(f"Generated schema with {len(fields)} fields using alternative method and saved to all locations")
                return schema_file
            else:
                print(f"Alternative schema extraction returned no columns")
        except Exception as alt_e:
            print(f"Alternative schema extraction also failed: {str(alt_e)}")
        
        # Generate an empty schema as last resort
        schema = {"type": "struct", "fields": []}
        
        # Save the schema to all required locations
        dbutils.fs.put(schema_file, json.dumps(schema, indent=2), overwrite=True)
        dbutils.fs.put(bronze_schema_file, json.dumps(schema, indent=2), overwrite=True)
        dbutils.fs.put(silver_schema_file, json.dumps(schema, indent=2), overwrite=True)
        
        print(f"Generated empty schema file as fallback and saved to all locations")
        return schema_file

# Extract schemas for all tables
schema_files = []
for table in tables:
    try:
        schema_file = extract_schema(table, schema_dir)
        schema_files.append(schema_file)
    except Exception as e:
        print(f"Error processing table {table}: {str(e)}")

print(f"Generated {len(schema_files)} schema files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate DLT Framework Specifications

# COMMAND ----------

def generate_dataflow_specs():
    """
    Generate DLT Framework specifications for bronze and silver layers.
    
    Returns:
        list: List of output files
    """
    output_files = []
    
    for table in tables_to_process:
        try:
            print(f"Generating dataflow specs for {table}")
            
            # Check if we have table info from the reference files
            if table in table_info_dict:
                table_info = table_info_dict[table]
                
                # Generate bronze configuration
                bronze_config = generate_bronze_config(table_info, bronze_catalog, bronze_target_schema, s3_bucket)
                
                # Generate silver configuration
                silver_config = generate_silver_config(
                    table_info, 
                    bronze_catalog, 
                    bronze_target_schema, 
                    silver_catalog, 
                    silver_schema
                )
            else:
                # If no table info is available, raise an error
                error_msg = f"No reference file found for table: {table}. Cannot generate dataflow specs without reference file."
                print(error_msg)
                raise ValueError(error_msg)
            
            # Generate output file paths
            bronze_output_file = f"{output_path}/{source_system}_{table}_bronze.json"
            silver_output_file = f"{output_path}/{source_system}_{table}_silver.json"
            
            # Write the files
            dbutils.fs.put(bronze_output_file, json.dumps(bronze_config, indent=2), overwrite=True)
            dbutils.fs.put(silver_output_file, json.dumps(silver_config, indent=2), overwrite=True)
            
            print(f"Generated bronze configuration: {bronze_output_file}")
            print(f"Generated silver configuration: {silver_output_file}")
            
            output_files.extend([bronze_output_file, silver_output_file])
            
        except Exception as e:
            print(f"Error generating dataflow specs for {table}: {str(e)}")
            print("Stack trace:")
            import traceback
            traceback.print_exc()
    
    return output_files

# Generate dataflow specs
output_files = generate_dataflow_specs()
print(f"Generated {len(output_files)} output files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Organize Output Files
# MAGIC 
# MAGIC Organize the output files into bronze and silver directories.

# COMMAND ----------

def organize_output_files():
    """
    Organize output files into bronze and silver directories.
    
    Returns:
        Dictionary mapping directories to the files they contain
    """
    # Create bronze and silver directories
    bronze_dir = f"{output_path}/bronze_{source_system}/dataflowspec"
    silver_dir = f"{output_path}/silver_{source_system}/dataflowspec"
    dbutils.fs.mkdirs(bronze_dir)
    dbutils.fs.mkdirs(silver_dir)
    
    # Create schema directories
    bronze_schema_dir = f"{output_path}/bronze_{source_system}/schemas"
    silver_schema_dir = f"{output_path}/silver_{source_system}/schemas"
    dbutils.fs.mkdirs(bronze_schema_dir)
    dbutils.fs.mkdirs(silver_schema_dir)
    
    # List all JSON files in the output directory
    output_files = dbutils.fs.ls(output_path)
    json_output_files = [file for file in output_files if file.name.endswith('.json')]
    
    result = {"bronze": [], "silver": []}
    
    for file in json_output_files:
        try:
            # Read the JSON file
            file_content = dbutils.fs.head(file.path)
            data = json.loads(file_content)
            
            # Extract dataFlowGroup
            dataflow_group = data.get("dataFlowGroup", "")
            dataflow_id = data.get("dataFlowId", "")
            table_name = data.get("targetDetails", {}).get("table", "")
            
            # Skip if no dataflow ID
            if not dataflow_id:
                continue
            
            # Determine target directory based on dataFlowGroup
            if "silver" in dataflow_group.lower():
                target_dir = silver_dir
                schema_dir = silver_schema_dir
                result["silver"].append(dataflow_id)
            else:
                target_dir = bronze_dir
                schema_dir = bronze_schema_dir
                result["bronze"].append(dataflow_id)
            
            # Copy schema file to the appropriate directory
            if table_name:
                schema_file_name = f"{source_system}_{table_name}_schema.json"
                source_schema_file = f"{schema_dir}/{schema_file_name}"
                
                # Check if schema file exists in the schema directory
                schema_exists = False
                try:
                    dbutils.fs.ls(source_schema_file)
                    schema_exists = True
                except:
                    pass
                
                if not schema_exists:
                    # Look for schema file in the extracted schemas
                    for schema_file in schema_files:
                        if schema_file.endswith(f"{source_system}_{table_name}_schema.json"):
                            # Copy the schema file
                            schema_content = dbutils.fs.head(schema_file)
                            dbutils.fs.put(source_schema_file, schema_content, overwrite=True)
                            schema_exists = True
                            break
                
                if not schema_exists:
                    # Create empty schema
                    empty_schema = {"type": "struct", "fields": []}
                    dbutils.fs.put(source_schema_file, json.dumps(empty_schema, indent=2), overwrite=True)
            
            # Use the correct filename with _main suffix
            if table_name:
                target_file = f"{target_dir}/{source_system}_{table_name}_main.json"
            else:
                # Fallback to dataflow_id if table name is not available
                target_file = f"{target_dir}/{dataflow_id}_main.json"
            
            # Write the updated data to the target file
            dbutils.fs.put(target_file, file_content, overwrite=True)
            
            print(f"Organized {file.name} to {target_file}")
            
            # Delete the original file
            dbutils.fs.rm(file.path)
            
        except Exception as e:
            print(f"Error processing {file.path}: {str(e)}")
    
    return result

# Organize output files
organized_files = organize_output_files()
print(f"Organized {len(organized_files['bronze'])} bronze files and {len(organized_files['silver'])} silver files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Data Quality Expectations
# MAGIC 
# MAGIC Generate data quality expectations for each table.

# COMMAND ----------

def generate_dqe_files():
    """
    Generate data quality expectation files for each table.
    
    Returns:
        List of generated DQE files
    """
    dqe_files = []
    
    # Create DQE directory
    dqe_dir = f"{output_path}/bronze_{source_system}/dqe"
    dbutils.fs.mkdirs(dqe_dir)
    
    # Generate DQE for each table
    for table in tables:
        try:
            # Extract the simple table name (without catalog and schema)
            simple_name = table.split('.')[-1]
            
            # Generate the DQE file path
            dqe_file = f"{dqe_dir}/{source_system}_{simple_name}_dqe.json"
            
            # Create a basic DQE
            dqe_content = {
                "expectations": [
                    {
                        "name": "Valid record count",
                        "expression": "count(*) > 0",
                        "level": "warning"
                    }
                ]
            }
            
            # Write the DQE file
            dbutils.fs.put(dqe_file, json.dumps(dqe_content, indent=2), overwrite=True)
            
            print(f"Generated DQE file: {dqe_file}")
            dqe_files.append(dqe_file)
        except Exception as e:
            print(f"Error generating DQE for {table}: {str(e)}")
    
    return dqe_files

# Generate DQE files
dqe_files = generate_dqe_files()
print(f"Generated {len(dqe_files)} DQE files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Generated JSON Files
# MAGIC 
# MAGIC Validate the generated dataflowspec JSON files against the JSON Schema definitions.

# COMMAND ----------

# Import the schema validator
from dlt_migration_tool.utils.schema_validator import SchemaValidator

def validate_json_files():
    """
    Validate all generated JSON files against JSON Schema definitions.
    
    Returns:
        Dictionary with validation results
    """
    # Initialize the validator
    validator = SchemaValidator()
    
    # Paths to validate
    bronze_path = f"{output_path}/bronze_{source_system}/dataflowspec"
    silver_path = f"{output_path}/silver_{source_system}/dataflowspec"
    
    # Create temporary directory for validation
    import tempfile
    import shutil
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Copy files from bronze path to temp directory for validation
        bronze_files = dbutils.fs.ls(bronze_path)
        for file in bronze_files:
            if file.name.endswith('.json'):
                # Read the file content
                file_content = dbutils.fs.head(file.path)
                
                # Write to temp directory
                temp_file_path = os.path.join(temp_dir, file.name)
                with open(temp_file_path, 'w') as f:
                    f.write(file_content)
        
        # Copy files from silver path to temp directory for validation
        silver_files = dbutils.fs.ls(silver_path)
        for file in silver_files:
            if file.name.endswith('.json'):
                # Read the file content
                file_content = dbutils.fs.head(file.path)
                
                # Write to temp directory
                temp_file_path = os.path.join(temp_dir, file.name)
                with open(temp_file_path, 'w') as f:
                    f.write(file_content)
        
        # Validate all files in the temp directory
        results = validator.validate_directory(temp_dir)
        
        return results
    finally:
        # Clean up the temp directory
        shutil.rmtree(temp_dir)

# Validate the generated JSON files
validation_results = validate_json_files()

print(f"JSON Schema Validation Results:")
print(f"- Valid files: {validation_results['valid']}")
print(f"- Invalid files: {validation_results['invalid']}")

# Show details for invalid files
if validation_results['invalid'] > 0:
    print("\nInvalid Files:")
    for result in validation_results['files']:
        if not result['valid']:
            print(f"- {os.path.basename(result['file'])}: {result.get('error', 'Unknown error')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accessing Files in the Volume
# MAGIC 
# MAGIC You can access the generated files in the Unity Catalog volume using the following paths:
# MAGIC 
# MAGIC - In Python/Scala: `/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{VOLUME_NAME}/{path}`
# MAGIC - In SQL: `{UC_CATALOG}.{UC_SCHEMA}.{VOLUME_NAME}/{path}`
# MAGIC 
# MAGIC For example:
# MAGIC 
# MAGIC ```sql
# MAGIC -- List files in the bronze dataflowspec directory
# MAGIC LIST '${UC_CATALOG}.${UC_SCHEMA}.${VOLUME_NAME}/output/${source_system}_${environment}/bronze_${source_system}/dataflowspec'
# MAGIC ```

# COMMAND ----------
