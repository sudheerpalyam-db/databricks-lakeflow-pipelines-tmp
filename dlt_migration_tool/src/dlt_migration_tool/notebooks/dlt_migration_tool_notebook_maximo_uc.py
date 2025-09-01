# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Migration Tool - Notebook Version (Unity Catalog Volume)
# MAGIC 
# MAGIC This notebook provides a complete workflow for the DLT Migration Tool, allowing you to:
# MAGIC 
# MAGIC 1. Convert legacy DLT-meta configurations to the new DLT Framework format
# MAGIC 2. Extract schemas from existing tables
# MAGIC 3. Process input files to generate DLT Framework specifications
# MAGIC 4. Organize output files into appropriate directories
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC - Access to the source and target tables
# MAGIC - Source code repository mounted in Databricks workspace
# MAGIC - Unity Catalog volume created for storing input/output files
# MAGIC - Input JSON files placed in the input directory

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Modules
# MAGIC 
# MAGIC This will import all the necessary modules directly from the source code.

# COMMAND ----------

# MAGIC %run ./imports

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure Module Path
# MAGIC 
# MAGIC This will ensure the module path is correctly set up.

# COMMAND ----------

import sys
import os
import json
import datetime
import tempfile
import shutil
import uuid
from pathlib import Path

# Add the parent directory to the Python path using os.getcwd() instead of __file__
current_dir = os.getcwd()
# Go up two levels from notebooks directory to reach the src directory
module_path = os.path.abspath(os.path.join(current_dir, "../.."))
if module_path not in sys.path:
    sys.path.append(module_path)
    print(f"Added module path: {module_path}")

# Try to import again to verify
try:
    from dlt_migration_tool.core.transformer import MigrationProcessor
    from dlt_migration_tool.audit import AuditFramework
    from dlt_migration_tool.utils.schema_validator import SchemaValidator
    print("Successfully imported required modules")
except ImportError as e:
    print(f"Import error after path modification: {e}")
    print(f"Current sys.path: {sys.path}")
    raise

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
dbutils.widgets.text("source_system", "maximo")
dbutils.widgets.text("tables", "asset,workorder") # Comma-separated list of tables to process

# Add widgets for bronze and silver catalogs/schemas from databricks.yml
dbutils.widgets.text("bronze_catalog", "edp_bronze_dev")
dbutils.widgets.text("bronze_schema", "maximo_lakeflow")
dbutils.widgets.text("silver_catalog", "edp_silver_dev")
dbutils.widgets.text("silver_schema", "maximo_ods_lakeflow")

# Get widget values
environment = dbutils.widgets.get("environment")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
volume_name = dbutils.widgets.get("volume")
source_system = dbutils.widgets.get("source_system")
tables_input = dbutils.widgets.get("tables")

# Get bronze and silver catalog/schema values
bronze_catalog = dbutils.widgets.get("bronze_catalog")
bronze_schema = dbutils.widgets.get("bronze_schema")
silver_catalog = dbutils.widgets.get("silver_catalog")
silver_schema = dbutils.widgets.get("silver_schema")

# Parse tables input (comma-separated list)
tables_to_process = [table.strip() for table in tables_input.split(",") if table.strip()]

print(f"Environment: {environment}")
print(f"Source System: {source_system}")
print(f"Tables to process: {', '.join(tables_to_process)}")
print(f"Bronze target: {bronze_catalog}.{bronze_schema}")
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

# Initialize audit framework
audit = AuditFramework(catalog=UC_CATALOG, schema=UC_SCHEMA)
run_id = audit.start_run(
    source_system=source_system, 
    environment=environment,
    notebook_path=dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract Schemas from Tables

# COMMAND ----------

# Generate tables to extract schemas from based on user input - using source_system instead of bronze_schema
tables = []
for table_name in tables_to_process:
    tables.append(f"{bronze_catalog}.{source_system}.{table_name}")

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
        table_name: Fully qualified table name (e.g., "edp_bronze_dev.maximo.asset")
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
    
    # Track table migration in audit framework
    bronze_target = f"{bronze_catalog}.{bronze_schema}.{simple_name}"
    silver_target = f"{silver_catalog}.{silver_schema}.{simple_name}"
    audit.track_table_migration(simple_name, bronze_target, silver_target)
    
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
        
        # Filter out specific metadata columns
        if "fields" in schema_dict:
            schema_dict["fields"] = [
                field for field in schema_dict["fields"] 
                if field.get("name", "") not in ["_change_type", "_commit_version", "_commit_timestamp", "meta_load_details", "__START_AT", "__END_AT"]
            ]
            
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
                    if row['col_name'] != '' and not row['col_name'].startswith('#') and row['col_name'] not in ["_change_type", "_commit_version", "_commit_timestamp", "meta_load_details", "__START_AT", "__END_AT"]:
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
# MAGIC ## Process Input Files
# MAGIC 
# MAGIC Process input files using the DLT Migration Tool.

# COMMAND ----------

def process_input_files():
    """
    Process input files using the DLT Migration Tool.
    
    Returns:
        List of output files
    """
    try:
        # Initialize processor
        processor = MigrationProcessor(verbose=True)
        
        # List all JSON files in the input directory
        input_files = dbutils.fs.ls(input_path)
        json_files = [file for file in input_files if file.name.endswith('.json')]
        
        if not json_files:
            print(f"No JSON files found in {input_path}")
            return []
        
        print(f"Found {len(json_files)} JSON files to process:")
        for file in json_files:
            print(f"- {file.name}")
        
        # Process each file
        output_files = []
        for file in json_files:
            try:
                # Skip silver transforms file - it will be processed separately
                if "silvertransforms" in file.name.lower():
                    print(f"Skipping silver transforms file: {file.name} - will process separately")
                    continue
                    
                print(f"Processing file: {file.path}")
                
                # Read the input file using Spark to handle large files
                try:
                    # First try to read using dbutils.fs.head
                    print(f"Attempting to read file using dbutils.fs.head: {file.path}")
                    input_content = dbutils.fs.head(file.path)
                    input_data = json.loads(input_content)
                    print(f"Successfully parsed JSON with {len(input_data)} entries")
                except Exception as e:
                    print(f"Error reading file with dbutils.fs.head: {str(e)}")
                    print("Trying alternative method using Spark DataFrame...")
                    
                    # Try reading with Spark
                    try:
                        # Read the JSON file as a text file first
                        df = spark.read.text(file.path)
                        
                        # Collect all lines and join them
                        all_text = "\n".join([row.value for row in df.collect()])
                        
                        # Parse the JSON
                        input_data = json.loads(all_text)
                        print(f"Successfully parsed JSON with Spark method: {len(input_data)} entries")
                    except Exception as spark_e:
                        print(f"Error reading with Spark method: {str(spark_e)}")
                        
                        # Try reading the file in chunks
                        print("Trying to read file in chunks...")
                        try:
                            # Use spark to read the file in chunks
                            df = spark.read.text(file.path)
                            
                            # Convert to pandas for easier processing
                            pdf = df.toPandas()
                            
                            # Join all lines
                            all_text = "".join(pdf["value"].tolist())
                            
                            # Try to parse the JSON
                            input_data = json.loads(all_text)
                            print(f"Successfully parsed JSON with chunk method: {len(input_data)} entries")
                        except Exception as chunk_e:
                            print(f"Error reading in chunks: {str(chunk_e)}")
                            
                            # Last resort: try to fix common JSON issues
                            print("Attempting to fix JSON format issues...")
                            try:
                                # Read as text and try to fix common JSON issues
                                all_text = "\n".join([row.value for row in df.collect()])
                                
                                # Fix common JSON issues
                                # 1. Try to find unterminated strings
                                fixed_text = all_text.replace('\\"', '"').replace('\\n', ' ')
                                
                                # 2. Try to balance quotes
                                quote_count = fixed_text.count('"')
                                if quote_count % 2 == 1:
                                    fixed_text = fixed_text + '"'
                                
                                # 3. Ensure the JSON is properly wrapped in [] or {}
                                if not (fixed_text.strip().startswith('[') and fixed_text.strip().endswith(']')) and \
                                   not (fixed_text.strip().startswith('{') and fixed_text.strip().endswith('}')):
                                    fixed_text = '[' + fixed_text + ']'
                                
                                # Try to parse the fixed JSON
                                input_data = json.loads(fixed_text)
                                print(f"Successfully parsed JSON after fixing format issues: {len(input_data)} entries")
                            except Exception as fix_e:
                                print(f"Failed to fix JSON format issues: {str(fix_e)}")
                                print("Cannot process this file due to JSON parsing errors.")
                                continue
                
                # Verify that the input file contains specs for the tables we want to process
                table_specs = []
                for spec in input_data:
                    try:
                        table_name = spec.get("source_details", {}).get("source_table", "")
                        if table_name in tables_to_process:
                            table_specs.append(spec)
                    except Exception as spec_e:
                        print(f"Error processing spec entry: {str(spec_e)}")
                        continue
                
                # Check if any of the specified tables are missing from the input file
                found_tables = []
                for spec in table_specs:
                    try:
                        table_name = spec.get("source_details", {}).get("source_table", "")
                        if table_name:
                            found_tables.append(table_name)
                    except Exception:
                        continue
                
                missing_tables = [table for table in tables_to_process if table not in found_tables]
                
                if missing_tables:
                    print(f"WARNING: The following tables were not found in the input file: {', '.join(missing_tables)}")
                    if not table_specs:
                        print(f"ERROR: None of the specified tables were found in the input file. Skipping this file.")
                        continue
                
                print(f"Found {len(table_specs)} table specifications to process")
                
                # Create a temporary file with only the specs for the tables we want to process
                temp_input_file = f"/tmp/{file.name}"
                with open(temp_input_file, 'w') as f:
                    json.dump(table_specs, f, indent=2)
                
                # Create a temporary output directory
                temp_output_dir = f"/tmp/output_{file.name.replace('.json', '')}"
                os.makedirs(temp_output_dir, exist_ok=True)
                
                # Process the file
                result = processor.process_file(temp_input_file, temp_output_dir)
                
                # Copy the output files to the volume
                for output_file in result:
                    output_file_path = Path(output_file)
                    output_name = output_file_path.name
                    
                    # Read the output file
                    with open(output_file, 'r') as f:
                        output_content = f.read()
                    
                    # Write to the volume
                    volume_output_path = f"{output_path}/{output_name}"
                    dbutils.fs.put(volume_output_path, output_content, overwrite=True)
                    
                    output_files.append(volume_output_path)
                
                print(f"Generated {len(result)} output files")
            except Exception as e:
                print(f"Error processing {file.path}: {str(e)}")
                print("Stack trace:")
                import traceback
                traceback.print_exc()
        
        return output_files
    except Exception as e:
        print(f"Error initializing MigrationProcessor: {str(e)}")
        print("Stack trace:")
        import traceback
        traceback.print_exc()
        return []

# Process input files
output_files = process_input_files()
print(f"Generated {len(output_files)} output files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process Silver Transforms
# MAGIC 
# MAGIC Process the silver transforms file to generate silver layer configurations.

# COMMAND ----------

def process_silver_transforms():
    """
    Process silver transforms file to generate silver layer configurations.
    
    Returns:
        List of generated silver configuration files
    """
    silver_files = []
    
    # Look for silver transforms file
    silver_transforms_file = None
    input_files = dbutils.fs.ls(input_path)
    for file in input_files:
        if "silvertransforms" in file.name.lower():
            silver_transforms_file = file.path
            break
    
    if not silver_transforms_file:
        print("No silver transforms file found")
        return silver_files
    
    print(f"Processing silver transforms file: {silver_transforms_file}")
    
    try:
        # Read the silver transforms file
        try:
            # First try to read using dbutils.fs.head
            print(f"Attempting to read silver transforms file using dbutils.fs.head: {silver_transforms_file}")
            silver_transforms_content = dbutils.fs.head(silver_transforms_file)
            silver_transforms = json.loads(silver_transforms_content)
            print(f"Successfully parsed silver transforms JSON with {len(silver_transforms)} entries")
        except Exception as e:
            print(f"Error reading silver transforms file with dbutils.fs.head: {str(e)}")
            print("Trying alternative method using Spark DataFrame...")
            
            # Try reading with Spark
            try:
                # Read the JSON file as a text file first
                df = spark.read.text(silver_transforms_file)
                
                # Collect all lines and join them
                all_text = "\n".join([row.value for row in df.collect()])
                
                # Parse the JSON
                silver_transforms = json.loads(all_text)
                print(f"Successfully parsed silver transforms JSON with Spark method: {len(silver_transforms)} entries")
            except Exception as spark_e:
                print(f"Error reading with Spark method: {str(spark_e)}")
                print("Cannot process silver transforms due to JSON parsing errors.")
                return silver_files
        
        # Print all table names in the silver transforms file for debugging
        all_tables_in_file = [transform.get("target_table", "") for transform in silver_transforms]
        print(f"Tables found in silver transforms file: {', '.join(filter(None, all_tables_in_file))}")
        
        # Filter transforms to only include the tables we want to process
        filtered_transforms = []
        for transform in silver_transforms:
            table_name = transform.get("target_table", "")
            if table_name in tables_to_process:
                filtered_transforms.append(transform)
                print(f"Found table '{table_name}' in silver transforms file")
            elif table_name:  # Only log if table_name is not empty
                print(f"Table '{table_name}' found in silver transforms but not in tables_to_process")
        
        # Check if any of the specified tables are missing from the silver transforms
        found_tables = [transform.get("target_table", "") for transform in filtered_transforms]
        missing_tables = [table for table in tables_to_process if table not in found_tables]
        
        if missing_tables:
            print(f"WARNING: The following tables were not found in the silver transforms file: {', '.join(missing_tables)}")
        
        if not filtered_transforms:
            print(f"ERROR: None of the specified tables were found in the silver transforms file. Skipping silver transforms processing.")
            print(f"Tables to process: {tables_to_process}")
            print(f"Tables in silver transforms: {all_tables_in_file}")
            return silver_files
        
        # Process each table in the filtered silver transforms
        for transform in filtered_transforms:
            try:
                # Extract table information
                table_name = transform.get("target_table", "")
                select_expressions = transform.get("select_exp", [])
                
                if not table_name:
                    print("Skipping entry with no target_table")
                    continue
                
                print(f"Processing silver transform for table: {table_name}")
                
                # Check if select_expressions is missing or empty
                if not select_expressions:
                    print(f"WARNING: No select expressions found for table '{table_name}' in silver transforms file")
                    print(f"Transform data for '{table_name}': {json.dumps(transform, indent=2)}")
                    # Use a default select expression if none is provided
                    select_expressions = [f"* as {table_name}"]
                    print(f"Using default select expression: {select_expressions}")
                else:
                    print(f"Found {len(select_expressions)} select expressions for table '{table_name}'")
                
                # Generate silver configuration
                # Define bronze and silver database names using the variables from databricks.yml
                bronze_database = f"{bronze_catalog}.{bronze_schema}"
                silver_database = f"{silver_catalog}.{silver_schema}"
                
                # Generate silver configuration
                silver_config = {
                    "dataFlowId": f"{source_system.lower()}_{table_name}_silver",
                    "dataFlowGroup": f"{source_system}_Silver",
                    "dataFlowType": "silver",
                    "sourceSystem": source_system,
                    "sourceType": "delta",
                    "sourceViewName": f"v_{table_name}",
                    "sourceDetails": {
                        "database": bronze_database,
                        "table": table_name,
                        "cdfEnabled": True,
                        "selectExp": select_expressions
                    },
                    "mode": "append",
                    "targetFormat": "delta",
                    "targetDetails": {
                        "database": silver_database,
                        "table": table_name,
                        "schemaPath": f"{source_system}_{table_name}_schema.json"
                    },
                    "cdcSettings": {
                        "ignore_null_updates": False
                    }
                }
                
                # Generate output file path
                silver_output_file = f"{output_path}/{source_system}_{table_name}_silver.json"
                
                # Write the file
                dbutils.fs.put(silver_output_file, json.dumps(silver_config, indent=2), overwrite=True)
                
                print(f"Generated silver configuration: {silver_output_file}")
                silver_files.append(silver_output_file)
                
            except Exception as e:
                print(f"Error processing silver transform for {table_name}: {str(e)}")
                print("Stack trace:")
                import traceback
                traceback.print_exc()
        
        return silver_files
    except Exception as e:
        print(f"Error reading silver transforms file: {str(e)}")
        print("Stack trace:")
        import traceback
        traceback.print_exc()
        return silver_files

# Process silver transforms
silver_files = process_silver_transforms()
print(f"Generated {len(silver_files)} silver configuration files")

# Add silver files to output files
output_files.extend(silver_files)

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
                
                # Update silver-specific fields
                if "sourceDetails" in data:
                    # Remove path and readerOptions
                    if "path" in data["sourceDetails"]:
                        del data["sourceDetails"]["path"]
                    if "readerOptions" in data["sourceDetails"]:
                        del data["sourceDetails"]["readerOptions"]
                    
                    # Set database and cdfEnabled using variables from databricks.yml
                    data["sourceDetails"]["database"] = f"{bronze_catalog}.{bronze_schema}"
                    data["sourceDetails"]["cdfEnabled"] = True
                
                # Update sourceType
                data["sourceType"] = "delta"
                
                # Add sourceViewName if not present
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
                    data["targetDetails"]["schemaPath"] = f"{source_system}_{table_name}_schema.json"
                
                result["silver"].append(dataflow_id)
            else:
                target_dir = bronze_dir
                schema_dir = bronze_schema_dir
                
                # Add bronze-specific fields
                # Set dataQualityExpectationsEnabled to False
                data["dataQualityExpectationsEnabled"] = False
                
                # Remove dataQualityExpectationsPath if present
                if "dataQualityExpectationsPath" in data:
                    del data["dataQualityExpectationsPath"]
                
                # Remove quarantineMode if present
                if "quarantineMode" in data:
                    del data["quarantineMode"]
                
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
            dbutils.fs.put(target_file, json.dumps(ordered_data, indent=2), overwrite=True)
            
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
