# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Migration Tool - Utilities for Unity Catalog Volumes
# MAGIC 
# MAGIC This notebook provides utility functions for setting up and managing the DLT Migration Tool environment in Databricks using Unity Catalog volumes.
# MAGIC 
# MAGIC ## Features
# MAGIC 
# MAGIC - Create and manage Unity Catalog volumes
# MAGIC - Upload sample files to volumes
# MAGIC - Check environment status
# MAGIC - Clean up output directories
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC 
# MAGIC - Unity Catalog must be enabled in your workspace
# MAGIC - You must have permissions to create and manage volumes in the specified catalog and schema

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import os
import json
from pathlib import Path

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Unity Catalog configuration
UC_CATALOG = "utilities_dev"
UC_SCHEMA = "erp_remediation"
VOLUME_NAME = "dlt_migration_tool"

# Default paths within the volume
DEFAULT_INPUT_DIR = "input"
DEFAULT_OUTPUT_DIR = "output"
DEFAULT_SCHEMA_DIR = "schemas"

# Full volume path
VOLUME_PATH = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{VOLUME_NAME}"

# Maximo pipelines project path in workspace
WORKSPACE_MAXIMO_PROJECT_PATH = "/Workspace/Users/sudheer.palyam@apa.com.au/databricks-lakeflow-pipelines/maximo"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Volume Management Functions

# COMMAND ----------

def create_volume():
    """
    Create the Unity Catalog volume if it doesn't exist.
    
    Returns:
        True if the volume was created or already exists, False otherwise
    """
    try:
        # Check if the volume exists
        volume_exists = False
        try:
            volumes = spark.sql(f"SHOW VOLUMES IN {UC_CATALOG}.{UC_SCHEMA}").collect()
            for volume in volumes:
                if volume.name == VOLUME_NAME:
                    volume_exists = True
                    break
        except:
            pass
        
        if not volume_exists:
            # Create the volume if it doesn't exist
            spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.{VOLUME_NAME}
            COMMENT 'Volume for DLT Migration Tool'
            """)
        
        print(f"Volume {UC_CATALOG}.{UC_SCHEMA}.{VOLUME_NAME} is ready")
        return True
    except Exception as e:
        print(f"Error creating volume: {str(e)}")
        return False

# COMMAND ----------

def create_directories():
    """
    Create the necessary directories within the Unity Catalog volume.
    
    Returns:
        Dictionary with the created paths
    """
    # Make sure the volume exists
    if not create_volume():
        raise Exception("Failed to create or access the volume")
    
    # Create main directories
    input_path = f"{VOLUME_PATH}/{DEFAULT_INPUT_DIR}"
    output_path = f"{VOLUME_PATH}/{DEFAULT_OUTPUT_DIR}"
    schema_path = f"{VOLUME_PATH}/{DEFAULT_SCHEMA_DIR}"
    
    dbutils.fs.mkdirs(input_path)
    dbutils.fs.mkdirs(output_path)
    dbutils.fs.mkdirs(schema_path)
    
    # Create bronze and silver subdirectories
    bronze_dir = f"{output_path}/bronze_maximo/dataflowspec"
    silver_dir = f"{output_path}/silver_maximo/dataflowspec"
    bronze_schema_dir = f"{output_path}/bronze_maximo/schemas"
    silver_schema_dir = f"{output_path}/silver_maximo/schemas"
    
    dbutils.fs.mkdirs(bronze_dir)
    dbutils.fs.mkdirs(silver_dir)
    dbutils.fs.mkdirs(bronze_schema_dir)
    dbutils.fs.mkdirs(silver_schema_dir)
    
    # Return the created paths
    paths = {
        "volume": VOLUME_PATH,
        "input": input_path,
        "output": output_path,
        "schema": schema_path,
        "bronze_dataflowspec": bronze_dir,
        "silver_dataflowspec": silver_dir,
        "bronze_schema": bronze_schema_dir,
        "silver_schema": silver_schema_dir
    }
    
    print("Created directories in Unity Catalog volume:")
    for name, path in paths.items():
        print(f"- {name}: {path}")
    
    return paths

# COMMAND ----------

def clean_directories(input_path=None, output_path=None, schema_path=None):
    """
    Clean the specified directories by removing all files.
    
    Args:
        input_path: Path to the input directory (optional)
        output_path: Path to the output directory (optional)
        schema_path: Path to the schema directory (optional)
        
    Returns:
        List of cleaned directories
    """
    # Make sure the volume exists
    if not create_volume():
        raise Exception("Failed to create or access the volume")
    
    # Set default paths if not provided
    if input_path is None:
        input_path = f"{VOLUME_PATH}/{DEFAULT_INPUT_DIR}"
    if output_path is None:
        output_path = f"{VOLUME_PATH}/{DEFAULT_OUTPUT_DIR}"
    if schema_path is None:
        schema_path = f"{VOLUME_PATH}/{DEFAULT_SCHEMA_DIR}"
    
    cleaned_dirs = []
    
    # Clean output directory
    try:
        files = dbutils.fs.ls(output_path)
        for file in files:
            dbutils.fs.rm(file.path, recurse=True)
        cleaned_dirs.append(output_path)
    except:
        pass
    
    # Clean input directory if specified
    try:
        files = dbutils.fs.ls(input_path)
        for file in files:
            if not file.isDir():
                dbutils.fs.rm(file.path)
        cleaned_dirs.append(input_path)
    except:
        pass
    
    # Clean schema directory if specified
    try:
        files = dbutils.fs.ls(schema_path)
        for file in files:
            if not file.isDir():
                dbutils.fs.rm(file.path)
        cleaned_dirs.append(schema_path)
    except:
        pass
    
    print(f"Cleaned {len(cleaned_dirs)} directories in Unity Catalog volume:")
    for dir_path in cleaned_dirs:
        print(f"- {dir_path}")
    
    return cleaned_dirs

def clean_output_directory():
    """
    Clean only the output directory in the Unity Catalog volume.
    
    This function removes all files and subdirectories in the output directory
    without affecting the input or schema directories.
    
    Returns:
        Path of the cleaned output directory
    """
    # Make sure the volume exists
    if not create_volume():
        raise Exception("Failed to create or access the volume")
    
    # Set output path
    output_path = f"{VOLUME_PATH}/{DEFAULT_OUTPUT_DIR}"
    
    # Clean output directory
    try:
        files = dbutils.fs.ls(output_path)
        for file in files:
            dbutils.fs.rm(file.path, recurse=True)
        
        print(f"Cleaned output directory: {output_path}")
    except Exception as e:
        print(f"Error cleaning output directory: {str(e)}")
        return None
    
    return output_path

# COMMAND ----------

# MAGIC %md
# MAGIC ## File Management Functions

# COMMAND ----------

def upload_sample_file(file_content, file_name, target_dir=None):
    """
    Upload a sample file to the Unity Catalog volume.
    
    Args:
        file_content: Content of the file
        file_name: Name of the file
        target_dir: Target directory within the volume (defaults to input directory)
        
    Returns:
        Path to the uploaded file
    """
    # Make sure the volume exists
    if not create_volume():
        raise Exception("Failed to create or access the volume")
    
    # Set default target directory if not provided
    if target_dir is None:
        target_dir = f"{VOLUME_PATH}/{DEFAULT_INPUT_DIR}"
    
    # Make sure the directory exists
    dbutils.fs.mkdirs(target_dir)
    
    # Copy the file
    target_path = f"{target_dir}/{file_name}"
    dbutils.fs.put(target_path, file_content, overwrite=True)
    
    print(f"Uploaded {file_name} to {target_path}")
    return target_path

# COMMAND ----------

def upload_sample_files_from_samples_dir(samples_dir, target_dir=None):
    """
    Upload sample files from a directory to the Unity Catalog volume.
    
    Args:
        samples_dir: Path to the samples directory
        target_dir: Target directory within the volume (defaults to input directory)
        
    Returns:
        List of uploaded files
    """
    # Make sure the volume exists
    if not create_volume():
        raise Exception("Failed to create or access the volume")
    
    # Set default target directory if not provided
    if target_dir is None:
        target_dir = f"{VOLUME_PATH}/{DEFAULT_INPUT_DIR}"
    
    # Make sure the target directory exists
    dbutils.fs.mkdirs(target_dir)
    
    # List files in the samples directory
    try:
        files = dbutils.fs.ls(samples_dir)
    except:
        print(f"Sample directory not found: {samples_dir}")
        return []
    
    # Copy all JSON files
    uploaded_files = []
    for file in files:
        if file.name.endswith('.json') and not file.isDir():
            # Read the file content
            file_content = dbutils.fs.head(file.path)
            
            # Write to the target path
            target_path = f"{target_dir}/{file.name}"
            dbutils.fs.put(target_path, file_content, overwrite=True)
            uploaded_files.append(target_path)
    
    print(f"Uploaded {len(uploaded_files)} sample files to {target_dir}:")
    for file in uploaded_files:
        print(f"- {os.path.basename(file)}")
    
    return uploaded_files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Environment Check Functions

# COMMAND ----------

def check_environment():
    """
    Check if the environment is set up correctly.
    
    Returns:
        Dictionary with the check results
    """
    # Make sure the volume exists
    volume_exists = create_volume()
    
    # Set paths
    input_path = f"{VOLUME_PATH}/{DEFAULT_INPUT_DIR}"
    output_path = f"{VOLUME_PATH}/{DEFAULT_OUTPUT_DIR}"
    
    results = {
        "volume_exists": volume_exists,
        "input_dir_exists": False,
        "output_dir_exists": False,
        "input_files": []
    }
    
    # Check for input directory
    try:
        dbutils.fs.ls(input_path)
        results["input_dir_exists"] = True
    except:
        pass
    
    # Check for output directory
    try:
        dbutils.fs.ls(output_path)
        results["output_dir_exists"] = True
    except:
        pass
    
    # Check for input files
    if results["input_dir_exists"]:
        try:
            files = dbutils.fs.ls(input_path)
            results["input_files"] = [file.name for file in files if file.name.endswith('.json') and not file.isDir()]
        except:
            pass
    
    # Print the results
    print("Environment Check Results:")
    print(f"- Volume exists: {results['volume_exists']}")
    print(f"- Input directory exists: {results['input_dir_exists']}")
    print(f"- Output directory exists: {results['output_dir_exists']}")
    
    if results["input_files"]:
        print(f"- Found {len(results['input_files'])} input files:")
        for file in results["input_files"]:
            print(f"  - {file}")
    else:
        print("- No input files found")
    
    return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Files in Volume Directories

# COMMAND ----------

def list_directory_contents(directory_path, recursive=False, indent=""):
    """
    List all files and directories in the specified directory.
    
    Args:
        directory_path: Path to the directory
        recursive: Whether to list files recursively
        indent: Indentation for nested directories
        
    Returns:
        Number of files found
    """
    try:
        files = dbutils.fs.ls(directory_path)
        file_count = 0
        
        for file in files:
            if file.isDir():
                print(f"{indent}Directory: {file.name}")
                if recursive:
                    file_count += list_directory_contents(file.path, recursive, indent + "  ")
            else:
                print(f"{indent}File: {file.name}")
                file_count += 1
        
        return file_count
    except Exception as e:
        print(f"{indent}Error listing directory {directory_path}: {str(e)}")
        return 0

def list_volume_contents(recursive=True):
    """
    List all files in the volume.
    
    Args:
        recursive: Whether to list files recursively
        
    Returns:
        Dictionary with counts of files in each directory
    """
    # Make sure the volume exists
    if not create_volume():
        raise Exception("Failed to create or access the volume")
    
    print(f"Contents of volume {VOLUME_PATH}:")
    
    # List the main directories
    counts = {}
    
    # List input directory
    input_path = f"{VOLUME_PATH}/{DEFAULT_INPUT_DIR}"
    print(f"\nInput Directory ({input_path}):")
    counts["input"] = list_directory_contents(input_path, recursive)
    
    # List output directory
    output_path = f"{VOLUME_PATH}/{DEFAULT_OUTPUT_DIR}"
    print(f"\nOutput Directory ({output_path}):")
    counts["output"] = list_directory_contents(output_path, recursive)
    
    # List schema directory
    schema_path = f"{VOLUME_PATH}/{DEFAULT_SCHEMA_DIR}"
    print(f"\nSchema Directory ({schema_path}):")
    counts["schema"] = list_directory_contents(schema_path, recursive)
    
    # Print summary
    print("\nSummary:")
    print(f"- Input files: {counts['input']}")
    print(f"- Output files: {counts['output']}")
    print(f"- Schema files: {counts['schema']}")
    print(f"- Total files: {sum(counts.values())}")
    
    return counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Copy Files to Maximo Pipelines Project

# COMMAND ----------

def copy_to_maximo_project(environment="dev", source_system="maximo"):
    """
    Copy the generated dataflow specs and schemas to the Maximo pipelines project in the workspace.
    
    Args:
        environment: Environment (dev, test, prod)
        source_system: Source system name
        
    Returns:
        Dictionary with counts of files copied
    """
    # Source paths in the volume
    volume_output_path = f"{VOLUME_PATH}/{DEFAULT_OUTPUT_DIR}/{source_system}_{environment}"
    bronze_dataflowspec_path = f"{volume_output_path}/bronze_{source_system}/dataflowspec"
    silver_dataflowspec_path = f"{volume_output_path}/silver_{source_system}/dataflowspec"
    
    # Target paths in the workspace project (using Databricks workspace paths without /dbfs prefix)
    workspace_bronze_dataflowspec_path = f"{WORKSPACE_MAXIMO_PROJECT_PATH}/src/dataflows/bronze_maximo"
    workspace_silver_dataflowspec_path = f"{WORKSPACE_MAXIMO_PROJECT_PATH}/src/dataflows/silver_maximo"
    
    # Count of files copied
    counts = {
        "bronze_dataflowspec": 0,
        "silver_dataflowspec": 0
    }
    
    print(f"Copying files for environment: {environment}, source system: {source_system}")
    
    # Copy bronze dataflowspec files
    try:
        bronze_dataflowspec_files = dbutils.fs.ls(bronze_dataflowspec_path)
        
        for file in bronze_dataflowspec_files:
            if file.name.endswith('.json') and not file.isDir():
                try:
                    # Read the file content
                    file_content = dbutils.fs.head(file.path)
                    
                    # Write to the workspace project using dbutils.fs
                    target_file_path = f"{workspace_bronze_dataflowspec_path}/{file.name}"
                    dbutils.fs.put(target_file_path, file_content, overwrite=True)
                    
                    counts["bronze_dataflowspec"] += 1
                    print(f"Copied {file.name} to {target_file_path}")
                except Exception as e:
                    print(f"Error copying {file.name}: {str(e)}")
    except Exception as e:
        print(f"Error accessing bronze dataflowspec files: {str(e)}")
    
    # Copy silver dataflowspec files
    try:
        silver_dataflowspec_files = dbutils.fs.ls(silver_dataflowspec_path)
        
        for file in silver_dataflowspec_files:
            if file.name.endswith('.json') and not file.isDir():
                try:
                    # Read the file content
                    file_content = dbutils.fs.head(file.path)
                    
                    # Write to the workspace project using dbutils.fs
                    target_file_path = f"{workspace_silver_dataflowspec_path}/{file.name}"
                    dbutils.fs.put(target_file_path, file_content, overwrite=True)
                    
                    counts["silver_dataflowspec"] += 1
                    print(f"Copied {file.name} to {target_file_path}")
                except Exception as e:
                    print(f"Error copying {file.name}: {str(e)}")
    except Exception as e:
        print(f"Error accessing silver dataflowspec files: {str(e)}")
    
    # Print summary
    total = sum(counts.values())
    print("\nCopy Summary:")
    print(f"- Bronze dataflowspec files: {counts['bronze_dataflowspec']}")
    print(f"- Silver dataflowspec files: {counts['silver_dataflowspec']}")
    print(f"- Total files: {total}")
    
    return counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 1: Set Up Environment

# COMMAND ----------

# Set up the environment
paths = create_directories()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 2: Upload Sample Files

# COMMAND ----------

# Example of uploading a sample file
sample_json = """
{
  "dataFlowId": "maximo_asset_bronze",
  "dataFlowGroup": "Maximo_Bronze",
  "sourceSystem": "Maximo",
  "sourceType": "cloudFiles",
  "sourceDetails": {
    "path": "s3://s3-staging-maximo-dev-apse2-058264204922/internal/MAXIMO/ASSET",
    "readerOptions": {
      "cloudFiles.format": "parquet",
      "cloudFiles.inferColumnTypes": "true"
    }
  },
  "targetFormat": "delta",
  "targetDetails": {
    "database": "edp_bronze_dev.maximo",
    "table": "asset"
  }
}
"""

# Commented out to avoid creating sample files by default
# upload_sample_file(sample_json, "sample_maximo_asset.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 3: Check Environment

# COMMAND ----------

# Check the environment
check_results = check_environment()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 4: Clean Output Directory

# COMMAND ----------

# Uncomment to clean the output directory
# clean_directories(output_path=f"{VOLUME_PATH}/{DEFAULT_OUTPUT_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 5: List All Files in Volume

# COMMAND ----------

# List all files in the volume
list_volume_contents()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example 6: Copy Files to Maximo Pipelines Project

# COMMAND ----------

# Copy files to the Maximo pipelines project
copy_to_maximo_project(environment="dev", source_system="maximo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Accessing Files in the Volume
# MAGIC 
# MAGIC You can access files in the volume using the following paths:
# MAGIC 
# MAGIC - In Python/Scala: `/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{VOLUME_NAME}/{path}`
# MAGIC - In SQL: `{UC_CATALOG}.{UC_SCHEMA}.{VOLUME_NAME}/{path}`
# MAGIC 
# MAGIC For example, to list files in the input directory:

# COMMAND ----------

# List files in the input directory
input_path = f"{VOLUME_PATH}/{DEFAULT_INPUT_DIR}"
try:
    files = dbutils.fs.ls(input_path)
    print(f"Files in input directory ({input_path}):")
    for file in files:
        print(f"- {file.name}")
except Exception as e:
    print(f"Error listing input directory: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display File Contents
# MAGIC 
# MAGIC You can also display the contents of specific files:

# COMMAND ----------

def display_file_contents(file_path):
    """
    Display the contents of a file.
    
    Args:
        file_path: Path to the file
    """
    try:
        # Read the file content
        content = dbutils.fs.head(file_path)
        
        # Try to parse as JSON for prettier display
        try:
            parsed_content = json.loads(content)
            print(json.dumps(parsed_content, indent=2))
        except:
            # If not valid JSON, display as is
            print(content)
    except Exception as e:
        print(f"Error reading file {file_path}: {str(e)}")

# Example: Uncomment to display a specific file
# display_file_contents(f"{VOLUME_PATH}/output/maximo_dev/bronze_maximo/dataflowspec/maximo_asset_main.json")

# COMMAND ----------
