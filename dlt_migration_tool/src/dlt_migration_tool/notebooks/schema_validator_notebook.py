# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Migration Tool - Schema Validation
# MAGIC 
# MAGIC This notebook provides functionality to validate generated dataflowspec JSON files against the JSON Schema definitions from the Databricks LakeFlow Framework.
# MAGIC 
# MAGIC ## Features
# MAGIC 
# MAGIC - Validate individual dataflowspec JSON files
# MAGIC - Validate all dataflowspec JSON files in a directory
# MAGIC - Generate validation reports
# MAGIC - Support for different dataflowspec types (standard, flow, materialized_view)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Modules
# MAGIC 
# MAGIC This will import all the necessary modules directly from the source code.

# COMMAND ----------

# MAGIC %run ./imports

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Schema Validator

# COMMAND ----------

from dlt_migration_tool.utils.schema_validator import SchemaValidator, validate_dataflowspec, validate_directory

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Unity Catalog configuration
UC_CATALOG = "utilities_dev"
UC_SCHEMA = "erp_remediation"
VOLUME_NAME = "dlt_migration_tool"

# Paths within the volume
OUTPUT_DIR = "output"

# Full volume path
VOLUME_PATH = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{VOLUME_NAME}"

# Full paths
output_path = os.path.join(VOLUME_PATH, OUTPUT_DIR)

# Directories to validate
bronze_dir = os.path.join(output_path, "bronze_maximo", "dataflowspec")
silver_dir = os.path.join(output_path, "silver_maximo", "dataflowspec")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Schema Validator

# COMMAND ----------

# Initialize the schema validator
validator = SchemaValidator()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Bronze Dataflowspecs

# COMMAND ----------

# Check if the bronze directory exists
if os.path.exists(bronze_dir):
    print(f"Validating Bronze dataflowspecs in {bronze_dir}...")
    bronze_results = validator.validate_directory(bronze_dir)
    
    print(f"Bronze validation results:")
    print(f"- Valid files: {bronze_results['valid']}")
    print(f"- Invalid files: {bronze_results['invalid']}")
    
    if bronze_results["invalid"] > 0:
        print("\nInvalid Bronze files:")
        for result in bronze_results["files"]:
            if not result["valid"]:
                print(f"ERROR: {os.path.basename(result['file'])}: {result.get('error', 'unknown error')}")
    else:
        print("\nAll Bronze dataflowspecs are valid!")
else:
    print(f"Bronze directory not found: {bronze_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Silver Dataflowspecs

# COMMAND ----------

# Check if the silver directory exists
if os.path.exists(silver_dir):
    print(f"Validating Silver dataflowspecs in {silver_dir}...")
    silver_results = validator.validate_directory(silver_dir)
    
    print(f"Silver validation results:")
    print(f"- Valid files: {silver_results['valid']}")
    print(f"- Invalid files: {silver_results['invalid']}")
    
    if silver_results["invalid"] > 0:
        print("\nInvalid Silver files:")
        for result in silver_results["files"]:
            if not result["valid"]:
                print(f"ERROR: {os.path.basename(result['file'])}: {result.get('error', 'unknown error')}")
    else:
        print("\nAll Silver dataflowspecs are valid!")
else:
    print(f"Silver directory not found: {silver_dir}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Validation Report

# COMMAND ----------

# Combine results
total_valid = 0
total_invalid = 0
all_files = []

if 'bronze_results' in locals():
    total_valid += bronze_results["valid"]
    total_invalid += bronze_results["invalid"]
    all_files.extend(bronze_results["files"])

if 'silver_results' in locals():
    total_valid += silver_results["valid"]
    total_invalid += silver_results["invalid"]
    all_files.extend(silver_results["files"])

# Generate report
print("=== Validation Report ===")
print(f"Total files validated: {total_valid + total_invalid}")
print(f"Valid files: {total_valid}")
print(f"Invalid files: {total_invalid}")

if total_invalid > 0:
    print("\nInvalid files:")
    for result in all_files:
        if not result["valid"]:
            print(f"ERROR: {os.path.basename(result['file'])}: {result.get('error', 'unknown error')}")
else:
    print("\nAll dataflowspecs are valid!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate a Specific File (Example)

# COMMAND ----------

# Example: Validate a specific file
# Replace with an actual file path if you want to test this
example_file = os.path.join(bronze_dir, "maximo_a_locations_main.json") if os.path.exists(bronze_dir) else None

if example_file and os.path.exists(example_file):
    print(f"Validating {example_file}...")
    result = validator.validate_file(example_file)
    
    if result["valid"]:
        print(f"VALID: {os.path.basename(example_file)} is valid according to schema {result.get('schema', 'unknown')}")
    else:
        print(f"ERROR: {os.path.basename(example_file)} is invalid: {result.get('error', 'unknown error')}")
else:
    print("Example file not found. Skipping specific file validation.")

# COMMAND ----------
