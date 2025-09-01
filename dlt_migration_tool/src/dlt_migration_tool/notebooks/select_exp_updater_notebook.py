# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Migration Tool - Select Expression Updater
# MAGIC 
# MAGIC This notebook updates silver dataflowspec JSON files with select expressions from a reference file.
# MAGIC 
# MAGIC ## Features
# MAGIC 
# MAGIC - Load select expressions from a reference file
# MAGIC - Update silver dataflowspec JSON files with the select expressions
# MAGIC - Generate a report of updated files

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Required Modules
# MAGIC 
# MAGIC This will import all the necessary modules directly from the source code.

# COMMAND ----------

# MAGIC %run ./imports

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Select Expression Updater

# COMMAND ----------

from dlt_migration_tool.utils.select_exp_updater import SelectExpUpdater, update_select_expressions

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
REFERENCE_FILE = "apasamples/actuals/silvertransforms_maximo.json"

# Full volume path
VOLUME_PATH = f"/Volumes/{UC_CATALOG}/{UC_SCHEMA}/{VOLUME_NAME}"

# Full paths
output_path = os.path.join(VOLUME_PATH, OUTPUT_DIR)
silver_dir = os.path.join(output_path, "silver_maximo", "dataflowspec")

# Reference file path - adjust this to the correct path
reference_file = os.path.join("/Users/sudheer.palyam/workspace/databricks/dlt_migration_tool", REFERENCE_FILE)

print(f"Silver directory: {silver_dir}")
print(f"Reference file: {reference_file}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Select Expressions

# COMMAND ----------

# Check if the reference file exists
if not os.path.exists(reference_file):
    print(f"Reference file not found: {reference_file}")
else:
    print(f"Reference file found: {reference_file}")
    
    # Check if the silver directory exists
    if not os.path.exists(silver_dir):
        print(f"Silver directory not found: {silver_dir}")
    else:
        print(f"Silver directory found: {silver_dir}")
        
        # Update select expressions
        results = update_select_expressions(reference_file, silver_dir)
        
        # Print summary
        updated_count = sum(1 for status in results.values() if status)
        print(f"Updated {updated_count} of {len(results)} files")
        
        # Print details of updated files
        print("\nUpdated files:")
        for file_path, status in results.items():
            if status:
                print(f"- {os.path.basename(file_path)}")
        
        # Print details of files that were not updated
        if len(results) - updated_count > 0:
            print("\nFiles not updated:")
            for file_path, status in results.items():
                if not status:
                    print(f"- {os.path.basename(file_path)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validate Updated Files

# COMMAND ----------

# Check a sample of updated files
def check_updated_files(silver_dir, max_files=5):
    """Check a sample of updated files to verify they contain select expressions."""
    json_files = [os.path.join(silver_dir, f) for f in os.listdir(silver_dir) 
                 if f.endswith('.json') and os.path.isfile(os.path.join(silver_dir, f))]
    
    sample_files = json_files[:max_files]
    
    print(f"Checking {len(sample_files)} sample files:")
    
    for file_path in sample_files:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            has_select_exp = (
                'sourceDetails' in data and 
                'selectExp' in data['sourceDetails'] and 
                isinstance(data['sourceDetails']['selectExp'], list) and
                len(data['sourceDetails']['selectExp']) > 0
            )
            
            table_name = data.get('targetDetails', {}).get('table', 'unknown')
            
            if has_select_exp:
                select_exp_count = len(data['sourceDetails']['selectExp'])
                print(f"VALID: {os.path.basename(file_path)} ({table_name}): {select_exp_count} select expressions")
            else:
                print(f"ERROR: {os.path.basename(file_path)} ({table_name}): No select expressions found")
        except Exception as e:
            print(f"Error checking {file_path}: {str(e)}")

# Check a sample of updated files
if 'results' in locals() and len(results) > 0:
    check_updated_files(silver_dir)
else:
    print("No files were processed, skipping validation.")

# COMMAND ----------
