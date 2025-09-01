# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Validation Framework - Bootstrap
# MAGIC 
# MAGIC This notebook initializes the validation framework by creating the necessary audit tables and configurations.

# COMMAND ----------

# MAGIC %pip install -U pytest

# COMMAND ----------

# Import required libraries
import sys
import os

# Add the parent directory to the Python path
current_dir = os.path.dirname(os.path.realpath("__file__"))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Import the framework modules
from dlt_validation_framework.src.config import ValidationConfig
from dlt_validation_framework.src.utils import save_validation_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Configuration

# COMMAND ----------

# Create default configuration
config = ValidationConfig()

# Create audit table if it doesn't exist
config.create_audit_table_if_not_exists()

# Display the audit table path
print(f"Audit table path: {config.get_audit_table_path()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Configuration to JSON (Optional)

# COMMAND ----------

# Save configuration to a file for future use
import json

config_path = "/dbfs/FileStore/dlt_validation_config.json"
save_validation_config(config.config, config_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Database Access

# COMMAND ----------

# Test access to source and target databases
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

for mapping in config.get_source_target_mappings():
    source_catalog = mapping["source_catalog"]
    source_schema = mapping["source_schema"]
    target_catalog = mapping["target_catalog"]
    target_schema = mapping["target_schema"]
    
    print(f"Testing access to {source_catalog}.{source_schema}")
    try:
        tables = spark.sql(f"SHOW TABLES FROM {source_catalog}.{source_schema}")
        print(f"Found {tables.count()} tables in source schema")
    except Exception as e:
        print(f"Error accessing source schema: {str(e)}")
    
    print(f"Testing access to {target_catalog}.{target_schema}")
    try:
        tables = spark.sql(f"SHOW TABLES FROM {target_catalog}.{target_schema}")
        print(f"Found {tables.count()} tables in target schema")
    except Exception as e:
        print(f"Error accessing target schema: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Audit Table Structure

# COMMAND ----------

# Verify the audit table structure
audit_path = config.get_audit_table_path()
try:
    audit_df = spark.table(audit_path)
    print("Audit table structure:")
    audit_df.printSchema()
except Exception as e:
    print(f"Error accessing audit table: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bootstrap Complete
# MAGIC 
# MAGIC The validation framework has been initialized successfully. You can now use the validation notebook to run validations.
