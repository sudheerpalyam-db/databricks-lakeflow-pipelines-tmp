# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Validation Framework - Custom Table Validation
# MAGIC 
# MAGIC This notebook allows you to run validations for specific tables with custom primary keys.

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
from dlt_validation_framework.src.validation import TableValidator
from dlt_validation_framework.src.main import ValidationFramework
from dlt_validation_framework.src.utils import get_all_tables_in_schema, load_validation_config

# COMMAND ----------

# MAGIC %md
# MAGIC ## Custom Configuration

# COMMAND ----------

# Define custom tables and their primary keys
custom_tables = {
    "asset": ["asset_id"],
    "commodities": ["commodity_id"],
    # Add more tables and their primary keys as needed
}

# Define source-target mapping
source_target_mapping = {
    "source_catalog": "edp_bronze_dev",
    "source_schema": "maximo",
    "target_catalog": "edp_bronze_dev",
    "target_schema": "maximo_lakeflow"
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Framework

# COMMAND ----------

# Initialize the validation framework
framework = ValidationFramework()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Validation for Custom Tables

# COMMAND ----------

# Get the list of tables to validate
tables = list(custom_tables.keys())
print(f"Validating tables: {tables}")

# Run validation
results = framework.validate_tables(
    tables=tables,
    source_target_mapping=source_target_mapping,
    primary_keys=custom_tables
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Results

# COMMAND ----------

# Display validation results
import json
print(json.dumps(results, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Audit Table

# COMMAND ----------

# Query the audit table for this run
run_id = results["run_id"]
audit_path = framework.config.get_audit_table_path()

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

audit_results = spark.sql(f"""
SELECT 
    source_table,
    validation_type,
    validation_status,
    source_count,
    target_count,
    mismatch_count,
    execution_time_seconds
FROM 
    {audit_path}
WHERE 
    run_id = '{run_id}'
ORDER BY
    source_table,
    validation_type
""")

display(audit_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show Detailed Mismatches

# COMMAND ----------

# Show tables with mismatches
mismatches = spark.sql(f"""
SELECT 
    source_table,
    validation_type,
    source_count,
    target_count,
    mismatch_count,
    mismatch_details
FROM 
    {audit_path}
WHERE 
    run_id = '{run_id}'
    AND validation_status = 'FAILED'
ORDER BY
    source_table,
    validation_type
""")

display(mismatches)
