# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Validation Framework - Run Validation
# MAGIC 
# MAGIC This notebook runs the validation framework to compare source and target tables.

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
# MAGIC ## Configuration

# COMMAND ----------

# Widget for selecting schema mapping
dbutils.widgets.dropdown("source_target_pair", "maximo", [
    "maximo", "maximo_ods", "workday", "workday_ods"
])

# Widget for table selection
dbutils.widgets.text("tables", "", "Tables to validate (comma-separated, leave empty for all)")

# Widget for primary keys
dbutils.widgets.text("primary_keys", "id", "Primary key columns (comma-separated)")

# COMMAND ----------

# Get widget values
source_target_pair = dbutils.widgets.get("source_target_pair")
tables_input = dbutils.widgets.get("tables")
primary_keys_input = dbutils.widgets.get("primary_keys")

# Parse tables input
tables = [t.strip() for t in tables_input.split(",")] if tables_input.strip() else None

# Parse primary keys input
primary_keys = [pk.strip() for pk in primary_keys_input.split(",")]

# Map source_target_pair to actual catalog and schema names
pair_mapping = {
    "maximo": {
        "source_catalog": "edp_bronze_dev",
        "source_schema": "maximo",
        "target_catalog": "edp_bronze_dev",
        "target_schema": "maximo_lakeflow"
    },
    "maximo_ods": {
        "source_catalog": "edp_silver_dev",
        "source_schema": "maximo_ods",
        "target_catalog": "edp_silver_dev",
        "target_schema": "maximo_ods_lakeflow"
    },
    "workday": {
        "source_catalog": "edp_bronze_dev",
        "source_schema": "workday",
        "target_catalog": "edp_bronze_dev",
        "target_schema": "workday_lakeflow"
    },
    "workday_ods": {
        "source_catalog": "edp_silver_dev",
        "source_schema": "workday_ods",
        "target_catalog": "edp_silver_dev",
        "target_schema": "workday_ods_lakeflow"
    }
}

# Get the mapping for the selected pair
source_target_mapping = pair_mapping.get(source_target_pair)

print(f"Source-Target Mapping: {source_target_mapping}")
print(f"Tables to validate: {tables if tables else 'All tables'}")
print(f"Primary keys: {primary_keys}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Framework

# COMMAND ----------

# Initialize the validation framework
framework = ValidationFramework()

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Available Tables

# COMMAND ----------

# List available tables in the source schema
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

source_catalog = source_target_mapping["source_catalog"]
source_schema = source_target_mapping["source_schema"]

print(f"Available tables in {source_catalog}.{source_schema}:")
available_tables = get_all_tables_in_schema(source_catalog, source_schema)
for table in available_tables:
    print(f"- {table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Validation

# COMMAND ----------

# Create a dictionary mapping tables to primary keys
if tables:
    table_pk_map = {table: primary_keys for table in tables}
else:
    table_pk_map = None

# Run validation
results = framework.validate_tables(
    tables=tables,
    source_target_mapping=source_target_mapping,
    primary_keys=table_pk_map
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
# MAGIC ## Summary Statistics

# COMMAND ----------

# Calculate summary statistics
summary = spark.sql(f"""
SELECT 
    validation_status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM {audit_path} WHERE run_id = '{run_id}'), 2) as percentage
FROM 
    {audit_path}
WHERE 
    run_id = '{run_id}'
GROUP BY
    validation_status
ORDER BY
    validation_status
""")

display(summary)

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
