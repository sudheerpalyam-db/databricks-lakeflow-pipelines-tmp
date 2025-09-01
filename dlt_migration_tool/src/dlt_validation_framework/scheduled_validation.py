# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Validation Framework - Scheduled Validation
# MAGIC 
# MAGIC This notebook is designed to be scheduled as a Databricks job to run validations on a regular basis.

# COMMAND ----------

# MAGIC %pip install -U pytest

# COMMAND ----------

# Import required libraries
import sys
import os
from datetime import datetime

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
dbutils.widgets.dropdown("source_target_pairs", "all", [
    "all", "maximo", "maximo_ods", "workday", "workday_ods"
])

# Widget for table selection
dbutils.widgets.text("tables", "", "Tables to validate (comma-separated, leave empty for all)")

# Widget for primary keys
dbutils.widgets.text("primary_keys", "id", "Primary key columns (comma-separated)")

# COMMAND ----------

# Get widget values
source_target_pairs = dbutils.widgets.get("source_target_pairs")
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

# Determine which mappings to use
if source_target_pairs == "all":
    source_target_mappings = list(pair_mapping.values())
else:
    source_target_mappings = [pair_mapping.get(source_target_pairs)]

print(f"Source-Target Mappings: {source_target_mappings}")
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
# MAGIC ## Run Validation for Each Mapping

# COMMAND ----------

# Create a dictionary mapping tables to primary keys
if tables:
    table_pk_map = {table: primary_keys for table in tables}
else:
    table_pk_map = None

# Track all results
all_results = []

# Run validation for each mapping
for mapping in source_target_mappings:
    print(f"Running validation for {mapping['source_schema']} to {mapping['target_schema']}...")
    
    # Run validation
    results = framework.validate_tables(
        tables=tables,
        source_target_mapping=mapping,
        primary_keys=table_pk_map
    )
    
    all_results.append(results)
    
    print(f"Completed validation for {mapping['source_schema']} to {mapping['target_schema']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Summary Report

# COMMAND ----------

# Generate a summary report
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# Get all run IDs from this execution
run_ids = [result["run_id"] for result in all_results]
run_ids_str = "', '".join(run_ids)

# Query the audit table for all runs in this execution
audit_path = framework.config.get_audit_table_path()

summary_query = f"""
SELECT 
    source_catalog,
    source_schema,
    source_table,
    validation_type,
    validation_status,
    source_count,
    target_count,
    mismatch_count
FROM 
    {audit_path}
WHERE 
    run_id IN ('{run_ids_str}')
ORDER BY
    source_catalog,
    source_schema,
    source_table,
    validation_type
"""

summary_results = spark.sql(summary_query)

# COMMAND ----------

# Display summary results
display(summary_results)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Report to Table

# COMMAND ----------

# Save the summary report to a timestamped table
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
report_table = f"{framework.config.config['audit_catalog']}.{framework.config.config['audit_schema']}.validation_report_{timestamp}"

summary_results.write.format("delta").mode("overwrite").saveAsTable(report_table)

print(f"Summary report saved to {report_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Validation Statistics

# COMMAND ----------

# Calculate validation statistics
stats_query = f"""
SELECT 
    source_schema,
    validation_status,
    COUNT(*) as count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (PARTITION BY source_schema), 2) as percentage
FROM 
    {audit_path}
WHERE 
    run_id IN ('{run_ids_str}')
GROUP BY
    source_schema,
    validation_status
ORDER BY
    source_schema,
    validation_status
"""

stats_results = spark.sql(stats_query)
display(stats_results)

# COMMAND ----------

# Show tables with mismatches
mismatches_query = f"""
SELECT 
    source_catalog,
    source_schema,
    source_table,
    validation_type,
    source_count,
    target_count,
    mismatch_count,
    mismatch_details
FROM 
    {audit_path}
WHERE 
    run_id IN ('{run_ids_str}')
    AND validation_status = 'FAILED'
ORDER BY
    source_catalog,
    source_schema,
    source_table,
    validation_type
"""

mismatches = spark.sql(mismatches_query)
display(mismatches)
