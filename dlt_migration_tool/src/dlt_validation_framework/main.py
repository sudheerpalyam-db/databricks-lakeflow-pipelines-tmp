# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Validation Framework - Main Notebook
# MAGIC 
# MAGIC This is the main entry point for the DLT Validation Framework. It provides a user interface to run different validation operations.

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
# MAGIC ## Setup Widgets

# COMMAND ----------

# Create widgets for user interaction
dbutils.widgets.dropdown("operation", "run_validation", [
    "bootstrap", "run_validation", "custom_validation", "view_results"
])

dbutils.widgets.dropdown("source_target_pair", "maximo", [
    "maximo", "maximo_ods", "workday", "workday_ods"
])

dbutils.widgets.text("tables", "", "Tables to validate (comma-separated, leave empty for all)")
dbutils.widgets.text("primary_keys", "id", "Primary key columns (comma-separated)")
dbutils.widgets.text("run_id", "", "Run ID for viewing results (leave empty for latest)")

# COMMAND ----------

# Get widget values
operation = dbutils.widgets.get("operation")
source_target_pair = dbutils.widgets.get("source_target_pair")
tables_input = dbutils.widgets.get("tables")
primary_keys_input = dbutils.widgets.get("primary_keys")
run_id_input = dbutils.widgets.get("run_id")

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

print(f"Operation: {operation}")
print(f"Source-Target Mapping: {source_target_mapping}")
print(f"Tables to validate: {tables if tables else 'All tables'}")
print(f"Primary keys: {primary_keys}")
print(f"Run ID: {run_id_input if run_id_input else 'Latest'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Framework

# COMMAND ----------

# Initialize the validation framework
framework = ValidationFramework()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Selected Operation

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

if operation == "bootstrap":
    # Bootstrap the framework
    print("Bootstrapping the validation framework...")
    
    # Create audit table if it doesn't exist
    framework.config.create_audit_table_if_not_exists()
    
    # Display the audit table path
    print(f"Audit table path: {framework.config.get_audit_table_path()}")
    
    # Test access to source and target databases
    for mapping in framework.config.get_source_target_mappings():
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
    
    print("Bootstrap complete!")

elif operation == "run_validation":
    # Run validation with the selected parameters
    print("Running validation...")
    
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
    
    # Display validation results
    import json
    print(json.dumps(results, indent=2))
    
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
    
    print("Validation results:")
    display(audit_results)

elif operation == "custom_validation":
    # Run custom validation for specific tables
    print("Running custom validation...")
    
    # Create a dictionary mapping tables to primary keys
    if tables:
        table_pk_map = {table: primary_keys for table in tables}
    else:
        # Use default tables and primary keys
        table_pk_map = {
            "asset": ["asset_id"],
            "commodities": ["commodity_id"]
        }
        tables = list(table_pk_map.keys())
    
    # Run validation
    results = framework.validate_tables(
        tables=tables,
        source_target_mapping=source_target_mapping,
        primary_keys=table_pk_map
    )
    
    # Display validation results
    import json
    print(json.dumps(results, indent=2))
    
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
    
    print("Custom validation results:")
    display(audit_results)

elif operation == "view_results":
    # View results from a previous run
    print("Viewing validation results...")
    
    # Get the audit table path
    audit_path = framework.config.get_audit_table_path()
    
    # Determine the run_id to use
    if run_id_input:
        run_id = run_id_input
    else:
        # Get the latest run_id
        latest_run = spark.sql(f"""
        SELECT run_id
        FROM {audit_path}
        ORDER BY run_timestamp DESC
        LIMIT 1
        """)
        
        if latest_run.count() > 0:
            run_id = latest_run.collect()[0]["run_id"]
            print(f"Using latest run ID: {run_id}")
        else:
            print("No previous runs found.")
            run_id = None
    
    if run_id:
        # Query the audit table for this run
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
        
        print(f"Results for run {run_id}:")
        display(audit_results)
        
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
        
        print("Summary statistics:")
        display(summary)
        
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
        
        if mismatches.count() > 0:
            print("Tables with mismatches:")
            display(mismatches)
        else:
            print("No mismatches found!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Operation Complete
# MAGIC 
# MAGIC The selected operation has been completed. You can change the operation and parameters using the widgets above and run the notebook again.
