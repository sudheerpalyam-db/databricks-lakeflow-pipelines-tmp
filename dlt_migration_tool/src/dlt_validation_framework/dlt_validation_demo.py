# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Migration Validation Framework - Demo
# MAGIC 
# MAGIC This notebook demonstrates how to use the DLT Validation Framework to compare tables before and after migration to Declarative Lakeflow.

# COMMAND ----------

# Import required libraries
import sys
import os
import json
import uuid
import logging
import traceback
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("ValidationFramework")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Validation Framework Modules

# COMMAND ----------

# Import the validation framework modules directly
from config import ValidationConfig
from validation import TableValidator

# COMMAND ----------

# MAGIC %md
# MAGIC ## ValidationFramework Class Definition

# COMMAND ----------

class ValidationFramework:
    """Main class for orchestrating the validation framework."""
    
    def __init__(self, config_path=None):
        """Initialize with optional config path."""
        self.spark = SparkSession.builder.getOrCreate()
        logger.info("Initializing ValidationFramework")
        
        # Load config from file if provided
        if config_path:
            logger.info(f"Loading configuration from {config_path}")
            with open(config_path, 'r') as f:
                config_dict = json.load(f)
            self.config = ValidationConfig(config_dict)
        else:
            logger.info("Using default configuration")
            self.config = ValidationConfig()
        
        # Initialize validator
        logger.info("Initializing TableValidator")
        self.validator = TableValidator(self.config)
        
        # Ensure audit table exists
        logger.info("Ensuring audit table exists")
        self.config.create_audit_table_if_not_exists()
        logger.info("ValidationFramework initialization complete")
    
    def validate_tables(self, tables=None, source_target_mapping=None, primary_keys=None):
        """
        Run validation for specified tables or all tables in a mapping.
        
        Args:
            tables: List of table names to validate (optional)
            source_target_mapping: Dictionary with source and target schema info (optional)
            primary_keys: Dictionary mapping table names to primary key lists (optional)
            
        Returns:
            Dictionary with validation summary
        """
        run_id = self.config.generate_run_id()
        logger.info(f"Starting validation run: {run_id}")
        
        # Get source-target mappings
        if source_target_mapping:
            mappings = [source_target_mapping]
            logger.info(f"Using provided source-target mapping: {source_target_mapping}")
        else:
            mappings = self.config.get_source_target_mappings()
            logger.info(f"Using configured source-target mappings: {mappings}")
        
        # Get validation types
        validation_types = self.config.get_validation_types()
        logger.info(f"Validation types to run: {validation_types}")
        
        # Track validation results
        results = {
            "run_id": run_id,
            "total_validations": 0,
            "successful_validations": 0,
            "failed_validations": 0,
            "error_validations": 0,
            "tables_validated": []
        }
        
        # Process each mapping
        for mapping in mappings:
            source_catalog = mapping["source_catalog"]
            source_schema = mapping["source_schema"]
            target_catalog = mapping["target_catalog"]
            target_schema = mapping["target_schema"]
            
            logger.info(f"Processing mapping: {source_catalog}.{source_schema} → {target_catalog}.{target_schema}")
            
            # Get tables to validate
            if tables:
                table_list = tables
                logger.info(f"Using provided tables list: {tables}")
            else:
                # Get all tables in the source schema
                logger.info(f"Fetching all tables from {source_catalog}.{source_schema}")
                try:
                    source_tables = self.spark.sql(f"SHOW TABLES FROM {source_catalog}.{source_schema}")
                    table_list = [row.tableName for row in source_tables.collect()]
                    logger.info(f"Found {len(table_list)} tables in {source_catalog}.{source_schema}")
                except Exception as e:
                    logger.error(f"Error fetching tables from {source_catalog}.{source_schema}: {str(e)}")
                    logger.error(traceback.format_exc())
                    table_list = []
            
            # Validate each table
            for table in table_list:
                logger.info(f"Starting validation for table: {table}")
                table_results = {
                    "table_name": table,
                    "validations": []
                }
                
                # Get primary keys for this table
                table_pk = None
                if primary_keys and table in primary_keys:
                    table_pk = primary_keys[table]
                    logger.info(f"Using primary keys for {table}: {table_pk}")
                else:
                    logger.info(f"No specific primary keys provided for {table}, will use defaults")
                
                # Run each validation type
                for validation_type in validation_types:
                    logger.info(f"Running {validation_type} validation for {table}")
                    
                    try:
                        if validation_type == "row_count":
                            logger.info(f"Executing row_count validation for {source_catalog}.{source_schema}.{table} vs {target_catalog}.{target_schema}.{table}")
                            result = self.validator.validate_row_count(
                                source_catalog, source_schema, table,
                                target_catalog, target_schema, table
                            )
                            logger.info(f"Row count validation for {table} completed with status: {result['validation_status']}")
                            
                        elif validation_type == "row_hash":
                            logger.info(f"Executing row_hash validation for {source_catalog}.{source_schema}.{table} vs {target_catalog}.{target_schema}.{table}")
                            logger.info(f"Primary keys for hash validation: {table_pk}")
                            result = self.validator.validate_row_hash(
                                source_catalog, source_schema, table,
                                target_catalog, target_schema, table,
                                primary_keys=table_pk
                            )
                            logger.info(f"Row hash validation for {table} completed with status: {result['validation_status']}")
                            if result['validation_status'] == 'ERROR':
                                logger.error(f"Row hash validation error: {result['mismatch_details']}")
                            
                        else:
                            logger.warning(f"Unknown validation type: {validation_type}, skipping")
                            continue
                    except Exception as e:
                        logger.error(f"Exception during {validation_type} validation for {table}: {str(e)}")
                        logger.error(traceback.format_exc())
                        result = {
                            "validation_type": validation_type,
                            "validation_status": "ERROR",
                            "source_count": None,
                            "target_count": None,
                            "mismatch_count": None,
                            "mismatch_details": f"Exception during validation: {str(e)}\n{traceback.format_exc()}",
                            "execution_time_seconds": 0
                        }
                    
                    # Record the result
                    logger.info(f"Recording result for {validation_type} validation of {table}")
                    try:
                        self.validator.record_validation_result(
                            run_id, source_catalog, source_schema, table,
                            target_catalog, target_schema, table, result
                        )
                    except Exception as e:
                        logger.error(f"Error recording validation result: {str(e)}")
                        logger.error(traceback.format_exc())
                    
                    # Update summary
                    results["total_validations"] += 1
                    if result["validation_status"] == "SUCCESS":
                        results["successful_validations"] += 1
                        logger.info(f"{validation_type} validation for {table} succeeded")
                    elif result["validation_status"] == "FAILED":
                        results["failed_validations"] += 1
                        logger.info(f"{validation_type} validation for {table} failed: {result['mismatch_count']} mismatches")
                    else:
                        results["error_validations"] += 1
                        logger.error(f"{validation_type} validation for {table} encountered an error")
                    
                    # Add to table results
                    table_results["validations"].append({
                        "type": validation_type,
                        "status": result["validation_status"],
                        "details": result["mismatch_details"] if result.get("mismatch_count") else "No mismatches"
                    })
                
                # Add table results to summary
                results["tables_validated"].append(table_results)
                logger.info(f"Completed all validations for table: {table}")
        
        logger.info(f"Validation run complete: {run_id}")
        logger.info(f"Total validations: {results['total_validations']}")
        logger.info(f"Successful validations: {results['successful_validations']}")
        logger.info(f"Failed validations: {results['failed_validations']}")
        logger.info(f"Error validations: {results['error_validations']}")
        
        return results

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Widgets

# COMMAND ----------

# Create widgets for user interaction
dbutils.widgets.dropdown("source_target_pair", "maximo", [
    "maximo", "maximo_ods", "workday", "workday_ods"
])

dbutils.widgets.text("tables", "asset,commodities", "Tables to validate (comma-separated, leave empty for all)")
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
# MAGIC ## Initialize the Framework

# COMMAND ----------

# Create a validation framework instance
framework = ValidationFramework()

# Create the audit table if it doesn't exist
framework.config.create_audit_table_if_not_exists()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Tables and Primary Keys

# COMMAND ----------

# Create a dictionary mapping tables to primary keys
if tables:
    table_pk_map = {table: primary_keys for table in tables}
else:
    table_pk_map = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Logs to File (Optional)

# COMMAND ----------

# Add this cell to save logs to a file
import logging
import datetime

# Configure file handler for saving logs
log_path = f"/dbfs/FileStore/validation_logs/validation_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
try:
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    
    # Configure file handler
    file_handler = logging.FileHandler(log_path)
    file_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    file_handler.setFormatter(formatter)
    
    # Add handler to both loggers
    root_logger = logging.getLogger()
    root_logger.addHandler(file_handler)
    
    print(f"Logs will be saved to: {log_path}")
except Exception as e:
    print(f"Error setting up log file: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Validation

# COMMAND ----------

# Run validation for the specified tables
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Show Error Details

# COMMAND ----------

# Show validation errors
errors = spark.sql(f"""
SELECT 
    source_table,
    validation_type,
    mismatch_details
FROM 
    {audit_path}
WHERE 
    run_id = '{run_id}'
    AND validation_status = 'ERROR'
ORDER BY
    source_table,
    validation_type
""")

display(errors)

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

# MAGIC %md
# MAGIC ## View Logs (if saved to file)

# COMMAND ----------

# View the saved log file (if it was created)
try:
    with open(log_path.replace("/dbfs", "dbfs:"), "r") as f:
        log_content = f.read()
    
    print("=== VALIDATION LOG ===")
    print(log_content)
except Exception as e:
    print(f"Error reading log file: {str(e)}")
