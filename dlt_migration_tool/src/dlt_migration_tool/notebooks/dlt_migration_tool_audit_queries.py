# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Migration Tool - Audit Framework
# MAGIC 
# MAGIC This notebook demonstrates how to use the audit framework to query and analyze migration runs.

# COMMAND ----------

# Import required modules
import os
import sys
import json
import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

# Add the source directory to the path
source_dir = "/Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/src"
if source_dir not in sys.path:
    sys.path.append(source_dir)

# Import audit framework
from dlt_migration_tool.audit import (
    get_migration_runs,
    get_table_migrations,
    get_migration_artifacts,
    get_migration_summary,
    print_migration_summary
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC 
# MAGIC Set up configuration parameters for the audit framework.

# COMMAND ----------

# Add widgets for configuration
dbutils.widgets.text("catalog", "utilities_dev")
dbutils.widgets.text("schema", "erp_remediation")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.dropdown("status", "ALL", ["ALL", "SUCCESS", "FAILED", "RUNNING"])
dbutils.widgets.text("limit", "100")

# Get widget values
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id") or None
source_system = dbutils.widgets.get("source_system") or None
table_name = dbutils.widgets.get("table_name") or None
status = dbutils.widgets.get("status")
limit = int(dbutils.widgets.get("limit"))

if status == "ALL":
    status = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Migration Runs
# MAGIC 
# MAGIC Get a list of recent migration runs.

# COMMAND ----------

# Query migration runs
runs_df = get_migration_runs(catalog=catalog, schema=schema, limit=limit, source_system=source_system, status=status)
display(runs_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Table Migrations
# MAGIC 
# MAGIC Get details of table migrations.

# COMMAND ----------

# Query table migrations
tables_df = get_table_migrations(run_id=run_id, table_name=table_name, status=status, catalog=catalog, schema=schema, limit=limit)
display(tables_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Migration Artifacts
# MAGIC 
# MAGIC Get details of artifacts generated during migrations.

# COMMAND ----------

# Query migration artifacts
artifacts_df = get_migration_artifacts(run_id=run_id, table_name=table_name, catalog=catalog, schema=schema, limit=limit)
display(artifacts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Print Migration Summary
# MAGIC 
# MAGIC Print a detailed summary of a specific migration run.

# COMMAND ----------

# Check if run_id is provided
if run_id:
    print_migration_summary(run_id=run_id, catalog=catalog, schema=schema)
else:
    print("Please provide a run_id in the widget to see a migration summary.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Queries
# MAGIC 
# MAGIC Example SQL queries to analyze migration data.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get migration success rate by source system
# MAGIC SELECT 
# MAGIC   source_system,
# MAGIC   COUNT(*) as total_runs,
# MAGIC   SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs,
# MAGIC   SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_runs,
# MAGIC   ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
# MAGIC FROM ${catalog}.${schema}.dlt_migration_runs
# MAGIC GROUP BY source_system
# MAGIC ORDER BY total_runs DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get average migration duration by source system
# MAGIC SELECT 
# MAGIC   source_system,
# MAGIC   COUNT(*) as total_runs,
# MAGIC   ROUND(AVG(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)), 2) as avg_duration_seconds,
# MAGIC   ROUND(AVG(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / 60, 2) as avg_duration_minutes
# MAGIC FROM ${catalog}.${schema}.dlt_migration_runs
# MAGIC WHERE end_time IS NOT NULL
# MAGIC GROUP BY source_system
# MAGIC ORDER BY avg_duration_seconds DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get table migration success rate
# MAGIC SELECT 
# MAGIC   table_name,
# MAGIC   source_system,
# MAGIC   COUNT(*) as total_migrations,
# MAGIC   SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_migrations,
# MAGIC   SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed_migrations,
# MAGIC   ROUND(SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as success_rate
# MAGIC FROM ${catalog}.${schema}.dlt_migration_tables
# MAGIC GROUP BY table_name, source_system
# MAGIC ORDER BY total_migrations DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get artifact counts by type
# MAGIC SELECT 
# MAGIC   artifact_type,
# MAGIC   COUNT(*) as count
# MAGIC FROM ${catalog}.${schema}.dlt_migration_artifacts
# MAGIC GROUP BY artifact_type
# MAGIC ORDER BY count DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get recent failures with error messages
# MAGIC SELECT 
# MAGIC   r.run_id,
# MAGIC   r.source_system,
# MAGIC   r.environment,
# MAGIC   r.start_time,
# MAGIC   r.error_message as run_error,
# MAGIC   t.table_name,
# MAGIC   t.status as table_status,
# MAGIC   t.error_message as table_error
# MAGIC FROM ${catalog}.${schema}.dlt_migration_runs r
# MAGIC LEFT JOIN ${catalog}.${schema}.dlt_migration_tables t ON r.run_id = t.run_id
# MAGIC WHERE r.status = 'FAILED' OR t.status = 'FAILED'
# MAGIC ORDER BY r.start_time DESC
# MAGIC LIMIT 100

# COMMAND ----------
