"""
DLT Migration Tool Audit Utils

This module provides utility functions for the DLT Migration Tool audit framework.
"""

from pyspark.sql import SparkSession
from typing import List, Dict, Any, Optional
import datetime
import json

def get_migration_runs(catalog: str = "utilities_dev", schema: str = "erp_remediation", 
                      limit: int = 100, source_system: str = None, status: str = None):
    """
    Get recent migration runs from the audit tables.
    
    Args:
        catalog: Unity Catalog name
        schema: Schema name
        limit: Maximum number of runs to return
        source_system: Filter by source system
        status: Filter by status
        
    Returns:
        DataFrame with migration runs
    """
    spark = SparkSession.builder.getOrCreate()
    
    query = f"SELECT * FROM {catalog}.{schema}.dlt_migration_runs"
    where_clauses = []
    
    if source_system:
        where_clauses.append(f"source_system = '{source_system}'")
    
    if status:
        where_clauses.append(f"status = '{status}'")
    
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    query += f" ORDER BY start_time DESC LIMIT {limit}"
    
    return spark.sql(query)

def get_table_migrations(run_id: str = None, table_name: str = None, status: str = None,
                        catalog: str = "utilities_dev", schema: str = "erp_remediation", limit: int = 100):
    """
    Get table migrations from the audit tables.
    
    Args:
        run_id: Filter by run ID
        table_name: Filter by table name
        status: Filter by status
        catalog: Unity Catalog name
        schema: Schema name
        limit: Maximum number of records to return
        
    Returns:
        DataFrame with table migrations
    """
    spark = SparkSession.builder.getOrCreate()
    
    query = f"SELECT * FROM {catalog}.{schema}.dlt_migration_tables"
    where_clauses = []
    
    if run_id:
        where_clauses.append(f"run_id = '{run_id}'")
    
    if table_name:
        where_clauses.append(f"table_name = '{table_name}'")
    
    if status:
        where_clauses.append(f"status = '{status}'")
    
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    query += f" ORDER BY start_time DESC LIMIT {limit}"
    
    return spark.sql(query)

def get_migration_artifacts(run_id: str = None, table_name: str = None, artifact_type: str = None,
                           catalog: str = "utilities_dev", schema: str = "erp_remediation", limit: int = 100):
    """
    Get migration artifacts from the audit tables.
    
    Args:
        run_id: Filter by run ID
        table_name: Filter by table name
        artifact_type: Filter by artifact type
        catalog: Unity Catalog name
        schema: Schema name
        limit: Maximum number of records to return
        
    Returns:
        DataFrame with migration artifacts
    """
    spark = SparkSession.builder.getOrCreate()
    
    query = f"SELECT * FROM {catalog}.{schema}.dlt_migration_artifacts"
    where_clauses = []
    
    if run_id:
        where_clauses.append(f"run_id = '{run_id}'")
    
    if table_name:
        where_clauses.append(f"table_name = '{table_name}'")
    
    if artifact_type:
        where_clauses.append(f"artifact_type = '{artifact_type}'")
    
    if where_clauses:
        query += " WHERE " + " AND ".join(where_clauses)
    
    query += f" ORDER BY creation_time DESC LIMIT {limit}"
    
    return spark.sql(query)

def get_migration_summary(run_id: str, catalog: str = "utilities_dev", schema: str = "erp_remediation"):
    """
    Get a comprehensive summary of a migration run.
    
    Args:
        run_id: Migration run ID
        catalog: Unity Catalog name
        schema: Schema name
        
    Returns:
        Dictionary with migration summary
    """
    spark = SparkSession.builder.getOrCreate()
    
    # Get run information
    run_df = spark.sql(f"""
        SELECT * FROM {catalog}.{schema}.dlt_migration_runs
        WHERE run_id = '{run_id}'
    """)
    
    if run_df.count() == 0:
        return {"error": f"Run ID {run_id} not found"}
    
    run_info = run_df.first().asDict()
    
    # Get tables information
    tables_df = spark.sql(f"""
        SELECT * FROM {catalog}.{schema}.dlt_migration_tables
        WHERE run_id = '{run_id}'
    """)
    
    tables_info = [row.asDict() for row in tables_df.collect()]
    
    # Get artifacts information
    artifacts_df = spark.sql(f"""
        SELECT * FROM {catalog}.{schema}.dlt_migration_artifacts
        WHERE run_id = '{run_id}'
    """)
    
    artifacts_info = [row.asDict() for row in artifacts_df.collect()]
    
    # Calculate statistics
    successful_tables = len([t for t in tables_info if t["status"] == "SUCCESS"])
    failed_tables = len([t for t in tables_info if t["status"] == "FAILED"])
    
    # Group artifacts by type
    artifacts_by_type = {}
    for artifact in artifacts_info:
        artifact_type = artifact["artifact_type"]
        if artifact_type not in artifacts_by_type:
            artifacts_by_type[artifact_type] = []
        artifacts_by_type[artifact_type].append(artifact)
    
    # Calculate duration if available
    duration_seconds = None
    if run_info["end_time"] and run_info["start_time"]:
        duration = run_info["end_time"] - run_info["start_time"]
        duration_seconds = duration.total_seconds()
    
    # Build summary
    summary = {
        "run_info": run_info,
        "statistics": {
            "total_tables": len(tables_info),
            "successful_tables": successful_tables,
            "failed_tables": failed_tables,
            "total_artifacts": len(artifacts_info),
            "duration_seconds": duration_seconds
        },
        "tables": tables_info,
        "artifacts_by_type": artifacts_by_type
    }
    
    return summary

def print_migration_summary(run_id: str, catalog: str = "utilities_dev", schema: str = "erp_remediation"):
    """
    Print a formatted summary of a migration run.
    
    Args:
        run_id: Migration run ID
        catalog: Unity Catalog name
        schema: Schema name
    """
    summary = get_migration_summary(run_id, catalog, schema)
    
    if "error" in summary:
        print(f"Error: {summary['error']}")
        return
    
    run_info = summary["run_info"]
    stats = summary["statistics"]
    
    print("=" * 80)
    print(f"DLT MIGRATION SUMMARY - RUN ID: {run_id}")
    print("=" * 80)
    print(f"Source System: {run_info['source_system']}")
    print(f"Environment: {run_info['environment']}")
    print(f"User: {run_info['user']}")
    print(f"Status: {run_info['status']}")
    print(f"Start Time: {run_info['start_time']}")
    print(f"End Time: {run_info['end_time'] if run_info['end_time'] else 'Running'}")
    
    if stats["duration_seconds"]:
        minutes, seconds = divmod(stats["duration_seconds"], 60)
        print(f"Duration: {int(minutes)} minutes, {int(seconds)} seconds")
    
    print("\nSTATISTICS:")
    print(f"Total Tables: {stats['total_tables']}")
    print(f"Successful Tables: {stats['successful_tables']}")
    print(f"Failed Tables: {stats['failed_tables']}")
    print(f"Total Artifacts: {stats['total_artifacts']}")
    
    print("\nTABLES:")
    for table in summary["tables"]:
        status_icon = "✅" if table["status"] == "SUCCESS" else "❌"
        print(f"{status_icon} {table['table_name']} - Bronze: {table['bronze_target']}, Silver: {table['silver_target']}")
        if table["error_message"]:
            print(f"   Error: {table['error_message']}")
    
    print("\nARTIFACTS:")
    for artifact_type, artifacts in summary["artifacts_by_type"].items():
        print(f"{artifact_type} ({len(artifacts)}):")
        for artifact in artifacts[:5]:  # Show only first 5 of each type
            table_info = f" - {artifact['table_name']}" if artifact['table_name'] else ""
            print(f"  {artifact['artifact_path']}{table_info}")
        if len(artifacts) > 5:
            print(f"  ... and {len(artifacts) - 5} more")
    
    print("=" * 80)
