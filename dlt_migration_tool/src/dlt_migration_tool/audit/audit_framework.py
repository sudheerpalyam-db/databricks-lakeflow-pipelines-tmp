"""
DLT Migration Tool Audit Framework

This module provides audit and tracking capabilities for the DLT Migration Tool.
It tracks all runs of the tool, which tables were migrated, any failures, 
which target tables were created, and paths to volumes where artifacts are saved.
"""

import json
import uuid
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType, MapType
from typing import List, Dict, Any, Optional

class AuditFramework:
    """
    Audit Framework for tracking DLT Migration Tool executions.
    
    This class provides methods to track runs, table migrations, failures,
    and output artifacts in Unity Catalog tables.
    """
    
    def __init__(self, catalog: str = "utilities_dev", schema: str = "erp_remediation"):
        """
        Initialize the Audit Framework.
        
        Args:
            catalog: Unity Catalog name (default: utilities_dev)
            schema: Schema name (default: erp_remediation)
        """
        self.catalog = catalog
        self.schema = schema
        self.spark = SparkSession.builder.getOrCreate()
        self.run_id = str(uuid.uuid4())
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.status = "RUNNING"
        self.source_system = None
        self.environment = None
        self.user = None
        self.notebook_path = None
        self.tables_processed = []
        self.failures = []
        self.artifacts = []
        
        # Create audit tables if they don't exist
        self._create_audit_tables()
    
    def _create_audit_tables(self):
        """Create audit tables if they don't exist."""
        
        # Migration Runs table
        runs_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("source_system", StringType(), True),
            StructField("environment", StringType(), True),
            StructField("user", StringType(), True),
            StructField("notebook_path", StringType(), True),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("status", StringType(), False),
            StructField("tables_count", StringType(), True),
            StructField("error_message", StringType(), True)
        ])
        
        # Table Migrations table
        tables_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("table_name", StringType(), False),
            StructField("source_system", StringType(), True),
            StructField("bronze_target", StringType(), True),
            StructField("silver_target", StringType(), True),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), True),
            StructField("status", StringType(), False),
            StructField("error_message", StringType(), True)
        ])
        
        # Artifacts table
        artifacts_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("table_name", StringType(), True),
            StructField("artifact_type", StringType(), False),
            StructField("artifact_path", StringType(), False),
            StructField("creation_time", TimestampType(), False)
        ])
        
        # Create the tables if they don't exist
        try:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog}.{self.schema}")
            
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.dlt_migration_runs (
                    run_id STRING NOT NULL,
                    source_system STRING,
                    environment STRING,
                    user STRING,
                    notebook_path STRING,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status STRING NOT NULL,
                    tables_count STRING,
                    error_message STRING
                )
                USING DELTA
            """)
            
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.dlt_migration_tables (
                    run_id STRING NOT NULL,
                    table_name STRING NOT NULL,
                    source_system STRING,
                    bronze_target STRING,
                    silver_target STRING,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP,
                    status STRING NOT NULL,
                    error_message STRING
                )
                USING DELTA
            """)
            
            self.spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {self.catalog}.{self.schema}.dlt_migration_artifacts (
                    run_id STRING NOT NULL,
                    table_name STRING,
                    artifact_type STRING NOT NULL,
                    artifact_path STRING NOT NULL,
                    creation_time TIMESTAMP NOT NULL
                )
                USING DELTA
            """)
            
        except Exception as e:
            print(f"Error creating audit tables: {str(e)}")
    
    def start_run(self, source_system: str, environment: str, notebook_path: str = None):
        """
        Start tracking a new migration run.
        
        Args:
            source_system: Source system name (e.g., "maximo", "workday")
            environment: Environment name (e.g., "dev", "test", "prod")
            notebook_path: Path to the notebook executing the migration
        """
        self.source_system = source_system
        self.environment = environment
        self.notebook_path = notebook_path
        
        # Get the current user
        try:
            self.user = self.spark.sql("SELECT current_user()").collect()[0][0]
        except:
            self.user = "unknown"
        
        # Insert run record
        self.spark.sql(f"""
            INSERT INTO {self.catalog}.{self.schema}.dlt_migration_runs
            VALUES (
                '{self.run_id}',
                '{self.source_system}',
                '{self.environment}',
                '{self.user}',
                '{self.notebook_path}',
                '{self.start_time.isoformat()}',
                NULL,
                '{self.status}',
                '0',
                NULL
            )
        """)
        
        print(f"Started migration run with ID: {self.run_id}")
        return self.run_id
    
    def end_run(self, status: str = "SUCCESS", error_message: str = None):
        """
        End the current migration run.
        
        Args:
            status: Run status (SUCCESS, FAILED, etc.)
            error_message: Error message if the run failed
        """
        self.end_time = datetime.datetime.now()
        self.status = status
        
        # Update run record
        error_clause = f"'{error_message}'" if error_message else "NULL"
        self.spark.sql(f"""
            UPDATE {self.catalog}.{self.schema}.dlt_migration_runs
            SET 
                end_time = '{self.end_time.isoformat()}',
                status = '{status}',
                tables_count = '{len(self.tables_processed)}',
                error_message = {error_clause}
            WHERE run_id = '{self.run_id}'
        """)
        
        print(f"Ended migration run {self.run_id} with status: {status}")
    
    def track_table_migration(self, table_name: str, bronze_target: str = None, silver_target: str = None):
        """
        Start tracking a table migration.
        
        Args:
            table_name: Name of the table being migrated
            bronze_target: Bronze target table (e.g., "edp_bronze_dev.maximo.asset")
            silver_target: Silver target table (e.g., "edp_silver_dev.maximo_ods.asset")
            
        Returns:
            Dictionary with tracking information
        """
        start_time = datetime.datetime.now()
        
        # Insert table migration record
        self.spark.sql(f"""
            INSERT INTO {self.catalog}.{self.schema}.dlt_migration_tables
            VALUES (
                '{self.run_id}',
                '{table_name}',
                '{self.source_system}',
                '{bronze_target}',
                '{silver_target}',
                '{start_time.isoformat()}',
                NULL,
                'RUNNING',
                NULL
            )
        """)
        
        tracking_info = {
            "table_name": table_name,
            "start_time": start_time,
            "bronze_target": bronze_target,
            "silver_target": silver_target
        }
        
        self.tables_processed.append(tracking_info)
        return tracking_info
    
    def complete_table_migration(self, table_name: str, status: str = "SUCCESS", error_message: str = None):
        """
        Complete tracking for a table migration.
        
        Args:
            table_name: Name of the table being migrated
            status: Migration status (SUCCESS, FAILED, etc.)
            error_message: Error message if the migration failed
        """
        end_time = datetime.datetime.now()
        
        # Update table migration record
        error_clause = f"'{error_message}'" if error_message else "NULL"
        self.spark.sql(f"""
            UPDATE {self.catalog}.{self.schema}.dlt_migration_tables
            SET 
                end_time = '{end_time.isoformat()}',
                status = '{status}',
                error_message = {error_clause}
            WHERE run_id = '{self.run_id}' AND table_name = '{table_name}'
        """)
        
        # Update tracking info in memory
        for table in self.tables_processed:
            if table["table_name"] == table_name:
                table["end_time"] = end_time
                table["status"] = status
                table["error_message"] = error_message
                break
        
        if status == "FAILED" and error_message:
            self.failures.append({
                "table_name": table_name,
                "error_message": error_message,
                "time": end_time
            })
    
    def track_artifact(self, artifact_type: str, artifact_path: str, table_name: str = None):
        """
        Track an artifact generated during migration.
        
        Args:
            artifact_type: Type of artifact (e.g., "bronze_spec", "silver_spec", "schema", "dqe")
            artifact_path: Path to the artifact
            table_name: Name of the associated table (optional)
        """
        creation_time = datetime.datetime.now()
        
        # Insert artifact record
        table_clause = f"'{table_name}'" if table_name else "NULL"
        self.spark.sql(f"""
            INSERT INTO {self.catalog}.{self.schema}.dlt_migration_artifacts
            VALUES (
                '{self.run_id}',
                {table_clause},
                '{artifact_type}',
                '{artifact_path}',
                '{creation_time.isoformat()}'
            )
        """)
        
        artifact_info = {
            "artifact_type": artifact_type,
            "artifact_path": artifact_path,
            "table_name": table_name,
            "creation_time": creation_time
        }
        
        self.artifacts.append(artifact_info)
        return artifact_info
    
    def get_run_summary(self):
        """
        Get a summary of the current migration run.
        
        Returns:
            Dictionary with run summary information
        """
        duration = None
        if self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        return {
            "run_id": self.run_id,
            "source_system": self.source_system,
            "environment": self.environment,
            "user": self.user,
            "notebook_path": self.notebook_path,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_seconds": duration,
            "status": self.status,
            "tables_processed": len(self.tables_processed),
            "failures": len(self.failures),
            "artifacts_generated": len(self.artifacts)
        }
    
    def get_tables_summary(self):
        """
        Get a summary of all tables processed in the current run.
        
        Returns:
            List of dictionaries with table information
        """
        return self.tables_processed
    
    def get_failures(self):
        """
        Get a list of failures encountered during the current run.
        
        Returns:
            List of dictionaries with failure information
        """
        return self.failures
    
    def get_artifacts(self):
        """
        Get a list of artifacts generated during the current run.
        
        Returns:
            List of dictionaries with artifact information
        """
        return self.artifacts
