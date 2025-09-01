"""
Configuration module for DLT Validation Framework.
This module contains configuration settings and utility functions for the validation framework.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, BooleanType, IntegerType, MapType
import uuid
from datetime import datetime
from pyspark.sql.functions import current_timestamp, col, lit

# Define the schemas for the validation tables
AUDIT_SCHEMA = StructType([
    StructField("run_id", StringType(), False),
    StructField("run_timestamp", TimestampType(), False),
    StructField("source_catalog", StringType(), False),
    StructField("source_schema", StringType(), False),
    StructField("source_table", StringType(), False),
    StructField("target_catalog", StringType(), False),
    StructField("target_schema", StringType(), False),
    StructField("target_table", StringType(), False),
    StructField("validation_type", StringType(), False),
    StructField("validation_status", StringType(), False),
    StructField("source_count", IntegerType(), True),
    StructField("target_count", IntegerType(), True),
    StructField("mismatch_count", IntegerType(), True),
    StructField("mismatch_details", StringType(), True),
    StructField("execution_time_seconds", IntegerType(), True)
])

# Default configuration
DEFAULT_CONFIG = {
    "audit_catalog": "utilities_dev",
    "audit_schema": "erp_remediation",
    "audit_table": "dlt_validation_results",
    "validation_types": ["row_count", "row_hash"],
    "count_tolerance_percentage": 0,
    "default_primary_keys": ["id"],
    "max_mismatch_details": 100,
    "source_target_mappings": [
        {
            "source_catalog": "edp_bronze_dev",
            "source_schema": "maximo",
            "target_catalog": "edp_bronze_dev",
            "target_schema": "maximo_lakeflow"
        },
        {
            "source_catalog": "edp_silver_dev",
            "source_schema": "maximo_ods",
            "target_catalog": "edp_silver_dev",
            "target_schema": "maximo_ods_lakeflow"
        },
        {
            "source_catalog": "edp_bronze_dev",
            "source_schema": "workday",
            "target_catalog": "edp_bronze_dev",
            "target_schema": "workday_lakeflow"
        },
        {
            "source_catalog": "edp_silver_dev",
            "source_schema": "workday_ods",
            "target_catalog": "edp_silver_dev",
            "target_schema": "workday_ods_lakeflow"
        }
    ]
}

class ValidationConfig:
    """Configuration class for validation framework."""
    
    def __init__(self, config=None):
        """Initialize with provided config or default."""
        self.config = config or DEFAULT_CONFIG
        self.spark = SparkSession.builder.getOrCreate()
        
    def get_audit_table_path(self):
        """Get the full path to the audit table."""
        catalog = self.config["audit_catalog"]
        schema = self.config["audit_schema"]
        table = self.config["audit_table"]
        return f"{catalog}.{schema}.{table}"
    
    def create_audit_table_if_not_exists(self):
        """Create the audit table if it doesn't exist."""
        audit_path = self.get_audit_table_path()
        
        # Check if table exists
        tables = self.spark.sql(f"SHOW TABLES FROM {self.config['audit_catalog']}.{self.config['audit_schema']}")
        table_exists = tables.filter(col("tableName") == self.config["audit_table"]).count() > 0
        
        if not table_exists:
            empty_df = self.spark.createDataFrame([], AUDIT_SCHEMA)
            empty_df.write.format("delta").mode("overwrite").saveAsTable(audit_path)
            print(f"Created audit table: {audit_path}")
        else:
            print(f"Audit table {audit_path} already exists")
    
    def generate_run_id(self):
        """Generate a unique run ID."""
        return str(uuid.uuid4())
    
    def get_source_target_mappings(self):
        """Get the source-target schema mappings."""
        return self.config["source_target_mappings"]
    
    def get_validation_types(self):
        """Get the validation types to run."""
        return self.config["validation_types"]
    
    def get_count_tolerance(self):
        """Get the tolerance percentage for count validation."""
        return self.config["count_tolerance_percentage"]
    
    def get_default_primary_keys(self):
        """Get the default primary keys."""
        return self.config["default_primary_keys"]
    
    def get_max_mismatch_details(self):
        """Get the maximum number of mismatches to record in details."""
        return self.config["max_mismatch_details"]
