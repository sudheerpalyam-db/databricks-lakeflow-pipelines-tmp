# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Migration Tool - Imports
# MAGIC 
# MAGIC This notebook imports all the necessary modules for the DLT Migration Tool.
# MAGIC It uses a robust approach to handle different environments (local development vs. Databricks workspace).

# COMMAND ----------

# Import required modules
import os
import sys
import json
import datetime
import tempfile
import shutil
import uuid
from pathlib import Path
from typing import List, Dict, Any, Optional

# Define possible source directories
source_dirs = [
    # Databricks workspace paths
    "/Workspace/Users/sudheer.palyam@apa.com.au/databricks-lakeflow-pipelines-tmp/dlt_migration_tool/src",
    "/Workspace/Repos/sudheer.palyam@apa.com.au/databricks-lakeflow-pipelines-tmp/dlt_migration_tool/src",
    # Local development paths
    "/Users/sudheer.palyam/workspace/databricks/databricks-lakeflow-pipelines-tmp/dlt_migration_tool/src",
    # Try relative paths
    "../src",
    "../../src"
]

# Function to find and add the source directory to sys.path
def setup_import_paths():
    """Find and add the source directory to sys.path"""
    for source_dir in source_dirs:
        if os.path.exists(source_dir):
            if source_dir not in sys.path:
                sys.path.insert(0, source_dir)
                print(f"Added {source_dir} to sys.path")
                
                # Also add the parent directory to handle package imports
                parent_dir = os.path.dirname(source_dir)
                if parent_dir not in sys.path:
                    sys.path.insert(0, parent_dir)
                    print(f"Added {parent_dir} to sys.path")
                return True
    return False

# Try to set up import paths
if not setup_import_paths():
    print("WARNING: Could not find source directory. Imports may fail.")

# COMMAND ----------

# Try to import required modules with detailed error handling
def import_modules():
    """Import required modules with detailed error handling"""
    try:
        # Import core modules
        from dlt_migration_tool.core.transformer import MigrationProcessor, ConfigTransformer
        from dlt_migration_tool.mapping.mapping_engine import MappingEngine, MappingProvider
        from dlt_migration_tool.utils.schema_validator import SchemaValidator
        from dlt_migration_tool.audit.audit_framework import AuditFramework
        
        # Test that the imports work by accessing a method or attribute
        processor = MigrationProcessor(verbose=True)
        
        print("✅ Successfully imported all required modules")
        return True
    except ImportError as e:
        print(f"❌ Import error: {e}")
        
        # Try to provide more detailed information about the error
        import traceback
        traceback.print_exc()
        
        # Try to check if the module files exist
        for module_path in [
            "dlt_migration_tool/core/transformer.py",
            "dlt_migration_tool/mapping/mapping_engine.py",
            "dlt_migration_tool/utils/schema_validator.py",
            "dlt_migration_tool/audit/audit_framework.py"
        ]:
            for source_dir in source_dirs:
                full_path = os.path.join(source_dir, module_path)
                if os.path.exists(full_path):
                    print(f"  - Module file exists: {full_path}")
                else:
                    print(f"  - Module file NOT found: {full_path}")
        
        return False

# Try to import the modules
import_success = import_modules()

# COMMAND ----------

# If imports failed, try to create a self-contained version of the required code
if not import_success:
    print("Attempting to create self-contained versions of required modules...")
    
    # Define the MigrationProcessor class inline
    class MigrationProcessor:
        """
        Self-contained version of MigrationProcessor for use in notebooks.
        This is a simplified version that includes only the essential functionality.
        """
        
        def __init__(self, verbose=False):
            """Initialize the migration processor."""
            self.verbose = verbose
            print("Initialized self-contained MigrationProcessor")
        
        def process_file(self, input_file, output_dir):
            """Process a single DLT-meta JSON file."""
            input_file = Path(input_file)
            output_dir = Path(output_dir)
            
            print(f"Processing file: {input_file}")
            
            # Ensure output directory exists
            os.makedirs(output_dir, exist_ok=True)
            
            # Read the input file
            with open(input_file, 'r') as f:
                source_data = json.load(f)
            
            # Process based on whether it's a single object or an array
            output_files = []
            if isinstance(source_data, list):
                print(f"Processing array with {len(source_data)} items")
                
                for i, item in enumerate(source_data):
                    # Transform the configuration
                    target_config = self._transform_config(item)
                    
                    # Generate output filename
                    output_file = output_dir / f"{input_file.stem}_item_{i}.json"
                    
                    # Write the output file
                    with open(output_file, 'w') as f:
                        json.dump(target_config, f, indent=2)
                    
                    output_files.append(output_file)
                    print(f"Wrote output file: {output_file}")
            else:
                # Transform the configuration
                target_config = self._transform_config(source_data)
                
                # Generate output filename
                output_file = output_dir / f"{input_file.stem}.json"
                
                # Write the output file
                with open(output_file, 'w') as f:
                    json.dump(target_config, f, indent=2)
                
                output_files.append(output_file)
                print(f"Wrote output file: {output_file}")
            
            return output_files
        
        def _transform_config(self, source_config):
            """Transform a configuration from DLT-meta to dataflowspec format."""
            # Create a basic target configuration
            target_config = {
                "dataFlowId": source_config.get("data_flow_id", ""),
                "dataFlowGroup": source_config.get("data_flow_group", ""),
                "dataFlowType": "standard",
                "sourceSystem": source_config.get("source_system", "").lower(),
                "sourceType": source_config.get("source_format", ""),
                "mode": "stream",
                "targetFormat": "delta"
            }
            
            # Add source details
            target_config["sourceDetails"] = {}
            if "source_database" in source_config:
                target_config["sourceDetails"]["database"] = source_config["source_database"]
            if "source_table" in source_config:
                target_config["sourceDetails"]["table"] = source_config["source_table"]
            if "source_path_dev" in source_config:
                target_config["sourceDetails"]["path"] = source_config["source_path_dev"]
            
            # Add target details
            target_config["targetDetails"] = {}
            if "bronze_table" in source_config:
                target_config["targetDetails"]["table"] = source_config["bronze_table"]
            if "bronze_table_path_dev" in source_config:
                target_config["targetDetails"]["path"] = source_config["bronze_table_path_dev"]
            if "bronze_partition_columns" in source_config:
                target_config["targetDetails"]["partitionColumns"] = source_config["bronze_partition_columns"]
            
            # Add schema path
            if "targetDetails" in target_config and "table" in target_config["targetDetails"]:
                table_name = target_config["targetDetails"]["table"]
                target_config["targetDetails"]["schemaPath"] = f"maximo_{table_name}_schema.json"
            
            return target_config
    
    # Define a simple SchemaValidator class
    class SchemaValidator:
        """
        Self-contained version of SchemaValidator for use in notebooks.
        This is a simplified version that includes only the essential functionality.
        """
        
        def __init__(self, schemas_dir=None):
            """Initialize the schema validator."""
            self.schemas_dir = schemas_dir
            print("Initialized self-contained SchemaValidator")
        
        def validate_file(self, file_path):
            """Validate a dataflowspec JSON file."""
            try:
                # Load the dataflowspec JSON
                with open(file_path, 'r') as f:
                    dataflowspec = json.load(f)
                
                # Perform basic validation
                required_fields = ["dataFlowId", "sourceType", "targetDetails"]
                missing_fields = [field for field in required_fields if field not in dataflowspec]
                
                if missing_fields:
                    return {
                        "valid": False,
                        "file": file_path,
                        "error": f"Missing required fields: {', '.join(missing_fields)}"
                    }
                
                return {
                    "valid": True,
                    "file": file_path
                }
            except Exception as e:
                return {
                    "valid": False,
                    "file": file_path,
                    "error": str(e)
                }
    
    # Define a simple AuditFramework class
    class AuditFramework:
        """
        Self-contained version of AuditFramework for use in notebooks.
        This is a simplified version that includes only the essential functionality.
        """
        
        def __init__(self, audit_dir=None):
            """Initialize the audit framework."""
            self.audit_dir = audit_dir or tempfile.mkdtemp()
            print("Initialized self-contained AuditFramework")
        
        def log_migration(self, source_file, output_files):
            """Log a migration operation."""
            timestamp = datetime.datetime.now().isoformat()
            log_entry = {
                "timestamp": timestamp,
                "source_file": str(source_file),
                "output_files": [str(f) for f in output_files]
            }
            
            log_file = os.path.join(self.audit_dir, f"migration_log_{timestamp}.json")
            with open(log_file, 'w') as f:
                json.dump(log_entry, f, indent=2)
            
            print(f"Logged migration to {log_file}")
            return log_entry
    
    print("✅ Created self-contained versions of required modules")

# COMMAND ----------

# Make sure the required classes are available
try:
    # Check if MigrationProcessor is available
    processor = MigrationProcessor(verbose=True)
    print("MigrationProcessor is available")
except NameError:
    print("ERROR: MigrationProcessor is not available")

# COMMAND ----------
