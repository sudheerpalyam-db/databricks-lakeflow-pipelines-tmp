# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Migration Tool - Imports
# MAGIC 
# MAGIC This notebook imports all the necessary modules for the DLT Migration Tool.

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

# Try direct imports first
try:
    from dlt_migration_tool.core.transformer import MigrationProcessor
    from dlt_migration_tool.audit import AuditFramework
    from dlt_migration_tool.utils.schema_validator import SchemaValidator
    print("Successfully imported modules directly")
except ImportError as e:
    print(f"Direct import failed: {e}")
    
    # Add the source directory to the path
    # First try the Databricks workspace path
    source_dir = "/Workspace/Users/sudheer.palyam@apa.com.au/databricks-lakeflow-pipelines-tmp/dlt_migration_tool/src"
    if not os.path.exists(source_dir):
        # Fall back to local path
        source_dir = "/Users/sudheer.palyam/workspace/databricks/databricks-lakeflow-pipelines-tmp/dlt_migration_tool/src"

    if source_dir not in sys.path:
        sys.path.insert(0, source_dir)
        print(f"Added {source_dir} to sys.path")
        
    # Also add the parent directory to handle relative imports
    parent_dir = os.path.dirname(os.path.dirname(source_dir))
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
        print(f"Added {parent_dir} to sys.path")
        
    # Try imports again after modifying sys.path
    try:
        from dlt_migration_tool.core.transformer import MigrationProcessor
        from dlt_migration_tool.audit import AuditFramework
        from dlt_migration_tool.utils.schema_validator import SchemaValidator
        print("Successfully imported modules after path modification")
    except ImportError as e2:
        print(f"Import still failing after path modification: {e2}")
        print(f"Current sys.path: {sys.path}")
        raise

# COMMAND ----------
