#!/usr/bin/env python3
"""
Script to organize Maximo dataflow files into bronze and silver directories.
"""

import os
import json
import shutil
import logging
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def organize_dataflow_files():
    """Organize dataflow files into bronze and silver directories."""
    # Define paths
    source_dir = Path("/Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/apasamples/output")
    target_base_dir = Path("/Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/apasamples/organized")
    bronze_dir = target_base_dir / "bronze_maximo" / "dataflowspec"
    silver_dir = target_base_dir / "silver_maximo" / "dataflowspec"
    
    # Create target directories
    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(silver_dir, exist_ok=True)
    
    # Process all JSON files
    for file_path in source_dir.glob("*.json"):
        try:
            # Read the JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Extract dataFlowId and dataFlowGroup
            dataflow_id = data.get("dataFlowId", "")
            dataflow_group = data.get("dataFlowGroup", "")
            
            # Determine target directory based on dataFlowGroup
            if "silver" in dataflow_group:
                target_dir = silver_dir
            else:
                target_dir = bronze_dir
            
            # Create target filename
            target_file = target_dir / f"{dataflow_id}.json"
            
            # Copy the file
            shutil.copy2(file_path, target_file)
            logger.info(f"Copied {file_path.name} to {target_file}")
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")
    
    logger.info("File organization complete")

if __name__ == "__main__":
    organize_dataflow_files()
