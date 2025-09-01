#!/usr/bin/env python3
"""
Script to update the silver dataflowspec files to match the expected format.
"""

import os
import logging
import sys
import json
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def update_silver_dataflowspecs(silver_dir):
    """Update silver dataflowspec files to match the expected format."""
    dataflowspec_dir = silver_dir / "dataflowspec"
    
    # Process all JSON files in the dataflowspec directory
    for file_path in dataflowspec_dir.glob("*.json"):
        try:
            # Read the JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Update the sourceDetails
            if "sourceDetails" in data:
                # Remove path and readerOptions
                if "path" in data["sourceDetails"]:
                    del data["sourceDetails"]["path"]
                if "readerOptions" in data["sourceDetails"]:
                    del data["sourceDetails"]["readerOptions"]
                
                # Set database and cdfEnabled
                data["sourceDetails"]["database"] = "edp_bronze_dev.maximo_lakeflow"
                data["sourceDetails"]["cdfEnabled"] = True
            
            # Update sourceType
            data["sourceType"] = "delta"
            
            # Update sourceViewName if present
            if "sourceViewName" in data:
                del data["sourceViewName"]
            
            # Convert cdcApplyChanges to cdcSettings
            if "cdcApplyChanges" in data:
                data["cdcSettings"] = data["cdcApplyChanges"]
                del data["cdcApplyChanges"]
                
                # Add ignore_null_updates if not present
                if "ignore_null_updates" not in data["cdcSettings"]:
                    data["cdcSettings"]["ignore_null_updates"] = False
            
            # Update schema path
            if "targetDetails" in data and "schemaPath" in data["targetDetails"]:
                table_name = data["targetDetails"]["table"]
                data["targetDetails"]["schemaPath"] = f"maximo_{table_name}_schema.json"
            
            # Write the updated data back to the file
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Updated {file_path}")
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")


def main():
    """Update the silver dataflowspec files."""
    output_dir = Path("/Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/apasamples/output")
    silver_dir = output_dir / "silver_maximo"
    
    logger.info("Updating silver dataflowspec files")
    update_silver_dataflowspecs(silver_dir)
    
    logger.info("Updates completed")


if __name__ == "__main__":
    main()
