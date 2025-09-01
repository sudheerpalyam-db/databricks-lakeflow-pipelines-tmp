#!/usr/bin/env python3
"""
Script to update the bronze dataflowspec files to match the expected format.
"""

import os
import logging
import sys
import json
from pathlib import Path

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def update_bronze_dataflowspecs(bronze_dir):
    """Update bronze dataflowspec files to match the expected format."""
    dataflowspec_dir = bronze_dir / "dataflowspec"
    
    # Process all JSON files in the dataflowspec directory
    for file_path in dataflowspec_dir.glob("*.json"):
        try:
            # Read the JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Add dataQualityExpectationsEnabled if not present
            if "dataQualityExpectationsEnabled" not in data:
                data["dataQualityExpectationsEnabled"] = True
            
            # Add dataQualityExpectationsPath if not present
            if "dataQualityExpectationsPath" not in data:
                table_name = data["targetDetails"]["table"]
                data["dataQualityExpectationsPath"] = f"./maximo_{table_name}_dqe.json"
            
            # Add quarantineMode if not present
            if "quarantineMode" not in data:
                data["quarantineMode"] = "table"
            
            # Add quarantineTargetDetails if not present
            if "quarantineTargetDetails" not in data:
                data["quarantineTargetDetails"] = {
                    "targetFormat": "delta"
                }
            
            # Remove cdcApplyChanges if present
            if "cdcApplyChanges" in data:
                del data["cdcApplyChanges"]
            
            # Write the updated data back to the file
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            logger.info(f"Updated {file_path}")
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")


def main():
    """Update the bronze dataflowspec files."""
    output_dir = Path("/Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/apasamples/output")
    bronze_dir = output_dir / "bronze_maximo"
    
    logger.info("Updating bronze dataflowspec files")
    update_bronze_dataflowspecs(bronze_dir)
    
    logger.info("Updates completed")


if __name__ == "__main__":
    main()
