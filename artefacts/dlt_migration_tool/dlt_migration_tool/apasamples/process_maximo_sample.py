#!/usr/bin/env python3
"""
Script to process a sample of the Maximo onboarding file.
"""

import os
import logging
import sys
import json
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.dlt_migration_tool.core.transformer import MigrationProcessor
from src.dlt_migration_tool.utils.logging_utils import LoggingUtils
from src.dlt_migration_tool.utils.file_utils import FileUtils


def create_sample_input(input_file, output_file, num_items=2):
    """Create a sample input file with a limited number of items."""
    with open(input_file, 'r') as f:
        data = json.load(f)
    
    # Take only the first few items
    sample_data = data[:num_items]
    
    # Write the sample data to the output file
    with open(output_file, 'w') as f:
        json.dump(sample_data, f, indent=2)
    
    return sample_data


def organize_output_files(output_dir):
    """Organize output files into bronze and silver directories."""
    # Create bronze and silver directories
    bronze_dir = output_dir / "bronze_maximo" / "dataflowspec"
    silver_dir = output_dir / "silver_maximo" / "dataflowspec"
    os.makedirs(bronze_dir, exist_ok=True)
    os.makedirs(silver_dir, exist_ok=True)
    
    # Create schema directories
    bronze_schema_dir = output_dir / "bronze_maximo" / "schemas"
    silver_schema_dir = output_dir / "silver_maximo" / "schemas"
    os.makedirs(bronze_schema_dir, exist_ok=True)
    os.makedirs(silver_schema_dir, exist_ok=True)
    
    # Process all JSON files in the output directory
    for file_path in output_dir.glob("*.json"):
        try:
            # Read the JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Extract dataFlowGroup
            dataflow_group = data.get("dataFlowGroup", "")
            dataflow_id = data.get("dataFlowId", "")
            
            # Skip if no dataflow ID
            if not dataflow_id:
                continue
            
            # Determine target directory based on dataFlowGroup
            if "silver" in dataflow_group:
                target_dir = silver_dir
                schema_dir = silver_schema_dir
            else:
                target_dir = bronze_dir
                schema_dir = bronze_schema_dir
            
            # Create target filename
            target_file = target_dir / f"{dataflow_id}.json"
            
            # Create empty schema file
            schema_file = schema_dir / f"{dataflow_id.replace('maximo_', '')}_schema.json"
            if not schema_file.exists():
                with open(schema_file, 'w') as f:
                    json.dump({"type": "struct", "fields": []}, f, indent=2)
            
            # Copy the file
            with open(target_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            # Delete the original file
            os.remove(file_path)
            
        except Exception as e:
            logger.error(f"Error processing {file_path}: {e}")


def main():
    """Process a sample of the Maximo onboarding file."""
    # Set up logging
    global logger
    logger = LoggingUtils.setup_logger("maximo_migration", level=logging.INFO)
    logger.info("Starting Maximo sample file migration")
    
    # Define input and output paths
    input_file = Path("/Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/apasamples/actuals/maximo_onboarding.json")
    sample_file = Path("/Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/apasamples/actuals/maximo_sample.json")
    output_dir = Path("/Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/apasamples/output")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Create a sample input file
    logger.info(f"Creating sample input file with 2 items")
    create_sample_input(input_file, sample_file, 2)
    
    # Initialize the processor
    processor = MigrationProcessor(verbose=True)
    
    # Process the file
    logger.info(f"Processing {sample_file}")
    output_files = processor.process_file(sample_file, output_dir)
    logger.info(f"Generated {len(output_files)} output files")
    
    # Organize output files
    logger.info("Organizing output files")
    organize_output_files(output_dir)
    
    logger.info("Migration completed")


if __name__ == "__main__":
    main()
