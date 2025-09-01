#!/usr/bin/env python3
"""
Script to process the Maximo onboarding file.
"""

import os
import logging
import sys
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.dlt_migration_tool.core.transformer import MigrationProcessor
from src.dlt_migration_tool.utils.logging_utils import LoggingUtils


def main():
    """Process the Maximo onboarding file."""
    # Set up logging
    logger = LoggingUtils.setup_logger("maximo_migration", level=logging.INFO)
    logger.info("Starting Maximo file migration")
    
    # Define input and output paths
    input_file = Path("/Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/apasamples/actuals/maximo_onboarding.json")
    output_dir = Path("/Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/apasamples/output")
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize the processor
    processor = MigrationProcessor(verbose=True)
    
    # Process the file
    logger.info(f"Processing {input_file}")
    output_files = processor.process_file(input_file, output_dir)
    logger.info(f"Generated {len(output_files)} output files")
    
    for output_file in output_files:
        logger.info(f"Output file: {output_file}")
    
    logger.info("Migration completed")


if __name__ == "__main__":
    main()
