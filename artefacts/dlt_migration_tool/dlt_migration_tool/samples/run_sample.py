#!/usr/bin/env python3
"""
Sample script to demonstrate using the DLT Migration Tool.
"""

import os
import logging
import sys
from pathlib import Path

# Add the parent directory to the Python path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dlt_migration_tool.core.transformer import MigrationProcessor
from dlt_migration_tool.utils.logging_utils import LoggingUtils


def main():
    """Run a sample migration."""
    # Set up logging
    logger = LoggingUtils.setup_logger("sample_run", level=logging.INFO)
    logger.info("Starting sample migration")
    
    # Define input and output paths
    script_dir = Path(__file__).parent
    input_dir = script_dir / "input"
    output_dir = script_dir / "output"
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Initialize the processor
    processor = MigrationProcessor(verbose=True)
    
    # Process the sample files
    logger.info("Processing onboarding_sample.json")
    # onboarding_file = input_dir / "onboarding_sample.json"
    onboarding_file = input_dir / "a_pmsequence_maximo_onboarding.json"

    onboarding_output = processor.process_file(onboarding_file, output_dir)
    logger.info(f"Generated {len(onboarding_output)} output files")
    
    # logger.info("Processing silver_transformations_sample.json")
    # transformations_file = input_dir / "silver_transformations_sample.json"
    # transformations_output = processor.process_file(transformations_file, output_dir)
    # logger.info(f"Generated {len(transformations_output)} output files")
    
    logger.info("Sample migration completed")
    logger.info(f"Output files are in: {output_dir}")


if __name__ == "__main__":
    main()
