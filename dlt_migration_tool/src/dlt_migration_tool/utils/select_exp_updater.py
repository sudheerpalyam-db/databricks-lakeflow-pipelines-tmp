"""
Utility to update silver dataflowspec JSON files with select expressions from a reference file.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

class SelectExpUpdater:
    """
    Updates silver dataflowspec JSON files with select expressions from a reference file.
    """
    
    def __init__(self, reference_file: str, silver_dir: str):
        """
        Initialize the updater.
        
        Args:
            reference_file: Path to the reference file containing select expressions
            silver_dir: Directory containing silver dataflowspec JSON files
        """
        self.reference_file = reference_file
        self.silver_dir = silver_dir
        self.select_exps = {}
        self._load_reference_file()
        
    def _load_reference_file(self):
        """
        Load select expressions from the reference file.
        """
        try:
            with open(self.reference_file, 'r') as f:
                reference_data = json.load(f)
                
            # Extract select expressions by target table
            for item in reference_data:
                if 'target_table' in item and 'select_exp' in item:
                    self.select_exps[item['target_table']] = item['select_exp']
            
            logger.info(f"Loaded select expressions for {len(self.select_exps)} tables from {self.reference_file}")
        except Exception as e:
            logger.error(f"Error loading reference file: {str(e)}")
            raise
    
    def update_silver_files(self) -> Dict[str, bool]:
        """
        Update all silver dataflowspec JSON files with select expressions.
        
        Returns:
            Dictionary mapping file paths to update status (True for updated, False for not updated)
        """
        results = {}
        
        # Find all JSON files in the silver directory
        if not os.path.exists(self.silver_dir):
            logger.error(f"Silver directory not found: {self.silver_dir}")
            return results
        
        json_files = [os.path.join(self.silver_dir, f) for f in os.listdir(self.silver_dir) 
                     if f.endswith('.json') and os.path.isfile(os.path.join(self.silver_dir, f))]
        
        logger.info(f"Found {len(json_files)} JSON files in {self.silver_dir}")
        
        # Process each file
        for file_path in json_files:
            results[file_path] = self.update_file(file_path)
        
        # Log summary
        updated_count = sum(1 for status in results.values() if status)
        logger.info(f"Updated {updated_count} of {len(results)} files")
        
        return results
    
    def update_file(self, file_path: str) -> bool:
        """
        Update a single silver dataflowspec JSON file with select expressions.
        
        Args:
            file_path: Path to the silver dataflowspec JSON file
            
        Returns:
            True if the file was updated, False otherwise
        """
        try:
            # Read the JSON file
            with open(file_path, 'r') as f:
                data = json.load(f)
            
            # Extract table name from the file
            table_name = None
            if 'targetDetails' in data and 'table' in data['targetDetails']:
                table_name = data['targetDetails']['table']
            
            # Skip if no table name found
            if not table_name:
                logger.warning(f"No table name found in {file_path}")
                return False
            
            # Check if we have select expressions for this table
            if table_name not in self.select_exps:
                logger.warning(f"No select expressions found for table {table_name}")
                return False
            
            # Update the select expressions
            if 'sourceDetails' not in data:
                data['sourceDetails'] = {}
            
            data['sourceDetails']['selectExp'] = self.select_exps[table_name]
            
            # Write the updated data back to the file
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=4)
            
            logger.info(f"Updated {file_path} with select expressions for {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error updating {file_path}: {str(e)}")
            return False


def update_select_expressions(reference_file: str, silver_dir: str) -> Dict[str, bool]:
    """
    Update silver dataflowspec JSON files with select expressions from a reference file.
    
    Args:
        reference_file: Path to the reference file containing select expressions
        silver_dir: Directory containing silver dataflowspec JSON files
        
    Returns:
        Dictionary mapping file paths to update status (True for updated, False for not updated)
    """
    updater = SelectExpUpdater(reference_file, silver_dir)
    return updater.update_silver_files()


if __name__ == "__main__":
    import argparse
    
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Update silver dataflowspec JSON files with select expressions')
    parser.add_argument('reference_file', help='Path to the reference file containing select expressions')
    parser.add_argument('silver_dir', help='Directory containing silver dataflowspec JSON files')
    args = parser.parse_args()
    
    # Update select expressions
    results = update_select_expressions(args.reference_file, args.silver_dir)
    
    # Print summary
    updated_count = sum(1 for status in results.values() if status)
    print(f"Updated {updated_count} of {len(results)} files")
