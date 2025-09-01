"""
Utility functions for file operations.
"""

import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Iterator

logger = logging.getLogger(__name__)


class FileUtils:
    """Utility class for file operations."""
    
    @staticmethod
    def read_json_file(file_path: Union[str, Path]) -> Any:
        """
        Read and parse a JSON file.
        
        Args:
            file_path: Path to the JSON file
            
        Returns:
            Parsed JSON content
            
        Raises:
            FileNotFoundError: If the file doesn't exist
            json.JSONDecodeError: If the file contains invalid JSON
        """
        file_path = Path(file_path)
        logger.debug(f"Reading JSON file: {file_path}")
        
        if not file_path.exists():
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        
        try:
            with open(file_path, 'r') as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in file {file_path}: {str(e)}")
            raise
    
    @staticmethod
    def write_json_file(data: Any, file_path: Union[str, Path], indent: int = 4) -> None:
        """
        Write data to a JSON file.
        
        Args:
            data: Data to write
            file_path: Path to the output file
            indent: JSON indentation level
            
        Raises:
            IOError: If the file cannot be written
        """
        file_path = Path(file_path)
        logger.debug(f"Writing JSON file: {file_path}")
        
        # Create directory if it doesn't exist
        os.makedirs(file_path.parent, exist_ok=True)
        
        try:
            with open(file_path, 'w') as f:
                json.dump(data, f, indent=indent)
            logger.info(f"Successfully wrote JSON to {file_path}")
        except IOError as e:
            logger.error(f"Error writing to file {file_path}: {str(e)}")
            raise
    
    @staticmethod
    def find_json_files(directory: Union[str, Path]) -> List[Path]:
        """
        Find all JSON files in a directory.
        
        Args:
            directory: Directory to search
            
        Returns:
            List of paths to JSON files
            
        Raises:
            FileNotFoundError: If the directory doesn't exist
        """
        directory = Path(directory)
        logger.debug(f"Finding JSON files in directory: {directory}")
        
        if not directory.exists():
            logger.error(f"Directory not found: {directory}")
            raise FileNotFoundError(f"Directory not found: {directory}")
        
        json_files = list(directory.glob("**/*.json"))
        logger.info(f"Found {len(json_files)} JSON files in {directory}")
        return json_files
    
    @staticmethod
    def ensure_directory_exists(directory: Union[str, Path]) -> None:
        """
        Ensure a directory exists, creating it if necessary.
        
        Args:
            directory: Directory path
        """
        directory = Path(directory)
        logger.debug(f"Ensuring directory exists: {directory}")
        
        os.makedirs(directory, exist_ok=True)
        logger.debug(f"Directory exists: {directory}")
    
    @staticmethod
    def generate_output_filename(input_path: Union[str, Path], 
                                output_dir: Union[str, Path],
                                suffix: Optional[str] = None) -> Path:
        """
        Generate an output filename based on the input path.
        
        Args:
            input_path: Path to the input file
            output_dir: Directory for the output file
            suffix: Optional suffix to add before the extension
            
        Returns:
            Path to the output file
        """
        input_path = Path(input_path)
        output_dir = Path(output_dir)
        
        # Extract the base filename without extension
        base_name = input_path.stem
        
        # Add suffix if provided
        if suffix:
            new_name = f"{base_name}_{suffix}.json"
        else:
            new_name = f"{base_name}.json"
        
        return output_dir / new_name
