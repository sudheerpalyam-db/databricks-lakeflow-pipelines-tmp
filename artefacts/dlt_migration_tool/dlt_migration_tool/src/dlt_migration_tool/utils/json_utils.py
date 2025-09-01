"""
Utility functions for JSON operations.
"""

import json
import logging
from typing import Dict, Any, List, Optional, Union, Tuple

logger = logging.getLogger(__name__)


class JsonUtils:
    """Utility class for JSON operations."""
    
    @staticmethod
    def get_nested_value(data: Dict[str, Any], path: str, default: Any = None) -> Any:
        """
        Get a value from a nested dictionary using a dot-separated path.
        
        Args:
            data: Dictionary to search
            path: Dot-separated path (e.g., "a.b.c")
            default: Default value to return if path not found
            
        Returns:
            Value at the specified path or default if not found
        """
        keys = path.split('.')
        current = data
        
        try:
            for key in keys:
                if isinstance(current, dict) and key in current:
                    current = current[key]
                else:
                    logger.debug(f"Path {path} not found in data")
                    return default
            return current
        except (KeyError, TypeError):
            logger.debug(f"Error accessing path {path} in data")
            return default
    
    @staticmethod
    def set_nested_value(data: Dict[str, Any], path: str, value: Any) -> None:
        """
        Set a value in a nested dictionary using a dot-separated path.
        Creates intermediate dictionaries as needed.
        
        Args:
            data: Dictionary to modify
            path: Dot-separated path (e.g., "a.b.c")
            value: Value to set
        """
        keys = path.split('.')
        current = data
        
        # Navigate to the parent of the final key
        for key in keys[:-1]:
            if key not in current or not isinstance(current[key], dict):
                current[key] = {}
            current = current[key]
        
        # Set the value at the final key
        current[keys[-1]] = value
        logger.debug(f"Set value at path {path}")
    
    @staticmethod
    def merge_dicts(dict1: Dict[str, Any], dict2: Dict[str, Any], 
                   overwrite: bool = True) -> Dict[str, Any]:
        """
        Merge two dictionaries recursively.
        
        Args:
            dict1: First dictionary
            dict2: Second dictionary (values from this dict take precedence if overwrite=True)
            overwrite: Whether to overwrite values in dict1 with values from dict2
            
        Returns:
            Merged dictionary
        """
        result = dict1.copy()
        
        for key, value in dict2.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                # Recursively merge nested dictionaries
                result[key] = JsonUtils.merge_dicts(result[key], value, overwrite)
            elif key not in result or overwrite:
                # Add or overwrite the value
                result[key] = value
        
        return result
    
    @staticmethod
    def validate_json_schema(data: Dict[str, Any], schema: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Validate JSON data against a schema.
        
        Args:
            data: JSON data to validate
            schema: JSON schema to validate against
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        try:
            import jsonschema
            validator = jsonschema.Draft7Validator(schema)
            errors = list(validator.iter_errors(data))
            
            if errors:
                error_messages = [error.message for error in errors]
                logger.warning(f"JSON validation failed: {error_messages}")
                return False, error_messages
            
            logger.debug("JSON validation passed")
            return True, []
        except ImportError:
            logger.warning("jsonschema package not installed, skipping validation")
            return True, ["jsonschema package not installed, skipping validation"]
