"""
Utility functions for logging.
"""

import logging
import sys
from typing import Optional, Union, TextIO


class LoggingUtils:
    """Utility class for logging configuration."""
    
    @staticmethod
    def setup_logger(
        name: str, 
        level: int = logging.INFO,
        log_format: Optional[str] = None,
        date_format: Optional[str] = None,
        stream: Optional[TextIO] = sys.stdout,
        file_path: Optional[str] = None
    ) -> logging.Logger:
        """
        Set up a logger with the specified configuration.
        
        Args:
            name: Logger name
            level: Logging level
            log_format: Log message format
            date_format: Date format for log messages
            stream: Stream to log to (default: sys.stdout)
            file_path: Optional file path to log to
            
        Returns:
            Configured logger
        """
        # Use default formats if not provided
        if log_format is None:
            log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        if date_format is None:
            date_format = '%Y-%m-%d %H:%M:%S'
        
        # Get or create logger
        logger = logging.getLogger(name)
        logger.setLevel(level)
        
        # Remove existing handlers to avoid duplicates
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # Create formatter
        formatter = logging.Formatter(log_format, date_format)
        
        # Add stream handler if requested
        if stream:
            stream_handler = logging.StreamHandler(stream)
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)
        
        # Add file handler if requested
        if file_path:
            file_handler = logging.FileHandler(file_path)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        return logger
    
    @staticmethod
    def set_log_level(logger: Union[logging.Logger, str], level: int) -> None:
        """
        Set the log level for a logger.
        
        Args:
            logger: Logger or logger name
            level: Logging level
        """
        if isinstance(logger, str):
            logger = logging.getLogger(logger)
        
        logger.setLevel(level)
    
    @staticmethod
    def get_log_level_from_string(level_str: str) -> int:
        """
        Convert a string log level to a logging level constant.
        
        Args:
            level_str: String log level (e.g., "INFO", "DEBUG")
            
        Returns:
            Logging level constant
            
        Raises:
            ValueError: If the string is not a valid log level
        """
        level_map = {
            "CRITICAL": logging.CRITICAL,
            "ERROR": logging.ERROR,
            "WARNING": logging.WARNING,
            "INFO": logging.INFO,
            "DEBUG": logging.DEBUG,
            "NOTSET": logging.NOTSET
        }
        
        upper_level = level_str.upper()
        if upper_level in level_map:
            return level_map[upper_level]
        
        raise ValueError(f"Invalid log level: {level_str}")
