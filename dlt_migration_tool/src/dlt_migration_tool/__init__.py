"""
DLT Migration Tool - A tool to migrate Databricks DLT-meta JSON configurations to DLT Framework dataflowspec.
"""

from .core import MigrationProcessor, ConfigTransformer
from .mapping import MappingEngine, DefaultMappingProvider, CustomMappingProvider, MappingProvider
from .utils import FileUtils, JsonUtils, LoggingUtils, SchemaValidator
from .audit import AuditFramework

__version__ = "0.1.0"

__all__ = [
    # Core
    "MigrationProcessor",
    "ConfigTransformer",
    
    # Mapping
    "MappingEngine",
    "DefaultMappingProvider",
    "CustomMappingProvider",
    "MappingProvider",
    
    # Utils
    "FileUtils",
    "JsonUtils",
    "LoggingUtils",
    "SchemaValidator",
    
    # Audit
    "AuditFramework"
]
