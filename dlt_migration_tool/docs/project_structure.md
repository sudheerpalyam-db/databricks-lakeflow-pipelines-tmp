# Databricks DLT Migration Tool

This document provides an overview of the project structure and components for the Databricks DLT Migration Tool.

## Project Structure

```
dlt_migration_tool/
├── src/
│   └── dlt_migration_tool/
│       ├── __init__.py
│       ├── __main__.py
│       ├── cli.py                     # Command line interface
│       ├── core/
│       │   ├── __init__.py
│       │   └── transformer.py         # Core transformation logic
│       ├── mapping/
│       │   ├── __init__.py
│       │   └── mapping_engine.py      # Mapping rules and engine
│       ├── notebooks/
│       │   ├── dlt_migration_tool_notebook.py  # Databricks notebook version
│       │   └── schema_extractor.py    # Schema extraction utilities
│       ├── schema_extractor.py        # Schema extraction functionality
│       ├── schema_generator.py        # Schema generation functionality
│       ├── schema_manager.py          # Schema management functionality
│       ├── schema_updater.py          # Schema update functionality
│       └── utils/
│           ├── __init__.py
│           ├── file_utils.py          # File handling utilities
│           ├── json_utils.py          # JSON processing utilities
│           ├── logging_utils.py       # Logging utilities
│           └── schema_utils.py        # Schema utilities
├── tests/
│   ├── integration/
│   │   └── test_integration.py        # Integration tests
│   └── unit/
│       ├── test_config_transformer.py # Unit tests for transformation
│       ├── test_mapping_engine.py     # Unit tests for mapping engine
│       └── test_migration_processor.py # Unit tests for processor
├── samples/                           # Sample input/output files
│   ├── input/
│   └── output/
├── docs/                              # Documentation
├── .gitignore                         # Git ignore file
├── clean.sh                           # Cleanup script
├── pyproject.toml                     # Project configuration
└── README.md                          # Project README
```

## Key Components

### Core Components

- **transformer.py**: Contains the `MigrationProcessor` class that handles the transformation of DLT-meta JSON to DLT Framework format.
- **mapping_engine.py**: Implements the mapping rules and logic for converting between formats.
- **schema_manager.py**: Manages schema extraction, generation, and updates.

### Utility Components

- **file_utils.py**: Handles file operations like reading, writing, and directory traversal.
- **json_utils.py**: Provides JSON manipulation utilities.
- **logging_utils.py**: Sets up and configures logging.
- **schema_utils.py**: Utilities for schema manipulation and validation.

### CLI and Entry Points

- **cli.py**: Command-line interface for the tool.
- **__main__.py**: Entry point for running the package as a module.
- **dlt_migration_tool_notebook.py**: Databricks notebook version of the tool.

## Development Workflow

1. Make changes to the codebase
2. Run tests: `pytest`
3. Clean the project: `./clean.sh`
4. Commit changes to Git
