# DLT Validation Framework

A framework for validating data between source and target tables in Databricks, particularly for comparing tables before and after conversion to Declarative Lakeflow (DLT).

## Overview

This validation framework provides tools to compare data between source and target tables, with a focus on validating row counts and row hash checks. It's designed to be configurable, allowing you to specify which tables to validate and which primary keys to use for comparison.

## Key Features

- Row count validation between source and target tables
- Row hash validation to ensure data integrity
- Configurable primary keys for table comparison
- Comprehensive audit logging of validation results
- Support for multiple source-target schema mappings
- Databricks notebook interface for easy execution

## Directory Structure

```
dlt_validation_framework/
├── src/
│   ├── __init__.py
│   ├── config.py       # Configuration settings and utilities
│   ├── validation.py   # Core validation logic
│   ├── main.py         # Main framework orchestration
│   └── utils.py        # Utility functions
├── bootstrap.py        # Notebook to initialize the framework
├── run_validation.py   # Notebook to run validations
├── scheduled_validation.py  # Notebook for scheduled validations
├── custom_table_validation.py  # Notebook for custom table validations
└── main.py             # Main entry point notebook
```

## Configuration

The framework is configured to validate data between the following schemas:

- Source: `edp_bronze_dev.maximo`, Target: `edp_bronze_dev.maximo_lakeflow`
- Source: `edp_silver_dev.maximo_ods`, Target: `edp_silver_dev.maximo_ods_lakeflow`
- Source: `edp_bronze_dev.workday`, Target: `edp_bronze_dev.workday_lakeflow`
- Source: `edp_silver_dev.workday_ods`, Target: `edp_silver_dev.workday_ods_lakeflow`

Validation results are stored in the `utilities_dev.erp_remediation.dlt_validation_results` table.

## Getting Started

1. Run the `bootstrap.py` notebook to initialize the framework and create necessary tables.
2. Use the `main.py` notebook to run validations with a user-friendly interface.
3. For specific table validations, use the `custom_table_validation.py` notebook.
4. For scheduled validations, configure the `scheduled_validation.py` notebook as a Databricks job.

## Usage Examples

### Basic Validation

To validate all tables in the maximo schema:

```python
from dlt_validation_framework.src.main import ValidationFramework

# Initialize the framework
framework = ValidationFramework()

# Define source-target mapping
mapping = {
    "source_catalog": "edp_bronze_dev",
    "source_schema": "maximo",
    "target_catalog": "edp_bronze_dev",
    "target_schema": "maximo_lakeflow"
}

# Run validation
results = framework.validate_tables(source_target_mapping=mapping)
```

### Custom Table Validation

To validate specific tables with custom primary keys:

```python
from dlt_validation_framework.src.main import ValidationFramework

# Initialize the framework
framework = ValidationFramework()

# Define tables and primary keys
tables = ["asset", "commodities"]
primary_keys = {
    "asset": ["asset_id"],
    "commodities": ["commodity_id"]
}

# Define source-target mapping
mapping = {
    "source_catalog": "edp_bronze_dev",
    "source_schema": "maximo",
    "target_catalog": "edp_bronze_dev",
    "target_schema": "maximo_lakeflow"
}

# Run validation
results = framework.validate_tables(
    tables=tables,
    source_target_mapping=mapping,
    primary_keys=primary_keys
)
```

## Audit Results

Validation results are stored in the audit table and include:

- Run ID and timestamp
- Source and target table information
- Validation type (row_count, row_hash)
- Validation status (SUCCESS, FAILED, ERROR)
- Source and target counts
- Mismatch count and details
- Execution time

## Scheduling

To schedule regular validations, configure the `scheduled_validation.py` notebook as a Databricks job with appropriate parameters.
