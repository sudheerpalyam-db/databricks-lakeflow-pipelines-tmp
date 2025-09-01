# DLT Migration Tool User Guide

## Table of Contents

1. [Introduction](#introduction)
2. [Overview](#overview)
3. [Getting Started](#getting-started)
4. [Workday Migration](#workday-migration)
5. [Maximo Migration](#maximo-migration)
6. [Validation Framework](#validation-framework)
7. [Troubleshooting](#troubleshooting)
8. [Best Practices](#best-practices)
9. [Reference](#reference)

## Introduction

The DLT Migration Tool is designed to simplify the migration of traditional Databricks pipelines to the Delta Live Tables (DLT) Framework. This user guide provides comprehensive instructions for using the tool with Workday and Maximo data sources, as well as validating the migrated data using the built-in validation framework.

## Overview

### Key Components

1. **Migration Notebooks**: Specialized notebooks for different data sources:
   - `dlt_migration_tool_notebook_workday_uc.py`: For Workday data sources
   - `dlt_migration_tool_notebook_maximo_uc.py`: For Maximo data sources

2. **Validation Framework**: Tools for validating data integrity between source and target tables:
   - Row count validation
   - Row hash validation
   - Detailed reporting

3. **Utilities**: Helper modules for schema extraction, validation, and transformation

### Migration Process

The migration process consists of these key steps:

1. **Extract CDC Logic**: Extract Change Data Capture logic from existing PySpark files
2. **Extract Schemas**: Extract schemas from existing tables
3. **Generate Specifications**: Generate DLT Framework dataflow specifications
4. **Organize Output**: Organize output files into appropriate directories
5. **Validate Results**: Validate the migrated data using the validation framework

## Getting Started

### Prerequisites

- Access to Databricks workspace with appropriate permissions
- Source and target tables accessible from your workspace
- Unity Catalog volume created for storing input/output files
- Source PySpark files uploaded to Unity Catalog volume (for Workday) or accessible in workspace (for Maximo)

### Installation

1. Clone the repository to your Databricks workspace:
   ```bash
   git clone https://github.com/your-organization/dlt_migration_tool.git
   ```

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up the Unity Catalog volume:
   ```sql
   CREATE VOLUME IF NOT EXISTS utilities_dev.erp_remediation.dlt_migration_tool;
   ```

## Workday Migration

The Workday migration tool is designed to process Workday data sources and generate appropriate DLT Framework specifications.

### Workday Migration Process

1. **Upload Source Files**: Upload Workday PySpark files to Unity Catalog volume
2. **Configure Parameters**: Set up migration parameters
3. **Run Migration**: Execute the migration notebook
4. **Review Output**: Review and validate the generated specifications

### Using the Workday UC Notebook

1. **Open the Notebook**:
   - Navigate to `src/dlt_migration_tool/notebooks/dlt_migration_tool_notebook_workday_uc.py`

2. **Configure Parameters**:

   | Parameter | Description | Example |
   |-----------|-------------|---------|
   | `environment` | Environment (dev, test, prod) | `dev` |
   | `catalog` | Unity Catalog name | `utilities_dev` |
   | `schema` | Schema name | `erp_remediation` |
   | `volume` | Volume name | `dlt_migration_tool` |
   | `source_system` | Source system name | `workday` |
   | `tables` | Comma-separated list of tables | `account_sets,bank_accounts` |
   | `source_volume_path` | Path to source files | `/Volumes/utilities_dev/erp_remediation/dlt_migration_tool/source/` |
   | `bronze_catalog` | Bronze catalog name | `edp_bronze_dev` |
   | `bronze_schema` | Bronze schema name | `workday` |
   | `bronze_target_schema` | Bronze target schema | `workday_lakeflow` |
   | `silver_catalog` | Silver catalog name | `edp_silver_dev` |
   | `silver_schema` | Silver schema name | `workday_ods_lakeflow` |
   | `s3_bucket` | S3 bucket name | `s3-staging-workday-dev-apse2-058264204922` |

3. **Run the Notebook**:
   - Execute the notebook cells sequentially
   - Monitor the output for any errors or warnings

4. **Review Output Files**:
   - Bronze dataflow specifications: `/Volumes/{catalog}.{schema}.{volume}/output/{source_system}_{environment}/bronze_{source_system}/dataflowspec/`
   - Silver dataflow specifications: `/Volumes/{catalog}.{schema}.{volume}/output/{source_system}_{environment}/silver_{source_system}/dataflowspec/`
   - Schema files: `/Volumes/{catalog}.{schema}.{volume}/output/{source_system}_{environment}/bronze_{source_system}/schemas/` and `/Volumes/{catalog}.{schema}.{volume}/output/{source_system}_{environment}/silver_{source_system}/schemas/`
   - Data Quality Expectations: `/Volumes/{catalog}.{schema}.{volume}/output/{source_system}_{environment}/bronze_{source_system}/dqe/`

### Workday-Specific Features

1. **Bronze Table Configuration**:
   - Automatically extracts `landing_parquet_path` from source files
   - Removes `.parquet` extension from paths when present
   - Uses schema inference instead of predefined schema files

2. **Silver Table Configuration**:
   - Configures CDC settings with appropriate primary keys
   - Sets up tracked columns based on match conditions
   - Configures sequence_by with timestamp function

3. **File Matching**:
   - Matches source files to tables based on `bronze_table_name` variable
   - Falls back to filename matching if variable not found
   - Recursively searches directories for Python files

## Maximo Migration

The Maximo migration tool is designed to process Maximo data sources and generate appropriate DLT Framework specifications.

### Maximo Migration Process

1. **Prepare Source Files**: Ensure Maximo PySpark files are accessible
2. **Configure Parameters**: Set up migration parameters
3. **Run Migration**: Execute the migration notebook
4. **Review Output**: Review and validate the generated specifications

### Using the Maximo UC Notebook

1. **Open the Notebook**:
   - Navigate to `src/dlt_migration_tool/notebooks/dlt_migration_tool_notebook_maximo_uc.py`

2. **Configure Parameters**:

   | Parameter | Description | Example |
   |-----------|-------------|---------|
   | `environment` | Environment (dev, test, prod) | `dev` |
   | `catalog` | Unity Catalog name | `utilities_dev` |
   | `schema` | Schema name | `erp_remediation` |
   | `volume` | Volume name | `dlt_migration_tool` |
   | `source_system` | Source system name | `maximo` |
   | `tables` | Comma-separated list of tables | `asset,commodities` |
   | `source_workspace_path` | Path to source files | `/Workspace/Repos/your-repo/maximo/` |
   | `bronze_catalog` | Bronze catalog name | `edp_bronze_dev` |
   | `bronze_schema` | Bronze schema name | `maximo` |
   | `bronze_target_schema` | Bronze target schema | `maximo_lakeflow` |
   | `silver_catalog` | Silver catalog name | `edp_silver_dev` |
   | `silver_schema` | Silver schema name | `maximo_ods_lakeflow` |

3. **Run the Notebook**:
   - Execute the notebook cells sequentially
   - Monitor the output for any errors or warnings

4. **Review Output Files**:
   - Bronze dataflow specifications: `/Volumes/{catalog}.{schema}.{volume}/output/{source_system}_{environment}/bronze_{source_system}/dataflowspec/`
   - Silver dataflow specifications: `/Volumes/{catalog}.{schema}.{volume}/output/{source_system}_{environment}/silver_{source_system}/dataflowspec/`
   - Schema files: `/Volumes/{catalog}.{schema}.{volume}/output/{source_system}_{environment}/bronze_{source_system}/schemas/` and `/Volumes/{catalog}.{schema}.{volume}/output/{source_system}_{environment}/silver_{source_system}/schemas/`
   - Data Quality Expectations: `/Volumes/{catalog}.{schema}.{volume}/output/{source_system}_{environment}/bronze_{source_system}/dqe/`

### Maximo-Specific Features

1. **Bronze Table Configuration**:
   - Extracts source path information from Maximo PySpark files
   - Configures appropriate reader options for JSON format
   - Sets up table properties for Change Data Feed

2. **Silver Table Configuration**:
   - Configures CDC settings with appropriate primary keys
   - Sets up sequence_by with timestamp function: `timestamp(DMS_CDC_TIMESTAMP)`
   - Configures SCD Type 2 for historical tracking

3. **File Matching**:
   - Matches source files to tables based on table name patterns
   - Supports both exact and partial matching

## Validation Framework

The DLT Validation Framework is designed to validate the data integrity between source and target tables, ensuring that the migration was successful.

### Validation Framework Components

1. **Configuration**: Setup and configuration of validation parameters
2. **Validation Engine**: Core validation logic for row count and row hash checks
3. **Reporting**: Generation of validation reports
4. **Scheduling**: Scheduling of validation runs

### Setting Up the Validation Framework

1. **Initialize the Framework**:
   - Run the bootstrap notebook: `src/dlt_validation_framework/bootstrap.py`
   - This creates necessary tables and default configurations

2. **Configure Validation**:
   - Open the configuration notebook: `src/dlt_validation_framework/config.py`
   - Add or modify validation configurations:

   ```python
   add_validation_config(
       source_catalog="edp_bronze_dev",
       source_schema="workday",
       source_table="account_sets",  # Specific table
       target_catalog="edp_bronze_dev",
       target_schema="workday_lakeflow",
       target_table="account_sets",
       primary_keys="id",  # Primary keys for row matching
       active=True
   )
   ```

### Running Validation

1. **Manual Validation**:
   - Open the validation notebook: `src/dlt_validation_framework/run_validation.py`
   - Run the validation:

   ```python
   # Run validation for all active configurations
   run_id = run_validation()
   
   # Run validation for specific tables
   run_id = run_validation(tables=["account_sets", "bank_accounts"])
   ```

2. **Scheduled Validation**:
   - Open the scheduling notebook: `src/dlt_validation_framework/scheduled_validation.py`
   - Configure the schedule:

   ```python
   # Schedule daily validation
   schedule_daily_validation()
   ```

   - Set up a Databricks job to run the notebook on a schedule

### Viewing Validation Results

1. **Query Results**:
   ```sql
   SELECT * FROM utilities_dev.erp_remediation.dlt_validation_results
   WHERE run_id = 'your-run-id'
   ```

2. **Generate Reports**:
   - Open the validation notebook: `src/dlt_validation_framework/validation.py`
   - Generate a report:

   ```python
   # Generate HTML report for the latest run
   html_report = generate_html_report()
   displayHTML(html_report)
   ```

3. **Analyze Failures**:
   ```python
   # Get failure summary
   failure_summary = get_failure_summary()
   display(failure_summary)
   ```

## Troubleshooting

### Common Issues and Solutions

#### Workday Migration Issues

1. **Missing Source Files**:
   - **Issue**: Source files not found in Unity Catalog volume
   - **Solution**: Ensure files are uploaded to the correct path and have proper permissions

2. **Bronze Table Name Not Found**:
   - **Issue**: Cannot find `bronze_table_name` variable in source files
   - **Solution**: Check source files for correct variable naming or use filename matching

3. **Schema Extraction Failures**:
   - **Issue**: Cannot extract schema from tables
   - **Solution**: Ensure tables exist or provide schema files manually

#### Maximo Migration Issues

1. **Source Path Issues**:
   - **Issue**: Cannot find source path in Maximo files
   - **Solution**: Check source files for correct path variables

2. **CDC Settings Issues**:
   - **Issue**: Incorrect CDC settings in silver configuration
   - **Solution**: Verify primary keys and sequence_by settings

#### Validation Framework Issues

1. **Missing Tables**:
   - **Issue**: Tables not found during validation
   - **Solution**: Ensure tables exist and are accessible

2. **Primary Key Issues**:
   - **Issue**: Row hash validation fails due to missing primary keys
   - **Solution**: Configure correct primary keys for each table

3. **Performance Issues**:
   - **Issue**: Validation takes too long for large tables
   - **Solution**: Use sampling or partitioning for large tables

## Best Practices

### Migration Best Practices

1. **Source File Preparation**:
   - Ensure source files are properly formatted and accessible
   - Check for required variables and patterns

2. **Parameter Configuration**:
   - Use consistent naming conventions for catalogs and schemas
   - Specify all required parameters

3. **Output Validation**:
   - Review generated specifications for correctness
   - Validate schema files against actual table structures

### Validation Best Practices

1. **Configuration**:
   - Configure validation for all critical tables
   - Specify correct primary keys for accurate row matching

2. **Scheduling**:
   - Schedule regular validation runs to catch issues early
   - Set up alerts for validation failures

3. **Reporting**:
   - Review validation reports regularly
   - Investigate and address any failures promptly

## Reference

### Notebook Parameters

#### Workday UC Notebook Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `environment` | Environment (dev, test, prod) | Yes | `dev` |
| `catalog` | Unity Catalog name | Yes | `utilities_dev` |
| `schema` | Schema name | Yes | `erp_remediation` |
| `volume` | Volume name | Yes | `dlt_migration_tool` |
| `source_system` | Source system name | Yes | `workday` |
| `tables` | Comma-separated list of tables | Yes | `account_sets,bank_accounts` |
| `source_volume_path` | Path to source files | Yes | `/Volumes/utilities_dev/erp_remediation/dlt_migration_tool/source/` |
| `bronze_catalog` | Bronze catalog name | Yes | `edp_bronze_dev` |
| `bronze_schema` | Bronze schema name | Yes | `workday` |
| `bronze_target_schema` | Bronze target schema | Yes | `workday_lakeflow` |
| `silver_catalog` | Silver catalog name | Yes | `edp_silver_dev` |
| `silver_schema` | Silver schema name | Yes | `workday_ods_lakeflow` |
| `s3_bucket` | S3 bucket name | No | Based on environment |

#### Maximo UC Notebook Parameters

| Parameter | Description | Required | Default |
|-----------|-------------|----------|---------|
| `environment` | Environment (dev, test, prod) | Yes | `dev` |
| `catalog` | Unity Catalog name | Yes | `utilities_dev` |
| `schema` | Schema name | Yes | `erp_remediation` |
| `volume` | Volume name | Yes | `dlt_migration_tool` |
| `source_system` | Source system name | Yes | `maximo` |
| `tables` | Comma-separated list of tables | Yes | `asset,commodities` |
| `source_workspace_path` | Path to source files | Yes | `/Workspace/Repos/your-repo/maximo/` |
| `bronze_catalog` | Bronze catalog name | Yes | `edp_bronze_dev` |
| `bronze_schema` | Bronze schema name | Yes | `maximo` |
| `bronze_target_schema` | Bronze target schema | Yes | `maximo_lakeflow` |
| `silver_catalog` | Silver catalog name | Yes | `edp_silver_dev` |
| `silver_schema` | Silver schema name | Yes | `maximo_ods_lakeflow` |

### Validation Framework API

#### Configuration API

```python
# Add validation configuration
add_validation_config(
    source_catalog,
    source_schema,
    source_table,
    target_catalog,
    target_schema,
    target_table,
    primary_keys,
    active=True
)

# Update validation configuration
update_validation_config(
    config_id,
    active=True,
    primary_keys="id,version"
)

# Get active configurations
get_active_configurations()
```

#### Validation API

```python
# Run validation
run_validation(
    tables=None,  # Optional list of tables
    run_id=None,  # Optional run ID
    source_target_mapping=None  # Optional mapping override
)

# Generate report
generate_html_report(run_id=None)  # Latest run if None

# Get failure summary
get_failure_summary(run_id=None)  # Latest run if None
```

#### Scheduling API

```python
# Schedule daily validation
schedule_daily_validation(
    start_time=None,  # Optional start time
    tables=None  # Optional list of tables
)
```

### Directory Structure

```
dlt_migration_tool/
├── src/
│   ├── dlt_migration_tool/
│   │   ├── notebooks/
│   │   │   ├── dlt_migration_tool_notebook_workday_uc.py
│   │   │   ├── dlt_migration_tool_notebook_maximo_uc.py
│   │   │   └── ...
│   │   ├── utils/
│   │   │   ├── workday_parser.py
│   │   │   ├── schema_utils.py
│   │   │   └── ...
│   │   └── ...
│   └── dlt_validation_framework/
│       ├── bootstrap.py
│       ├── config.py
│       ├── validation.py
│       └── ...
├── USER_GUIDE.md
└── README.md
```
