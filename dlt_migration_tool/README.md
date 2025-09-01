# DLT Migration Validation Framework

A comprehensive framework for validating data migration from traditional pipelines to Delta Live Tables (DLT).

## Overview

This framework provides tools to validate the data integrity between source tables and their corresponding DLT-migrated target tables. It focuses on two key validation aspects:

1. **Row Count Validation**: Ensures that the number of rows in source and target tables match.
2. **Row Hash Validation**: Ensures that the content of the data matches by comparing hash values of rows.

## Directory Structure

```
dlt_migration_tool/
├── validation_framework/
│   ├── config.py           # Configuration management module
│   ├── validator.py        # Core validation engine
│   ├── reporting.py        # Reporting and visualization module
│   ├── scheduler.py        # Scheduling functionality
│   └── setup.py            # Initial setup and configuration
└── README.md               # This documentation file
```

## Key Features

- **Configurable Validation**: Easily configure which tables to validate through a simple interface.
- **Comprehensive Validation**: Validates both row counts and data content through row hashing.
- **Detailed Reporting**: Generates detailed HTML reports of validation results.
- **Audit Trail**: Maintains a complete audit trail of all validation runs and results.
- **Scheduling**: Built-in scheduling functionality for regular validation runs.
- **Visualization**: Visual representation of validation trends and failure patterns.

## Getting Started

### 1. Setup

Run the `setup.py` notebook to create the necessary tables and default configurations:

```python
%run /Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/validation_framework/setup
```

This will create:
- Validation configuration table
- Validation runs table
- Validation results table

And set up default configurations for:
- Maximo Bronze: `edp_bronze_dev.maximo` → `edp_bronze_dev.maximo_lakeflow`
- Maximo Silver: `edp_silver_dev.maximo_ods` → `edp_silver_dev.maximo_ods_lakeflow`
- Workday Bronze: `edp_bronze_dev.workday` → `edp_bronze_dev.workday_lakeflow`
- Workday Silver: `edp_silver_dev.workday_ods` → `edp_silver_dev.workday_ods_lakeflow`

### 2. Configuration

To add or modify validation configurations:

```python
%run /Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/validation_framework/config

# Add a new configuration
add_validation_config(
    source_catalog="edp_bronze_dev",
    source_schema="maximo",
    source_table="asset",  # Specific table
    target_catalog="edp_bronze_dev",
    target_schema="maximo_lakeflow",
    target_table="asset",
    primary_keys="assetnum,siteid",  # Primary keys for row matching
    active=True
)

# Update an existing configuration
update_validation_config(
    config_id="your-config-id",
    active=True,
    primary_keys="id,version"
)

# View active configurations
display(get_active_configurations())
```

### 3. Running Validation

To run validation manually:

```python
%run /Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/validation_framework/validator

# Run validation for all active configurations
run_id = run_validation()
```

### 4. Viewing Reports

To generate and view reports:

```python
%run /Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/validation_framework/reporting

# Generate HTML report for the latest run
html_report = generate_html_report()
displayHTML(html_report)

# Save the report to a file
report_path = save_html_report()

# View failure summary
display(get_failure_summary())

# Visualize trends
visualize_trend_report(days=30)
```

### 5. Scheduling

To schedule daily validation runs:

```python
%run /Users/sudheer.palyam/workspace/databricks/dlt_migration_tool/validation_framework/scheduler

# Run the scheduled validation
results = schedule_daily_validation()
```

You can also set up a Databricks job to run the scheduler notebook daily.

## Audit Tables

All validation data is stored in the following tables under the `utilities_dev.erp_remediation` schema:

1. **validation_config**: Stores configuration for table validations
2. **validation_runs**: Stores information about each validation run
3. **validation_results**: Stores detailed results for each validated table

## Logging

Detailed logs are stored in the `utilities_dev.erp_remediation.dlt_migration_tool` volume.

## Best Practices

1. **Primary Keys**: Always specify primary keys when possible for more accurate row-level validation.
2. **Regular Validation**: Schedule validation to run daily to catch any issues early.
3. **Incremental Validation**: For very large tables, consider validating only the most recent data.
4. **Review Reports**: Regularly review validation reports to identify patterns in failures.

## Troubleshooting

Common issues and solutions:

1. **Missing Tables**: If target tables are missing, check that the DLT pipeline has run successfully.
2. **Schema Differences**: Differences in column names or data types can cause hash validation failures.
3. **Performance Issues**: For very large tables, consider adding sampling or partitioning to the validation logic.
