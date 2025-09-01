# DLT Migration Tool

A Python-based automation tool to migrate Databricks DLT-meta JSON configurations to the new DLT Framework dataflowspec format.

## Overview

This tool helps migrate existing DLT pipelines from the old DLT-meta format to the new DLT Framework format efficiently and reliably.

## Features

- Convert DLT-meta JSON configurations to DLT Framework dataflowspec format
- Process individual files or entire directories
- Maintain directory structure when processing recursively
- Detailed logging and error reporting
- Customizable mapping rules

## Installation

### From Source

```bash
git clone https://your-gitlab-repo/dlt-migration-tool.git
cd dlt-migration-tool
pip install -e .
```

## Usage

### Command Line Interface

```bash
# Process a single file
dlt-migrate -i /path/to/input.json -o /path/to/output/dir

# Process a directory
dlt-migrate -i /path/to/input/dir -o /path/to/output/dir

# Process a directory recursively with verbose output
dlt-migrate -i /path/to/input/dir -o /path/to/output/dir -r -v

# Save logs to a file
dlt-migrate -i /path/to/input/dir -o /path/to/output/dir --log-file migration.log
```

### Python API

```python
from dlt_migration_tool.core.transformer import MigrationProcessor

# Initialize the processor
processor = MigrationProcessor(verbose=True)

# Process a single file
output_files = processor.process_file(
    "/path/to/input.json", 
    "/path/to/output/dir"
)

# Process a directory
results = processor.process_directory(
    "/path/to/input/dir",
    "/path/to/output/dir",
    recursive=True
)
```

## Configuration Format Examples

### Input Format (DLT-meta)

```json
{
  "data_flow_id": "100",
  "data_flow_group": "A1",
  "source_system": "MYSQL",
  "source_format": "cloudFiles",
  "source_details": {
    "source_database": "APP",
    "source_table": "CUSTOMERS",
    "source_path_it": "{dbfs_path}/cdc_raw/customers"
  },
  "bronze_database_it": "bronze_it_{run_id}",
  "bronze_table": "customers_cdc",
  "bronze_reader_options": {
    "cloudFiles.format": "json",
    "cloudFiles.inferColumnTypes": "true",
    "cloudFiles.rescuedDataColumn": "_rescued_data"
  }
}
```

### Output Format (DLT Framework dataflowspec)

```json
{
  "dataFlowId": "100",
  "dataFlowGroup": "A1",
  "dataFlowType": "standard",
  "sourceSystem": "MYSQL",
  "sourceType": "cloudFiles",
  "sourceViewName": "v_customers_cdc",
  "sourceDetails": {
    "database": "APP",
    "table": "CUSTOMERS",
    "path": "{dbfs_path}/cdc_raw/customers",
    "readerOptions": {
      "cloudFiles.format": "json",
      "cloudFiles.inferColumnTypes": "true",
      "cloudFiles.rescuedDataColumn": "_rescued_data"
    }
  },
  "mode": "stream",
  "targetFormat": "delta",
  "targetDetails": {
    "table": "customers_cdc",
    "tableProperties": {
      "delta.enableChangeDataFeed": "true"
    }
  }
}
```

## Development

### Setup Development Environment

```bash
# Clone the repository
git clone https://your-gitlab-repo/dlt-migration-tool.git
cd dlt-migration-tool

# Create and activate a virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install development dependencies
pip install -e ".[dev]"
```

### Run Tests

```bash
pytest
```

### Run Linters and Formatters

```bash
# Format code
black src tests

# Sort imports
isort src tests

# Type checking
mypy src

# Linting
flake8 src tests
```

### Project Cleanup

To clean up Python cache files and other unnecessary files before committing:

```bash
./clean.sh
```

## License

This project is proprietary and confidential. Unauthorized copying, transferring or reproduction of the contents, via any medium is strictly prohibited.
