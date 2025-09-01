# DLT Migration Tool - Workday Implementation

This document describes the Workday implementation for the DLT Migration Tool.

## Overview

The Workday implementation extends the DLT Migration Tool to support Workday XML data. Unlike Maximo, which provides JSON data, Workday data comes in XML format and requires special processing to extract and transform the data.

## Key Components

### XML Processing

Workday data is provided in XML format, which requires specialized processing:

1. **XML Parsing**: Using `xmltodict` to convert XML to Python dictionaries
2. **Element Transformation**: Custom transformation functions to handle Workday's specific XML structure
3. **Field Extraction**: UDFs (User Defined Functions) to extract and transform fields from the XML

### Implementation Details

The Workday implementation includes:

- **XML Parsing Utilities**: Functions to parse and transform XML data
- **Workday-specific Configuration**: Settings for Workday bronze and silver tables
- **Schema Extraction**: Adapted schema extraction for Workday tables
- **DQE Generation**: Data Quality Expectations for Workday data

## Usage

### Prerequisites

1. Install required packages:
   ```
   pip install xmltodict
   ```

2. Ensure Workday XML files are available in the input directory

### Running the Migration

1. Use the `dlt_migration_tool_notebook_workday_uc.py` notebook
2. Configure the parameters:
   - `environment`: dev, test, or prod
   - `source_system`: workday
   - `tables`: comma-separated list of tables to process
   - `bronze_catalog`, `bronze_schema`, `silver_catalog`, `silver_schema`: target locations

## XML Processing Details

### Transformation Logic

The Workday XML transformation handles several specific patterns:

1. **ID Elements**: Special handling for Workday ID elements
   ```xml
   <ID type="WID">1234</ID>
   ```

2. **Type Attributes**: Processing type attributes
   ```xml
   <Field Type="Name">Value</Field>
   ```

3. **Nested Structures**: Handling nested XML structures

### Schema Handling

Schemas are extracted from existing tables when available. If tables don't exist yet, empty schemas are created and can be populated later.

## Differences from Maximo Implementation

1. **Data Format**: Workday uses XML instead of JSON
2. **Parsing Logic**: Requires XML-specific parsing and transformation
3. **Field Extraction**: Uses regex pattern matching for extracting fields
4. **Reference Implementation**: Based on the Finance directory implementation

## Troubleshooting

Common issues:

1. **XML Parsing Errors**: Check XML format and structure
2. **Missing Tables**: Ensure tables exist or provide schema files
3. **Field Extraction Failures**: Review regex patterns and XML structure

## References

- Workday API Documentation
- Finance directory implementation: `~/workspace/databricks/apa-databricks-medallion-workday-develop/src/Finance/`
