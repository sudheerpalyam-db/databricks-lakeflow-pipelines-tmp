"""
Tests for schema_validator module.
"""

import os
import json
import tempfile
import pytest
from dlt_migration_tool.utils.schema_validator import SchemaValidator, validate_dataflowspec, validate_directory

# Sample dataflowspec for testing
VALID_STANDARD_DATAFLOWSPEC = {
    "dataFlowId": "100",
    "dataFlowGroup": "bronze_maximo",
    "dataFlowType": "standard",
    "sourceSystem": "MYSQL",
    "sourceType": "cloudFiles",
    "sourceViewName": "v_customers_cdc",
    "sourceDetails": {
        "path": "/path/to/data",
        "readerOptions": {
            "cloudFiles.format": "json",
            "cloudFiles.inferColumnTypes": "true"
        }
    },
    "mode": "stream",
    "targetFormat": "delta",
    "targetDetails": {
        "table": "customers_cdc",
        "tableProperties": {
            "delta.enableChangeDataFeed": "true"
        }
    },
    "dataQualityExpectationsEnabled": True,
    "dataQualityExpectationsPath": "./customers_cdc_dqe.json",
    "quarantineMode": "table",
    "quarantineTargetDetails": {
        "targetFormat": "delta"
    }
}

INVALID_DATAFLOWSPEC = {
    "dataFlowId": "100",
    "dataFlowGroup": "bronze_maximo",
    # Missing required field: dataFlowType
    "sourceSystem": "MYSQL",
    "sourceType": "cloudFiles",
    # Missing required field: sourceViewName
    "sourceDetails": {
        "path": "/path/to/data"
    },
    # Missing required field: mode
    "targetFormat": "delta",
    "targetDetails": {
        "table": "customers_cdc"
    }
}


def test_schema_validator_initialization():
    """Test that SchemaValidator initializes correctly."""
    validator = SchemaValidator()
    assert validator is not None
    assert len(validator.schemas) > 0
    assert "spec_standard.json" in validator.schemas


def test_determine_schema():
    """Test that the correct schema is determined based on the dataflowspec."""
    validator = SchemaValidator()
    
    # Standard dataflowspec
    schema_file = validator._determine_schema({
        "dataFlowType": "standard"
    })
    assert schema_file == "spec_standard.json"
    
    # Flow dataflowspec
    schema_file = validator._determine_schema({
        "dataFlowType": "flow"
    })
    assert schema_file == "spec_flows.json"
    
    # Materialized view dataflowspec
    schema_file = validator._determine_schema({
        "dataFlowType": "materialized_view"
    })
    assert schema_file == "spec_materialized_views.json"
    
    # Unknown dataflowspec type
    schema_file = validator._determine_schema({
        "dataFlowType": "unknown"
    })
    assert schema_file == "spec_standard.json"
    
    # No dataflowspec type
    schema_file = validator._determine_schema({})
    assert schema_file == "spec_standard.json"


def test_validate_file():
    """Test validating a dataflowspec file."""
    validator = SchemaValidator()
    
    # Create a temporary file with a valid dataflowspec
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as temp_file:
        json.dump(VALID_STANDARD_DATAFLOWSPEC, temp_file)
        temp_file_path = temp_file.name
    
    try:
        # Test validating a valid file
        result = validator.validate_file(temp_file_path)
        assert result["valid"] is True
        assert result["file"] == temp_file_path
        assert result["schema"] == "spec_standard.json"
        
        # Create a temporary file with an invalid dataflowspec
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as temp_file:
            json.dump(INVALID_DATAFLOWSPEC, temp_file)
            invalid_temp_file_path = temp_file.name
        
        # Test validating an invalid file
        result = validator.validate_file(invalid_temp_file_path)
        assert result["valid"] is False
        assert result["file"] == invalid_temp_file_path
        assert "error" in result
        
        # Clean up the invalid file
        os.unlink(invalid_temp_file_path)
        
        # Test validating a non-existent file
        result = validator.validate_file("non_existent_file.json")
        assert result["valid"] is False
        assert "error" in result
    finally:
        # Clean up the valid file
        os.unlink(temp_file_path)


def test_validate_directory():
    """Test validating a directory of dataflowspec files."""
    validator = SchemaValidator()
    
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a valid dataflowspec file
        valid_file_path = os.path.join(temp_dir, "valid.json")
        with open(valid_file_path, "w") as f:
            json.dump(VALID_STANDARD_DATAFLOWSPEC, f)
        
        # Create an invalid dataflowspec file
        invalid_file_path = os.path.join(temp_dir, "invalid.json")
        with open(invalid_file_path, "w") as f:
            json.dump(INVALID_DATAFLOWSPEC, f)
        
        # Create a non-JSON file
        non_json_file_path = os.path.join(temp_dir, "not_json.txt")
        with open(non_json_file_path, "w") as f:
            f.write("This is not a JSON file")
        
        # Test validating the directory
        results = validator.validate_directory(temp_dir)
        assert results["valid"] == 1
        assert results["invalid"] == 1
        assert len(results["files"]) == 2  # Only JSON files should be validated
        
        # Test validating the directory recursively
        # Create a subdirectory with another valid file
        subdir = os.path.join(temp_dir, "subdir")
        os.makedirs(subdir)
        subdir_file_path = os.path.join(subdir, "valid_subdir.json")
        with open(subdir_file_path, "w") as f:
            json.dump(VALID_STANDARD_DATAFLOWSPEC, f)
        
        results = validator.validate_directory(temp_dir, recursive=True)
        assert results["valid"] == 2
        assert results["invalid"] == 1
        assert len(results["files"]) == 3


def test_validate_dataflowspec_function():
    """Test the validate_dataflowspec function."""
    # Create a temporary file with a valid dataflowspec
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as temp_file:
        json.dump(VALID_STANDARD_DATAFLOWSPEC, temp_file)
        temp_file_path = temp_file.name
    
    try:
        # Test validating a valid file
        result = validate_dataflowspec(temp_file_path)
        assert result["valid"] is True
        assert result["file"] == temp_file_path
        assert result["schema"] == "spec_standard.json"
    finally:
        # Clean up
        os.unlink(temp_file_path)


def test_validate_directory_function():
    """Test the validate_directory function."""
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create a valid dataflowspec file
        valid_file_path = os.path.join(temp_dir, "valid.json")
        with open(valid_file_path, "w") as f:
            json.dump(VALID_STANDARD_DATAFLOWSPEC, f)
        
        # Test validating the directory
        results = validate_directory(temp_dir)
        assert results["valid"] == 1
        assert results["invalid"] == 0
        assert len(results["files"]) == 1
