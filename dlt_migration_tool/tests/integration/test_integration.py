"""
Integration tests for the DLT migration tool.
"""

import unittest
import tempfile
import json
import os
import shutil
from pathlib import Path

from dlt_migration_tool.core.transformer import MigrationProcessor


class TestDLTMigrationIntegration(unittest.TestCase):
    """Integration tests for the DLT migration tool."""
    
    def setUp(self):
        """Set up test fixtures."""
        # Create temporary directories for testing
        self.temp_dir = tempfile.TemporaryDirectory()
        self.test_dir = Path(self.temp_dir.name)
        self.input_dir = self.test_dir / "input"
        self.output_dir = self.test_dir / "output"
        self.input_dir.mkdir()
        self.output_dir.mkdir()
        
        # Create test input files
        self.create_test_files()
        
        # Initialize the processor
        self.processor = MigrationProcessor(verbose=True)
    
    def tearDown(self):
        """Clean up after tests."""
        self.temp_dir.cleanup()
    
    def create_test_files(self):
        """Create test input files."""
        # Create a single object file
        single_config = {
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
            },
            "bronze_table_path_it": "{dbfs_path}/data/bronze/customers",
            "bronze_data_quality_expectations_json_it": "file:///Workspace{repo_path}/integration-tests/conf/dlt-meta/dqe/customers/bronze_data_quality_expectations.json"
        }
        
        with open(self.input_dir / "single_config.json", 'w') as f:
            json.dump(single_config, f, indent=2)
        
        # Create an array file
        array_config = [
            {
                "target_table": "customers",
                "target_partition_cols": [""],
                "select_exp": ["address", "email", "firstname", "id", "lastname", "operation_date", "operation", "_rescued_data"],
                "where_clause": [""]
            },
            {
                "target_table": "transactions",
                "target_partition_cols": [""],
                "select_exp": ["id", "customer_id", "amount", "item_count", "operation_date", "operation", "_rescued_data"],
                "where_clause": [""]
            }
        ]
        
        with open(self.input_dir / "array_config.json", 'w') as f:
            json.dump(array_config, f, indent=2)
        
        # Create a file with CDC apply changes
        cdc_config = {
            "data_flow_id": "101",
            "data_flow_group": "A1",
            "source_system": "MYSQL",
            "source_format": "delta",
            "silver_database_it": "silver_it_{run_id}",
            "silver_table": "transactions",
            "silver_cdc_apply_changes": {
                "keys": ["id"],
                "sequence_by": "operation_date",
                "scd_type": "1",
                "apply_as_deletes": "operation = 'DELETE'",
                "except_column_list": ["operation", "operation_date", "_rescued_data"]
            },
            "silver_table_path_it": "{dbfs_path}/data/silver/transactions"
        }
        
        with open(self.input_dir / "cdc_config.json", 'w') as f:
            json.dump(cdc_config, f, indent=2)
        
        # Create a subdirectory with a file
        subdir = self.input_dir / "subdir"
        subdir.mkdir()
        
        subdir_config = {
            "data_flow_id": "103",
            "data_flow_group": "A2",
            "source_system": "Sensor Device",
            "source_format": "eventhub",
            "bronze_table": "bronze_iot",
            "bronze_partition_columns": "date"
        }
        
        with open(subdir / "subdir_config.json", 'w') as f:
            json.dump(subdir_config, f, indent=2)
    
    def test_process_single_file(self):
        """Test processing a single file."""
        input_file = self.input_dir / "single_config.json"
        
        # Process the file
        output_files = self.processor.process_file(input_file, self.output_dir)
        
        # Verify the output file was created
        self.assertEqual(len(output_files), 1)
        output_file = output_files[0]
        self.assertTrue(output_file.exists())
        
        # Verify the output file content
        with open(output_file, 'r') as f:
            output_data = json.load(f)
        
        # Check required fields
        self.assertEqual(output_data["dataFlowId"], "100")
        self.assertEqual(output_data["dataFlowGroup"], "A1")
        self.assertEqual(output_data["sourceSystem"], "MYSQL")
        self.assertEqual(output_data["sourceType"], "cloudFiles")
        
        # Check nested fields
        self.assertIn("sourceDetails", output_data)
        source_details = output_data["sourceDetails"]
        self.assertEqual(source_details.get("database"), "APP")
        self.assertEqual(source_details.get("table"), "CUSTOMERS")
        self.assertEqual(source_details.get("path"), "{dbfs_path}/cdc_raw/customers")
        
        # Check reader options
        self.assertIn("readerOptions", source_details)
        reader_options = source_details["readerOptions"]
        self.assertEqual(reader_options.get("cloudFiles.format"), "json")
        
        # Check target details
        self.assertIn("targetDetails", output_data)
        target_details = output_data["targetDetails"]
        self.assertEqual(target_details.get("table"), "customers_cdc")
        self.assertEqual(target_details.get("path"), "{dbfs_path}/data/bronze/customers")
        
        # Check data quality expectations
        self.assertTrue(output_data.get("dataQualityExpectationsEnabled", False))
        self.assertTrue(output_data.get("dataQualityExpectationsPath", "").endswith("bronze_data_quality_expectations.json"))
    
    def test_process_array_file(self):
        """Test processing a file with an array of configurations."""
        input_file = self.input_dir / "array_config.json"
        
        # Process the file
        output_files = self.processor.process_file(input_file, self.output_dir)
        
        # Verify two output files were created
        self.assertEqual(len(output_files), 2)
        for output_file in output_files:
            self.assertTrue(output_file.exists())
    
    def test_process_cdc_file(self):
        """Test processing a file with CDC apply changes."""
        input_file = self.input_dir / "cdc_config.json"
        
        # Process the file
        output_files = self.processor.process_file(input_file, self.output_dir)
        
        # Verify the output file was created
        self.assertEqual(len(output_files), 1)
        output_file = output_files[0]
        self.assertTrue(output_file.exists())
        
        # Verify the output file content
        with open(output_file, 'r') as f:
            output_data = json.load(f)
        
        # Check CDC apply changes
        self.assertIn("cdcApplyChanges", output_data)
        cdc = output_data["cdcApplyChanges"]
        self.assertEqual(cdc.get("keys"), ["id"])
        self.assertEqual(cdc.get("sequence_by"), "operation_date")
        self.assertEqual(cdc.get("scd_type"), "1")
        self.assertEqual(cdc.get("where"), "operation = 'DELETE'")
    
    def test_process_directory(self):
        """Test processing a directory."""
        # Process the directory
        results = self.processor.process_directory(self.input_dir, self.output_dir)
        
        # Verify all files were processed
        self.assertEqual(len(results), 4)  # 3 files in root dir + 1 in subdir
        
        # Verify output files were created
        output_files = list(Path(self.output_dir).glob("**/*.json"))
        self.assertEqual(len(output_files), 5)  # 4 input files, but array_config.json produces 2 outputs
        
        # Verify subdirectory structure was maintained
        subdir_output = self.output_dir / "subdir"
        self.assertTrue(subdir_output.exists())
        self.assertTrue((subdir_output / "subdir_config.json").exists())


if __name__ == '__main__':
    unittest.main()
