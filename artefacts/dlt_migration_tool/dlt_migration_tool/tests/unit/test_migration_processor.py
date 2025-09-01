"""
Unit tests for the migration processor.
"""

import unittest
import tempfile
import json
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

from dlt_migration_tool.core.transformer import MigrationProcessor


class TestMigrationProcessor(unittest.TestCase):
    """Tests for the MigrationProcessor class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_transformer = MagicMock()
        self.processor = MigrationProcessor(self.mock_transformer)
        
        # Create temporary directories for testing
        self.temp_dir = tempfile.TemporaryDirectory()
        self.input_dir = Path(self.temp_dir.name) / "input"
        self.output_dir = Path(self.temp_dir.name) / "output"
        self.input_dir.mkdir()
        self.output_dir.mkdir()
    
    def tearDown(self):
        """Clean up after tests."""
        self.temp_dir.cleanup()
    
    def test_process_file_single_object(self):
        """Test processing a file with a single JSON object."""
        # Create a test input file
        input_file = self.input_dir / "test.json"
        test_data = {
            "data_flow_id": "test_id",
            "data_flow_group": "test_group"
        }
        with open(input_file, 'w') as f:
            json.dump(test_data, f)
        
        # Set up mock transformer
        transformed_data = {
            "dataFlowId": "test_id",
            "dataFlowGroup": "test_group",
            "sourceType": "delta",
            "targetDetails": {
                "table": "test_table"
            }
        }
        self.mock_transformer.transform_config.return_value = transformed_data
        
        # Process the file
        output_files = self.processor.process_file(input_file, self.output_dir)
        
        # Verify the transformer was called
        self.mock_transformer.transform_config.assert_called_once()
        
        # Verify the output file was created
        self.assertEqual(len(output_files), 1)
        output_file = output_files[0]
        self.assertTrue(output_file.exists())
        
        # Verify the output file content
        with open(output_file, 'r') as f:
            output_data = json.load(f)
        self.assertEqual(output_data, transformed_data)
    
    def test_process_file_array(self):
        """Test processing a file with an array of JSON objects."""
        # Create a test input file
        input_file = self.input_dir / "test_array.json"
        test_data = [
            {
                "data_flow_id": "test_id1",
                "data_flow_group": "test_group1"
            },
            {
                "data_flow_id": "test_id2",
                "data_flow_group": "test_group2"
            }
        ]
        with open(input_file, 'w') as f:
            json.dump(test_data, f)
        
        # Set up mock transformer
        transformed_data1 = {
            "dataFlowId": "test_id1",
            "dataFlowGroup": "test_group1",
            "sourceType": "delta",
            "targetDetails": {
                "table": "test_table1"
            }
        }
        transformed_data2 = {
            "dataFlowId": "test_id2",
            "dataFlowGroup": "test_group2",
            "sourceType": "delta",
            "targetDetails": {
                "table": "test_table2"
            }
        }
        self.mock_transformer.transform_config.side_effect = [
            transformed_data1,
            transformed_data2
        ]
        
        # Process the file
        output_files = self.processor.process_file(input_file, self.output_dir)
        
        # Verify the transformer was called twice
        self.assertEqual(self.mock_transformer.transform_config.call_count, 2)
        
        # Verify two output files were created
        self.assertEqual(len(output_files), 2)
        for output_file in output_files:
            self.assertTrue(output_file.exists())
        
        # Verify the output file contents
        with open(output_files[0], 'r') as f:
            output_data1 = json.load(f)
        with open(output_files[1], 'r') as f:
            output_data2 = json.load(f)
        
        self.assertEqual(output_data1, transformed_data1)
        self.assertEqual(output_data2, transformed_data2)
    
    def test_process_directory(self):
        """Test processing a directory of JSON files."""
        # Create test input files
        file1 = self.input_dir / "test1.json"
        file2 = self.input_dir / "test2.json"
        subdir = self.input_dir / "subdir"
        subdir.mkdir()
        file3 = subdir / "test3.json"
        
        test_data1 = {"data_flow_id": "test_id1"}
        test_data2 = {"data_flow_id": "test_id2"}
        test_data3 = {"data_flow_id": "test_id3"}
        
        with open(file1, 'w') as f:
            json.dump(test_data1, f)
        with open(file2, 'w') as f:
            json.dump(test_data2, f)
        with open(file3, 'w') as f:
            json.dump(test_data3, f)
        
        # Set up mock transformer
        transformed_data1 = {
            "dataFlowId": "test_id1",
            "sourceType": "delta",
            "targetDetails": {"table": "test_table1"}
        }
        transformed_data2 = {
            "dataFlowId": "test_id2",
            "sourceType": "delta",
            "targetDetails": {"table": "test_table2"}
        }
        transformed_data3 = {
            "dataFlowId": "test_id3",
            "sourceType": "delta",
            "targetDetails": {"table": "test_table3"}
        }
        
        # Mock process_file instead of transform_config to simplify the test
        with patch.object(self.processor, 'process_file') as mock_process_file:
            mock_process_file.side_effect = [
                [Path(self.output_dir / "test1.json")],
                [Path(self.output_dir / "test2.json")],
                [Path(self.output_dir / "subdir" / "test3.json")]
            ]
            
            # Process the directory
            results = self.processor.process_directory(self.input_dir, self.output_dir)
            
            # Verify process_file was called for each file
            self.assertEqual(mock_process_file.call_count, 3)
            
            # Verify the results
            self.assertEqual(len(results), 3)
            
            # Check that all input files are in the results
            input_files = set(results.keys())
            expected_files = {file1, file2, file3}
            self.assertEqual(input_files, expected_files)


if __name__ == '__main__':
    unittest.main()
