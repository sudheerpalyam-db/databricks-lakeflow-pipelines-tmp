"""
Unit tests for the mapping engine.
"""

import unittest
from unittest.mock import patch

from dlt_migration_tool.mapping.mapping_engine import (
    MappingEngine,
    DefaultMappingProvider,
    CustomMappingProvider
)


class TestMappingEngine(unittest.TestCase):
    """Tests for the MappingEngine class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.engine = MappingEngine()
    
    def test_default_mapping_provider(self):
        """Test that the default mapping provider is used when none is provided."""
        self.assertIsInstance(self.engine.mapping_provider, DefaultMappingProvider)
    
    def test_custom_mapping_provider(self):
        """Test that a custom mapping provider can be used."""
        custom_rules = {"test_key": "test_value"}
        provider = CustomMappingProvider(custom_rules)
        engine = MappingEngine(provider)
        self.assertEqual(engine.mapping_rules, custom_rules)
    
    def test_transform_config_basic(self):
        """Test basic configuration transformation."""
        source_config = {
            "data_flow_id": "test_id",
            "data_flow_group": "test_group",
            "source_system": "test_system"
        }
        
        expected_keys = [
            "dataFlowId", 
            "dataFlowGroup", 
            "sourceSystem",
            "dataFlowType",
            "mode",
            "targetFormat",
            "sourceDetails",
            "targetDetails"
        ]
        
        result = self.engine.transform_config(source_config)
        
        # Check that all expected keys are present
        for key in expected_keys:
            self.assertIn(key, result)
        
        # Check that values were correctly mapped
        self.assertEqual(result["dataFlowId"], "test_id")
        self.assertEqual(result["dataFlowGroup"], "test_group")
        self.assertEqual(result["sourceSystem"], "test_system")
    
    def test_transform_config_source_details(self):
        """Test transformation of source details."""
        source_config = {
            "source_details": {
                "source_database": "test_db",
                "source_table": "test_table",
                "source_path_it": "/test/path"
            }
        }
        
        result = self.engine.transform_config(source_config)
        
        # Check that source details were correctly mapped
        self.assertIn("sourceDetails", result)
        source_details = result["sourceDetails"]
        self.assertEqual(source_details.get("database"), "test_db")
        self.assertEqual(source_details.get("table"), "test_table")
        self.assertEqual(source_details.get("path"), "/test/path")
    
    def test_transform_config_reader_options(self):
        """Test transformation of reader options."""
        source_config = {
            "bronze_reader_options": {
                "option1": "value1",
                "option2": "value2"
            }
        }
        
        result = self.engine.transform_config(source_config)
        
        # Check that reader options were correctly mapped
        self.assertIn("sourceDetails", result)
        self.assertIn("readerOptions", result["sourceDetails"])
        reader_options = result["sourceDetails"]["readerOptions"]
        self.assertEqual(reader_options.get("option1"), "value1")
        self.assertEqual(reader_options.get("option2"), "value2")
    
    def test_transform_config_cdc_apply_changes(self):
        """Test transformation of CDC apply changes."""
        source_config = {
            "silver_cdc_apply_changes": {
                "keys": ["id"],
                "sequence_by": "timestamp",
                "scd_type": "1",
                "apply_as_deletes": "operation = 'DELETE'"
            }
        }
        
        result = self.engine.transform_config(source_config)
        
        # Check that CDC apply changes were correctly mapped
        self.assertIn("cdcApplyChanges", result)
        cdc = result["cdcApplyChanges"]
        self.assertEqual(cdc.get("keys"), ["id"])
        self.assertEqual(cdc.get("sequence_by"), "timestamp")
        self.assertEqual(cdc.get("scd_type"), "1")
        self.assertEqual(cdc.get("where"), "operation = 'DELETE'")
    
    def test_transform_extract_file_path(self):
        """Test the extract file path transformation."""
        # Test Workspace path
        workspace_path = "file:///Workspace/path/to/expectations.json"
        result = self.engine._transform_extract_file_path(workspace_path)
        self.assertEqual(result, "./expectations.json")
        
        # Test dbfs path
        dbfs_path = "{dbfs_path}/path/to/expectations.json"
        result = self.engine._transform_extract_file_path(dbfs_path)
        self.assertEqual(result, "./expectations.json")
        
        # Test regular path
        regular_path = "/path/to/expectations.json"
        result = self.engine._transform_extract_file_path(regular_path)
        self.assertEqual(result, regular_path)
    
    def test_determine_data_level(self):
        """Test data level determination."""
        # Bronze level
        bronze_config = {"bronze_table": "test_table"}
        self.assertEqual(self.engine._determine_data_level(bronze_config), "bronze")
        
        # Silver level
        silver_config = {"bronze_table": "test_bronze", "silver_table": "test_silver"}
        self.assertEqual(self.engine._determine_data_level(silver_config), "silver")
        
        # Unknown level
        unknown_config = {"other_key": "value"}
        self.assertEqual(self.engine._determine_data_level(unknown_config), "unknown")


if __name__ == '__main__':
    unittest.main()
