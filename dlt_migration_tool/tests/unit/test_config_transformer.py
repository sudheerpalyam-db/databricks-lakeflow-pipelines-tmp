"""
Unit tests for the configuration transformer.
"""

import unittest
from unittest.mock import patch, MagicMock

from dlt_migration_tool.core.transformer import ConfigTransformer


class TestConfigTransformer(unittest.TestCase):
    """Tests for the ConfigTransformer class."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.mock_mapping_engine = MagicMock()
        self.transformer = ConfigTransformer(self.mock_mapping_engine)
    
    def test_transform_config(self):
        """Test transforming a single configuration."""
        # Set up mock return value
        mock_transformed = {
            "dataFlowId": "test_id",
            "sourceType": "delta",
            "targetDetails": {
                "table": "test_table"
            }
        }
        self.mock_mapping_engine.transform_config.return_value = mock_transformed
        
        # Call the method
        source_config = {"data_flow_id": "test_id"}
        result = self.transformer.transform_config(source_config)
        
        # Verify the mapping engine was called
        self.mock_mapping_engine.transform_config.assert_called_once_with(source_config)
        
        # Verify the result
        self.assertEqual(result, mock_transformed)
    
    def test_transform_configs(self):
        """Test transforming multiple configurations."""
        # Set up mock return values
        mock_transformed1 = {
            "dataFlowId": "test_id1",
            "sourceType": "delta",
            "targetDetails": {
                "table": "test_table1"
            }
        }
        mock_transformed2 = {
            "dataFlowId": "test_id2",
            "sourceType": "delta",
            "targetDetails": {
                "table": "test_table2"
            }
        }
        self.mock_mapping_engine.transform_config.side_effect = [
            mock_transformed1,
            mock_transformed2
        ]
        
        # Call the method
        source_configs = [
            {"data_flow_id": "test_id1"},
            {"data_flow_id": "test_id2"}
        ]
        results = self.transformer.transform_configs(source_configs)
        
        # Verify the mapping engine was called for each config
        self.assertEqual(self.mock_mapping_engine.transform_config.call_count, 2)
        
        # Verify the results
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0], mock_transformed1)
        self.assertEqual(results[1], mock_transformed2)
    
    def test_validate_transformed_config_valid(self):
        """Test validation of a valid transformed configuration."""
        valid_config = {
            "dataFlowId": "test_id",
            "sourceType": "delta",
            "targetDetails": {
                "table": "test_table"
            }
        }
        
        # This should not raise an exception
        self.transformer._validate_transformed_config(valid_config)
    
    def test_validate_transformed_config_missing_fields(self):
        """Test validation of a configuration with missing required fields."""
        # Missing dataFlowId
        invalid_config1 = {
            "sourceType": "delta",
            "targetDetails": {
                "table": "test_table"
            }
        }
        with self.assertRaises(ValueError):
            self.transformer._validate_transformed_config(invalid_config1)
        
        # Missing sourceType
        invalid_config2 = {
            "dataFlowId": "test_id",
            "targetDetails": {
                "table": "test_table"
            }
        }
        with self.assertRaises(ValueError):
            self.transformer._validate_transformed_config(invalid_config2)
        
        # Missing targetDetails
        invalid_config3 = {
            "dataFlowId": "test_id",
            "sourceType": "delta"
        }
        with self.assertRaises(ValueError):
            self.transformer._validate_transformed_config(invalid_config3)
        
        # Missing table in targetDetails
        invalid_config4 = {
            "dataFlowId": "test_id",
            "sourceType": "delta",
            "targetDetails": {}
        }
        with self.assertRaises(ValueError):
            self.transformer._validate_transformed_config(invalid_config4)


if __name__ == '__main__':
    unittest.main()
