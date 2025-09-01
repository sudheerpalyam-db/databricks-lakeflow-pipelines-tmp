"""
Mapping engine for transforming DLT configurations.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional, List, Callable, Union
from abc import ABC, abstractmethod

from ..utils.json_utils import JsonUtils

logger = logging.getLogger(__name__)


class MappingProvider(ABC):
    """Abstract base class for mapping providers."""
    
    @abstractmethod
    def get_mapping_rules(self) -> Dict[str, Any]:
        """
        Get mapping rules for transforming configurations.
        
        Returns:
            Dictionary of mapping rules
        """
        pass


class DefaultMappingProvider(MappingProvider):
    """Default implementation of mapping provider with predefined rules."""
    
    def get_mapping_rules(self) -> Dict[str, Any]:
        """
        Get default mapping rules for transforming DLT-meta to dataflowspec.
        
        Returns:
            Dictionary of mapping rules
        """
        return {
            # Basic mappings
            "data_flow_id": {"target": "dataFlowId", "type": "transform", "transform": "format_dataflow_id"},
            "data_flow_group": {"target": "dataFlowGroup", "type": "transform", "transform": "format_dataflow_group"},
            "source_system": {"target": "sourceSystem", "type": "transform", "transform": "lowercase_transform"},
            
            # Source mappings
            "source_format": {"target": "sourceType", "type": "direct"},
            
            # Source details mapping - Bronze
            "source_details": {
                "type": "object",
                "target": "sourceDetails",
                "mappings": {
                    "source_database": {"target": "database", "type": "direct"},
                    "source_table": {"target": "table", "type": "direct"},
                    "source_path_dev": {"target": "path", "type": "direct"}
                }
            },
            
            # Bronze reader options
            "bronze_reader_options": {
                "type": "object",
                "target": "sourceDetails.readerOptions",
                "direct_copy": True
            },
            
            # Bronze table
            "bronze_table": {"target": "targetDetails.table", "type": "direct"},
            "bronze_table_path_dev": {"target": "targetDetails.path", "type": "direct"},
            "bronze_partition_columns": {"target": "targetDetails.partitionColumns", "type": "direct"},
            
            # Silver mappings
            "silver_table": {"target": "targetDetails.table", "type": "direct"},
            "silver_table_path_dev": {"target": "targetDetails.path", "type": "direct"},
            "silver_partition_columns": {"target": "targetDetails.partitionColumns", "type": "direct"},
            
            # CDC apply changes
            "silver_cdc_apply_changes": {
                "type": "object",
                "target": "cdcSettings",
                "mappings": {
                    "keys": {"target": "keys", "type": "direct"},
                    "sequence_by": {"target": "sequence_by", "type": "direct"},
                    "scd_type": {"target": "scd_type", "type": "direct"},
                    "apply_as_deletes": {"target": "where", "type": "direct"},
                    "except_column_list": {"target": "except_column_list", "type": "direct"}
                }
            },
            
            # Data quality
            "bronze_data_quality_expectations_json_it": {
                "type": "transform",
                "target": "dataQualityExpectationsPath",
                "transform": "format_dqe_path"
            }
        }


class CustomMappingProvider(MappingProvider):
    """Custom mapping provider that allows for user-defined rules."""
    
    def __init__(self, mapping_rules: Dict[str, Any]):
        """
        Initialize with custom mapping rules.
        
        Args:
            mapping_rules: Custom mapping rules
        """
        self._mapping_rules = mapping_rules
    
    def get_mapping_rules(self) -> Dict[str, Any]:
        """
        Get custom mapping rules.
        
        Returns:
            Dictionary of mapping rules
        """
        return self._mapping_rules


class MappingEngine:
    """Engine for transforming configurations based on mapping rules."""
    
    def __init__(self, mapping_provider: Optional[MappingProvider] = None):
        """
        Initialize the mapping engine.
        
        Args:
            mapping_provider: Provider for mapping rules (default: DefaultMappingProvider)
        """
        self.mapping_provider = mapping_provider or DefaultMappingProvider()
        self.mapping_rules = self.mapping_provider.get_mapping_rules()
        self.transformers = self._register_transformers()
        logger.debug("Initialized MappingEngine with mapping rules")
    
    def _register_transformers(self) -> Dict[str, Callable]:
        """
        Register transformation functions.
        
        Returns:
            Dictionary of transformation functions
        """
        return {
            "extract_file_path": self._transform_extract_file_path,
            "convert_to_array": self._transform_convert_to_array,
            "merge_properties": self._transform_merge_properties,
            "format_dataflow_id": self._transform_format_dataflow_id,
            "format_dataflow_group": self._transform_format_dataflow_group,
            "lowercase_transform": self._transform_lowercase,
            "format_dqe_path": self._transform_format_dqe_path
        }
    
    def _transform_extract_file_path(self, value: str) -> str:
        """
        Extract filename from a file path.
        
        Args:
            value: File path
            
        Returns:
            Extracted filename
        """
        if not value:
            return ""
        
        # Handle Workspace paths
        if "file:///Workspace" in value:
            parts = value.split("/")
            return f"./{parts[-1]}"
        
        # Handle dbfs paths
        if "{dbfs_path}" in value:
            parts = value.split("/")
            return f"./{parts[-1]}"
        
        # Default: just return the path as is
        return value
    
    def _transform_format_dataflow_id(self, value: str) -> str:
        """
        Format dataflow ID to match the expected convention.
        
        Args:
            value: Original dataflow ID
            
        Returns:
            Formatted dataflow ID
        """
        if not value:
            return ""
        
        # Format as maximo_table_name
        if value.isdigit():
            table_name = ""
            if hasattr(self, 'current_source_config') and self.current_source_config:
                if self._determine_data_level(self.current_source_config) == "bronze":
                    table_name = self.current_source_config.get('bronze_table', '')
                else:
                    table_name = self.current_source_config.get('silver_table', '')
            
            if table_name:
                return f"maximo_{table_name}"
            return f"maximo_item_{value}"
        
        return value
    
    def _transform_format_dataflow_group(self, value: str) -> str:
        """
        Format dataflow group to match the expected convention.
        
        Args:
            value: Original dataflow group
            
        Returns:
            Formatted dataflow group
        """
        if not value:
            return "maximo_bronze"
        
        # Determine if this is bronze or silver
        data_level = "bronze"
        if hasattr(self, 'current_source_config') and self.current_source_config:
            data_level = self._determine_data_level(self.current_source_config)
        
        if data_level == "bronze":
            return "maximo_bronze"
        else:
            return "silver_maximo"
    
    def _transform_lowercase(self, value: str) -> str:
        """
        Convert a string to lowercase.
        
        Args:
            value: Original string
            
        Returns:
            Lowercase string
        """
        if not value:
            return ""
        
        return value.lower()
    
    def _transform_format_dqe_path(self, value: str) -> str:
        """
        Format data quality expectations path.
        
        Args:
            value: Original path
            
        Returns:
            Formatted path
        """
        if not value:
            return ""
        
        # Extract the table name from the source config
        table_name = ""
        if hasattr(self, 'current_source_config') and self.current_source_config:
            table_name = self.current_source_config.get('bronze_table', '')
        
        if table_name:
            return f"./maximo_{table_name}_dqe.json"
        
        # Default: extract filename from path
        return self._transform_extract_file_path(value)
    
    def _transform_convert_to_array(self, value: Any) -> List[Any]:
        """
        Convert a value to an array if it's not already.
        
        Args:
            value: Value to convert
            
        Returns:
            Value as an array
        """
        if isinstance(value, list):
            return value
        elif value:
            return [value]
        else:
            return []
    
    def _transform_merge_properties(self, value: Dict[str, Any], 
                                   target_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Merge properties from value into target_dict.
        
        Args:
            value: Source dictionary
            target_dict: Target dictionary
            
        Returns:
            Merged dictionary
        """
        if not isinstance(value, dict):
            return target_dict
        
        result = target_dict.copy()
        for k, v in value.items():
            result[k] = v
        
        return result
    
    def transform_config(self, source_config: Dict[str, Any], force_level: Optional[str] = None) -> Dict[str, Any]:
        """
        Transform a source configuration to the target format.
        
        Args:
            source_config: Source configuration
            force_level: Force the data level to "bronze" or "silver" (optional)
            
        Returns:
            Transformed configuration
        """
        logger.info("Transforming configuration")
        target_config = {}
        
        # Store the source config for use in transformers
        self.current_source_config = source_config
        
        # Determine if this is bronze or silver
        data_level = force_level if force_level else self._determine_data_level(source_config)
        
        # Set default values
        self._set_default_values(target_config, data_level)
        
        # Apply mapping rules
        for source_key, rule in self.mapping_rules.items():
            if source_key in source_config:
                self._apply_mapping_rule(source_config, target_config, source_key, rule)
        
        # Handle special cases
        self._handle_special_cases(source_config, target_config, data_level)
        
        # Clean up the reference to source config
        self.current_source_config = None
        
        logger.info("Configuration transformation completed")
        return target_config
    
    def _set_default_values(self, config: Dict[str, Any], data_level: str) -> None:
        """
        Set default values in the target configuration.
        
        Args:
            config: Target configuration to modify
            data_level: Data level (bronze or silver)
        """
        # Set standard defaults
        config["dataFlowType"] = "standard"
        config["mode"] = "stream"
        config["targetFormat"] = "delta"
        
        # Initialize empty objects for common sections
        config["sourceDetails"] = {}
        config["targetDetails"] = {}
        
        # Set data level specific defaults
        if data_level == "silver":
            config["sourceType"] = "delta"
            config["sourceDetails"]["cdfEnabled"] = True
        
        logger.debug(f"Set default values in target configuration for {data_level} level")
    
    def _apply_mapping_rule(self, 
                           source_config: Dict[str, Any],
                           target_config: Dict[str, Any],
                           source_key: str,
                           rule: Any) -> None:
        """
        Apply a mapping rule to transform a field.
        
        Args:
            source_config: Source configuration
            target_config: Target configuration to modify
            source_key: Key in the source configuration
            rule: Mapping rule to apply
        """
        source_value = source_config.get(source_key)
        
        # Skip if source value is None or empty string or "N/A"
        if source_value is None or source_value == "" or source_value == "N/A":
            return
        
        # Handle string rules (direct mapping)
        if isinstance(rule, str):
            JsonUtils.set_nested_value(target_config, rule, source_value)
            logger.debug(f"Applied direct mapping: {source_key} -> {rule}")
            return
        
        # Handle dictionary rules
        if isinstance(rule, dict):
            rule_type = rule.get("type", "direct")
            target = rule.get("target")
            
            if rule_type == "direct":
                # Simple field mapping
                JsonUtils.set_nested_value(target_config, target, source_value)
                logger.debug(f"Applied direct mapping: {source_key} -> {target}")
            
            elif rule_type == "transform":
                # Apply a transformation function
                transform_name = rule.get("transform")
                if transform_name in self.transformers:
                    transformed_value = self.transformers[transform_name](source_value)
                    if transformed_value is not None and transformed_value != "":
                        JsonUtils.set_nested_value(target_config, target, transformed_value)
                        logger.debug(f"Applied transformation {transform_name}: {source_key} -> {target}")
            
            elif rule_type == "object":
                # Map a dictionary to a new structure
                if isinstance(source_value, dict):
                    if rule.get("direct_copy", False):
                        # Direct copy of all properties
                        JsonUtils.set_nested_value(target_config, target, source_value)
                        logger.debug(f"Applied direct object copy: {source_key} -> {target}")
                    else:
                        # Apply nested mappings
                        mappings = rule.get("mappings", {})
                        for src_nested_key, nested_rule in mappings.items():
                            if src_nested_key in source_value and source_value[src_nested_key] != "N/A":
                                nested_target = nested_rule.get("target")
                                full_target = f"{target}.{nested_target}" if nested_target else target
                                JsonUtils.set_nested_value(
                                    target_config, 
                                    full_target, 
                                    source_value[src_nested_key]
                                )
                                logger.debug(f"Applied nested mapping: {source_key}.{src_nested_key} -> {full_target}")
            
            elif rule_type == "conditional":
                # Apply conditional mapping
                condition = rule.get("condition")
                if condition == "value_exists" and source_value:
                    JsonUtils.set_nested_value(target_config, target, rule.get("value"))
                    logger.debug(f"Applied conditional mapping: {source_key} -> {target}")
    
    def _handle_special_cases(self, 
                             source_config: Dict[str, Any],
                             target_config: Dict[str, Any],
                             data_level: str) -> None:
        """
        Handle special cases that don't fit the standard mapping rules.
        
        Args:
            source_config: Source configuration
            target_config: Target configuration to modify
            data_level: Data level (bronze or silver)
        """
        logger.debug(f"Handling special cases for {data_level} level")
        
        # Format the dataFlowId based on table name
        table_name = None
        if data_level == "bronze":
            table_name = source_config.get('bronze_table', '')
        else:
            table_name = source_config.get('silver_table', '')
            
            # If silver table is N/A, use bronze table name instead
            if table_name == "N/A":
                table_name = source_config.get('bronze_table', '')
        
        if table_name and table_name != "N/A":
            target_config["dataFlowId"] = f"maximo_{table_name}"
        
        # Set the correct dataFlowGroup based on level
        target_config["dataFlowGroup"] = "bronze_maximo" if data_level == "bronze" else "silver_maximo"
        
        # Handle table properties
        if "targetDetails" in target_config and "tableProperties" not in target_config["targetDetails"]:
            target_config["targetDetails"]["tableProperties"] = {
                "delta.enableChangeDataFeed": "true"
            }
        
        # Handle schema path
        if "targetDetails" in target_config and "table" in target_config["targetDetails"]:
            table_value = target_config["targetDetails"]["table"]
            
            # Skip if table is N/A
            if table_value != "N/A":
                schema_file = f"maximo_{table_value}_schema.json"
                target_config["targetDetails"]["schemaPath"] = schema_file
        
        # Handle data quality expectations for bronze level
        if data_level == "bronze":
            target_config["dataQualityExpectationsEnabled"] = False
            if "dataQualityExpectationsPath" not in target_config and table_name and table_name != "N/A":
                target_config["dataQualityExpectationsPath"] = f"./maximo_{table_name}_dqe.json"
            
            # Remove fields that shouldn't be in bronze level
            if "cdcSettings" in target_config:
                del target_config["cdcSettings"]
            
            # Add sourceViewName for bronze level
            if table_name and table_name != "N/A":
                target_config["sourceViewName"] = f"v_{table_name}"
            
            # Remove database and table from sourceDetails for bronze level
            if "sourceDetails" in target_config:
                if "database" in target_config["sourceDetails"]:
                    del target_config["sourceDetails"]["database"]
                if "table" in target_config["sourceDetails"]:
                    del target_config["sourceDetails"]["table"]
        
        # Handle silver specific cases
        if data_level == "silver":
            # Set source details for silver level
            if "sourceDetails" in target_config:
                # Remove path and readerOptions for silver
                if "path" in target_config["sourceDetails"]:
                    del target_config["sourceDetails"]["path"]
                if "readerOptions" in target_config["sourceDetails"]:
                    del target_config["sourceDetails"]["readerOptions"]
                
                # Set database for silver
                if "database" not in target_config["sourceDetails"]:
                    target_config["sourceDetails"]["database"] = f"edp_bronze_dev.maximo_lakeflow"
                elif target_config["sourceDetails"]["database"] == "N/A":
                    target_config["sourceDetails"]["database"] = f"edp_bronze_dev.maximo_lakeflow"
                
                # Add cdfEnabled flag
                target_config["sourceDetails"]["cdfEnabled"] = True
            
            # Convert cdcApplyChanges to cdcSettings
            if "cdcApplyChanges" in target_config:
                target_config["cdcSettings"] = target_config["cdcApplyChanges"]
                del target_config["cdcApplyChanges"]
                
                # Add ignore_null_updates if not present
                if "cdcSettings" in target_config and "ignore_null_updates" not in target_config["cdcSettings"]:
                    target_config["cdcSettings"]["ignore_null_updates"] = False
                    
                # Update sequence_by to ensure it's wrapped in timestamp() function
                if "cdcSettings" in target_config and "sequence_by" in target_config["cdcSettings"]:
                    seq_by_value = target_config["cdcSettings"]["sequence_by"]
                    if seq_by_value == "DMS_CDC_TIMESTAMP":
                        target_config["cdcSettings"]["sequence_by"] = "timestamp(DMS_CDC_TIMESTAMP)"
                    elif not seq_by_value.startswith("timestamp("):
                        # If it's not already wrapped in timestamp(), wrap it
                        target_config["cdcSettings"]["sequence_by"] = f"timestamp({seq_by_value})"
            
            # Skip generating silver config if silver_table is N/A
            if "targetDetails" in target_config and "table" in target_config["targetDetails"]:
                if target_config["targetDetails"]["table"] == "N/A":
                    # Use bronze table name instead
                    bronze_table = source_config.get('bronze_table', '')
                    if bronze_table and bronze_table != "N/A":
                        target_config["targetDetails"]["table"] = bronze_table
                        
                        # Update schema path
                        if "schemaPath" in target_config["targetDetails"]:
                            target_config["targetDetails"]["schemaPath"] = f"maximo_{bronze_table}_schema.json"
            
            # Remove sourceViewName from silver level
            if "sourceViewName" in target_config:
                del target_config["sourceViewName"]
        
        # Remove path from targetDetails if it's "Ignore" or "ignore"
        if "targetDetails" in target_config and "path" in target_config["targetDetails"]:
            path_value = target_config["targetDetails"]["path"]
            if path_value == "Ignore" or path_value == "ignore":
                del target_config["targetDetails"]["path"]
    
    def _determine_data_level(self, config: Dict[str, Any]) -> str:
        """
        Determine if this is a bronze or silver level configuration.
        
        Args:
            config: Source configuration
            
        Returns:
            Data level: "bronze", "silver", or "unknown"
        """
        # Check if silver_database_dev is not "N/A" and silver_table is not "N/A"
        if (config.get("silver_database_dev") and config.get("silver_database_dev") != "N/A" and 
            config.get("silver_table") and config.get("silver_table") != "N/A"):
            return "silver"
        else:
            return "bronze"
