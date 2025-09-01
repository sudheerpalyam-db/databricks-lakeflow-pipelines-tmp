"""
Core configuration transformer for DLT migration.
"""

import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional, List, Union, Tuple

# Use absolute imports instead of relative imports
from dlt_migration_tool.mapping.mapping_engine import MappingEngine, MappingProvider
from dlt_migration_tool.utils.json_utils import JsonUtils
from dlt_migration_tool.utils.file_utils import FileUtils

logger = logging.getLogger(__name__)


class ConfigTransformer:
    """
    Transforms DLT-meta configurations to DLT Framework dataflowspec format.
    """
    
    def __init__(self, mapping_engine: Optional[MappingEngine] = None):
        """
        Initialize the configuration transformer.
        
        Args:
            mapping_engine: Engine for mapping configurations (default: new MappingEngine)
        """
        self.mapping_engine = mapping_engine or MappingEngine()
        logger.debug("Initialized ConfigTransformer")
    
    def transform_config(self, source_config: Dict[str, Any], force_level: Optional[str] = None) -> Dict[str, Any]:
        """
        Transform a single configuration from DLT-meta to dataflowspec format.
        
        Args:
            source_config: Source configuration in DLT-meta format
            force_level: Force the data level to "bronze" or "silver" (optional)
            
        Returns:
            Transformed configuration in dataflowspec format
        """
        logger.info("Transforming configuration")
        
        # Use the mapping engine to transform the configuration
        target_config = self.mapping_engine.transform_config(source_config, force_level)
        
        # Validate the transformed configuration
        self._validate_transformed_config(target_config)
        
        logger.info("Configuration transformation completed")
        return target_config
    
    def transform_configs(self, source_configs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Transform multiple configurations.
        
        Args:
            source_configs: List of source configurations
            
        Returns:
            List of transformed configurations
        """
        logger.info(f"Transforming {len(source_configs)} configurations")
        
        target_configs = []
        for i, config in enumerate(source_configs):
            logger.info(f"Transforming configuration {i+1}/{len(source_configs)}")
            target_config = self.transform_config(config)
            target_configs.append(target_config)
        
        logger.info(f"Completed transformation of {len(source_configs)} configurations")
        return target_configs
    
    def _validate_transformed_config(self, config: Dict[str, Any]) -> None:
        """
        Validate a transformed configuration.
        
        Args:
            config: Transformed configuration
            
        Raises:
            ValueError: If the configuration is invalid
        """
        # Check for required fields
        required_fields = ["dataFlowId", "sourceType", "targetDetails"]
        missing_fields = [field for field in required_fields if field not in config]
        
        if missing_fields:
            error_msg = f"Transformed configuration is missing required fields: {', '.join(missing_fields)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Check for target details required fields
        if "targetDetails" in config:
            target_details = config["targetDetails"]
            target_required_fields = ["table"]
            missing_target_fields = [field for field in target_required_fields if field not in target_details]
            
            if missing_target_fields:
                error_msg = f"targetDetails is missing required fields: {', '.join(missing_target_fields)}"
                logger.error(error_msg)
                raise ValueError(error_msg)
        
        logger.debug("Transformed configuration validation passed")


class MigrationProcessor:
    """
    Processes DLT-meta JSON files and converts them to DLT Framework dataflowspec format.
    """
    
    def __init__(
        self, 
        transformer: Optional[ConfigTransformer] = None,
        verbose: bool = False
    ):
        """
        Initialize the migration processor.
        
        Args:
            transformer: Configuration transformer (default: new ConfigTransformer)
            verbose: Whether to enable verbose logging
        """
        self.transformer = transformer or ConfigTransformer()
        self.verbose = verbose
        
        # Set up logging level based on verbosity
        if verbose:
            logger.setLevel(logging.DEBUG)
        
        logger.debug("Initialized MigrationProcessor")
    
    def process_file(
        self, 
        input_file: Union[str, Path], 
        output_dir: Union[str, Path]
    ) -> List[Path]:
        """
        Process a single DLT-meta JSON file.
        
        Args:
            input_file: Path to the input file
            output_dir: Directory to save output files
            
        Returns:
            List of paths to the generated output files
            
        Raises:
            FileNotFoundError: If the input file doesn't exist
            ValueError: If the input file contains invalid JSON
        """
        input_file = Path(input_file)
        output_dir = Path(output_dir)
        
        logger.info(f"Processing file: {input_file}")
        
        # Ensure output directory exists
        FileUtils.ensure_directory_exists(output_dir)
        
        # Read the input file
        source_data = FileUtils.read_json_file(input_file)
        
        # Get the base filename without extension for output naming
        base_filename = input_file.stem
        
        # Process based on whether it's a single object or an array
        output_files = []
        if isinstance(source_data, list):
            logger.info(f"Processing array with {len(source_data)} items")
            
            for i, item in enumerate(source_data):
                # Check if this is a Maximo pattern
                if self._is_maximo_pattern(item):
                    # Generate both bronze and silver configurations
                    bronze_config, silver_config = self._generate_maximo_configs(item)
                    
                    # Generate bronze output filename
                    bronze_output_file = FileUtils.generate_output_filename(
                        input_file, 
                        output_dir, 
                        f"item_{i}_bronze"
                    )
                    
                    # Generate silver output filename
                    silver_output_file = FileUtils.generate_output_filename(
                        input_file, 
                        output_dir, 
                        f"item_{i}_silver"
                    )
                    
                    # Write the output files
                    FileUtils.write_json_file(bronze_config, bronze_output_file)
                    FileUtils.write_json_file(silver_config, silver_output_file)
                    
                    output_files.append(bronze_output_file)
                    output_files.append(silver_output_file)
                    
                    logger.info(f"Wrote bronze output file: {bronze_output_file}")
                    logger.info(f"Wrote silver output file: {silver_output_file}")
                else:
                    # Standard transformation for non-Maximo patterns
                    target_config = self.transformer.transform_config(item)
                    
                    # Generate output filename
                    output_file = FileUtils.generate_output_filename(
                        input_file, 
                        output_dir, 
                        f"item_{i}"
                    )
                    
                    # Write the output file
                    FileUtils.write_json_file(target_config, output_file)
                    output_files.append(output_file)
                    
                    logger.info(f"Wrote output file: {output_file}")
        else:
            # Check if this is a Maximo pattern
            if self._is_maximo_pattern(source_data):
                # Generate both bronze and silver configurations
                bronze_config, silver_config = self._generate_maximo_configs(source_data)
                
                # Use the input filename as part of the output filename to ensure uniqueness
                bronze_output_file = output_dir / f"{base_filename}_bronze.json"
                silver_output_file = output_dir / f"{base_filename}_silver.json"
                
                # Write the output files
                FileUtils.write_json_file(bronze_config, bronze_output_file)
                FileUtils.write_json_file(silver_config, silver_output_file)
                
                output_files.append(bronze_output_file)
                output_files.append(silver_output_file)
                
                logger.info(f"Wrote bronze output file: {bronze_output_file}")
                logger.info(f"Wrote silver output file: {silver_output_file}")
            else:
                # Standard transformation for non-Maximo patterns
                target_config = self.transformer.transform_config(source_data)
                
                # Generate output filename
                output_file = FileUtils.generate_output_filename(input_file, output_dir)
                
                # Write the output file
                FileUtils.write_json_file(target_config, output_file)
                output_files.append(output_file)
                
                logger.info(f"Wrote output file: {output_file}")
        
        return output_files
        
    def _is_maximo_pattern(self, config: Dict[str, Any]) -> bool:
        """
        Check if a configuration follows the Maximo pattern.
        
        Args:
            config: Configuration to check
            
        Returns:
            True if this is a Maximo pattern, False otherwise
        """
        # Check if this is a Maximo pattern based on source system and required fields
        is_maximo = (
            config.get("source_system") == "Maximo" or 
            "Maximo" in config.get("data_flow_group", "") or
            "maximo" in config.get("source_system", "").lower()
        )
        
        has_bronze_fields = (
            "bronze_table" in config and 
            "source_format" in config
        )
        
        # For Maximo pattern, we only need bronze fields as the minimum requirement
        # We'll always generate both bronze and silver configs for Maximo pattern
        return is_maximo and has_bronze_fields
    
    def _generate_maximo_configs(self, source_config: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
        """
        Generate both bronze and silver configurations for a Maximo pattern.
        
        Args:
            source_config: Source configuration
            
        Returns:
            Tuple of (bronze_config, silver_config)
        """
        # Create a copy of the source config for bronze
        bronze_source = source_config.copy()
        
        # Create a copy of the source config for silver
        silver_source = source_config.copy()
        
        # Force the data level for each config
        bronze_config = self.transformer.transform_config(bronze_source, force_level="bronze")
        silver_config = self.transformer.transform_config(silver_source, force_level="silver")
        
        return bronze_config, silver_config
    
    def process_directory(
        self, 
        input_dir: Union[str, Path], 
        output_dir: Union[str, Path],
        recursive: bool = True
    ) -> Dict[Path, List[Path]]:
        """
        Process all JSON files in a directory.
        
        Args:
            input_dir: Directory containing input files
            output_dir: Directory to save output files
            recursive: Whether to process subdirectories recursively
            
        Returns:
            Dictionary mapping input files to their output files
            
        Raises:
            FileNotFoundError: If the input directory doesn't exist
        """
        input_dir = Path(input_dir)
        output_dir = Path(output_dir)
        
        logger.info(f"Processing directory: {input_dir}")
        
        # Find all JSON files
        pattern = "**/*.json" if recursive else "*.json"
        json_files = list(input_dir.glob(pattern))
        
        if not json_files:
            logger.warning(f"No JSON files found in {input_dir}")
            return {}
        
        logger.info(f"Found {len(json_files)} JSON files to process")
        
        # Process each file
        results = {}
        for input_file in json_files:
            try:
                # Calculate relative path to maintain directory structure
                rel_path = input_file.relative_to(input_dir)
                file_output_dir = output_dir / rel_path.parent
                
                # Process the file
                output_files = self.process_file(input_file, file_output_dir)
                results[input_file] = output_files
            except Exception as e:
                logger.error(f"Error processing {input_file}: {str(e)}")
                results[input_file] = []
        
        logger.info(f"Processed {len(results)} files")
        return results
