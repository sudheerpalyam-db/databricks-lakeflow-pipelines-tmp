"""
Workday Parser Module

This module provides functionality to parse Workday PySpark files and extract CDC logic
for use in the DLT Migration Tool.
"""

import os
import re
import logging
import base64

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WorkdayTableInfo:
    """Class to store Workday table information extracted from PySpark files"""
    
    def __init__(self, table_name):
        self.table_name = table_name
        self.bronze_table_name = None
        self.bronze_table = None
        self.bronze_columns = []
        self.landing_parquet_path = None
        
        self.silver_table_name = None
        self.silver_full_table = None
        self.silver_merge_key = None
        self.silver_merge_key_condition = None
        self.silver_final_merge_key_condition = None
        self.silver_columns = []
        self.silver_columns_values = []
        self.silver_match_condition = None
    
    def __str__(self):
        return (f"Table: {self.table_name}\n"
                f"Bronze Table: {self.bronze_table_name}\n"
                f"Landing Parquet Path: {self.landing_parquet_path}\n"
                f"Silver Table: {self.silver_table_name}\n"
                f"Silver Merge Key: {self.silver_merge_key}\n"
                f"Silver Columns: {', '.join(self.silver_columns)}\n"
                f"Silver Match Condition: {self.silver_match_condition}")


def extract_variable_value(content, variable_name):
    """
    Extract the value of a variable from PySpark file content.
    
    Args:
        content (str): The content of the PySpark file
        variable_name (str): The name of the variable to extract
        
    Returns:
        str: The value of the variable, or None if not found
    """
    # Pattern to match variable assignment
    pattern = rf"{variable_name}\s*=\s*['\"]([^'\"]*)['\"]"
    match = re.search(pattern, content)
    
    if match:
        return match.group(1)
    
    return None


def extract_list_variable(content, variable_name):
    """
    Extract a comma-separated list variable from PySpark file content.
    
    Args:
        content (str): The content of the PySpark file
        variable_name (str): The name of the variable to extract
        
    Returns:
        list: The list of values, or empty list if not found
    """
    # Pattern to match list variable assignment
    pattern = rf"{variable_name}\s*=\s*['\"]([^'\"]*)['\"]"
    match = re.search(pattern, content)
    
    if match:
        values = match.group(1).split(',')
        return [value.strip() for value in values]
    
    return []


def parse_workday_file(file_path):
    """
    Parse a Workday PySpark file to extract CDC logic and table information.
    
    Args:
        file_path (str): Path to the PySpark file
        
    Returns:
        WorkdayTableInfo: Object containing extracted information
    """
    try:
        # Extract table name from file path
        file_name = os.path.basename(file_path)
        table_name = os.path.splitext(file_name)[0].lower()
        
        table_info = WorkdayTableInfo(table_name)
        
        with open(file_path, 'r') as f:
            content = f.read()
        
        # Extract bronze table information
        table_info.bronze_table_name = extract_variable_value(content, "bronze_table_name")
        table_info.bronze_table = extract_variable_value(content, "bronze_table")
        table_info.bronze_columns = extract_list_variable(content, "bronze_columns")
        
        # Extract silver table information
        table_info.silver_table_name = extract_variable_value(content, "silver_table1")
        table_info.silver_full_table = extract_variable_value(content, "silver_full_table1")
        table_info.silver_merge_key = extract_variable_value(content, "silvermergekey1")
        table_info.silver_merge_key_condition = extract_variable_value(content, "silvermergekeycondition1")
        table_info.silver_final_merge_key_condition = extract_variable_value(content, "silverfinalmergekeycondition1")
        table_info.silver_columns = extract_list_variable(content, "silvercolumns1")
        table_info.silver_columns_values = extract_list_variable(content, "silvercolumns1_values")
        table_info.silver_match_condition = extract_variable_value(content, "silvermatchcondition1")
        
        return table_info
    
    except Exception as e:
        logger.error(f"Error parsing file {file_path}: {str(e)}")
        return None


def find_workday_files(directory):
    """
    Find all Workday PySpark files in a directory.
    
    Args:
        directory (str): Directory to search
        
    Returns:
        list: List of file paths
    """
    workday_files = []
    
    try:
        for file in os.listdir(directory):
            file_path = os.path.join(directory, file)
            if os.path.isfile(file_path) and file.endswith('.py') and not file.startswith('('):
                workday_files.append(file_path)
    except Exception as e:
        logger.error(f"Error finding Workday files in {directory}: {str(e)}")
    
    return workday_files


def read_databricks_workspace_file(file_path):
    """
    Read a file from Databricks workspace using dbutils.
    
    Args:
        file_path (str): Workspace path to the file
        
    Returns:
        str: Content of the file
    """
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        
        # Read the file content
        content = dbutils.workspace.get(file_path)
        return content
    except Exception as e:
        logger.error(f"Error reading workspace file {file_path}: {str(e)}")
        return None


def read_volume_file(file_path):
    """
    Read a file from Unity Catalog volume using dbutils.
    
    Args:
        file_path (str): Volume path to the file (e.g., /Volumes/catalog/schema/volume/file.py)
        
    Returns:
        str: Content of the file
    """
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        
        # Read the file content
        content = dbutils.fs.head(file_path)
        return content
    except Exception as e:
        logger.error(f"Error reading volume file {file_path}: {str(e)}")
        return None


def parse_workday_workspace_file(file_path):
    """
    Parse a Workday PySpark file from Databricks workspace to extract CDC logic.
    
    Args:
        file_path (str): Workspace path to the PySpark file
        
    Returns:
        WorkdayTableInfo: Object containing extracted information
    """
    try:
        # Extract table name from file path
        file_name = os.path.basename(file_path)
        table_name = os.path.splitext(file_name)[0].lower()
        
        table_info = WorkdayTableInfo(table_name)
        
        # Read the file content from workspace
        content = read_databricks_workspace_file(file_path)
        if not content:
            logger.error(f"Could not read content from {file_path}")
            return None
        
        # Extract bronze table information
        table_info.bronze_table_name = extract_variable_value(content, "bronze_table_name")
        table_info.bronze_table = extract_variable_value(content, "bronze_table")
        table_info.bronze_columns = extract_list_variable(content, "bronze_columns")
        
        # Extract silver table information
        table_info.silver_table_name = extract_variable_value(content, "silver_table1")
        table_info.silver_full_table = extract_variable_value(content, "silver_full_table1")
        table_info.silver_merge_key = extract_variable_value(content, "silvermergekey1")
        table_info.silver_merge_key_condition = extract_variable_value(content, "silvermergekeycondition1")
        table_info.silver_final_merge_key_condition = extract_variable_value(content, "silverfinalmergekeycondition1")
        table_info.silver_columns = extract_list_variable(content, "silvercolumns1")
        table_info.silver_columns_values = extract_list_variable(content, "silvercolumns1_values")
        table_info.silver_match_condition = extract_variable_value(content, "silvermatchcondition1")
        
        return table_info
    
    except Exception as e:
        logger.error(f"Error parsing workspace file {file_path}: {str(e)}")
        return None


def find_workday_workspace_files(workspace_path):
    """
    Find all Workday PySpark files in a Databricks workspace directory.
    
    Args:
        workspace_path (str): Workspace directory to search
        
    Returns:
        list: List of file paths
    """
    workday_files = []
    
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        
        # List files in the workspace directory
        files = dbutils.workspace.ls(workspace_path)
        
        # Filter for Python files
        for file_info in files:
            if file_info.path.endswith('.py') and not os.path.basename(file_info.path).startswith('('):
                workday_files.append(file_info.path)
    except Exception as e:
        logger.error(f"Error finding Workday files in workspace {workspace_path}: {str(e)}")
    
    return workday_files


def parse_workday_volume_file(file_path):
    """
    Parse a Workday PySpark file from Unity Catalog volume to extract CDC logic.
    
    Args:
        file_path (str): Volume path to the PySpark file
        
    Returns:
        WorkdayTableInfo: Object containing extracted information
    """
    try:
        # Read the file content from volume
        content = read_volume_file(file_path)
        if not content:
            logger.error(f"Could not read content from {file_path}")
            return None
        
        # Extract bronze table name from content
        bronze_table_name = extract_variable_value(content, "bronze_table_name")
        if not bronze_table_name:
            logger.error(f"No bronze_table_name found in {file_path}")
            # Fall back to filename-based approach
            file_name = os.path.basename(file_path)
            table_name = os.path.splitext(file_name)[0].lower()
            logger.info(f"Using filename as table name: {table_name}")
        else:
            # Use the bronze_table_name as the table name
            table_name = bronze_table_name.lower()
            logger.info(f"Using bronze_table_name as table name: {table_name}")
        
        table_info = WorkdayTableInfo(table_name)
        
        # Set bronze table information
        table_info.bronze_table_name = bronze_table_name
        table_info.bronze_table = extract_variable_value(content, "bronze_table")
        table_info.bronze_columns = extract_list_variable(content, "bronze_columns")
        table_info.landing_parquet_path = extract_variable_value(content, "landing_parquet_path")
        
        # Extract silver table information
        table_info.silver_table_name = extract_variable_value(content, "silver_table1")
        table_info.silver_full_table = extract_variable_value(content, "silver_full_table1")
        table_info.silver_merge_key = extract_variable_value(content, "silvermergekey1")
        table_info.silver_merge_key_condition = extract_variable_value(content, "silvermergekeycondition1")
        table_info.silver_final_merge_key_condition = extract_variable_value(content, "silverfinalmergekeycondition1")
        table_info.silver_columns = extract_list_variable(content, "silvercolumns1")
        table_info.silver_columns_values = extract_list_variable(content, "silvercolumns1_values")
        table_info.silver_match_condition = extract_variable_value(content, "silvermatchcondition1")
        
        return table_info
    
    except Exception as e:
        logger.error(f"Error parsing volume file {file_path}: {str(e)}")
        return None


def find_workday_volume_files(volume_path):
    """
    Find all Workday PySpark files in a Unity Catalog volume directory recursively.
    
    Args:
        volume_path (str): Volume directory to search
        
    Returns:
        list: List of file paths
    """
    workday_files = []
    
    try:
        import IPython
        dbutils = IPython.get_ipython().user_ns["dbutils"]
        
        def list_files_recursively(path):
            """Helper function to recursively list files in a directory"""
            result = []
            try:
                # List files in the current directory
                files = dbutils.fs.ls(path)
                
                for file_info in files:
                    # Check if it's a Python file
                    if file_info.path.endswith('.py') and not os.path.basename(file_info.path).startswith('('):
                        result.append(file_info.path)
                    # If it's a directory, recurse into it
                    elif file_info.isDir():
                        # Ensure path ends with slash for directories
                        dir_path = file_info.path
                        if not dir_path.endswith('/'):
                            dir_path += '/'
                        # Recursively get files from subdirectory
                        result.extend(list_files_recursively(dir_path))
            except Exception as e:
                logger.error(f"Error listing files in {path}: {str(e)}")
            
            return result
        
        # Start the recursive search
        logger.info(f"Starting recursive search for Python files in {volume_path}")
        workday_files = list_files_recursively(volume_path)
        logger.info(f"Found {len(workday_files)} Python files in {volume_path} and its subdirectories")
        
    except Exception as e:
        logger.error(f"Error finding Workday files in volume {volume_path}: {str(e)}")
    
    return workday_files


def generate_bronze_config(table_info, bronze_catalog, bronze_target_schema, s3_bucket=None):
    """
    Generate bronze dataflow spec configuration for a table.
    
    Args:
        table_info (WorkdayTableInfo): Table information
        bronze_catalog (str): Bronze catalog name
        bronze_target_schema (str): Bronze target schema name
        s3_bucket (str, optional): S3 bucket name to use. Defaults to None.
        
    Returns:
        dict: Bronze configuration
    """
    # Always use landing_parquet_path from the source code for sourceDetails.path
    if table_info.landing_parquet_path:
        # Extract the directory path by removing the .parquet extension and keeping the directory
        landing_parquet_path = table_info.landing_parquet_path
        if landing_parquet_path.endswith('.parquet'):
            source_path = landing_parquet_path[:-8]  # Remove the '.parquet' extension
        else:
            source_path = landing_parquet_path
    else:
        # Use the provided S3 bucket or a default one
        bucket = s3_bucket or "s3-staging-workday-dev-apse2-058264204922"
        source_path = f"s3://{bucket}/Finance/{table_info.table_name.title()}/"
    
    return {
        "dataFlowId": f"workday_{table_info.bronze_table_name}",
        "dataFlowGroup": "bronze_workday",
        "dataFlowType": "standard",
        "sourceSystem": "workday",
        "sourceType": "cloudFiles",
        "sourceViewName": f"v_{table_info.bronze_table_name}",
        "sourceDetails": {
            "path": source_path,
            "readerOptions": {
                "cloudFiles.format": "parquet",
                "cloudFiles.inferColumnTypes": "true",
                "cloudFiles.rescuedDataColumn": "_rescued_data",
                "header": "true"
            }
        },
        "mode": "stream",
        "targetFormat": "delta",
        "targetDetails": {
            "database": f"{bronze_catalog}.{bronze_target_schema}",
            "table": table_info.bronze_table_name,
            # "schemaPath": f"workday_{table_info.bronze_table_name}_schema.json",
            "tableProperties": {
                "delta.enableChangeDataFeed": "true"
            }
        },
        "dataQualityExpectationsEnabled": False
    }


def generate_silver_config(table_info, bronze_catalog, bronze_target_schema, silver_catalog, silver_schema):
    """
    Generate silver dataflow spec configuration for a table.
    
    Args:
        table_info (WorkdayTableInfo): Table information
        bronze_catalog (str): Bronze catalog name
        bronze_target_schema (str): Bronze target schema name
        silver_catalog (str): Silver catalog name
        silver_schema (str): Silver schema name
        
    Returns:
        dict: Silver configuration
    """
    # Extract the column names for the match condition
    match_columns = []
    if table_info.silver_match_condition:
        # Extract column names from the match condition
        pattern = r's\.(\w+)\s+<>'
        matches = re.findall(pattern, table_info.silver_match_condition)
        match_columns = matches
    
    return {
        "dataFlowId": f"workday_{table_info.bronze_table_name}_silver",
        "dataFlowGroup": "silver_workday",
        "dataFlowType": "silver",
        "sourceSystem": "workday",
        "sourceType": "delta",
        "sourceViewName": f"v_{table_info.bronze_table_name}",
        "sourceDetails": {
            "database": f"{bronze_catalog}.{bronze_target_schema}",
            "table": table_info.bronze_table_name,
            "cdfEnabled": True
        },
        "mode": "stream",
        "targetFormat": "delta",
        "targetDetails": {
            "database": f"{silver_catalog}.{silver_schema}",
            "table": table_info.bronze_table_name,
            # "schemaPath": f"workday_{table_info.bronze_table_name}_schema.json"
        },
        "cdcSettings": {
            "ignore_null_updates": False,
            "primary_keys": [table_info.silver_merge_key] if table_info.silver_merge_key else [],
            "tracked_columns": match_columns,
            "sequence_by": "timestamp(load_datetime)",
            "scd_type": "2",
            "except_column_list": ["load_datetime"]
        }
    }
