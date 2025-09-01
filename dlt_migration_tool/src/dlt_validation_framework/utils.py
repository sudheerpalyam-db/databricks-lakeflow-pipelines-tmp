"""
Utility functions for the DLT Validation Framework.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, sha2, concat_ws
import json

def get_table_primary_keys(catalog, schema, table, default_keys=None):
    """
    Attempt to determine primary keys for a table.
    
    First tries to get primary keys from table metadata.
    If not available, falls back to default keys.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table: Table name
        default_keys: Default primary keys to use if none found
        
    Returns:
        List of primary key column names
    """
    spark = SparkSession.builder.getOrCreate()
    
    # Try to get primary keys from table metadata
    try:
        # This approach may vary depending on your specific metadata storage
        metadata_query = f"""
        SELECT column_name 
        FROM {catalog}.information_schema.columns
        WHERE table_schema = '{schema}'
          AND table_name = '{table}'
          AND is_primary_key = 'YES'
        """
        
        pk_df = spark.sql(metadata_query)
        
        if pk_df.count() > 0:
            return [row.column_name for row in pk_df.collect()]
    except:
        # If query fails, continue to fallback
        pass
    
    # Fallback: Check for common primary key column names
    try:
        columns = spark.table(f"{catalog}.{schema}.{table}").columns
        common_pk_names = ['id', 'pk', f'{table}_id', f'{table}_key']
        
        for pk_name in common_pk_names:
            if pk_name in columns:
                return [pk_name]
    except:
        pass
    
    # Return default keys if provided, otherwise empty list
    return default_keys if default_keys else []

def get_all_tables_in_schema(catalog, schema):
    """
    Get all tables in a schema.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        
    Returns:
        List of table names
    """
    spark = SparkSession.builder.getOrCreate()
    tables = spark.sql(f"SHOW TABLES FROM {catalog}.{schema}")
    return [row.tableName for row in tables.collect()]

def save_validation_config(config_dict, path):
    """
    Save validation configuration to a JSON file.
    
    Args:
        config_dict: Configuration dictionary
        path: File path to save to
        
    Returns:
        None
    """
    with open(path, 'w') as f:
        json.dump(config_dict, f, indent=2)
    
    print(f"Configuration saved to {path}")

def load_validation_config(path):
    """
    Load validation configuration from a JSON file.
    
    Args:
        path: File path to load from
        
    Returns:
        Configuration dictionary
    """
    with open(path, 'r') as f:
        config_dict = json.load(f)
    
    print(f"Configuration loaded from {path}")
    return config_dict

def compare_table_schemas(source_catalog, source_schema, source_table,
                         target_catalog, target_schema, target_table):
    """
    Compare the schemas of two tables.
    
    Args:
        source_catalog: Source catalog name
        source_schema: Source schema name
        source_table: Source table name
        target_catalog: Target catalog name
        target_schema: Target schema name
        target_table: Target table name
        
    Returns:
        Dictionary with schema comparison results
    """
    spark = SparkSession.builder.getOrCreate()
    
    # Get source and target schemas
    source_df = spark.table(f"{source_catalog}.{source_schema}.{source_table}")
    target_df = spark.table(f"{target_catalog}.{target_schema}.{target_table}")
    
    source_schema = source_df.schema
    target_schema = target_df.schema
    
    # Compare column names
    source_cols = set(source_df.columns)
    target_cols = set(target_df.columns)
    
    common_cols = source_cols.intersection(target_cols)
    source_only_cols = source_cols - target_cols
    target_only_cols = target_cols - source_cols
    
    # Compare data types for common columns
    type_mismatches = []
    for col_name in common_cols:
        source_type = next(field.dataType for field in source_schema.fields if field.name == col_name)
        target_type = next(field.dataType for field in target_schema.fields if field.name == col_name)
        
        if str(source_type) != str(target_type):
            type_mismatches.append({
                "column": col_name,
                "source_type": str(source_type),
                "target_type": str(target_type)
            })
    
    # Create result
    result = {
        "source_columns_count": len(source_cols),
        "target_columns_count": len(target_cols),
        "common_columns_count": len(common_cols),
        "source_only_columns": list(source_only_cols),
        "target_only_columns": list(target_only_cols),
        "type_mismatches": type_mismatches,
        "schema_identical": len(source_only_cols) == 0 and len(target_only_cols) == 0 and len(type_mismatches) == 0
    }
    
    return result
