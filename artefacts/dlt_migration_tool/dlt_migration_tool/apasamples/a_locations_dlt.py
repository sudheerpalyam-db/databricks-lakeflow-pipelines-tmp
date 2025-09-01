import dlt
from pyspark.sql.functions import col, expr, current_timestamp
from pyspark.sql.types import *

# Define the DLT pipeline for Maximo A_LOCATIONS
@dlt.table(
    name="asset",
    comment="Bronze table for Maximo asset data",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    },
    temporary=False,
    partition_cols=None,
    path=None,  # Will use default path
    schema=None,  # Will infer schema
)
@dlt.expect_all_or_drop({"valid_data": "value IS NOT NULL"})
def maximo_a_locations_bronze():
    """
    Read Maximo A_LOCATIONS data from S3 using Autoloader and write to bronze table
    """
    # Define options for Autoloader
    options = {
        "cloudFiles.format": "parquet",
        "cloudFiles.inferColumnTypes": "true",
        "cloudFiles.rescuedDataColumn": "_rescued_data",
        "header": "true"
    }
    
    # Read from S3 using Autoloader
    return (
        spark.readStream
        .format("cloudFiles")
        .options(**options)
        .load("s3://s3-staging-maximo-dev-apse2-058264204922/internal/MAXIMO/A_LOCATIONS")
        .withColumn("_ingest_timestamp", current_timestamp())
    )


# Define the silver table that reads from the bronze table
@dlt.table(
    name="a_locations",
    comment="Silver table for Maximo a_locations data",
    table_properties={
        "delta.enableChangeDataFeed": "true"
    },
    temporary=False,
    partition_cols=None,
    path=None,  # Will use default path
)
def maximo_a_locations_silver():
    """
    Read from bronze table and apply transformations for silver layer
    """
    # Read from bronze table
    bronze_df = dlt.read("maximo_a_locations_bronze")
    
    # Apply transformations
    # Note: In a real implementation, you would apply more specific transformations
    # based on your business requirements
    return (
        bronze_df
        .withColumn("processed_timestamp", current_timestamp())
        # Add additional transformations as needed
        # For example, data cleansing, type conversions, etc.
    )