"""
Validation module for DLT Validation Framework.
This module contains the core validation logic for comparing source and target tables.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, lit, sha2, concat_ws, collect_list, to_json, struct, when,
    xxhash64, array, map_keys, map_values, expr, coalesce
)
from pyspark.sql.types import StructType
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DLTValidator")

class TableValidator:
    """Class to validate data between source and target tables."""
    
    def __init__(self, config):
        """Initialize with the validation configuration."""
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()
        logger.info("TableValidator initialized with configuration")
    
    def validate_row_count(self, source_catalog, source_schema, source_table, 
                          target_catalog, target_schema, target_table):
        """
        Validate that the row counts match between source and target tables.
        
        Args:
            source_catalog: Source catalog name
            source_schema: Source schema name
            source_table: Source table name
            target_catalog: Target catalog name
            target_schema: Target schema name
            target_table: Target table name
            
        Returns:
            Dictionary with validation results
        """
        start_time = time.time()
        logger.info(f"Starting row count validation for {source_catalog}.{source_schema}.{source_table} vs {target_catalog}.{target_schema}.{target_table}")
        
        # Get source and target counts
        source_path = f"{source_catalog}.{source_schema}.{source_table}"
        target_path = f"{target_catalog}.{target_schema}.{target_table}"
        
        try:
            logger.info(f"Counting rows in source table: {source_path}")
            source_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {source_path}").collect()[0]['count']
            logger.info(f"Source count: {source_count}")
            
            logger.info(f"Counting rows in target table: {target_path}")
            target_count = self.spark.sql(f"SELECT COUNT(*) as count FROM {target_path}").collect()[0]['count']
            logger.info(f"Target count: {target_count}")
            
            # Calculate difference and tolerance
            tolerance = self.config.get_count_tolerance()
            logger.info(f"Count tolerance: {tolerance}%")
            
            count_diff = abs(source_count - target_count)
            max_count = max(source_count, target_count)
            diff_percentage = (count_diff / max_count * 100) if max_count > 0 else 0
            logger.info(f"Count difference: {count_diff} ({diff_percentage:.2f}%)")
            
            # Determine if validation passed
            validation_status = "SUCCESS" if diff_percentage <= tolerance else "FAILED"
            logger.info(f"Row count validation status: {validation_status}")
            
            # Create result
            result = {
                "validation_type": "row_count",
                "validation_status": validation_status,
                "source_count": source_count,
                "target_count": target_count,
                "mismatch_count": count_diff,
                "mismatch_details": f"Count difference: {count_diff} ({diff_percentage:.2f}%)",
                "execution_time_seconds": int(time.time() - start_time)
            }
            
            return result
        
        except Exception as e:
            logger.error(f"Error in row count validation: {str(e)}", exc_info=True)
            return {
                "validation_type": "row_count",
                "validation_status": "ERROR",
                "source_count": None,
                "target_count": None,
                "mismatch_count": None,
                "mismatch_details": str(e),
                "execution_time_seconds": int(time.time() - start_time)
            }
    
    def validate_row_hash(self, source_catalog, source_schema, source_table, 
                         target_catalog, target_schema, target_table, primary_keys=None):
        """
        Validate that the row hashes match between source and target tables using xxhash64.
        Uses a comprehensive approach to compare rows with full outer join.
        
        Args:
            source_catalog: Source catalog name
            source_schema: Source schema name
            source_table: Source table name
            target_catalog: Target catalog name
            target_schema: Target schema name
            target_table: Target table name
            primary_keys: List of primary key columns for joining
            
        Returns:
            Dictionary with validation results
        """
        start_time = time.time()
        logger.info(f"Starting row hash validation for {source_catalog}.{source_schema}.{source_table} vs {target_catalog}.{target_schema}.{target_table}")
        
        # Get source and target paths
        source_path = f"{source_catalog}.{source_schema}.{source_table}"
        target_path = f"{target_catalog}.{target_schema}.{target_table}"
        
        try:
            # Get schemas for both tables
            logger.info(f"Loading source table: {source_path}")
            source_df = self.spark.table(source_path)
            source_count = source_df.count()
            logger.info(f"Source table loaded with {source_count} rows")
            
            logger.info(f"Loading target table: {target_path}")
            target_df = self.spark.table(target_path)
            target_count = target_df.count()
            logger.info(f"Target table loaded with {target_count} rows")
            
            # Determine primary keys
            if not primary_keys:
                primary_keys = self.config.get_default_primary_keys()
                logger.info(f"Using default primary keys: {primary_keys}")
            else:
                logger.info(f"Using provided primary keys: {primary_keys}")
                
            # Verify primary keys exist in both tables
            source_cols = source_df.columns
            target_cols = target_df.columns
            
            logger.info(f"Source columns: {source_cols}")
            logger.info(f"Target columns: {target_cols}")
            
            # Verify primary keys exist in both tables
            valid_pk_cols = [pk for pk in primary_keys if pk in source_cols and pk in target_cols]
            logger.info(f"Valid primary key columns: {valid_pk_cols}")
            
            if not valid_pk_cols:
                logger.error(f"No valid primary keys found. Tried: {primary_keys}")
                return {
                    "validation_type": "row_hash",
                    "validation_status": "ERROR",
                    "source_count": source_count,
                    "target_count": target_count,
                    "mismatch_count": None,
                    "mismatch_details": f"No valid primary keys found. Tried: {primary_keys}",
                    "execution_time_seconds": int(time.time() - start_time)
                }
            
            # Check for duplicate keys in source and target
            logger.info(f"Checking for duplicate keys in source table using keys: {valid_pk_cols}")
            source_key_counts = source_df.groupBy(*valid_pk_cols).count().filter(col("count") > 1)
            source_dup_count = source_key_counts.count()
            logger.info(f"Found {source_dup_count} duplicate key groups in source")
            
            logger.info(f"Checking for duplicate keys in target table using keys: {valid_pk_cols}")
            target_key_counts = target_df.groupBy(*valid_pk_cols).count().filter(col("count") > 1)
            target_dup_count = target_key_counts.count()
            logger.info(f"Found {target_dup_count} duplicate key groups in target")
            
            if source_dup_count > 0:
                dup_keys = source_key_counts.limit(5).collect()
                dup_details = ", ".join([str({pk: row[pk] for pk in valid_pk_cols}) for row in dup_keys])
                logger.error(f"Duplicate keys found in source table: {dup_details}...")
                return {
                    "validation_type": "row_hash",
                    "validation_status": "ERROR",
                    "source_count": source_count,
                    "target_count": target_count,
                    "mismatch_count": None,
                    "mismatch_details": f"Duplicate keys found in source table: {dup_details}...",
                    "execution_time_seconds": int(time.time() - start_time)
                }
                
            if target_dup_count > 0:
                dup_keys = target_key_counts.limit(5).collect()
                dup_details = ", ".join([str({pk: row[pk] for pk in valid_pk_cols}) for row in dup_keys])
                logger.error(f"Duplicate keys found in target table: {dup_details}...")
                return {
                    "validation_type": "row_hash",
                    "validation_status": "ERROR",
                    "source_count": source_count,
                    "target_count": target_count,
                    "mismatch_count": None,
                    "mismatch_details": f"Duplicate keys found in target table: {dup_details}...",
                    "execution_time_seconds": int(time.time() - start_time)
                }
            
            # Identify metadata columns to exclude from comparison
            def is_metadata_column(col_name):
                return col_name.startswith('_') or col_name.startswith('dlt_') or col_name in valid_pk_cols
            
            # Get business columns for comparison (excluding primary keys and metadata columns)
            source_business_cols = [c for c in source_cols if not is_metadata_column(c)]
            target_business_cols = [c for c in target_cols if not is_metadata_column(c)]
            
            logger.info(f"Source business columns: {source_business_cols}")
            logger.info(f"Target business columns: {target_business_cols}")
            
            # Find common business columns
            common_business_cols = list(set(source_business_cols).intersection(set(target_business_cols)))
            common_business_cols.sort()  # Sort for consistent ordering
            logger.info(f"Common business columns for comparison: {common_business_cols}")
            
            if not common_business_cols:
                logger.error("No common business columns found for comparison")
                return {
                    "validation_type": "row_hash",
                    "validation_status": "ERROR",
                    "source_count": source_count,
                    "target_count": target_count,
                    "mismatch_count": None,
                    "mismatch_details": "No common business columns found for comparison",
                    "execution_time_seconds": int(time.time() - start_time)
                }
            
            # Helper function to generate row hash for complex types
            def generate_row_hash(df, columns):
                logger.info(f"Generating row hash for {len(columns)} columns")
                # Create a list of expressions to handle different column types
                hash_expressions = []
                
                for column in columns:
                    col_type = df.schema[column].dataType
                    
                    # Handle complex types by converting to JSON string first
                    if isinstance(col_type, StructType) or str(col_type).startswith("ArrayType") or str(col_type).startswith("MapType"):
                        logger.debug(f"Column {column} is complex type {col_type}, converting to JSON")
                        hash_expressions.append(coalesce(to_json(col(column)), lit("null")))
                    else:
                        logger.debug(f"Column {column} is simple type {col_type}, casting to string")
                        hash_expressions.append(coalesce(col(column).cast("string"), lit("null")))
                
                # Concatenate all expressions and compute xxhash64
                if hash_expressions:
                    return xxhash64(concat_ws("||", *hash_expressions))
                else:
                    return lit("empty_row")
            
            # Generate row hashes for source and target
            logger.info("Generating row hashes for source table")
            source_with_hash = source_df.select(
                *valid_pk_cols,
                generate_row_hash(source_df, common_business_cols).alias("row_hash")
            )
            logger.info(f"Source hash generation complete, sample hash: {source_with_hash.limit(1).collect()}")
            
            logger.info("Generating row hashes for target table")
            target_with_hash = target_df.select(
                *valid_pk_cols,
                generate_row_hash(target_df, common_business_cols).alias("row_hash")
            )
            logger.info(f"Target hash generation complete, sample hash: {target_with_hash.limit(1).collect()}")
            
            # Perform full outer join to identify all differences
            join_condition = " AND ".join([f"COALESCE(s.{pk}, t.{pk}) = COALESCE(t.{pk}, s.{pk})" for pk in valid_pk_cols])
            logger.info(f"Join condition: {join_condition}")
            
            comparison_query = f"""
            SELECT 
                {', '.join([f'COALESCE(s.{pk}, t.{pk}) as {pk}' for pk in valid_pk_cols])},
                s.row_hash as src_hash,
                t.row_hash as tgt_hash,
                CASE 
                    WHEN s.row_hash IS NULL THEN 'extra_in_target'
                    WHEN t.row_hash IS NULL THEN 'missing_in_target'
                    WHEN s.row_hash = t.row_hash THEN 'match'
                    ELSE 'changed'
                END as diff_status
            FROM 
                source_with_hash s
            FULL OUTER JOIN 
                target_with_hash t
            ON 
                {join_condition}
            """
            logger.info("Executing comparison query with full outer join")
            logger.debug(f"SQL Query: {comparison_query}")
            
            # Register temp views
            source_with_hash.createOrReplaceTempView("source_with_hash")
            target_with_hash.createOrReplaceTempView("target_with_hash")
            
            # Execute the comparison
            row_level_diff = self.spark.sql(comparison_query)
            logger.info(f"Comparison complete, result has {row_level_diff.count()} rows")
            
            # Create summary
            logger.info("Generating summary statistics")
            row_level_summary = row_level_diff.groupBy("diff_status").count()
            
            # Display summary for debugging
            logger.info("Diff status summary:")
            for row in row_level_summary.collect():
                logger.info(f"  {row['diff_status']}: {row['count']} rows")
            
            # Get counts for each status
            summary_map = {row["diff_status"]: row["count"] for row in row_level_summary.collect()}
            
            # Calculate total mismatches
            match_count = summary_map.get("match", 0)
            changed_count = summary_map.get("changed", 0)
            missing_count = summary_map.get("missing_in_target", 0)
            extra_count = summary_map.get("extra_in_target", 0)
            
            total_mismatches = changed_count + missing_count + extra_count
            logger.info(f"Total mismatches: {total_mismatches}")
            
            # Determine validation status
            validation_status = "SUCCESS" if total_mismatches == 0 else "FAILED"
            logger.info(f"Validation status: {validation_status}")
            
            # Get mismatch details (limited to max_mismatch_details)
            max_details = self.config.get_max_mismatch_details()
            logger.info(f"Collecting up to {max_details} mismatch examples")
            mismatch_details_df = row_level_diff.filter(col("diff_status") != "match").limit(max_details)
            
            # Format mismatch details as string
            mismatch_details_str = "\n".join([
                f"Status: {row['diff_status']}, " + 
                f"Keys: {', '.join([f'{pk}={row[pk]}' for pk in valid_pk_cols])}, " +
                f"Source Hash: {row['src_hash'] if row['src_hash'] else 'NULL'}, " +
                f"Target Hash: {row['tgt_hash'] if row['tgt_hash'] else 'NULL'}"
                for row in mismatch_details_df.collect()
            ])
            
            if total_mismatches > max_details:
                mismatch_details_str += f"\n... and {total_mismatches - max_details} more mismatches"
            
            # Create result
            result = {
                "validation_type": "row_hash",
                "validation_status": validation_status,
                "source_count": source_count,
                "target_count": target_count,
                "mismatch_count": total_mismatches,
                "mismatch_details": f"Summary: {match_count} matches, {changed_count} changed, {missing_count} missing in target, {extra_count} extra in target\n\n{mismatch_details_str if total_mismatches > 0 else 'No mismatches'}",
                "execution_time_seconds": int(time.time() - start_time)
            }
            
            # Save detailed results as temporary views for further analysis
            logger.info("Creating temporary views for detailed analysis")
            row_level_diff.createOrReplaceTempView("row_level_diff")
            row_level_summary.createOrReplaceTempView("row_level_summary")
            logger.info("Created temporary views: row_level_diff, row_level_summary")
            
            logger.info(f"Row hash validation completed in {int(time.time() - start_time)} seconds")
            return result
            
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"Error in row hash validation: {str(e)}")
            logger.error(f"Traceback: {error_trace}")
            return {
                "validation_type": "row_hash",
                "validation_status": "ERROR",
                "source_count": None,
                "target_count": None,
                "mismatch_count": None,
                "mismatch_details": f"{str(e)}\n{error_trace}",
                "execution_time_seconds": int(time.time() - start_time)
            }
    
    def record_validation_result(self, run_id, source_catalog, source_schema, source_table,
                               target_catalog, target_schema, target_table, validation_result):
        """
        Record validation result to the audit table.
        
        Args:
            run_id: Unique run identifier
            source_catalog: Source catalog name
            source_schema: Source schema name
            source_table: Source table name
            target_catalog: Target catalog name
            target_schema: Target schema name
            target_table: Target table name
            validation_result: Dictionary with validation results
            
        Returns:
            None
        """
        logger.info(f"Recording validation result for {source_table}, type: {validation_result['validation_type']}, status: {validation_result['validation_status']}")
        
        # Create a row for the audit table
        audit_data = [(
            run_id,
            None,  # run_timestamp will be set below
            source_catalog,
            source_schema,
            source_table,
            target_catalog,
            target_schema,
            target_table,
            validation_result["validation_type"],
            validation_result["validation_status"],
            validation_result["source_count"],
            validation_result["target_count"],
            validation_result["mismatch_count"],
            validation_result["mismatch_details"],
            validation_result["execution_time_seconds"]
        )]
        
        # Create DataFrame with the audit schema
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
        from pyspark.sql.functions import current_timestamp
        
        audit_schema = StructType([
            StructField("run_id", StringType(), False),
            StructField("run_timestamp", TimestampType(), True),
            StructField("source_catalog", StringType(), False),
            StructField("source_schema", StringType(), False),
            StructField("source_table", StringType(), False),
            StructField("target_catalog", StringType(), False),
            StructField("target_schema", StringType(), False),
            StructField("target_table", StringType(), False),
            StructField("validation_type", StringType(), False),
            StructField("validation_status", StringType(), False),
            StructField("source_count", IntegerType(), True),
            StructField("target_count", IntegerType(), True),
            StructField("mismatch_count", IntegerType(), True),
            StructField("mismatch_details", StringType(), True),
            StructField("execution_time_seconds", IntegerType(), True)
        ])
        
        audit_df = self.spark.createDataFrame(audit_data, schema=audit_schema)
        audit_df = audit_df.withColumn("run_timestamp", current_timestamp())
        
        # Write to the audit table
        audit_path = self.config.get_audit_table_path()
        logger.info(f"Writing validation result to audit table: {audit_path}")
        
        try:
            audit_df.write.format("delta").mode("append").saveAsTable(audit_path)
            logger.info("Successfully recorded validation result to audit table")
        except Exception as e:
            logger.error(f"Error writing to audit table: {str(e)}", exc_info=True)
