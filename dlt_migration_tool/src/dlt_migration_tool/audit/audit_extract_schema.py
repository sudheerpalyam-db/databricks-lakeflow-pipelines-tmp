# Add audit tracking to the extract_schema function
def extract_schema(table_name, output_dir):
    """
    Extract schema from a table and save it to a file.
    
    Args:
        table_name: Fully qualified table name (e.g., "edp_bronze_dev.maximo.asset")
        output_dir: Directory to save the schema file
    
    Returns:
        Path to the schema file
    """
    # Extract the simple table name (without catalog and schema)
    simple_name = table_name.split('.')[-1]
    
    # Start tracking table migration
    tracking_info = audit.track_table_migration(
        table_name=simple_name,
        bronze_target=f"{bronze_catalog}.{bronze_schema}.{simple_name}",
        silver_target=f"{silver_catalog}.{silver_schema}.{simple_name}"
    )
    
    # Generate the schema file paths - save directly to both bronze and silver schema directories
    schema_file = f"{output_dir}/{source_system}_{simple_name}_schema.json"
    bronze_schema_file = f"{bronze_schema_dir}/{source_system}_{simple_name}_schema.json"
    silver_schema_file = f"{silver_schema_dir}/{source_system}_{simple_name}_schema.json"
    
    try:
        print(f"Attempting to extract schema from table: {table_name}")
        
        # First check if the table exists
        try:
            # Check if the table exists using spark.catalog
            table_exists = False
            for db in spark.catalog.listDatabases():
                if db.name == table_name.split('.')[0]:
                    for tbl in spark.catalog.listTables(db.name):
                        if f"{db.name}.{tbl.name}" == '.'.join(table_name.split('.')[:2]):
                            table_exists = True
                            break
            
            if not table_exists:
                print(f"WARNING: Table {table_name} does not exist in the catalog")
                raise ValueError(f"Table {table_name} does not exist")
        except Exception as e:
            print(f"Error checking table existence: {str(e)}")
            # Continue anyway as the table might still be accessible
        
        # Try to read the table
        print(f"Reading table: {table_name}")
        df = spark.read.table(table_name)
        
        # Check if the dataframe is empty
        row_count = df.count()
        print(f"Table {table_name} has {row_count} rows")
        
        # Get the schema
        schema_json = df.schema.json()
        schema_dict = json.loads(schema_json)
        
        # Print schema information for debugging
        print(f"Schema for {table_name} has {len(schema_dict.get('fields', []))} fields")
        
        # Save the schema to all required locations
        dbutils.fs.put(schema_file, json.dumps(schema_dict, indent=2), overwrite=True)
        dbutils.fs.put(bronze_schema_file, json.dumps(schema_dict, indent=2), overwrite=True)
        dbutils.fs.put(silver_schema_file, json.dumps(schema_dict, indent=2), overwrite=True)
        
        # Track artifacts
        audit.track_artifact(artifact_type="schema", artifact_path=schema_file, table_name=simple_name)
        audit.track_artifact(artifact_type="bronze_schema", artifact_path=bronze_schema_file, table_name=simple_name)
        audit.track_artifact(artifact_type="silver_schema", artifact_path=silver_schema_file, table_name=simple_name)
        
        # Verify the files were written correctly
        try:
            written_content = dbutils.fs.head(schema_file)
            written_schema = json.loads(written_content)
            print(f"Successfully wrote schema with {len(written_schema.get('fields', []))} fields to:")
            print(f"- {schema_file}")
            print(f"- {bronze_schema_file}")
            print(f"- {silver_schema_file}")
        except Exception as e:
            print(f"WARNING: Could not verify written schema files: {str(e)}")
        
        # Complete table migration tracking
        audit.complete_table_migration(table_name=simple_name, status="SUCCESS")
        
        return schema_file
    except Exception as e:
        error_msg = f"ERROR extracting schema from {table_name}: {str(e)}"
        print(error_msg)
        print("Stack trace:")
        import traceback
        traceback.print_exc()
        
        # Try an alternative approach for schema extraction
        try:
            print(f"Attempting alternative schema extraction for {table_name}")
            # Try to get schema using SQL approach
            schema_df = spark.sql(f"DESCRIBE TABLE {table_name}")
            
            if schema_df.count() > 0:
                print(f"Found {schema_df.count()} columns using SQL DESCRIBE")
                
                # Convert the SQL schema format to the expected JSON format
                fields = []
                for row in schema_df.collect():
                    if row['col_name'] != '' and not row['col_name'].startswith('#'):
                        field = {
                            "name": row['col_name'],
                            "type": row['data_type'],
                            "nullable": True,
                            "metadata": {}
                        }
                        fields.append(field)
                
                schema = {"type": "struct", "fields": fields}
                
                # Save the schema to all required locations
                dbutils.fs.put(schema_file, json.dumps(schema, indent=2), overwrite=True)
                dbutils.fs.put(bronze_schema_file, json.dumps(schema, indent=2), overwrite=True)
                dbutils.fs.put(silver_schema_file, json.dumps(schema, indent=2), overwrite=True)
                
                # Track artifacts
                audit.track_artifact(artifact_type="schema", artifact_path=schema_file, table_name=simple_name)
                audit.track_artifact(artifact_type="bronze_schema", artifact_path=bronze_schema_file, table_name=simple_name)
                audit.track_artifact(artifact_type="silver_schema", artifact_path=silver_schema_file, table_name=simple_name)
                
                print(f"Generated schema with {len(fields)} fields using alternative method and saved to all locations")
                
                # Complete table migration tracking
                audit.complete_table_migration(table_name=simple_name, status="SUCCESS")
                
                return schema_file
            else:
                print(f"Alternative schema extraction returned no columns")
        except Exception as alt_e:
            print(f"Alternative schema extraction also failed: {str(alt_e)}")
        
        # Generate an empty schema as last resort
        schema = {"type": "struct", "fields": []}
        
        # Save the schema to all required locations
        dbutils.fs.put(schema_file, json.dumps(schema, indent=2), overwrite=True)
        dbutils.fs.put(bronze_schema_file, json.dumps(schema, indent=2), overwrite=True)
        dbutils.fs.put(silver_schema_file, json.dumps(schema, indent=2), overwrite=True)
        
        # Track artifacts
        audit.track_artifact(artifact_type="schema", artifact_path=schema_file, table_name=simple_name)
        audit.track_artifact(artifact_type="bronze_schema", artifact_path=bronze_schema_file, table_name=simple_name)
        audit.track_artifact(artifact_type="silver_schema", artifact_path=silver_schema_file, table_name=simple_name)
        
        print(f"Generated empty schema file as fallback and saved to all locations")
        
        # Complete table migration tracking with failure
        audit.complete_table_migration(table_name=simple_name, status="FAILED", error_message=error_msg)
        
        return schema_file
