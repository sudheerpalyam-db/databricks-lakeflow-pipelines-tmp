# Add audit tracking to the process_input_files function
def process_input_files():
    """
    Process input files using the DLT Migration Tool.
    
    Returns:
        List of output files
    """
    try:
        # Initialize processor
        processor = MigrationProcessor(verbose=True)
        
        # List all JSON files in the input directory
        input_files = dbutils.fs.ls(input_path)
        json_files = [file for file in input_files if file.name.endswith('.json')]
        
        if not json_files:
            print(f"No JSON files found in {input_path}")
            return []
        
        print(f"Found {len(json_files)} JSON files to process:")
        for file in json_files:
            print(f"- {file.name}")
            # Track input file artifact
            audit.track_artifact(artifact_type="input_file", artifact_path=file.path)
        
        # Process each file
        output_files = []
        for file in json_files:
            try:
                # Skip silver transforms file - it will be processed separately
                if "silvertransforms" in file.name.lower():
                    print(f"Skipping silver transforms file: {file.name} - will process separately")
                    continue
                    
                print(f"Processing file: {file.path}")
                
                # Read the input file using Spark to handle large files
                try:
                    # First try to read using dbutils.fs.head
                    print(f"Attempting to read file using dbutils.fs.head: {file.path}")
                    input_content = dbutils.fs.head(file.path)
                    input_data = json.loads(input_content)
                    print(f"Successfully parsed JSON with {len(input_data)} entries")
                except Exception as e:
                    print(f"Error reading file with dbutils.fs.head: {str(e)}")
                    print("Trying alternative method using Spark DataFrame...")
                    
                    # Try reading with Spark
                    try:
                        # Read the JSON file as a text file first
                        df = spark.read.text(file.path)
                        
                        # Collect all lines and join them
                        all_text = "\n".join([row.value for row in df.collect()])
                        
                        # Parse the JSON
                        input_data = json.loads(all_text)
                        print(f"Successfully parsed JSON with Spark method: {len(input_data)} entries")
                    except Exception as spark_e:
                        print(f"Error reading with Spark method: {str(spark_e)}")
                        
                        # Try reading the file in chunks
                        print("Trying to read file in chunks...")
                        try:
                            # Use spark to read the file in chunks
                            df = spark.read.text(file.path)
                            
                            # Convert to pandas for easier processing
                            pdf = df.toPandas()
                            
                            # Join all lines
                            all_text = "".join(pdf["value"].tolist())
                            
                            # Try to parse the JSON
                            input_data = json.loads(all_text)
                            print(f"Successfully parsed JSON with chunk method: {len(input_data)} entries")
                        except Exception as chunk_e:
                            print(f"Error reading in chunks: {str(chunk_e)}")
                            
                            # Last resort: try to fix common JSON issues
                            print("Attempting to fix JSON format issues...")
                            try:
                                # Read as text and try to fix common JSON issues
                                all_text = "\n".join([row.value for row in df.collect()])
                                
                                # Fix common JSON issues
                                # 1. Try to find unterminated strings
                                fixed_text = all_text.replace('\\"', '"').replace('\\n', ' ')
                                
                                # 2. Try to balance quotes
                                quote_count = fixed_text.count('"')
                                if quote_count % 2 == 1:
                                    fixed_text = fixed_text + '"'
                                
                                # 3. Ensure the JSON is properly wrapped in [] or {}
                                if not (fixed_text.strip().startswith('[') and fixed_text.strip().endswith(']')) and \
                                   not (fixed_text.strip().startswith('{') and fixed_text.strip().endswith('}')):
                                    fixed_text = '[' + fixed_text + ']'
                                
                                # Try to parse the fixed JSON
                                input_data = json.loads(fixed_text)
                                print(f"Successfully parsed JSON after fixing format issues: {len(input_data)} entries")
                            except Exception as fix_e:
                                error_msg = f"Failed to fix JSON format issues: {str(fix_e)}"
                                print(error_msg)
                                print("Cannot process this file due to JSON parsing errors.")
                                # Track failure in audit
                                audit.track_artifact(artifact_type="failed_input", artifact_path=file.path, 
                                                    table_name="multiple")
                                continue
                
                # Verify that the input file contains specs for the tables we want to process
                table_specs = []
                for spec in input_data:
                    try:
                        table_name = spec.get("source_details", {}).get("source_table", "")
                        if table_name in tables_to_process:
                            table_specs.append(spec)
                    except Exception as spec_e:
                        print(f"Error processing spec entry: {str(spec_e)}")
                        continue
                
                # Check if any of the specified tables are missing from the input file
                found_tables = []
                for spec in table_specs:
                    try:
                        table_name = spec.get("source_details", {}).get("source_table", "")
                        if table_name:
                            found_tables.append(table_name)
                    except Exception:
                        continue
                
                missing_tables = [table for table in tables_to_process if table not in found_tables]
                
                if missing_tables:
                    print(f"WARNING: The following tables were not found in the input file: {', '.join(missing_tables)}")
                    if not table_specs:
                        error_msg = f"ERROR: None of the specified tables were found in the input file. Skipping this file."
                        print(error_msg)
                        # Track failure in audit
                        audit.track_artifact(artifact_type="skipped_input", artifact_path=file.path, 
                                            table_name="multiple")
                        continue
                
                print(f"Found {len(table_specs)} table specifications to process")
                
                # Create a temporary file with only the specs for the tables we want to process
                temp_input_file = f"/tmp/{file.name}"
                with open(temp_input_file, 'w') as f:
                    json.dump(table_specs, f, indent=2)
                
                # Create a temporary output directory
                temp_output_dir = f"/tmp/output_{file.name.replace('.json', '')}"
                os.makedirs(temp_output_dir, exist_ok=True)
                
                # Process the file
                result = processor.process_file(temp_input_file, temp_output_dir)
                
                # Copy the output files to the volume
                for output_file in result:
                    output_file_path = Path(output_file)
                    output_name = output_file_path.name
                    
                    # Read the output file
                    with open(output_file, 'r') as f:
                        output_content = f.read()
                    
                    # Write to the volume
                    volume_output_path = f"{output_path}/{output_name}"
                    dbutils.fs.put(volume_output_path, output_content, overwrite=True)
                    
                    # Track output artifact
                    table_name = None
                    if "_" in output_name:
                        parts = output_name.split("_")
                        if len(parts) > 1:
                            table_name = parts[1].split(".")[0]  # Extract table name from file name
                    
                    artifact_type = "bronze_spec" if "bronze" in output_name.lower() else "silver_spec"
                    audit.track_artifact(artifact_type=artifact_type, artifact_path=volume_output_path, 
                                        table_name=table_name)
                    
                    output_files.append(volume_output_path)
                
                print(f"Generated {len(result)} output files")
            except Exception as e:
                error_msg = f"Error processing {file.path}: {str(e)}"
                print(error_msg)
                print("Stack trace:")
                import traceback
                traceback.print_exc()
                
                # Track failure in audit
                audit.track_artifact(artifact_type="failed_processing", artifact_path=file.path)
        
        return output_files
    except Exception as e:
        error_msg = f"Error initializing MigrationProcessor: {str(e)}"
        print(error_msg)
        print("Stack trace:")
        import traceback
        traceback.print_exc()
        
        # Update audit run with error
        audit.end_run(status="FAILED", error_message=error_msg)
        
        return []
