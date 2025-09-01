# Databricks notebook source
# MAGIC %md
# MAGIC # Manual Upload of Workday Files to Unity Catalog Volume
# MAGIC 
# MAGIC This notebook provides simple commands to manually copy Workday PySpark files from a Databricks workspace to a Unity Catalog volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC 
# MAGIC Set up configuration parameters for the file upload.

# COMMAND ----------

# Add widgets for configuration
dbutils.widgets.text("source_path", "/Workspace/Users/sudheer.palyam@apa.com.au/apa-databricks-medallion-workday/src/Finance")
dbutils.widgets.text("target_volume_path", "/Volumes/utilities_dev/erp_remediation/dlt_migration_tool/source/")
dbutils.widgets.text("file_extension", ".py")

# Get widget values
source_path = dbutils.widgets.get("source_path")
target_volume_path = dbutils.widgets.get("target_volume_path")
file_extension = dbutils.widgets.get("file_extension")

print(f"Source path: {source_path}")
print(f"Target volume path: {target_volume_path}")
print(f"File extension: {file_extension}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Files in Source Directory
# MAGIC 
# MAGIC List all Python files in the source directory.

# COMMAND ----------

# List files in the source directory
files = dbutils.workspace.ls(source_path)

# Filter for Python files
py_files = [file for file in files if file.path.endswith(file_extension)]

print(f"Found {len(py_files)} Python files in {source_path}:")
for file in py_files:
    print(f"- {file.path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Target Directory
# MAGIC 
# MAGIC Create the target directory in the volume if it doesn't exist.

# COMMAND ----------

# Create the target directory if it doesn't exist
dbutils.fs.mkdirs(target_volume_path)
print(f"Target directory created/verified: {target_volume_path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Copy Commands
# MAGIC 
# MAGIC The following cells demonstrate how to manually copy files from the workspace to the volume.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example: Copy a Single File
# MAGIC 
# MAGIC This example shows how to copy a single file from the workspace to the volume.

# COMMAND ----------

# Example for copying a single file
def copy_file(workspace_path, volume_path):
    """
    Copy a file from workspace to volume.
    
    Args:
        workspace_path: Path to the file in the workspace
        volume_path: Path to the file in the volume
    """
    try:
        # Read the file content
        content = dbutils.workspace.get(workspace_path)
        
        # Write to the volume
        dbutils.fs.put(volume_path, content, overwrite=True)
        print(f"Successfully copied {workspace_path} to {volume_path}")
    except Exception as e:
        print(f"Error copying {workspace_path}: {str(e)}")

# Example usage (uncomment to run)
# file_path = "/Workspace/Users/sudheer.palyam@apa.com.au/apa-databricks-medallion-workday/src/Finance/account_sets.py"
# target_file = target_volume_path + "account_sets.py"
# copy_file(file_path, target_file)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Copy All Files
# MAGIC 
# MAGIC This cell will copy all Python files from the source directory to the target volume.

# COMMAND ----------

# Copy all Python files from the source directory to the target volume
copied_files = 0
failed_files = 0

for file in py_files:
    try:
        file_name = file.path.split("/")[-1]
        source_file_path = file.path
        target_file_path = target_volume_path + file_name
        
        # Read the file content
        content = dbutils.workspace.get(source_file_path)
        
        # Write to the volume
        dbutils.fs.put(target_file_path, content, overwrite=True)
        
        print(f"Copied {file_name}")
        copied_files += 1
    except Exception as e:
        print(f"Error copying {file.path}: {str(e)}")
        failed_files += 1

print(f"\nCopy complete: {copied_files} files copied, {failed_files} failed")

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Files in Target Volume
# MAGIC 
# MAGIC List all files in the target volume to verify the upload.

# COMMAND ----------

print(f"Files in {target_volume_path}:")
for file_info in dbutils.fs.ls(target_volume_path):
    print(f"- {file_info.name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Manual Commands for Copy-Paste
# MAGIC 
# MAGIC Here are some commands you can copy and paste into a notebook cell to perform specific operations:

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC # List files in a workspace directory
# MAGIC files = dbutils.workspace.ls("/Workspace/Users/your_username/your_directory")
# MAGIC display(files)
# MAGIC 
# MAGIC # Read a file from workspace
# MAGIC content = dbutils.workspace.get("/Workspace/Users/your_username/your_directory/your_file.py")
# MAGIC 
# MAGIC # Write a file to volume
# MAGIC dbutils.fs.put("/Volumes/your_catalog/your_schema/your_volume/your_file.py", content, overwrite=True)
# MAGIC 
# MAGIC # List files in a volume
# MAGIC files = dbutils.fs.ls("/Volumes/your_catalog/your_schema/your_volume/")
# MAGIC display(files)
# MAGIC 
# MAGIC # Read a file from volume
# MAGIC content = dbutils.fs.head("/Volumes/your_catalog/your_schema/your_volume/your_file.py")
# MAGIC print(content)
# MAGIC ```

# COMMAND ----------
