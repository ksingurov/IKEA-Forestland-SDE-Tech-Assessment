import pyspark.sql.functions as f
from pyspark.sql.types import StringType
from datetime import datetime

# Get today's date
today_date = datetime.now().strftime("%Y%m%d")
year, month, day = datetime.now().strftime("%Y %m %d").split()

# Define storage accounts and container
source_storage_account = "dsprdsawf1"
destination_storage_account = "dsprdedw"
container = "oem1"
source_folder = "folder1"

# Define source and destination base paths
source_base_path = f"abfss://{container}@{source_storage_account}.dfs.core.windows.net/{source_folder}"
destination_base_path = f"abfss://{container}@{destination_storage_account}.dfs.core.windows.net/{year}/{month}/{day}"
previous_base_path = f"abfss://{container}@{destination_storage_account}.dfs.core.windows.net/previous"

# Function to load parquet file from storage
def load_parquet(file_path):
    """Load parquet file from the storage.
    
    Args:
        file_path (str): The path to the parquet file.
    
    Returns:
        DataFrame: The loaded DataFrame.
    """
    return spark.read.parquet(file_path)

# Function to retrieve all subfolders dynamically from the given base path
def get_subfolders(base_path):
    """Retrieve all subfolders dynamically from the given base path.
    
    Args:
        base_path (str): The base path to list subfolders from.
    
    Returns:
        list: A list of subfolder names.
    """
    return [f.name for f in dbutils.fs.ls(base_path) if f.isDir()]

# Function to identify new and modified records based on hashed column comparison
def get_delta_records(new_df, prev_df):
    """Identify new and modified records based on hashed column comparison.
    
    Args:
        new_df (DataFrame): The new DataFrame.
        prev_df (DataFrame): The previous DataFrame.
    
    Returns:
        DataFrame: The DataFrame containing delta records.
    """
    new_df = new_df.withColumn("hash", f.sha2(f.concat_ws("||", *new_df.columns), 256).cast(StringType()))
    if prev_df is None:
        return new_df.drop("hash")
    delta_df = new_df.join(prev_df, on="hash", how="left_anti")
    return delta_df.drop("hash")

# Function to get the next sequential file name
def get_next_file_name(destination_path):
    """Get the next sequential file name in the destination path.
    
    Args:
        destination_path (str): The destination path to check for existing files.
    
    Returns:
        str: The next sequential file name.
    """
    existing_files = [f.name for f in dbutils.fs.ls(destination_path) if not f.isDir()]
    if not existing_files:
        return "01.parquet"
    existing_files.sort()
    last_file = existing_files[-1]
    last_number = int(last_file.split(".")[0])
    next_number = last_number + 1
    return f"{next_number:02d}.parquet"

# Function to check if a file exists in Azure storage
def file_exists(path):
    """Check if a file exists in Azure storage.
    
    Args:
        path (str): The path to the file.
    
    Returns:
        bool: True if the file exists, False otherwise.
    """
    try:
        return len(dbutils.fs.ls(path)) > 0
    except:
        return False

# Main script logic
subfolders = get_subfolders(source_base_path)

for subfolder in subfolders:
    source_path = f"{source_base_path}/{subfolder}/{today_date}.parquet"
    destination_path = f"{destination_base_path}/{subfolder}"
    previous_path = f"{previous_base_path}/{subfolder}/{today_date}.parquet"
    
    try:
        # Load current version
        new_df = load_parquet(source_path)
    except Exception as e:
        print(f"Error loading current version for {subfolder}: {e}")
        continue
    
    try:
        # Load previous version if exists and matches the date, otherwise assign None
        if file_exists(previous_path) and previous_path.endswith(f"{today_date}.parquet"):
            previous_df = load_parquet(previous_path)
        else:
            previous_df = None
    except Exception as e:
        print(f"Error loading previous version for {subfolder}: {e}")
        continue
    
    # Compute delta records
    delta_df = get_delta_records(new_df, previous_df)
    if delta_df.count() > 0:
        delta_df = delta_df.withColumn("load_time", f.current_timestamp())
        next_file_name = get_next_file_name(destination_path)
        delta_df.write.parquet(f"{destination_path}/{next_file_name}")
    
    # Update previous version with only the hash column
    new_df.select("hash").write.mode("overwrite").parquet(previous_path)
