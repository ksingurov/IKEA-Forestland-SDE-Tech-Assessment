from delta.tables import DeltaTable
import sys

# Function to read data based on file format
def read_data(file_format, path):
    """
    Reads data from the specified path based on the file format.

    Parameters:
    - file_format: Format of the files (e.g., "csv", "parquet").
    - path: Path to the data files.

    Returns:
    - DataFrame containing the read data.
    """
    if file_format == "csv":
        return spark.read.format("csv").option("header", "true").load(path)
    elif file_format == "parquet":
        return spark.read.format("parquet").load(path)
    else:
        raise ValueError(f"Unsupported file format: {file_format}")

# Function to process data from landing zone to bronze layer
def landing_to_bronze(file_format, landing_zone_path, bronze_path, match_column, update_columns):
    """
    Monitors the landing zone for new or updated data.
    Loads raw data into a staging area (Bronze layer) for further processing.

    Parameters:
    - file_format: Format of the files in the landing zone (e.g., "csv", "parquet").
    - landing_zone_path: Path to the landing zone.
    - bronze_path: Path to the Bronze layer.
    - match_column: Column to match for updates.
    - update_columns: List of columns to update.
    """
    # Read new data from Landing Zone
    df_new = read_data(file_format, landing_zone_path)

    # Load the existing Bronze Table
    bronze_table = DeltaTable.forPath(spark, bronze_path)

    # Define the merge condition
    merge_condition = f"bronze.{match_column} = new_data.{match_column}"

    # Define the columns to update when matched
    update_set = {col: f"new_data.{col}" for col in update_columns}

    # Define the columns to insert when not matched
    insert_values = {col: f"new_data.{col}" for col in update_columns}
    insert_values[match_column] = f"new_data.{match_column}"

    # Perform MERGE to update tags when match_column matches
    bronze_table.alias("bronze").merge(
        df_new.alias("new_data"),
        merge_condition
    ).whenMatchedUpdate(
        set=update_set
    ).whenNotMatchedInsert(
        values=insert_values
    ).execute()

# Main block to retrieve parameters and call the landing_to_bronze function
if __name__ == "__main__":
    try:
        # Try to get parameters from dbutils
        file_format = dbutils.widgets.get("file_format")
        landing_zone_path = dbutils.widgets.get("landing_zone_path")
        bronze_path = dbutils.widgets.get("bronze_path")
        match_column = dbutils.widgets.get("match_column")
        update_columns = dbutils.widgets.get("update_columns").split(",")
    except Exception as e:
        print(f"Failed to retrieve parameters from dbutils: {e}")
        try:
            # Try to get parameters from sys.argv
            file_format = sys.argv[1]
            landing_zone_path = sys.argv[2]
            bronze_path = sys.argv[3]
            match_column = sys.argv[4]
            update_columns = sys.argv[5].split(",")
        except Exception as e:
            print(f"Failed to retrieve parameters from sys.argv: {e}")
            sys.exit(1)

    # Call the landing_to_bronze function with the retrieved parameters
    landing_to_bronze(file_format, landing_zone_path, bronze_path, match_column, update_columns)
