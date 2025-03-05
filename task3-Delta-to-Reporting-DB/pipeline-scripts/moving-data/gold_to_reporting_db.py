import pyodbc
import pandas as pd
import sys

def gold_to_reporting_db(gold_table_path, reporting_db_connection_string, reporting_table_name, last_processed_timestamp):
    """
    Moves data from the Gold layer to the reporting database.

    Parameters:
    - gold_table_path: Path to the gold layer Delta table.
    - reporting_db_connection_string: Reporting DB connection string.
    - reporting_table_name: Reporting DB table name.
    - last_processed_timestamp: The last processed DateTimeStamp.
    """
    # 1. Load Data from Gold Layer (Delta Table)
    # Filter data to only get rows with DateTimeStamp greater than the last processed timestamp
    gold_df = spark.read.format("delta").load(gold_table_path)

    # Filter only rows that have a DateTimeStamp greater than the last processed timestamp
    gold_df_filtered = gold_df.filter(gold_df.DateTimeStamp > last_processed_timestamp)

    # If needed, you can also apply further transformations here

    # 2. Convert the filtered data to Pandas DataFrame for interaction with the database
    gold_pandas_df = gold_df_filtered.toPandas()

    # 3. Set up Connection to Reporting Database (Azure SQL, Synapse, etc.)
    conn = pyodbc.connect(reporting_db_connection_string)
    cursor = conn.cursor()

    # 4. Insert Data into Reporting Database Table
    # Assuming that the Reporting Database table schema matches the DataFrame schema
    for index, row in gold_pandas_df.iterrows():
        # Construct an insert query for each row
        sql = f"""
            INSERT INTO {reporting_table_name} (DateTimeStamp, Tag1, Tag2, Tag3)
            VALUES (?, ?, ?, ?)
        """
        cursor.execute(sql, row['DateTimeStamp'], row['Tag1'], row['Tag2'], row['Tag3'])

    # Commit the transaction
    conn.commit()

    # 5. Close the connection
    cursor.close()
    conn.close()

    print("Data successfully moved from Gold Layer to Reporting Database!")

# Main block to retrieve parameters and call the gold_to_reporting_db function
if __name__ == "__main__":
    try:
        # Try to get parameters from dbutils
        gold_table_path = dbutils.widgets.get("gold_table_path")
        reporting_db_connection_string = dbutils.widgets.get("reporting_db_connection_string")
        reporting_table_name = dbutils.widgets.get("reporting_table_name")
        last_processed_timestamp = dbutils.widgets.get("last_processed_timestamp")
    except Exception as e:
        print(f"Failed to retrieve parameters from dbutils: {e}")
        try:
            # Try to get parameters from sys.argv
            gold_table_path = sys.argv[1]
            reporting_db_connection_string = sys.argv[2]
            reporting_table_name = sys.argv[3]
            last_processed_timestamp = sys.argv[4]
        except Exception as e:
            print(f"Failed to retrieve parameters from sys.argv: {e}")
            sys.exit(1)

    # Call the gold_to_reporting_db function with the retrieved parameters
    gold_to_reporting_db(gold_table_path, reporting_db_connection_string, reporting_table_name, last_processed_timestamp)
