import sys
import pyspark.sql.functions as f
from delta.tables import DeltaTable

# Function to validate tags in the DataFrame
def validate_tags(df):
    """
    Validates the tags in the DataFrame.
    
    Parameters:
    - df: The DataFrame to validate
    
    Returns:
    - DataFrame with additional columns `valid_record` and `invalid_reason`
    """
    # Validation rules
    valid_tag1 = (df["Tag1"].isin(["Temp_01", "Temp_02", "Temp_03"]))  # Example set of valid tags
    valid_tag2 = (df["Tag2"].isNotNull())  # Tag2 should not be null
    valid_tag3 = (df["Tag3"].isNotNull())  # Tag3 should not be null

    # Adding the `valid_record` column (TRUE if all validations pass)
    df = df.withColumn("valid_record", f.when(valid_tag1 & valid_tag2 & valid_tag3, f.lit(True)).otherwise(f.lit(False)))

    # Adding the `invalid_reason` column (list of failed validations)
    df = df.withColumn(
        "invalid_reason",
        f.when(~valid_tag1, f.lit("Tag1"))
        .when(~valid_tag2, f.lit("Tag2"))
        .when(~valid_tag3, f.lit("Tag3"))
        .otherwise(f.lit(None))
    )

    # Return the updated DataFrame with validation columns
    return df

# Function to process data from bronze layer to silver layer
def bronze_to_silver(bronze_path, silver_path):
    """
    Moves data from Bronze to Silver layer after validation.
    Applies MERGE for incremental updates to Silver.
    
    Parameters:
    - bronze_path: Path to the Bronze layer (Delta table).
    - silver_path: Path to the Silver layer (Delta table).
    """
    # Step 1: Read the data from Bronze layer (Delta table)
    bronze_df = spark.read.format("delta").load(bronze_path)

    # Step 2: Apply validation
    validated_df = validate_tags(bronze_df)

    # Step 3: Load the Silver table (Delta)
    silver_table = DeltaTable.forPath(spark, silver_path)

    # Step 4: Merge new/updated valid records into Silver table
    merge_condition = "silver.DateTimeStamp = new_data.DateTimeStamp"
    
    # Define the update and insert actions
    update_set = {
        "Tag1": "new_data.Tag1",
        "Tag2": "new_data.Tag2",
        "Tag3": "new_data.Tag3",
        "last_updated": "new_data.last_updated"
    }

    insert_values = {
        "DateTimeStamp": "new_data.DateTimeStamp",
        "Tag1": "new_data.Tag1",
        "Tag2": "new_data.Tag2",
        "Tag3": "new_data.Tag3",
        "last_updated": "new_data.last_updated",
        "valid_record": "new_data.valid_record",
        "invalid_reason": "new_data.invalid_reason"
    }

    # Perform the MERGE operation to update or insert records into Silver table
    silver_table.alias("silver").merge(
        validated_df.alias("new_data"),
        merge_condition
    ).whenMatchedUpdate(
        condition="silver.valid_record = TRUE AND new_data.valid_record = TRUE",
        set=update_set
    ).whenNotMatchedInsert(
        condition="new_data.valid_record = TRUE",
        values=insert_values
    ).execute()

    # Optionally, you can log the number of updated or inserted records
    updated_count = silver_table.alias("silver").merge(
        validated_df.alias("new_data"),
        merge_condition
    ).whenMatchedUpdate(
        condition="silver.valid_record = TRUE AND new_data.valid_record = TRUE",
        set=update_set
    ).whenNotMatchedInsert(
        condition="new_data.valid_record = TRUE",
        values=insert_values
    ).getUpdateCount()

    print(f"Records updated or inserted: {updated_count}")

# Main block to retrieve parameters and call the bronze_to_silver function
if __name__ == "__main__":
    try:
        # Try to get parameters from dbutils
        bronze_path = dbutils.widgets.get("bronze_path")
        silver_path = dbutils.widgets.get("silver_path")
    except Exception as e:
        print(f"Failed to retrieve parameters from dbutils: {e}")
        try:
            # Try to get parameters from sys.argv
            bronze_path = sys.argv[1]
            silver_path = sys.argv[2]
        except Exception as e:
            print(f"Failed to retrieve parameters from sys.argv: {e}")
            sys.exit(1)

    # Call the bronze_to_silver function with the retrieved parameters
    bronze_to_silver(bronze_path, silver_path)
