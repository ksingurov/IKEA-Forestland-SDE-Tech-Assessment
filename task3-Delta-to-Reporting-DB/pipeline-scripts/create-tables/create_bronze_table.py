from pyspark.sql.types import StructType, StructField, StringType

def create_bronze_table(bronze_path):
    """
    Creates the Bronze table.

    Parameters:
    - bronze_path: Path to the Bronze layer.
    """

    # Define schema for the Bronze table
    schema = StructType([
        StructField("DateTimeStamp", StringType(), True),
        StructField("Tag1", StringType(), True),
        StructField("Tag2", StringType(), True),
        StructField("Tag3", StringType(), True)
    ])

    # Create an empty DataFrame with the schema
    df = spark.createDataFrame([], schema)

    # Write the empty DataFrame to the Bronze path as a Delta table
    df.write.format("delta").save(bronze_path)

if __name__ == "__main__":
    bronze_path = "abfss://container@storageaccount.dfs.core.windows.net/bronze/"
    create_bronze_table(bronze_path)
