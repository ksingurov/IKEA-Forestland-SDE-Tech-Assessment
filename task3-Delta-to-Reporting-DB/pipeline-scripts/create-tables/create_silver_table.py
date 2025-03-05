from pyspark.sql.types import StructType, StructField, StringType

def create_silver_table(silver_path):
    """
    Creates the Silver table.

    Parameters:
    - silver_path: Path to the Silver layer.
    """

    # Define schema for the Silver table
    schema = StructType([
        StructField("DateTimeStamp", StringType(), True),
        StructField("Tag1", StringType(), True),
        StructField("Tag2", StringType(), True),
        StructField("Tag3", StringType(), True)
    ])

    # Create an empty DataFrame with the schema
    df = spark.createDataFrame([], schema)

    # Write the empty DataFrame to the Silver path as a Delta table
    df.write.format("delta").save(silver_path)

if __name__ == "__main__":
    silver_path = "abfss://container@storageaccount.dfs.core.windows.net/silver/"
    create_silver_table(silver_path)
