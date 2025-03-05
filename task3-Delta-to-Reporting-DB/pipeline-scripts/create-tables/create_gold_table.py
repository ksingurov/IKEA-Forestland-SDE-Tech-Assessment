from pyspark.sql.types import StructType, StructField, StringType

def create_gold_table(gold_path):
    """
    Creates the Gold table.

    Parameters:
    - gold_path: Path to the Gold layer.
    """

    # Define schema for the Gold table
    schema = StructType([
        StructField("DateTimeStamp", StringType(), True),
        StructField("Tag1", StringType(), True),
        StructField("Tag2", StringType(), True),
        StructField("Tag3", StringType(), True)
    ])

    # Create an empty DataFrame with the schema
    df = spark.createDataFrame([], schema)

    # Write the empty DataFrame to the Gold path as a Delta table
    df.write.format("delta").save(gold_path)

if __name__ == "__main__":
    gold_path = "abfss://container@storageaccount.dfs.core.windows.net/gold/"
    create_gold_table(gold_path)
