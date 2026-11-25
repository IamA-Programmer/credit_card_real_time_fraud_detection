from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
# Define the schema explicitly based on the JSON structure
from pyspark.sql.types import StructType, StructField, StringType

@dp.table(name = "finguard.bronze.fraud_watchlist",comment = "Fraud Watchlist raw stream data ingested by auto loader")
def fraud_watchlist_bronze() -> DataFrame:
    # Create a Dataframe
# 1. format("cloudFiles") means we are using Autoloader
# 2. For Autoloader only readStream is supported, so we can use readStream for batch ingestion saying the triggerType(availableNow = True)

    schema = StructType([
        StructField("watchlist_id", StringType(), True),
        StructField("watchlist_type", StringType(), True),
        StructField("entity_id", StringType(), True),
        StructField("risk_level", StringType(), True),
        StructField("action", StringType(), True),
        StructField("reason_code", StringType(), True),
        StructField("reason_description", StringType(), True),
        StructField("status", StringType(), True),
        StructField("effective_from", StringType(), True),
        StructField("reported_by", StringType(), True),
        StructField("reported_source", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
        StructField("_rescued_data", StringType(), True)
    ])

    """ The below path is schema path which is like checkpoint, so as we use SDP(Declarative pipeline) this will be done automatically
    .option("cloudFiles.schemaLocation","/Volumes/finguard/source/fraud_watchlist/schema/")
    Previously we used option("cloudFiles.inferColumnTypes", "true"), but it is not working, so the schema is created manually, but we need to check this """

    streaming_df = (
        spark.readStream.format("cloudFiles")
                        .option("cloudFiles.format","json")
                        .schema(schema)
                        .load('/Volumes/finguard/source/fraud_watchlist/source_data/') #source path
    )

    # Important
    # Drop Dulplicates, one thing I have identified is if we have same data in the volume, and if we have same data from source system, the json is not overwriting, it is simply appending the json files inside the volume.

    # So write the logic to remove duplicates

    # select the necessary column
    parsed_streaming_df = streaming_df.select(
        F.col("watchlist_id"),
        F.col("watchlist_type"),
        F.col("entity_id"),
        F.col("risk_level"),
        F.col("action"),
        F.col("reason_code"),
        F.col("reason_description"),
        F.col("status"),
        F.col("effective_from"),
        F.col("reported_by"),
        F.col("reported_source"),
        F.col("country"),
        F.col("city"),
        F.col("_rescued_data"), # This is the column which is created by autoloader, if any data has been corrupted, that data is captured in this column by auto loader
        F.col("_metadata.file_path").alias("source_file_path"),
        F.current_timestamp().alias("ingestion_timestamp")
    )
    # Advantage is we don't need checkpoint location we can directly return once it is streamed
    return parsed_streaming_df


