from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
# Define the schema explicitly based on the JSON structure
from pyspark.sql.types import StructType, StructField, StringType

@dp.table(name = "finguard.silver.fraud_watchlist",comment = "Cleaned Fraud Watchlist")
def fraud_watchlist_silver() -> DataFrame:

    bronze_df = (
        spark.readStream.table("finguard.bronze.fraud_watchlist")
    )

    # select the necessary column
    cleaned_df = bronze_df.select(
        # Check for what is column mapping
        F.upper(F.col("watchlist_id")).alias("watchlist_id"),
        F.col("watchlist_type"),
        F.upper(F.col("entity_id")).alias("entity_id"),
        F.upper(F.col("risk_level")).alias("risk_level"),
        F.upper(F.col("action")).alias("action"),
        F.col("reason_code"),
        F.col("reason_description"),
        F.col("status"),
        F.to_timestamp(F.col("effective_from"), "dd-MMM-yyyy HH:mm:ss").alias("effective_from"),
        F.col("reported_by"),
        F.col("reported_source"),
        F.col("country"),
        F.col("city"),
        F.col("source_file_path"),
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp"),
        F.current_timestamp().alias("silver_ingestion_timestamp")
    )
    # Advantage is we don't need checkpoint location we can directly return once it is streamed
    return cleaned_df


