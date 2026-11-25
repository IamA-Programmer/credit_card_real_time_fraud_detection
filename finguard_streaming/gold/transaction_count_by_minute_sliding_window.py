from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

# Here we are taking the transaction count came in a minute, so we will be using window.
# There are two types of window - Tumbling window and Sliding window
# Tumbling window fixed window and sliding window means fixed window + slide interval
@dp.table(
    name = "finguard.gold.transaction_count_by_minute_sliding_window", comment = "Alert details where transactions has been performed with value higher than what is configured by customer"
)
def transaction_count_by_minute_sliding_window() -> DataFrame:

    transactions = spark.readStream.table("finguard.silver.transactions")
    # So that we can tell it how much late data we are allowing it, so it will wait for this time and will do the aggregation
    transactions_with_watermark = transactions.withWatermark("transaction_timestamp","5 minutes")

    transaction_count_df = transactions_with_watermark.groupBy(
        # This will be our Sliding window, the window size will be 5minutes and the slide interval will be 1 minute,
        # So 4 minutes those will keep overlapping again and again
        F.window("transaction_timestamp","5 minutes","1 minute")
    ).agg(
        F.count("*").alias("transaction_count")
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count")
    )
    return transaction_count_df

