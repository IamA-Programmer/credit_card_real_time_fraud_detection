from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

# Here we are taking the transaction count came in a minute, so we will be using window.
# There are two types of window - Tumbling window and Sliding window
# Tumbling window means fixed window and sliding window means fixed window + slide interval
@dp.table(
    name = "finguard.gold.transaction_count_by_minute", comment = "Identifying the numbers of transactions in a minute"
)
def transaction_count_by_minute() -> DataFrame:

    transactions = spark.readStream.table("finguard.silver.transactions")
    # So that we can tell it how much late data we are allowing it, so it will wait for this time and will do the aggregation
    transactions_with_watermark = transactions.withWatermark("transaction_timestamp","5 minutes")

    transaction_count_df = transactions_with_watermark.groupBy(
        # This will be our Tumbling window, so for every 1min it will count the transactions
        F.window("transaction_timestamp","1 minute"),
        F.col("card_number")
    ).agg(
        F.count("*").alias("transaction_count"),
        F.sum("amount").alias("total_amount")
    ).withColumn(
        "is_high_velocity",
        F.when(
            F.col("transaction_count") >= 5,
            1
        ).otherwise(0)
    ).
    select(
        F.col("card_number"),
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        F.col("transaction_count")
    )
    return transaction_count_df

