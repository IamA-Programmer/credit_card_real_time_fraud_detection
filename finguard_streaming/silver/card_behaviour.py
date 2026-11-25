from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name = "finguard.silver.card_behaviour",
    comment = "Historical spending behaviour for each card"
)
def card_behaviour() -> DataFrame:
    # only read the table, don't read the stream table
    df = spark.read.table("finguard.silver.transactions")

    transactions = df.filter(
        # Taking the transaction happened since last 30 days
        F.col("transaction_timestamp") >= F.current_timestamp() - F.expr("INTERVAL 30 DAYS")
    ).group_by("card_number")
    .agg(
        F.avg("amount").alias("avg_amount_30d"),
        F.stddev("amount").alias("stddev_amount_30d"),
        F.max("amount").alias("max_amount_30d"),
        F.min("amount").alias("min_amount_30d"),
        F.count("*").alias("txn_count_30d")
    )

    return transactions

