from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name = "finguard.gold.transaction_features",
    comment = "Transaction enriched with fraud detection features like amount, country and time features"
)
# This feature checks whether the current amount is much higher than the cardholder's normal spending and adding a velocity feature to identify count of transaction in a short period
def transaction_features() -> DataFrame:

    transactions = (
        spark.readStream
        .table("finguard.silver.transactions")
        .alias("txn")
    )

    card_behaviour = (
        spark.read
        .table("finguard.silver.card_behaviour")
        .alias("history")
    )

    customer = (
        spark.read
        .table("finguard.silver.customers")
        .select(
            "card_number",
            "customer_id",
            F.col("country").alias("customer_origin_country"),
            F.col("city").alias("customer_origin_city")
        )
        .alias("customer")
    )

    # Stream and static join, joining the card behaviour and customers 
    enriched = (
        transactions
        .join(
            card_behaviour,
            on="card_number",
            how="left"
        )
        .join(
            customer,
            on="card_number",
            how="left"
        )
        .select(
            "txn.*",
            F.col("history.avg_amount_30d"),
            F.col("history.stddev_amount_30d"),
            F.col("customer.customer_id"),
            F.col("customer.customer_origin_country"),
            F.col("customer.customer_origin_city")
        )
    )

    # Rename transaction country
    enriched = enriched.withColumnRenamed(
        "country",
        "transaction_country"
    )

    result = (
        enriched
        .withColumn(
            "amount_difference",
            F.when(
                F.col("avg_amount_30d").isNotNull(),
                F.col("amount") - F.col("avg_amount_30d")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "amount_zscore",
            F.when(
                F.col("stddev_amount_30d") > 0,
                (
                    F.col("amount") - F.col("avg_amount_30d")
                ) / F.col("stddev_amount_30d")
            ).otherwise(F.lit(0.0))
        )
        .withColumn(
            "is_amount_anomaly",
            F.when(
                F.col("amount_zscore") >= 3.0,
                1
            ).otherwise(0)
        )
        .withColumn(
            "customer_data_available",
            F.when(
                F.col("customer_id").isNotNull(),
                1
            ).otherwise(0)
        )
        .withColumn(
            "is_country_mismatch",
            F.when(
                F.col("transaction_country").isNull()
                | F.col("customer_origin_country").isNull(),
                F.lit(None).cast("int")
            )
            .when(
                F.upper(F.trim(F.col("transaction_country"))) !=
                F.upper(F.trim(F.col("customer_origin_country"))),
                1
            )
            .otherwise(0)
        )
        .withColumn(
            "transaction_hour",
            F.hour(F.col("transaction_timestamp"))
        )
        .withColumn(
            "is_high_risk_hour",
            F.when(
                F.col("transaction_hour").between(0, 4),
                1
            ).otherwise(0)
        )
    )

    return result