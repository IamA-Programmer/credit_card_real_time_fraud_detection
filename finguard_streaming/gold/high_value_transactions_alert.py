from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F

@dp.table(
    name = "finguard.gold.high_value_transactions_alert", comment = "Alert customer if there is high value transactions or when the rule based fraud score is greater than or equal to 2"
)
def high_value_transacions_alert() -> DataFrame:

    transactions = spark.readStream.table("finguard.gold.transaction_features").alias("txn")

    velocity = spark.read.table("finguard.gold.transaction_count_by_minute").alias("velocity")

    # In this place we won't read the customers in streaming table, because to join both the streaming tables
    # we need to do some additional changes
    customers = spark.read.table("finguard.silver.customers")

    transaction_with_velocity = (
        transactions.join(
            velocity,(
                (
                    F.col("txn.card_number") == F.col("velocity.card_number")
                )
                &
                (
                    F.col("txn.transaction_timestamp") >= F.col("velocity.window_start")
                )
                &
                (
                    F.col("txn.transaction_timestamp") < F.col("velocity.window_end")
                )
            ),
            "left"
        )
        .select(
            "txn.*",
            F.coelesce(
                F.col("velocity.transaction_count"),
                F.lit(1)
            ).alias("transaction_count"),

            F.coelesce(
                F.col("velocity.total_amount"),
                F.col("txn.amount")
            ).alias("total_amount_5min"),

            F.coelesce(
                F.col("velocity.is_high_velocity"),
                F.lit(0)
            ).alias("is_high_velocity")
            
        )
    )
    transaction_with_fraud_score = (
        transaction_with_velocity
        .withColumn(
            "rule_fraud_score",

            F.coelesce(
                F.col("is_amount_anamoly"),
                F.lit(0)
            )
            + F.coelesce(
                F.col("is_country_mismatch"),
                F.lit(0)
            )
            + F.coelesce(
                F.col("is_high_velocity"),
                F.lit(0)
            )
            + F.coelesce(
                F.col("is_high_risk_hour"),
                F.lit(0)
            )
        )
    )

    # Join the scored transaction with customers
    transactions_with_customer = (
        transaction_with_fraud_score.alias("txn")
        .join(
            customers,
            F.col("txn.customer_id") == F.col("customer.customer_id"),
            "left"
        )
    )

    # Reusable condition
    high_value_condition = (
        F.col("txn.amount") > F.col("customer.transaction_limit")
    )

    fraud_features_condition = (
        F.col("txn.rule_fraud_score") >= F.lit(2)
    )

    # Apply the OR condition
    joined_df = (
        transaction_with_customer.filter(high_value_condition | fraud_features_condition)
        .select(
            F.concat_ws(
                "-",
                F.lit("ALERT"),
                F.col("txn.transaction_id")
            ).alias("alert_id"),

            # Identify why the alert was generated.
            F.when(
                high_value_condition & fraud_features_condition,
                F.lit("HIGH_VALUE_AND_FRAUD_FEATURES")
            )
            .when(
                high_value_condition,
                F.lit("HIGH_VALUE_TRANSACTION")
            )
            .otherwise(
                F.lit("FRAUD_FEATURES_DETECTED")
            )
            .alias("alert_type"),

            F.current_timestamp().alias("alert_timestamp"),

            # Every row in this table is suspicious because
            # normal transactions were removed by the filter.
            F.lit("SUSPICIOUS").alias("rule_fraud_prediction"),

            F.col("txn.rule_fraud_score"),

            # Transaction and customer information
            F.col("txn.transaction_id"),
            F.col("txn.customer_id"),
            F.col("txn.card_number"),

            F.col("customer.email").alias("customer_email_id"),

            F.concat_ws(
                " ",
                F.col("customer.first_name"),
                F.col("customer.last_name")
            ).alias("customer_name"),

            F.col("txn.amount").alias("transaction_amount"),
            F.col("customer.transaction_limit"),

            # Velocity information
            F.col("txn.transaction_count"),
            F.col("txn.total_amount_5min"),

            # Fraud feature columns
            F.coalesce(
                F.col("txn.is_amount_anomaly"),
                F.lit(0)
            ).alias("is_amount_anomaly"),

            F.coalesce(
                F.col("txn.is_country_mismatch"),
                F.lit(0)
            ).alias("is_country_mismatch"),

            F.coalesce(
                F.col("txn.is_high_velocity"),
                F.lit(0)
            ).alias("is_high_velocity"),

            F.coalesce(
                F.col("txn.is_high_risk_hour"),
                F.lit(0)
            ).alias("is_high_risk_hour"),

            # Remaining transaction information
            F.col("txn.currency"),
            F.col("txn.merchant_name"),
            F.col("txn.merchant_category"),
            F.col("txn.transaction_type"),
            F.col("txn.payment_channel"),
            F.col("txn.city"),
            F.col("txn.country"),
            F.col("txn.is_international"),
            F.col("txn.transaction_timestamp"),
            F.col("txn.status")
        )
    )
    return joined_df

















