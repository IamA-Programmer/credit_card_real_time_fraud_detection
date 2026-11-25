from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.dataframe import DataFrame

@dp.table(name = "finguard.silver.customers_changes", comment = "Parsed and cleaned customers table")
@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")
def customers_changes() -> DataFrame:
    # Though it is batch ingestion we need to read that as Stream, because to take incremental part automatically
    bronze_df = spark.readStream.table("finguard.bronze.customers")

    transformed_df = bronze_df.select(
        F.col("customer_id"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("gender"),
        F.col("age"),
        F.col("city"),
        F.col("state"),
        F.col("country"),
        F.col("annual_income"),
        F.col("customer_segment"),
        F.to_date(F.col("account_open_date"), "yyyy-MM-dd").alias("account_open_date"),
        F.col("risk_score"),
        F.col("preferred_spending_min"),
        F.col("preferred_spending_max"),
        F.col("preferred_city"),
        F.col("preferred_country"),
        F.col("trusted_device_id"),
        F.col("card_number"),
        F.col("card_type"),
        # F.col("email"),
        F.lit("kishore.l.contact@gmail.com").alias("email"),
        F.col("transaction_limit"),
        F.col("update_timestamp"),
        F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    return transformed_df

# Step:2 - CREATE FINAL SILVER CUSTOMER TABLE

# This the final table that will contain the latest version of each customer
# We don't have function here because AUTO CDC will populate and maintain this table
dp.create_streaming_table(
    name = "finguard.silver.customers",
    comment = "Latest customer state maintained using AUTO CDC"
)

# STEP:3 - APPLY CDC

dp.create_auto_cdc_flow(
    # Final table where we want the latest customers and historical record
    target = "finguard.silver.customers",

    # From which source we are reading the customer table, we are reading from silver customer changes table
    source = "finguard.silver.customers_changes",

    # Unique Identifier for a customer
    keys = ["customer_id"],

    # Decides which customer record is newer
    sequence_by = F.col("update_timestamp"),

    # SCD Type 2 means keeps the latest record by stored the historical records as well
    stored_as_scd_type = 2

)

