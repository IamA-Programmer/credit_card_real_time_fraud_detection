from pyspark import pipelines as dp
from pyspark.sql.dataframe import DataFrame
import json
from pyspark.sql.functions import col
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, BooleanType, TimestampType

@dp.table(name = "finguard.silver.invalid_transactions",comment = "Unparsed and invalid transactions data, basically these data's are failed in the schema validation process. Transactions that failed in schema/JSON parsing")
@dp.expect_or_drop("valid_transaction_id","transaction_id IS NOT NULL")
@dp.expect_or_drop("valid_customer_id","customer_id IS NOT NULL")
@dp.expect_or_drop("valid_card_number","card_number IS NOT NULL")
@dp.expect_or_drop("valid_merchant_id","merchant_id IS NOT NULL")
@dp.expect("valid_amount","amount > 0")
def silver_invalid_transactions() -> DataFrame:
    bronze_df = spark.readStream.table("finguard.bronze.transactions")

    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("card_number", StringType(), True),
        StructField("merchant_id", StringType(), True),
        StructField("merchant_name", StringType(), True),
        StructField("merchant_category", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("payment_channel", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("transaction_timestamp", TimestampType(), True),
        StructField("is_international", BooleanType(), True),
        StructField("status", StringType(), True)

        # Capture the malformed type
        StructField("corrupted_record",StringType(), True)
    ])

    # If there are no data, in the bronze streaming the pipeline will be moves into the idle state but still the pipeline will be running

    transformed_df = bronze_df.select(
        # Take each key,value and assign the data type for each value and create each key,value pair as a column
        # By Default from_json operates in PERMISSIVE mode, Malformed/incompatible data can result in null rather than automatically stopping your pipeline.
        # The columnNameofCorruptRecord does the schema validation and if the data doesn't satisfy the schema it will be stored in the corrupted_record column (Quarantine table)
        F.from_json(col("value"), schema,{"mode":"PERMISSIVE", "columnNameofCorruptRecord":"corrupted_record"}).alias("data"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.col("ingestion_timestamp").alias("bronze_ingestion_timestamp")
    ).select(
        # select all the columns, we use *
        F.col("data.*"),
        F.col("kafka_topic"),
        F.col("kafka_partition"),
        F.col("kafka_offset"),
        F.col("kafka_timestamp"),
        F.col("bronze_ingestion_timestamp"),
        F.current_timestamp().alias("silver_ingestion_timestamp")
    )

    invalid_transactions = transformed_df.filter(F.col("corrupted_record").isNotNull())

    return invalid_transactions
