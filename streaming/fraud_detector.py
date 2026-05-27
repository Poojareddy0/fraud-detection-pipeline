"""
PySpark Structured Streaming — Fraud Detection Engine.

Architecture:
  Event Hubs → parse → watermark → z-score anomaly detection
             → rolling window rules → enrich → Bronze write
             → bad records → dead-letter zone

Detection logic:
  1. Z-score on amount per customer (5-min rolling window) — flags statistical outliers
  2. High-risk country + card-not-present combination
  3. Rapid transaction velocity (>5 txns in 2-min window per customer)
  4. Amount threshold breach (>$2,000 card-not-present)
"""
from __future__ import annotations

import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
EVENTHUB_CONN_STR   = os.environ["EVENTHUB_CONNECTION_STRING"]
ADLS_ACCOUNT        = os.environ["ADLS_ACCOUNT_NAME"]
ADLS_KEY            = os.environ["ADLS_KEY"]
BRONZE_PATH         = f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/transactions"
DEAD_LETTER_PATH    = f"abfss://dead-letter@{ADLS_ACCOUNT}.dfs.core.windows.net/transactions"
CHECKPOINT_BRONZE   = f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/_checkpoints/transactions"
CHECKPOINT_DL       = f"abfss://dead-letter@{ADLS_ACCOUNT}.dfs.core.windows.net/_checkpoints/transactions"

HIGH_RISK_COUNTRIES = {"NG", "RU", "VN", "UA", "KP", "IR"}

# ── Schema ─────────────────────────────────────────────────────────────────────
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",      StringType(),    False),
    StructField("customer_id",         StringType(),    False),
    StructField("account_id",          StringType(),    False),
    StructField("amount",              DoubleType(),    False),
    StructField("merchant_id",         StringType(),    False),
    StructField("merchant_category",   StringType(),    False),
    StructField("merchant_country",    StringType(),    False),
    StructField("card_present",        BooleanType(),   False),
    StructField("transaction_ts",      TimestampType(), False),
    StructField("ip_address",          StringType(),    True),
    StructField("device_fingerprint",  StringType(),    True),
    StructField("latitude",            DoubleType(),    True),
    StructField("longitude",           DoubleType(),    True),
])


def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("fraud-detection-streaming")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_BRONZE)
        .config(f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net", ADLS_KEY)
        # Event Hubs connector config
        .config("spark.jars.packages",
                "com.microsoft.azure:azure-eventhubs-spark_2.12:2.3.22,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_event_hub_stream(spark: SparkSession):
    """Read raw bytes from Azure Event Hubs via the Kafka-compatible endpoint."""
    eh_conf = {
        "eventhubs.connectionString": spark.sparkContext._jvm.org.apache.spark
            .eventhubs.EventHubsUtils.encrypt(EVENTHUB_CONN_STR),
        "eventhubs.consumerGroup": "spark-consumer",
        "eventhubs.startingPosition": '{"offset": "-1", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}',
    }
    return (
        spark.readStream
        .format("eventhubs")
        .options(**eh_conf)
        .load()
    )


def parse_and_validate(raw_df):
    """
    Parse JSON payload from Event Hubs body.
    Split into valid records and dead-letter records (malformed JSON / schema mismatch).
    """
    parsed = (
        raw_df
        .select(F.from_json(F.col("body").cast("string"), TRANSACTION_SCHEMA).alias("data"))
    )

    # Valid: all required fields present and amount > 0
    valid = (
        parsed
        .filter(
            F.col("data.transaction_id").isNotNull() &
            F.col("data.customer_id").isNotNull() &
            F.col("data.amount").isNotNull() &
            (F.col("data.amount") > 0)
        )
        .select("data.*")
        .withColumn("ingested_at", F.current_timestamp())
    )

    # Dead-letter: anything that failed parsing
    dead_letter = (
        parsed
        .filter(F.col("data.transaction_id").isNull())
        .withColumn("raw_body", F.col("data").cast("string"))
        .withColumn("dl_reason", F.lit("parse_failure"))
        .withColumn("dl_ts", F.current_timestamp())
        .select("raw_body", "dl_reason", "dl_ts")
    )

    return valid, dead_letter


def apply_fraud_detection(valid_df):
    """
    Apply multi-rule fraud scoring. Each rule contributes a flag.
    A transaction is marked suspicious if ANY rule fires.

    Rules:
      1. z_score_flag    — amount z-score > 3.0 within 5-min customer window
      2. high_risk_flag  — high-risk country + card not present
      3. velocity_flag   — >5 transactions in 2-min window for same customer
      4. threshold_flag  — amount > $2,000 and card not present
    """
    watermarked = valid_df.withWatermark("transaction_ts", "10 minutes")

    # ── Rule 1: Z-score on rolling 5-min amount per customer ──────────────────
    window_5min = F.window("transaction_ts", "5 minutes")

    customer_stats = (
        watermarked
        .groupBy("customer_id", window_5min)
        .agg(
            F.avg("amount").alias("avg_amount"),
            F.stddev("amount").alias("std_amount"),
            F.count("*").alias("txn_count_5min"),
        )
    )

    enriched = (
        watermarked.alias("t")
        .join(
            customer_stats.alias("s"),
            on=(
                (F.col("t.customer_id") == F.col("s.customer_id")) &
                F.col("t.transaction_ts").between(
                    F.col("s.window.start"), F.col("s.window.end")
                )
            ),
            how="left",
        )
        .withColumn(
            "z_score",
            F.when(
                F.col("s.std_amount").isNotNull() & (F.col("s.std_amount") > 0),
                (F.col("t.amount") - F.col("s.avg_amount")) / F.col("s.std_amount"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn("z_score_flag", F.col("z_score") > 3.0)
    )

    # ── Rule 2: High-risk country + card not present ───────────────────────────
    high_risk_list = F.array([F.lit(c) for c in sorted(HIGH_RISK_COUNTRIES)])
    enriched = enriched.withColumn(
        "high_risk_flag",
        F.array_contains(high_risk_list, F.col("t.merchant_country")) &
        (~F.col("t.card_present")),
    )

    # ── Rule 3: Velocity — >5 txns per customer in 2 min ─────────────────────
    velocity_window = F.window("transaction_ts", "2 minutes")
    velocity_counts = (
        watermarked
        .groupBy("customer_id", velocity_window)
        .agg(F.count("*").alias("velocity_count"))
    )
    enriched = (
        enriched
        .join(
            velocity_counts.alias("v"),
            on=(
                (F.col("t.customer_id") == F.col("v.customer_id")) &
                F.col("t.transaction_ts").between(
                    F.col("v.window.start"), F.col("v.window.end")
                )
            ),
            how="left",
        )
        .withColumn("velocity_flag", F.col("v.velocity_count") > 5)
    )

    # ── Rule 4: High absolute amount + card not present ───────────────────────
    enriched = enriched.withColumn(
        "threshold_flag",
        (~F.col("t.card_present")) & (F.col("t.amount") > 2000.0),
    )

    # ── Composite fraud score ──────────────────────────────────────────────────
    enriched = enriched.withColumn(
        "fraud_score",
        (F.col("z_score_flag").cast("int") * 40) +
        (F.col("high_risk_flag").cast("int") * 30) +
        (F.col("velocity_flag").cast("int") * 20) +
        (F.col("threshold_flag").cast("int") * 10),
    )
    enriched = enriched.withColumn(
        "is_suspicious", F.col("fraud_score") >= 40
    )
    enriched = enriched.withColumn(
        "fraud_rules_fired",
        F.concat_ws(
            ",",
            F.when(F.col("z_score_flag"), F.lit("z_score")),
            F.when(F.col("high_risk_flag"), F.lit("high_risk_country")),
            F.when(F.col("velocity_flag"), F.lit("velocity")),
            F.when(F.col("threshold_flag"), F.lit("amount_threshold")),
        ),
    )

    return enriched.select(
        F.col("t.transaction_id"),
        F.col("t.customer_id"),
        F.col("t.account_id"),
        F.col("t.amount"),
        F.col("t.merchant_id"),
        F.col("t.merchant_category"),
        F.col("t.merchant_country"),
        F.col("t.card_present"),
        F.col("t.transaction_ts"),
        F.col("t.ip_address"),
        F.col("t.device_fingerprint"),
        F.col("t.latitude"),
        F.col("t.longitude"),
        F.col("z_score"),
        F.col("z_score_flag"),
        F.col("high_risk_flag"),
        F.col("velocity_flag"),
        F.col("threshold_flag"),
        F.col("fraud_score"),
        F.col("is_suspicious"),
        F.col("fraud_rules_fired"),
        F.col("t.ingested_at"),
    )


def main() -> None:
    spark = build_spark_session()
    raw = read_event_hub_stream(spark)
    valid, dead_letter = parse_and_validate(raw)
    scored = apply_fraud_detection(valid)

    # ── Write Bronze (all scored records) ─────────────────────────────────────
    bronze_query = (
        scored
        .writeStream
        .outputMode("append")
        .format("delta")
        .option("checkpointLocation", CHECKPOINT_BRONZE)
        .partitionBy("merchant_category")
        .start(BRONZE_PATH)
    )

    # ── Write Dead-letter zone ─────────────────────────────────────────────────
    dl_query = (
        dead_letter
        .writeStream
        .outputMode("append")
        .format("json")
        .option("checkpointLocation", CHECKPOINT_DL)
        .start(DEAD_LETTER_PATH)
    )

    logger.info("Streaming queries started. Bronze=%s | DeadLetter=%s",
                BRONZE_PATH, DEAD_LETTER_PATH)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
