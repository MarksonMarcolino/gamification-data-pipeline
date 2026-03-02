"""
AWS Glue ETL Job: Streaks & Bestie Points — Raw to Curated Layer

This PySpark job runs on an hourly schedule and performs:
  1. Read new raw events from S3 (partitioned by date/hour)
  2. Deduplicate using idempotency_key
  3. Validate and enforce schema
  4. Enrich with user dimension data
  5. Compute daily aggregates
  6. Write curated data to S3 in Parquet format
  7. Trigger Redshift COPY via bookmark

Input:  s3://luzia-data-lake/raw/year=YYYY/month=MM/day=DD/hour=HH/
Output: s3://luzia-data-lake/curated/bp_events/ (deduplicated events)
        s3://luzia-data-lake/curated/daily_summaries/ (aggregated)
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# ── Glue job setup ──────────────────────────────────────────────────────────

args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "RAW_PATH",
    "CURATED_PATH",
    "USER_DIM_PATH",
])

sc = SparkContext()
glue_context = GlueContext(sc)
spark = glue_context.spark_session
job = Job(glue_context)
job.init(args["JOB_NAME"], args)

RAW_PATH = args["RAW_PATH"]            # s3://luzia-data-lake/raw/
CURATED_PATH = args["CURATED_PATH"]    # s3://luzia-data-lake/curated/
USER_DIM_PATH = args["USER_DIM_PATH"]  # s3://luzia-data-lake/dimensions/users/


# ── Step 1: Read raw events ────────────────────────────────────────────────

raw_df = (
    spark.read.parquet(RAW_PATH)
    .filter(F.col("event_type").isNotNull())
    .filter(F.col("user_id").isNotNull())
)

print(f"Raw records read: {raw_df.count()}")


# ── Step 2: Deduplicate ────────────────────────────────────────────────────

# Use window function: keep the earliest record per idempotency_key
dedup_window = Window.partitionBy("idempotency_key").orderBy(F.col("created_at").asc())

deduped_df = (
    raw_df
    .withColumn("_row_num", F.row_number().over(dedup_window))
    .filter(F.col("_row_num") == 1)
    .drop("_row_num")
)

duplicates_removed = raw_df.count() - deduped_df.count()
print(f"Duplicates removed: {duplicates_removed}")


# ── Step 3: Schema validation & type casting ───────────────────────────────

validated_df = (
    deduped_df
    .withColumn("points_awarded", F.col("points_awarded").cast("integer"))
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("ttl", F.col("ttl").cast("long"))
    # Extract date partition from created_at
    .withColumn("event_date", F.to_date("created_at"))
    # Flag anomalous records (points outside expected range)
    .withColumn(
        "is_anomalous",
        F.when(
            (F.col("points_awarded") < 0) | (F.col("points_awarded") > 100),
            F.lit(True),
        ).otherwise(F.lit(False)),
    )
)

# Separate anomalous records for investigation
anomalies_df = validated_df.filter(F.col("is_anomalous"))
clean_df = validated_df.filter(~F.col("is_anomalous")).drop("is_anomalous")

if anomalies_df.count() > 0:
    print(f"Anomalous records flagged: {anomalies_df.count()}")
    anomalies_df.write.mode("append").partitionBy("event_date").parquet(
        f"{CURATED_PATH}/anomalies/"
    )


# ── Step 4: Enrich with user dimensions ────────────────────────────────────

user_dim_df = spark.read.parquet(USER_DIM_PATH).select(
    "user_id", "country", "signup_date", "user_segment"
)

enriched_df = clean_df.join(user_dim_df, on="user_id", how="left")


# ── Step 5: Compute daily summaries ────────────────────────────────────────

daily_summaries_df = (
    enriched_df
    .groupBy("user_id", "event_date", "country", "user_segment")
    .agg(
        F.count("*").alias("total_events"),
        F.sum("points_awarded").alias("total_points"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.min("created_at").alias("first_event_at"),
        F.max("created_at").alias("last_event_at"),
        F.collect_set("event_type").alias("event_types"),
    )
    .withColumn("event_types", F.concat_ws(",", "event_types"))
)


# ── Step 6: Write curated data ─────────────────────────────────────────────

# Deduplicated events — partitioned by date for efficient Redshift COPY
enriched_df.drop("is_anomalous").write.mode("append").partitionBy("event_date").parquet(
    f"{CURATED_PATH}/bp_events/"
)

# Daily summaries — one row per user per day
daily_summaries_df.write.mode("append").partitionBy("event_date").parquet(
    f"{CURATED_PATH}/daily_summaries/"
)

print(f"Curated events written: {enriched_df.count()}")
print(f"Daily summaries written: {daily_summaries_df.count()}")


# ── Done ───────────────────────────────────────────────────────────────────

job.commit()
print("ETL job completed successfully.")
