"""Flatten raw GH Archive JSON into a typed, partitioned Parquet dataset."""

from __future__ import annotations

import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def flatten(df):
    return df.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.to_timestamp("created_at").alias("created_at"),
        F.col("actor.id").alias("actor_id"),
        F.col("actor.login").alias("actor_login"),
        F.col("repo.id").alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.col("org.login").alias("org_login"),
        F.to_json("payload").alias("payload_json"),
    ).withColumn("event_date", F.to_date("created_at"))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="e.g. gs://bucket/raw/gharchive/dt=2026-01-01/")
    parser.add_argument("--output", required=True, help="e.g. gs://bucket/staged/events/")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("oss-pulse-flatten-events").getOrCreate()
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    flatten(spark.read.json(args.input)).write.mode("overwrite").partitionBy(
        "event_date"
    ).parquet(args.output)

    spark.stop()


if __name__ == "__main__":
    main()
