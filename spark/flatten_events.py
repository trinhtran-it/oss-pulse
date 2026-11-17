"""Flatten one day of raw GH Archive events into typed Parquet.

    spark-submit spark/flatten_events.py --date 2026-10-01 \\
        --input  gs://BUCKET/raw/gharchive/dt=2026-10-01/ \\
        --output gs://BUCKET/processed/events

- Reads gzip NDJSON with an explicit schema (no inference): fast and stable across payload drift.
- Flattens the fields confirmed in docs/DATA_NOTES.md; keeps the raw payload as ``payload_json``.
- Derives event_date / event_hour (UTC) and repo_owner.
- Drops corrupt / invalid rows and rows outside --date, then de-duplicates on event_id.
- Overwrites exactly one partition: ``{output}/dt={date}/``. Other dates are never touched.
- Fails (exit 1) without writing if there is no input or too many bad rows, so a good partition
  is never replaced by a bad one.

Same code locally (file paths) and on Dataproc Serverless (gs:// paths).
Compatible with Spark 3.5 and 4.x (ANSI mode safe: unparseable values become NULL).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from typing import Any

from pyspark import StorageLevel
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

log = logging.getLogger("spark.flatten_events")

CORRUPT_COL = "_corrupt_record"

# Only the payload fields confirmed in docs/DATA_NOTES.md. Fields that disappeared upstream
# (e.g. size/distinct_size after ~Oct 2025) simply come back NULL for newer data.
PAYLOAD_SCHEMA = T.StructType(
    [
        T.StructField("action", T.StringType()),
        T.StructField("number", T.LongType()),
        T.StructField("ref", T.StringType()),
        T.StructField("ref_type", T.StringType()),
        T.StructField("size", T.LongType()),
        T.StructField("distinct_size", T.LongType()),
        T.StructField(
            "pull_request",
            T.StructType(
                [
                    T.StructField("number", T.LongType()),
                    T.StructField("merged", T.BooleanType()),
                ]
            ),
        ),
        T.StructField("issue", T.StructType([T.StructField("number", T.LongType())])),
        T.StructField("release", T.StructType([T.StructField("tag_name", T.StringType())])),
    ]
)


def _id_and(name: str) -> T.StructType:
    return T.StructType([T.StructField("id", T.LongType()), T.StructField(name, T.StringType())])


RAW_SCHEMA = T.StructType(
    [
        T.StructField("id", T.StringType()),  # string in the source; keep it that way
        T.StructField("type", T.StringType()),
        T.StructField("created_at", T.StringType()),  # parsed explicitly below (UTC)
        T.StructField("public", T.BooleanType()),
        T.StructField("actor", _id_and("login")),
        T.StructField("repo", _id_and("name")),
        T.StructField("org", _id_and("login")),
        # StringType on a JSON object makes Spark keep the object's raw JSON text.
        T.StructField("payload", T.StringType()),
        T.StructField(CORRUPT_COL, T.StringType()),
    ]
)

OUTPUT_COLUMNS = [
    "event_id",
    "event_type",
    "created_at",
    "event_date",
    "event_hour",
    "actor_id",
    "actor_login",
    "repo_id",
    "repo_name",
    "repo_owner",
    "org_id",
    "org_login",
    "is_public",
    "payload_action",
    "pr_number",
    "pr_merged",
    "issue_number",
    "push_size",
    "push_distinct_size",
    "ref",
    "ref_type",
    "release_tag",
    "payload_json",
    "source_file",
]


@dataclass
class Metrics:
    input_rows: int = 0
    corrupt_rows: int = 0  # not valid JSON
    invalid_rows: int = 0  # valid JSON but missing event_id / event_type / created_at
    out_of_range_rows: int = 0  # created_at not on --date
    duplicate_rows: int = 0
    output_rows: int = 0

    @property
    def bad_rows(self) -> int:
        return self.corrupt_rows + self.invalid_rows


# --- Spark ----------------------------------------------------------------------------------


def build_spark(app_name: str = "oss-pulse-flatten-events") -> SparkSession:
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")  # event_date/hour must not follow the VM tz
        # AQE merges small shuffle partitions; size-first so a day becomes a few ~128 MB files
        # instead of one small file per core.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", str(128 * 1024 * 1024))
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")  # BigQuery-friendly
        .getOrCreate()
    )
    spark.conf.set("spark.sql.session.timeZone", "UTC")  # also when the session already existed
    return spark


def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read gzip NDJSON (a file, a directory tree such as dt=.../hr=.../, or a glob)."""
    return (
        spark.read.schema(RAW_SCHEMA)
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", CORRUPT_COL)
        # Read hr=XX/ sub-directories as plain files, without adding an `hr` partition column.
        .option("recursiveFileLookup", "true")
        .json(path)
    )


def flatten(raw: DataFrame) -> DataFrame:
    """Raw events -> one flat, typed row per event (plus a private _is_corrupt flag)."""
    p = "_payload"
    return (
        raw.withColumn(p, F.from_json(F.col("payload"), PAYLOAD_SCHEMA))
        .select(
            F.col("id").alias("event_id"),
            F.col("type").alias("event_type"),
            F.try_to_timestamp(F.col("created_at")).alias("created_at"),
            F.col("actor.id").alias("actor_id"),
            F.col("actor.login").alias("actor_login"),
            F.col("repo.id").alias("repo_id"),
            F.col("repo.name").alias("repo_name"),
            F.col("org.id").alias("org_id"),
            F.col("org.login").alias("org_login"),
            F.col("public").alias("is_public"),
            F.col(f"{p}.action").alias("payload_action"),
            F.coalesce(F.col(f"{p}.number"), F.col(f"{p}.pull_request.number")).alias("pr_number"),
            F.col(f"{p}.pull_request.merged").alias("pr_merged"),  # before ~Oct 2025 only
            F.col(f"{p}.issue.number").alias("issue_number"),
            F.col(f"{p}.size").alias("push_size"),  # before ~Oct 2025 only
            F.col(f"{p}.distinct_size").alias("push_distinct_size"),  # before ~Oct 2025 only
            F.col(f"{p}.ref").alias("ref"),
            F.col(f"{p}.ref_type").alias("ref_type"),
            F.col(f"{p}.release.tag_name").alias("release_tag"),
            F.col("payload").alias("payload_json"),
            F.col("_metadata.file_path").alias("source_file"),
            F.col(CORRUPT_COL).isNotNull().alias("_is_corrupt"),
        )
        .withColumn("event_date", F.to_date("created_at"))
        .withColumn("event_hour", F.hour("created_at"))
        .withColumn("repo_owner", F.substring_index("repo_name", "/", 1))
    )


def _is_valid() -> Column:
    return (
        ~F.col("_is_corrupt")
        & F.col("event_id").isNotNull()
        & F.col("event_type").isNotNull()
        & F.col("created_at").isNotNull()
    )


def compute_metrics(flat: DataFrame, day: date) -> Metrics:
    """All counts in a single pass over ``flat``."""
    valid = _is_valid()
    in_range = valid & (F.col("event_date") == F.lit(day))

    def count_if(cond: Column) -> Column:
        return F.sum(F.when(cond, 1).otherwise(0))

    row = flat.agg(
        F.count(F.lit(1)).alias("input_rows"),
        count_if(F.col("_is_corrupt")).alias("corrupt_rows"),
        count_if(~F.col("_is_corrupt") & ~valid).alias("invalid_rows"),
        count_if(valid & ~in_range).alias("out_of_range_rows"),
        count_if(in_range).alias("in_range_rows"),
        F.count_distinct(F.when(in_range, F.col("event_id"))).alias("output_rows"),
    ).first()

    stats = {k: int(v or 0) for k, v in row.asDict().items()}
    return Metrics(
        input_rows=stats["input_rows"],
        corrupt_rows=stats["corrupt_rows"],
        invalid_rows=stats["invalid_rows"],
        out_of_range_rows=stats["out_of_range_rows"],
        duplicate_rows=stats["in_range_rows"] - stats["output_rows"],
        output_rows=stats["output_rows"],
    )


def clean(flat: DataFrame, day: date) -> DataFrame:
    """Keep valid rows for ``day``, one row per event_id, in the published column order."""
    return (
        flat.where(_is_valid() & (F.col("event_date") == F.lit(day)))
        .dropDuplicates(["event_id"])
        .select(*OUTPUT_COLUMNS)
    )


def partition_path(output: str, day: date) -> str:
    return f"{output.rstrip('/')}/dt={day.isoformat()}"


def write_partition(df: DataFrame, output: str, day: date) -> str:
    """Overwrite exactly ``{output}/dt={day}/``; sibling dates are untouched."""
    path = partition_path(output, day)
    df.write.mode("overwrite").option("compression", "snappy").parquet(path)
    return path


def check_quality(
    m: Metrics, max_bad_fraction: float, max_out_of_range_fraction: float = 0.01
) -> str | None:
    """Return a reason to abort before writing, or None if the data is acceptable."""
    if m.input_rows == 0:
        return "no input rows (was ingestion run for this date?)"
    if m.output_rows == 0:
        return "no valid rows for this date"
    bad_fraction = m.bad_rows / m.input_rows
    if bad_fraction > max_bad_fraction:
        return (
            f"bad row fraction {bad_fraction:.4%} exceeds --max-bad-fraction {max_bad_fraction:.4%}"
        )
    # Many rows outside --date usually means a time-zone bug or a wrong --input path, not bad
    # source data: dropping them silently would lose a large slice of the day.
    out_of_range_fraction = m.out_of_range_rows / m.input_rows
    if out_of_range_fraction > max_out_of_range_fraction:
        return (
            f"out-of-range fraction {out_of_range_fraction:.4%} exceeds "
            f"--max-out-of-range-fraction {max_out_of_range_fraction:.4%} "
            "(time-zone bug or wrong --input?)"
        )
    return None


# --- Logging --------------------------------------------------------------------------------


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(getattr(record, "fields", {}))
        return json.dumps(payload, default=str)


def configure_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    log.handlers[:] = [handler]
    log.setLevel(logging.INFO)
    log.propagate = False


def _log(level: int, msg: str, **fields: Any) -> None:
    log.log(level, msg, extra={"fields": fields})


# --- CLI ------------------------------------------------------------------------------------


def _date_arg(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid date {text!r}, expected YYYY-MM-DD") from None


def _fraction_arg(text: str) -> float:
    try:
        value = float(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected a number, got {text!r}") from None
    if not 0.0 <= value <= 1.0:
        raise argparse.ArgumentTypeError("must be between 0 and 1")
    return value


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flatten one day of GH Archive events.")
    parser.add_argument("--date", required=True, type=_date_arg, help="UTC day, YYYY-MM-DD")
    parser.add_argument(
        "--input", required=True, help="raw files: file, dir or glob (local or gs://)"
    )
    parser.add_argument(
        "--output", required=True, help="dataset root; writes {output}/dt={date}/ (local or gs://)"
    )
    parser.add_argument(
        "--max-bad-fraction",
        type=_fraction_arg,
        default=0.01,
        help="abort if corrupt+invalid rows exceed this share of input (default: 0.01)",
    )
    parser.add_argument(
        "--max-out-of-range-fraction",
        type=_fraction_arg,
        default=0.01,
        help="abort if rows dated outside --date exceed this share of input (default: 0.01)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None, spark: SparkSession | None = None) -> int:
    args = parse_args(argv)
    configure_logging()
    start = time.monotonic()
    own_session = spark is None
    spark = spark or build_spark()

    try:
        _log(
            logging.INFO,
            "flatten started",
            event="flatten_start",
            date=args.date.isoformat(),
            input=args.input,
            output=args.output,
            spark_version=spark.version,
        )
        flat = flatten(read_raw(spark, args.input)).persist(StorageLevel.MEMORY_AND_DISK)
        metrics = compute_metrics(flat, args.date)
        summary = {"date": args.date.isoformat(), **asdict(metrics)}

        reason = check_quality(metrics, args.max_bad_fraction, args.max_out_of_range_fraction)
        if reason:
            _log(logging.ERROR, f"aborted: {reason}", event="flatten_aborted", **summary)
            return 1

        path = write_partition(clean(flat, args.date), args.output, args.date)
        _log(
            logging.INFO,
            f"{metrics.output_rows} rows written ({metrics.duplicate_rows} duplicates removed)",
            event="flatten_summary",
            output_path=path,
            duration_s=round(time.monotonic() - start, 1),
            **summary,
        )
        return 0
    finally:
        if own_session:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())
