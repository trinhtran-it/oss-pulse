"""Reconcile Flink's 5-minute windows in Postgres with a recount from the Kafka topic.

For the last N closed windows: count distinct event ids per window straight from
`wiki.recentchange` and compare with the sum Flink wrote. A gap is what the job dropped:
events that arrived more than 1 minute after their window (the watermark), or a real bug.

    make stream-check [WINDOWS=6]

Reads Kafka from a timestamp (offsets_for_times), not from the beginning, so it only scans the
checked time range. Needs the stream stack running (127.0.0.1:19092 and 127.0.0.1:5433).
Exit code 1 if any window differs by more than --tolerance (default 1%).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta

WINDOW = timedelta(minutes=5)
SLACK = timedelta(minutes=10)  # read a bit before/after: produce time != event time


def floor_window(ts: datetime) -> datetime:
    return ts - timedelta(minutes=ts.minute % 5, seconds=ts.second, microseconds=ts.microsecond)


def recount(records, first: datetime, last_end: datetime) -> dict[datetime, set[str]]:
    """Distinct event ids per window start, for windows in [first, last_end)."""
    ids: dict[datetime, set[str]] = defaultdict(set)
    for rec in records:
        ts = datetime.fromisoformat(rec["event_time"].replace("Z", "+00:00")).astimezone(UTC)
        start = floor_window(ts.replace(tzinfo=None))
        if first <= start < last_end:
            ids[start].add(rec["event_id"])
    return ids


def flink_windows(dsn: dict, n: int) -> dict[datetime, int]:
    import psycopg

    with psycopg.connect(**dsn) as conn:
        rows = conn.execute(
            "select window_start, edits from wiki_edits_5min_total"
            " order by window_start desc limit %s",
            (n + 1,),
        ).fetchall()
    # drop the newest window: it may still be receiving late rows from Flink's buffer
    return {start: int(edits) for start, edits in sorted(rows)[:-1]} if len(rows) > 1 else {}


def read_topic(bootstrap: str, topic: str, start: datetime, end: datetime):
    from confluent_kafka import Consumer, TopicPartition

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "group.id": f"stream-check-{os.getpid()}",  # throwaway group, commits nothing
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
        start_ms = int(start.replace(tzinfo=UTC).timestamp() * 1000)
        end_ms = int(end.replace(tzinfo=UTC).timestamp() * 1000)
        wanted = [TopicPartition(topic, p, start_ms) for p in partitions]
        offsets = consumer.offsets_for_times(wanted, timeout=10)
        consumer.assign(offsets)
        done: set[int] = set()
        while len(done) < len(partitions):
            msg = consumer.poll(5.0)
            if msg is None:
                break  # caught up with the end of the topic
            if msg.error():
                continue
            if msg.timestamp()[1] > end_ms:
                done.add(msg.partition())
                continue
            yield json.loads(msg.value())
    finally:
        consumer.close()


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--windows", type=int, default=6, help="closed windows to check")
    p.add_argument("--tolerance", type=float, default=0.01, help="max relative gap per window")
    args = p.parse_args(argv)

    dsn = {
        "host": "127.0.0.1",
        "port": 5433,
        "dbname": "streaming",
        "user": os.environ.get("STREAM_PG_USER", "flink"),
        "password": os.environ.get("STREAM_PG_PASSWORD", "flink"),
    }
    flink = flink_windows(dsn, args.windows)
    if not flink:
        print("No closed windows in Postgres yet: wait ~7 minutes after `make stream-up`.")
        return 1
    first, last = min(flink), max(flink)
    records = read_topic(
        os.environ.get("KAFKA_BOOTSTRAP", "127.0.0.1:19092"),
        os.environ.get("KAFKA_TOPIC", "wiki.recentchange"),
        first - SLACK,
        last + WINDOW + SLACK,
    )
    topic = {start: len(ids) for start, ids in recount(records, first, last + WINDOW).items()}

    print(f"{'window (UTC)':<18}{'topic':>9}{'flink':>9}{'gap':>8}{'gap %':>8}")
    worst = 0.0
    for start in sorted(flink):
        expected, got = topic.get(start, 0), flink[start]
        gap = expected - got
        pct = gap / expected if expected else 0.0
        worst = max(worst, abs(pct))
        print(f"{start:%Y-%m-%d %H:%M}  {expected:>9,}{got:>9,}{gap:>8,}{pct:>8.2%}")
    print(f"worst gap {worst:.2%} (tolerance {args.tolerance:.0%})")
    return 1 if worst > args.tolerance else 0


if __name__ == "__main__":
    sys.exit(main())
