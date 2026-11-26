"""Wikimedia EventStreams (recentchange, SSE) -> Redpanda topic `wiki.recentchange`.

Every edit or page creation on any Wikimedia wiki (~20-50 events/s) is reduced to a small JSON
record, keyed by wiki, and produced to Kafka (Redpanda). Flink aggregates it in 5-minute windows.

Delivery and recovery:
* SSE `id:` lines carry the stream's Kafka offsets. The latest one is saved to STATE_FILE; on a
  reconnect or restart it is sent back as Last-Event-ID, so the stream resumes where it stopped
  (no gap). Events in flight may be sent twice: at-least-once. Flink counts DISTINCT event ids per
  window, so a replayed event never changes a result.
* Kafka producer: acks=all + idempotence (no duplicates from producer retries), zstd, small linger.
* Reconnects with exponential backoff; SIGTERM/SIGINT flush the producer and save the offset.

Privacy: editor names (and the IP addresses of anonymous editors) are never stored. They are
replaced by a salted SHA-256 (`editor_hash`), enough to count distinct editors.

Env: KAFKA_BOOTSTRAP (default 127.0.0.1:19092), KAFKA_TOPIC (wiki.recentchange),
STREAM_URL, STREAM_USER_AGENT, STATE_FILE, EDITOR_HASH_SALT, LOG_EVERY_S.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import signal
import sys
import time
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

log = logging.getLogger("streaming.wiki_producer")

STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia asks clients to identify themselves. Kept generic here; set STREAM_USER_AGENT to add
# your own contact details if you run this for real.
DEFAULT_USER_AGENT = "oss-pulse-streaming/0.1 (data engineering demo)"
KEPT_TYPES = frozenset({"edit", "new"})  # skip log, categorize and external entries


# --- Server-Sent Events ---------------------------------------------------------------------


@dataclass(frozen=True)
class SseEvent:
    id: str | None
    event: str
    data: str


def parse_sse(lines: Iterable[str]) -> Iterator[SseEvent]:
    """Minimal SSE parser (WHATWG): fields until a blank line; ':' lines are comments."""
    event_id: str | None = None
    event_type = "message"
    data: list[str] = []
    for raw in lines:
        line = raw.rstrip("\r")
        if line == "":
            if data:
                yield SseEvent(event_id, event_type, "\n".join(data))
            event_type, data = "message", []
            continue  # the last id carries over to the next event, as the spec says
        if line.startswith(":"):
            continue
        name, _, value = line.partition(":")
        value = value[1:] if value.startswith(" ") else value
        if name == "data":
            data.append(value)
        elif name == "id":
            event_id = value
        elif name == "event":
            event_type = value
    if data:  # stream ended without the final blank line
        yield SseEvent(event_id, event_type, "\n".join(data))


# --- Records --------------------------------------------------------------------------------


def editor_hash(user: str | None, salt: str) -> str | None:
    if not user:
        return None
    return hashlib.sha256(f"{salt}:{user}".encode()).hexdigest()[:16]


def to_record(change: Mapping[str, Any], salt: str) -> dict[str, Any] | None:
    """The fields Flink needs, or None for change types we don't count."""
    if change.get("type") not in KEPT_TYPES:
        return None
    meta = change.get("meta") or {}
    if not meta.get("id") or not meta.get("dt") or not change.get("wiki"):
        return None
    length = change.get("length") or {}
    return {
        "event_id": meta["id"],
        "event_time": meta["dt"],  # ISO 8601 UTC, e.g. 2026-11-25T10:00:00Z
        "wiki": change["wiki"],
        "domain": meta.get("domain"),
        "change_type": change["type"],
        "is_bot": bool(change.get("bot")),
        "is_minor": bool(change.get("minor")),
        "namespace": change.get("namespace"),
        "editor_hash": editor_hash(change.get("user"), salt),
        "bytes_delta": (length.get("new") or 0) - (length.get("old") or 0),
    }


# --- Resume position ------------------------------------------------------------------------


class OffsetStore:
    """Persists the last SSE id (the stream's Kafka offsets) for Last-Event-ID resumes."""

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> str | None:
        try:
            text = self.path.read_text().strip()
        except FileNotFoundError:
            return None
        return text or None

    def save(self, last_event_id: str) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(last_event_id)
        tmp.replace(self.path)  # atomic: a crash never leaves a half-written offset


def backoff(attempt: int, cap: float = 60.0) -> float:
    ceiling = min(cap, 2.0**attempt)
    return ceiling / 2 + random.random() * ceiling / 2


# --- Producer loop --------------------------------------------------------------------------


@dataclass
class Stats:
    received: int = 0
    produced: int = 0
    skipped: int = 0
    bad_json: int = 0
    delivery_errors: int = 0
    reconnects: int = 0
    last_event_time: str | None = None
    started: float = field(default_factory=time.monotonic)

    def as_log(self) -> dict[str, Any]:
        lag = None
        if self.last_event_time:
            event_ts = datetime.fromisoformat(self.last_event_time.replace("Z", "+00:00"))
            lag = round((datetime.now(UTC) - event_ts).total_seconds(), 1)
        elapsed = max(time.monotonic() - self.started, 1e-9)
        return {
            "received": self.received,
            "produced": self.produced,
            "skipped": self.skipped,
            "bad_json": self.bad_json,
            "delivery_errors": self.delivery_errors,
            "reconnects": self.reconnects,
            "events_per_s": round(self.received / elapsed, 1),
            "source_lag_s": lag,
        }


class Stop(Exception):
    """Raised from the signal handler to leave the stream loop."""


def make_producer(bootstrap: str) -> Any:
    from confluent_kafka import Producer

    return Producer(
        {
            "bootstrap.servers": bootstrap,
            "acks": "all",
            "enable.idempotence": True,  # broker drops duplicates caused by retries
            "compression.type": "zstd",
            "linger.ms": 50,
            "client.id": "wiki-producer",
        }
    )


def run(config: Mapping[str, str]) -> int:
    topic = config.get("KAFKA_TOPIC", "wiki.recentchange")
    salt = config.get("EDITOR_HASH_SALT", "oss-pulse")
    log_every = float(config.get("LOG_EVERY_S", "30"))
    store = OffsetStore(Path(config.get("STATE_FILE", "/state/last_event_id")))
    producer = make_producer(config.get("KAFKA_BOOTSTRAP", "127.0.0.1:19092"))
    stats = Stats()

    def on_delivery(err: Any, _msg: Any) -> None:
        if err is not None:
            stats.delivery_errors += 1
            log.error(json.dumps({"event": "delivery_error", "error": str(err)}))

    def stop(signum: int, _frame: Any) -> None:
        raise Stop(signal.Signals(signum).name)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    last_id = store.load()
    attempt = 0
    next_log = time.monotonic() + log_every
    try:
        while True:
            headers = {
                "Accept": "text/event-stream",
                "User-Agent": config.get("STREAM_USER_AGENT", DEFAULT_USER_AGENT),
            }
            if last_id:
                headers["Last-Event-ID"] = last_id
            try:
                with requests.get(
                    config.get("STREAM_URL", STREAM_URL),
                    headers=headers,
                    stream=True,
                    timeout=(10, 60),
                ) as resp:
                    resp.raise_for_status()
                    log.info(json.dumps({"event": "connected", "resumed": bool(last_id)}))
                    attempt = 0
                    lines = resp.iter_lines(decode_unicode=True)
                    for sse in parse_sse(line or "" for line in lines):
                        stats.received += 1
                        try:
                            change = json.loads(sse.data)
                        except json.JSONDecodeError:
                            stats.bad_json += 1
                            continue
                        record = to_record(change, salt)
                        if record is None:
                            stats.skipped += 1
                        else:
                            producer.produce(
                                topic,
                                key=record["wiki"],
                                value=json.dumps(record, separators=(",", ":")),
                                on_delivery=on_delivery,
                            )
                            stats.produced += 1
                            stats.last_event_time = record["event_time"]
                        producer.poll(0)  # serve delivery callbacks
                        if sse.id:
                            last_id = sse.id
                        if time.monotonic() >= next_log:
                            producer.flush(10)  # offsets saved only after the broker has the data
                            if last_id:
                                store.save(last_id)
                            log.info(json.dumps({"event": "progress", **stats.as_log()}))
                            next_log = time.monotonic() + log_every
            except (requests.RequestException, ValueError) as exc:
                attempt += 1
                stats.reconnects += 1
                wait = backoff(attempt)
                log.warning(
                    json.dumps(
                        {"event": "reconnect", "error": str(exc)[:200], "wait_s": round(wait, 1)}
                    )
                )
                time.sleep(wait)
    except Stop as reason:
        log.info(json.dumps({"event": "stopping", "signal": str(reason)}))
    finally:
        remaining = producer.flush(30)
        if last_id and remaining == 0:
            store.save(last_id)
        log.info(json.dumps({"event": "stopped", "unflushed": remaining, **stats.as_log()}))
    return 0


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    return run(os.environ)


if __name__ == "__main__":
    sys.exit(main())
