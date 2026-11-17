"""Ingest GH Archive hourly files into the GCS raw layer.

    python -m ingestion.gharchive --date 2026-10-01 [--hours 0-23] [--force] [--workers 4]

Each hour is streamed from ``https://data.gharchive.org/{YYYY-MM-DD}-{H}.json.gz`` straight into
``gs://$GCS_BUCKET/raw/gharchive/dt={YYYY-MM-DD}/hr={HH}/{YYYY-MM-DD}-{H}.json.gz``.
Nothing is written to local disk.

Configuration (environment variables):
    GCS_BUCKET           required  target bucket
    GCS_RAW_PREFIX       optional  object prefix               (default: raw/gharchive)
    GHARCHIVE_BASE_URL   optional  source base URL             (default: https://data.gharchive.org)
    INGEST_MAX_ATTEMPTS  optional  attempts per hour, incl. 1st (default: 3)
    GCP_PROJECT_ID       optional  project for the GCS client  (default: from credentials)

Exit codes:
    0  every requested hour is ok/skipped (or only missing, with --allow-missing)
    1  at least one hour failed after all retries -> the orchestrator should retry
    2  usage or configuration error
    3  no failures, but some hours are missing (HTTP 404: not published yet / known gap)
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import os
import random
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any, Protocol, TypeVar

import requests
from google.api_core import exceptions as google_exceptions
from requests.adapters import HTTPAdapter
from urllib3.exceptions import ProtocolError, ReadTimeoutError

DEFAULT_BASE_URL = "https://data.gharchive.org"
DEFAULT_PREFIX = "raw/gharchive"
DEFAULT_MAX_ATTEMPTS = 3
UPLOAD_CHUNK_SIZE = 8 * 1024 * 1024  # GCS resumable uploads require a multiple of 256 KiB
HTTP_TIMEOUT = (10, 60)  # (connect, read) seconds
UPLOAD_TIMEOUT = 120  # seconds per upload request
USER_AGENT = "oss-pulse-ingestion/0.1"

EXIT_OK, EXIT_FAILED, EXIT_USAGE, EXIT_MISSING = 0, 1, 2, 3

log = logging.getLogger("ingestion.gharchive")

T = TypeVar("T")


# --- Pure helpers ---------------------------------------------------------------------------


def _check_hour(hour: int) -> None:
    if not 0 <= hour <= 23:
        raise ValueError(f"hour must be between 0 and 23, got {hour}")


def source_filename(day: date, hour: int) -> str:
    """GH Archive file name. The hour is NOT zero-padded: ``2026-10-01-5.json.gz``."""
    _check_hour(hour)
    return f"{day:%Y-%m-%d}-{hour}.json.gz"


def source_url(day: date, hour: int, base_url: str = DEFAULT_BASE_URL) -> str:
    return f"{base_url.rstrip('/')}/{source_filename(day, hour)}"


def gcs_object_name(day: date, hour: int, prefix: str = DEFAULT_PREFIX) -> str:
    """Hive-style partitioned path; ``hr`` is zero-padded so partitions sort correctly."""
    return f"{prefix.strip('/')}/dt={day:%Y-%m-%d}/hr={hour:02d}/{source_filename(day, hour)}"


def _parse_hour(text: str) -> int:
    text = text.strip()
    if not text.isdigit():
        raise ValueError(f"invalid hour {text!r}")
    hour = int(text)
    _check_hour(hour)
    return hour


def parse_hours(spec: str) -> list[int]:
    """Parse ``"0-23"``, ``"15"`` or ``"0-5,12,20-23"`` into a sorted, de-duplicated list."""
    hours: set[int] = set()
    for part in spec.split(","):
        if not part.strip():
            raise ValueError(f"empty hour in {spec!r}")
        if "-" in part:
            start_text, end_text = part.split("-", 1)
            start, end = _parse_hour(start_text), _parse_hour(end_text)
            if start > end:
                raise ValueError(f"hour range {part.strip()!r} is reversed")
            hours.update(range(start, end + 1))
        else:
            hours.add(_parse_hour(part))
    return sorted(hours)


def backoff_delay(
    attempt: int,
    *,
    base: float = 2.0,
    cap: float = 60.0,
    rand: Callable[[], float] = random.random,
) -> float:
    """Exponential backoff with jitter: a value in [d/2, d] where d = min(cap, base * 2^(n-1))."""
    ceiling = min(cap, base * 2 ** (attempt - 1))
    return ceiling / 2 + rand() * ceiling / 2


# --- Retries --------------------------------------------------------------------------------


class TransientError(Exception):
    """A failure worth retrying: HTTP 5xx/429 or a truncated transfer."""


class SourceMissing(Exception):
    """The source file does not exist (HTTP 404): not published yet, or a known gap."""


class ConfigError(Exception):
    """Missing or invalid configuration."""


RETRYABLE_ERRORS: tuple[type[BaseException], ...] = (
    TransientError,
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
    ProtocolError,  # connection dropped while reading the raw stream
    ReadTimeoutError,
    google_exceptions.ServerError,  # GCS 5xx
    google_exceptions.TooManyRequests,  # GCS 429
)


def with_retries(
    fn: Callable[[], T],
    *,
    attempts: int,
    retryable: tuple[type[BaseException], ...] = RETRYABLE_ERRORS,
    sleep: Callable[[float], None] = time.sleep,
    delay: Callable[[int], float] = backoff_delay,
    on_retry: Callable[[int, float, BaseException], None] | None = None,
) -> T:
    """Call ``fn`` up to ``attempts`` times, backing off between retryable failures."""
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except retryable as exc:
            if attempt == attempts:
                raise
            wait = delay(attempt)
            if on_retry is not None:
                on_retry(attempt, wait, exc)
            sleep(wait)
    raise AssertionError("unreachable")


# --- Config ---------------------------------------------------------------------------------


@dataclass(frozen=True)
class Config:
    bucket: str
    prefix: str = DEFAULT_PREFIX
    base_url: str = DEFAULT_BASE_URL
    max_attempts: int = DEFAULT_MAX_ATTEMPTS
    project: str | None = None

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> Config:
        bucket = env.get("GCS_BUCKET", "").strip()
        if not bucket:
            raise ConfigError("GCS_BUCKET is not set")
        raw_attempts = env.get("INGEST_MAX_ATTEMPTS", str(DEFAULT_MAX_ATTEMPTS))
        try:
            max_attempts = int(raw_attempts)
        except ValueError:
            raise ConfigError(f"INGEST_MAX_ATTEMPTS must be an integer, got {raw_attempts!r}")
        if max_attempts < 1:
            raise ConfigError("INGEST_MAX_ATTEMPTS must be >= 1")
        return cls(
            bucket=bucket,
            prefix=env.get("GCS_RAW_PREFIX", DEFAULT_PREFIX),
            base_url=env.get("GHARCHIVE_BASE_URL", DEFAULT_BASE_URL),
            max_attempts=max_attempts,
            project=env.get("GCP_PROJECT_ID") or None,
        )


# --- Transfer -------------------------------------------------------------------------------


class BlobLike(Protocol):
    size: int | None

    def upload_from_file(self, file_obj: Any, **kwargs: Any) -> None: ...

    def delete(self) -> None: ...


class BucketLike(Protocol):
    name: str

    def blob(self, blob_name: str, chunk_size: int | None = None) -> BlobLike: ...

    def get_blob(self, blob_name: str) -> BlobLike | None: ...


class Status(StrEnum):
    OK = "ok"
    SKIPPED = "skipped"
    MISSING = "missing"
    FAILED = "failed"


@dataclass
class HourResult:
    hour: int
    status: Status
    gcs_uri: str
    bytes: int = 0
    duration_s: float = 0.0
    attempts: int = 0
    error: str | None = None


class _CountingReader:
    """Forward-only file-like wrapper that counts bytes. GCS resumable uploads call ``tell()``."""

    def __init__(self, raw: Any) -> None:
        self._raw = raw
        self.bytes_read = 0

    def read(self, size: int | None = -1) -> bytes:
        chunk = self._raw.read() if size is None or size < 0 else self._raw.read(size)
        self.bytes_read += len(chunk)
        return chunk

    def tell(self) -> int:
        return self.bytes_read

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return False


def _raise_for_status(resp: requests.Response, url: str) -> None:
    if resp.status_code == 404:
        raise SourceMissing(url)
    if resp.status_code == 429 or resp.status_code >= 500:
        raise TransientError(f"HTTP {resp.status_code} for {url}")
    resp.raise_for_status()  # other 4xx: not retried


def _content_length(resp: requests.Response) -> int | None:
    value = resp.headers.get("Content-Length", "")
    return int(value) if value.isdigit() else None


def _transfer(
    url: str,
    object_name: str,
    *,
    bucket: BucketLike,
    session: requests.Session,
    force: bool,
) -> tuple[Status, int]:
    """One attempt: stream ``url`` into ``object_name``. Returns (status, bytes)."""
    with session.get(url, stream=True, timeout=HTTP_TIMEOUT) as resp:
        _raise_for_status(resp, url)
        expected = _content_length(resp)

        if not force and expected is not None:
            existing = bucket.get_blob(object_name)
            if existing is not None and existing.size == expected:
                return Status.SKIPPED, expected

        # Keep the gzip bytes exactly as served, even if a proxy adds Content-Encoding.
        if hasattr(resp.raw, "decode_content"):
            resp.raw.decode_content = False
        reader = _CountingReader(resp.raw)
        blob = bucket.blob(object_name, chunk_size=UPLOAD_CHUNK_SIZE)
        # The resumable upload only creates the object once the stream ends, so a failed attempt
        # never leaves a partial object. retry=None: the stream can't be rewound, so the whole
        # hour is retried by with_retries instead.
        blob.upload_from_file(
            reader, content_type="application/gzip", retry=None, timeout=UPLOAD_TIMEOUT
        )

        if expected is not None and reader.bytes_read != expected:
            with contextlib.suppress(google_exceptions.NotFound):
                blob.delete()  # never leave a truncated file for Spark to read
            raise TransientError(
                f"size mismatch for {url}: got {reader.bytes_read} bytes, expected {expected}"
            )
        return Status.OK, reader.bytes_read


def ingest_hour(
    day: date,
    hour: int,
    *,
    config: Config,
    bucket: BucketLike,
    session: requests.Session,
    force: bool = False,
    sleep: Callable[[float], None] = time.sleep,
) -> HourResult:
    """Ingest one hour with retries. Never raises: the outcome is in the returned status."""
    url = source_url(day, hour, config.base_url)
    object_name = gcs_object_name(day, hour, config.prefix)
    result = HourResult(
        hour=hour, status=Status.FAILED, gcs_uri=f"gs://{bucket.name}/{object_name}"
    )
    tries = 0
    start = time.monotonic()

    def attempt() -> tuple[Status, int]:
        nonlocal tries
        tries += 1
        return _transfer(url, object_name, bucket=bucket, session=session, force=force)

    def on_retry(attempt_no: int, wait: float, exc: BaseException) -> None:
        _log(
            logging.WARNING,
            "retrying hour",
            event="retry",
            date=day.isoformat(),
            hour=hour,
            attempt=attempt_no,
            wait_s=round(wait, 2),
            error=_describe(exc),
        )

    try:
        result.status, result.bytes = with_retries(
            attempt, attempts=config.max_attempts, sleep=sleep, on_retry=on_retry
        )
    except SourceMissing:
        result.status = Status.MISSING
    except Exception as exc:  # one bad hour must not abort the others; reported via exit code
        result.status, result.error = Status.FAILED, _describe(exc)

    result.attempts = tries
    result.duration_s = round(time.monotonic() - start, 3)
    _log_hour(day, result)
    return result


def run(
    day: date,
    hours: Sequence[int],
    *,
    config: Config,
    bucket: BucketLike,
    session: requests.Session,
    force: bool = False,
    workers: int = 4,
    sleep: Callable[[float], None] = time.sleep,
) -> list[HourResult]:
    """Ingest ``hours`` of ``day`` in parallel. Results are returned in hour order."""

    def one(hour: int) -> HourResult:
        return ingest_hour(
            day, hour, config=config, bucket=bucket, session=session, force=force, sleep=sleep
        )

    with ThreadPoolExecutor(max_workers=max(1, min(workers, len(hours)))) as pool:
        return list(pool.map(one, hours))


def summarize(day: date, results: Sequence[HourResult], *, allow_missing: bool = False) -> int:
    """Log the end-of-run summary and return the process exit code."""
    hours_with = {s: [r.hour for r in results if r.status is s] for s in Status}
    done = len(hours_with[Status.OK]) + len(hours_with[Status.SKIPPED])
    missing, failed = hours_with[Status.MISSING], hours_with[Status.FAILED]

    if failed:
        code = EXIT_FAILED
    elif missing and not allow_missing:
        code = EXIT_MISSING
    else:
        code = EXIT_OK

    message = f"{done}/{len(results)} ok"
    if missing:
        message += f"; missing hours {missing}"
    if failed:
        message += f"; failed hours {failed}"

    _log(
        logging.INFO if code == EXIT_OK else logging.ERROR,
        message,
        event="run_summary",
        date=day.isoformat(),
        requested=len(results),
        ok=len(hours_with[Status.OK]),
        skipped=len(hours_with[Status.SKIPPED]),
        missing_hours=missing,
        failed_hours=failed,
        bytes=sum(r.bytes for r in results if r.status is Status.OK),
        exit_code=code,
    )
    return code


# --- Logging --------------------------------------------------------------------------------


class JsonFormatter(logging.Formatter):
    """One JSON object per line; structured fields come from ``extra={"fields": {...}}``."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname.lower(),
            "logger": record.name,
            "msg": record.getMessage(),
        }
        payload.update(getattr(record, "fields", {}))
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(logging.WARNING)  # third-party libraries: warnings and above only
    log.setLevel(level)


def _log(level: int, msg: str, **fields: Any) -> None:
    log.log(level, msg, extra={"fields": fields})


def _log_hour(day: date, r: HourResult) -> None:
    level = {Status.MISSING: logging.WARNING, Status.FAILED: logging.ERROR}.get(
        r.status, logging.INFO
    )
    _log(
        level,
        f"hour {r.hour:02d} {r.status}",
        event="hour_done",
        date=day.isoformat(),
        hour=r.hour,
        status=str(r.status),
        bytes=r.bytes,
        duration_s=r.duration_s,
        attempts=r.attempts,
        gcs_uri=r.gcs_uri,
        error=r.error,
    )


def _describe(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"


# --- CLI ------------------------------------------------------------------------------------


def _date_arg(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid date {text!r}, expected YYYY-MM-DD")


def _hours_arg(text: str) -> list[int]:
    try:
        return parse_hours(text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc))


def _positive_int(text: str) -> int:
    if not text.isdigit() or int(text) < 1:
        raise argparse.ArgumentTypeError(f"expected a positive integer, got {text!r}")
    return int(text)


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion.gharchive",
        description="Stream GH Archive hourly files into gs://$GCS_BUCKET/raw/gharchive/.",
    )
    parser.add_argument("--date", required=True, type=_date_arg, help="UTC day, YYYY-MM-DD")
    parser.add_argument(
        "--hours",
        default=parse_hours("0-23"),
        type=_hours_arg,
        help="UTC hours, e.g. 0-23 (default), 15, 0-5,12",
    )
    parser.add_argument(
        "--force", action="store_true", help="re-upload even if the object exists with same size"
    )
    parser.add_argument(
        "--allow-missing", action="store_true", help="exit 0 if the only problem is 404 hours"
    )
    parser.add_argument(
        "--workers", type=_positive_int, default=4, help="parallel transfers (default: 4)"
    )
    parser.add_argument(
        "--log-level",
        default=os.environ.get("LOG_LEVEL", "INFO").upper(),
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    return parser.parse_args(argv)


def _make_session(pool_size: int) -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=1, pool_maxsize=pool_size)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers["User-Agent"] = USER_AGENT
    return session


def _make_bucket(config: Config) -> BucketLike:
    from google.cloud import storage  # imported lazily: unit tests never need credentials

    # No bucket.reload()/exists(): that needs storage.buckets.get, which the pipeline SA lacks.
    return storage.Client(project=config.project).bucket(config.bucket)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    configure_logging(args.log_level)

    try:
        config = Config.from_env()
    except ConfigError as exc:
        _log(logging.ERROR, str(exc), event="config_error")
        return EXIT_USAGE

    from google.auth.exceptions import DefaultCredentialsError

    session = _make_session(args.workers)
    try:
        bucket = _make_bucket(config)
        _log(
            logging.INFO,
            "run started",
            event="run_start",
            date=args.date.isoformat(),
            hours=args.hours,
            bucket=config.bucket,
            force=args.force,
            workers=args.workers,
        )
        results = run(
            args.date,
            args.hours,
            config=config,
            bucket=bucket,
            session=session,
            force=args.force,
            workers=args.workers,
        )
    except DefaultCredentialsError as exc:
        _log(logging.ERROR, "no GCP credentials found", event="config_error", error=str(exc))
        return EXIT_USAGE
    finally:
        session.close()

    return summarize(args.date, results, allow_missing=args.allow_missing)


if __name__ == "__main__":
    sys.exit(main())
