"""Enrich the most-starred repositories with GitHub metadata (language, topics, license, ...).

GH Archive events carry no repository attributes. This job calls the GitHub REST API for the
top repositories by stars and appends one row per call to BigQuery `raw.github_repo_metadata`;
dbt keeps the latest row per repo (stg_github_repos -> dim_repo_github).

    python -m ingestion.github_repos             # top 1,000 repos of the latest 30 loaded days
    python -m ingestion.github_repos --dry-run   # only list what it would fetch

Design:
* By repo_id (GET /repositories/{id}), never by name: names change (dim_repo_history).
* Incremental cursor: a repo is fetched only if it was never fetched or its last fetch is older
  than --refresh-days. New repos first, then by stars. A run that stops early (rate limit,
  --max-requests) is simply continued by the next run.
* Conditional requests: the stored ETag is sent as If-None-Match; GitHub answers 304 for an
  unchanged repo, and 304s don't count against the rate limit.
* Rate limits: stop cleanly when X-RateLimit-Remaining reaches --rate-reserve or the primary
  limit is hit (never sleep for an hour); wait out a secondary limit (Retry-After, capped);
  retry 5xx and network errors with backoff. Requests are sequential, as GitHub recommends.
* Rows are written in batches with BigQuery load jobs (free), so progress survives a crash.

Env: GCP_PROJECT_ID, GCP_REGION, BQ_DATASET_RAW, BQ_DATASET_MARTS, and a token in GITHUB_TOKEN
or AIRFLOW_VAR_GITHUB_TOKEN_SECRET (fine-grained, public repositories read-only). Without a token
GitHub allows 60 requests/hour, so a run is capped at 50.

Exit codes: 0 ok (also when stopped by the rate limit), 1 too many failed requests, 2 config error.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime
from enum import StrEnum
from typing import Any

import requests

from ingestion.gharchive import backoff_delay, configure_logging

log = logging.getLogger("ingestion.github_repos")

DEFAULT_API_URL = "https://api.github.com"
API_VERSION = "2022-11-28"
USER_AGENT = "oss-pulse-enrichment/0.1"
HTTP_TIMEOUT = (10, 30)
MAX_ATTEMPTS = 3
MAX_SECONDARY_WAIT = 120.0  # seconds; a longer Retry-After ends the run instead
UNAUTHENTICATED_CAP = 50  # GitHub allows 60/hour without a token
DESCRIPTION_MAX = 500
EXIT_OK, EXIT_FAILED, EXIT_USAGE = 0, 1, 2

# raw.github_repo_metadata (warehouse/ddl/github_repo_metadata.sql), in column order.
SCHEMA_FIELDS: tuple[tuple[str, str, str], ...] = (
    ("repo_id", "INT64", "REQUIRED"),
    ("fetched_at", "TIMESTAMP", "REQUIRED"),
    ("status", "STRING", "REQUIRED"),
    ("http_status", "INT64", "NULLABLE"),
    ("etag", "STRING", "NULLABLE"),
    ("full_name", "STRING", "NULLABLE"),
    ("description", "STRING", "NULLABLE"),
    ("language", "STRING", "NULLABLE"),
    ("topics", "STRING", "REPEATED"),
    ("license_spdx", "STRING", "NULLABLE"),
    ("stargazers_count", "INT64", "NULLABLE"),
    ("forks_count", "INT64", "NULLABLE"),
    ("open_issues_count", "INT64", "NULLABLE"),
    ("is_fork", "BOOL", "NULLABLE"),
    ("is_archived", "BOOL", "NULLABLE"),
    ("repo_created_at", "TIMESTAMP", "NULLABLE"),
    ("pushed_at", "TIMESTAMP", "NULLABLE"),
)


class ConfigError(Exception):
    """Missing or invalid configuration."""


class RateLimitExhausted(Exception):
    """The primary rate limit is used up (or a secondary limit asks for too long a wait)."""

    def __init__(self, reset_at: int | None):
        super().__init__(f"rate limit exhausted, resets at {reset_at}")
        self.reset_at = reset_at


class Outcome(StrEnum):
    OK = "ok"  # 200: metadata in the row
    NOT_MODIFIED = "not_modified"  # 304: unchanged since the stored ETag
    NOT_FOUND = "not_found"  # 404: deleted, made private, or never visible
    BLOCKED = "blocked"  # 451 or 403 "access blocked" (DMCA / terms of service)
    FAILED = "failed"  # gave up after retries: not written, retried next run


@dataclass(frozen=True)
class RateLimit:
    remaining: int | None = None
    reset_at: int | None = None  # epoch seconds


@dataclass(frozen=True)
class Fetch:
    repo_id: int
    outcome: Outcome
    http_status: int | None = None
    etag: str | None = None
    data: Mapping[str, Any] = field(default_factory=dict)
    error: str | None = None


# --- Pure helpers ---------------------------------------------------------------------------


def request_headers(token: str | None, etag: str | None) -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": API_VERSION,
        "User-Agent": USER_AGENT,
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if etag:
        headers["If-None-Match"] = etag
    return headers


def rate_limit_from(headers: Mapping[str, str]) -> RateLimit:
    def _int(name: str) -> int | None:
        try:
            return int(headers[name])
        except (KeyError, ValueError):
            return None

    return RateLimit(_int("X-RateLimit-Remaining"), _int("X-RateLimit-Reset"))


def secondary_wait(headers: Mapping[str, str], now: float) -> float:
    """Seconds to wait after a secondary rate limit: Retry-After, else until reset, else 60."""
    retry_after = headers.get("Retry-After")
    if retry_after is not None:
        try:
            return max(0.0, float(retry_after))
        except ValueError:
            pass
    reset = rate_limit_from(headers).reset_at
    if reset is not None and headers.get("X-RateLimit-Remaining") == "0":
        return max(0.0, reset - now)
    return 60.0


def parse_repo(payload: Mapping[str, Any]) -> dict[str, Any]:
    """The columns we keep from GET /repositories/{id}."""
    description = payload.get("description")
    license_ = payload.get("license") or {}
    return {
        "full_name": (payload.get("full_name") or "").lower() or None,
        "description": description[:DESCRIPTION_MAX] if description else None,
        "language": payload.get("language"),
        "topics": [t.lower() for t in payload.get("topics") or []],
        "license_spdx": license_.get("spdx_id"),
        "stargazers_count": payload.get("stargazers_count"),
        "forks_count": payload.get("forks_count"),
        "open_issues_count": payload.get("open_issues_count"),
        "is_fork": payload.get("fork"),
        "is_archived": payload.get("archived"),
        "repo_created_at": payload.get("created_at"),
        "pushed_at": payload.get("pushed_at"),
    }


def to_row(fetch: Fetch, fetched_at: datetime) -> dict[str, Any]:
    row: dict[str, Any] = {name: None for name, _, _ in SCHEMA_FIELDS}
    row.update(fetch.data)
    row.update(
        repo_id=fetch.repo_id,
        fetched_at=fetched_at.isoformat(),
        status=str(fetch.outcome),
        http_status=fetch.http_status,
        etag=fetch.etag,
    )
    row["topics"] = row["topics"] or []
    return row


# --- One request ----------------------------------------------------------------------------


def fetch_repo(
    session: requests.Session,
    repo_id: int,
    etag: str | None,
    *,
    token: str | None,
    api_url: str = DEFAULT_API_URL,
    max_attempts: int = MAX_ATTEMPTS,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.time,
) -> tuple[Fetch, RateLimit]:
    """GET /repositories/{repo_id}. Raises RateLimitExhausted to end the run cleanly."""
    url = f"{api_url}/repositories/{repo_id}"
    rate = RateLimit()
    last_error = "no attempt"
    for attempt in range(1, max_attempts + 1):
        try:
            resp = session.get(url, headers=request_headers(token, etag), timeout=HTTP_TIMEOUT)
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < max_attempts:
                sleep(backoff_delay(attempt, base=1.0, cap=20.0))
            continue

        rate = rate_limit_from(resp.headers)
        status = resp.status_code
        if status == 200:
            data = parse_repo(resp.json())
            return Fetch(repo_id, Outcome.OK, status, resp.headers.get("ETag"), data), rate
        if status == 304:
            return Fetch(repo_id, Outcome.NOT_MODIFIED, status, etag), rate
        if status == 404:
            return Fetch(repo_id, Outcome.NOT_FOUND, status), rate
        if status == 451:
            return Fetch(repo_id, Outcome.BLOCKED, status), rate
        if status in (403, 429):
            if rate.remaining == 0:
                raise RateLimitExhausted(rate.reset_at)
            message = _message(resp)
            if status == 403 and "rate limit" not in message.lower():
                # e.g. "Repository access blocked": not a limit, retrying won't help
                return Fetch(repo_id, Outcome.BLOCKED, status, error=message[:200]), rate
            wait = secondary_wait(resp.headers, clock())
            if wait > MAX_SECONDARY_WAIT:
                raise RateLimitExhausted(rate.reset_at)
            _log(logging.WARNING, "secondary rate limit", event="secondary_limit", wait_s=wait)
            sleep(wait)
            last_error = f"HTTP {status} secondary rate limit"
            continue
        if status >= 500:
            last_error = f"HTTP {status}"
            if attempt < max_attempts:
                sleep(backoff_delay(attempt, base=1.0, cap=20.0))
            continue
        return Fetch(repo_id, Outcome.FAILED, status, error=f"unexpected HTTP {status}"), rate
    return Fetch(repo_id, Outcome.FAILED, error=last_error), rate


def _message(resp: requests.Response) -> str:
    try:
        return str(resp.json().get("message", ""))
    except ValueError:
        return resp.text[:200] if isinstance(resp.text, str) else ""


# --- A run ----------------------------------------------------------------------------------


@dataclass
class Summary:
    candidates: int = 0
    counts: dict[str, int] = field(default_factory=lambda: {str(o): 0 for o in Outcome})
    rows_written: int = 0
    stopped: str | None = None  # why the run ended before the last candidate
    rate_remaining: int | None = None

    @property
    def attempted(self) -> int:
        return sum(self.counts.values())


def run(
    candidates: Sequence[tuple[int, str | None]],
    *,
    fetch: Callable[[int, str | None], tuple[Fetch, RateLimit]],
    write: Callable[[list[dict[str, Any]]], None],
    rate_reserve: int = 100,
    batch_size: int = 200,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Summary:
    """Fetch each candidate in order, writing rows in batches; stop early on rate limits."""
    summary = Summary(candidates=len(candidates))
    batch: list[dict[str, Any]] = []

    def flush() -> None:
        if batch:
            write(list(batch))
            summary.rows_written += len(batch)
            batch.clear()

    try:
        for repo_id, etag in candidates:
            remaining = summary.rate_remaining
            if remaining is not None and remaining <= rate_reserve:
                summary.stopped = f"rate limit reserve reached ({remaining} left)"
                break
            try:
                result, rate = fetch(repo_id, etag)
            except RateLimitExhausted as exc:
                summary.stopped = f"rate limit exhausted (resets at {exc.reset_at})"
                break
            summary.rate_remaining = rate.remaining
            summary.counts[str(result.outcome)] += 1
            if result.outcome is Outcome.FAILED:
                _log(
                    logging.WARNING,
                    "fetch failed",
                    event="fetch_failed",
                    repo_id=repo_id,
                    error=result.error,
                )
                continue  # not written: the cursor picks it up next run
            batch.append(to_row(result, clock()))
            if len(batch) >= batch_size:
                flush()
    finally:
        flush()
    return summary


def exit_code(summary: Summary) -> int:
    failed = summary.counts[str(Outcome.FAILED)]
    return EXIT_FAILED if failed > max(5, summary.attempted // 10) else EXIT_OK


# --- BigQuery -------------------------------------------------------------------------------


@dataclass(frozen=True)
class Config:
    project: str
    location: str
    raw_dataset: str
    marts_dataset: str
    token: str | None

    @property
    def metadata_table(self) -> str:
        return f"{self.project}.{self.raw_dataset}.github_repo_metadata"

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> Config:
        project = env.get("GCP_PROJECT_ID", "").strip()
        location = env.get("GCP_REGION", "").strip()
        if not project or not location:
            raise ConfigError("GCP_PROJECT_ID and GCP_REGION must be set")
        token = env.get("GITHUB_TOKEN") or env.get("AIRFLOW_VAR_GITHUB_TOKEN_SECRET") or ""
        return cls(
            project=project,
            location=location,
            raw_dataset=env.get("BQ_DATASET_RAW", "oss_pulse_raw"),
            marts_dataset=env.get("BQ_DATASET_MARTS", "oss_pulse_marts"),
            token=token.strip() or None,
        )


CANDIDATES_SQL = """
with top_repos as (
    select repo_id, sum(stars) as stars
    from `{project}.{marts}.agg_repo_daily`
    where event_date between date_sub(@as_of, interval @lookback_days - 1 day) and @as_of
    group by repo_id
    having stars > 0
    order by stars desc, repo_id
    limit @top_n
),
latest as (
    select
        repo_id,
        max(fetched_at) as last_fetched_at,
        array_agg(etag ignore nulls order by fetched_at desc limit 1)[safe_offset(0)] as etag
    from `{project}.{raw}.github_repo_metadata`
    where repo_id in (select repo_id from top_repos)
    group by repo_id
)
select t.repo_id, l.etag
from top_repos as t
left join latest as l using (repo_id)
where l.last_fetched_at is null
   or l.last_fetched_at < timestamp_sub(@now, interval @refresh_days day)
order by l.last_fetched_at is not null, t.stars desc, t.repo_id
limit @max_requests
"""


LATEST_DAY_SQL = """
select max(parse_date('%Y%m%d', partition_id)) as day
from `{project}.{marts}`.INFORMATION_SCHEMA.PARTITIONS
where table_name = 'agg_repo_daily'
  and regexp_contains(partition_id, r'^[0-9]{{8}}$') and total_rows > 0
"""


def latest_loaded_day(client: Any, config: Config) -> date | None:
    """Latest non-empty day of agg_repo_daily, from partition metadata (same rule as the reports).

    The ranking window ends at the latest LOADED day, not at today, so a paused pipeline or a
    finished backfill still ranks a full window instead of an empty one.
    """
    sql = LATEST_DAY_SQL.format(project=config.project, marts=config.marts_dataset)
    rows = list(client.query(sql, location=config.location).result())
    return rows[0].day if rows else None


def select_candidates(
    client: Any,
    config: Config,
    *,
    as_of: date,
    now: datetime,
    top_n: int,
    lookback_days: int,
    refresh_days: int,
    max_requests: int,
) -> list[tuple[int, str | None]]:
    from google.cloud import bigquery

    sql = CANDIDATES_SQL.format(
        project=config.project, marts=config.marts_dataset, raw=config.raw_dataset
    )
    params = [
        bigquery.ScalarQueryParameter("as_of", "DATE", as_of),
        bigquery.ScalarQueryParameter("now", "TIMESTAMP", now),
        bigquery.ScalarQueryParameter("top_n", "INT64", top_n),
        bigquery.ScalarQueryParameter("lookback_days", "INT64", lookback_days),
        bigquery.ScalarQueryParameter("refresh_days", "INT64", refresh_days),
        bigquery.ScalarQueryParameter("max_requests", "INT64", max_requests),
    ]
    job = client.query(
        sql, job_config=bigquery.QueryJobConfig(query_parameters=params), location=config.location
    )
    return [(int(r.repo_id), r.etag) for r in job.result()]


def bigquery_writer(client: Any, config: Config) -> Callable[[list[dict[str, Any]]], None]:
    from google.cloud import bigquery

    schema = [bigquery.SchemaField(n, t, mode=m) for n, t, m in SCHEMA_FIELDS]
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition=bigquery.CreateDisposition.CREATE_NEVER,
    )

    def write(rows: list[dict[str, Any]]) -> None:
        job = client.load_table_from_json(
            rows, config.metadata_table, job_config=job_config, location=config.location
        )
        job.result(timeout=300)
        _log(logging.INFO, "rows written", event="batch_written", rows=len(rows))

    return write


# --- CLI ------------------------------------------------------------------------------------


def _log(level: int, msg: str, **fields: Any) -> None:
    log.log(level, msg, extra={"fields": fields})


def _date_arg(text: str) -> date:
    try:
        return date.fromisoformat(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {text!r}") from None


def _positive(text: str) -> int:
    value = int(text)
    if value < 1:
        raise argparse.ArgumentTypeError("must be >= 1")
    return value


def _non_negative(text: str) -> int:
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError("must be >= 0")
    return value


def _parse_args(argv: Sequence[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Enrich top repositories with GitHub metadata.")
    p.add_argument(
        "--as-of",
        type=_date_arg,
        default=None,
        help="last day of the star ranking window (default: latest day loaded in agg_repo_daily)",
    )
    p.add_argument("--top-n", type=_positive, default=1000, help="repos to keep enriched")
    p.add_argument("--lookback-days", type=_positive, default=30, help="star ranking window")
    p.add_argument(
        "--refresh-days", type=_non_negative, default=7, help="re-fetch after N days (0: always)"
    )
    p.add_argument("--max-requests", type=_positive, default=1000, help="API calls per run")
    p.add_argument(
        "--rate-reserve", type=int, default=100, help="stop when this many requests are left"
    )
    p.add_argument("--dry-run", action="store_true", help="list candidates, call nothing")
    p.add_argument("--log-level", default="INFO")
    return p.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)
    configure_logging(args.log_level)
    log.setLevel(args.log_level)
    try:
        config = Config.from_env()
    except ConfigError as exc:
        _log(logging.ERROR, str(exc), event="config_error")
        return EXIT_USAGE

    max_requests = args.max_requests
    if not config.token:
        max_requests = min(max_requests, UNAUTHENTICATED_CAP)
        _log(logging.WARNING, "no GitHub token: capped at 50 requests", event="no_token")

    from google.cloud import bigquery

    client = bigquery.Client(project=config.project, location=config.location)
    as_of = args.as_of or latest_loaded_day(client, config)
    if as_of is None:
        _log(logging.ERROR, "agg_repo_daily has no data yet", event="config_error")
        return EXIT_USAGE
    now = datetime.now(UTC)
    candidates = select_candidates(
        client,
        config,
        as_of=as_of,
        now=now,
        top_n=args.top_n,
        lookback_days=args.lookback_days,
        refresh_days=args.refresh_days,
        max_requests=max_requests,
    )
    _log(
        logging.INFO,
        "candidates selected",
        event="run_start",
        as_of=as_of.isoformat(),
        candidates=len(candidates),
        with_etag=sum(1 for _, e in candidates if e),
        authenticated=bool(config.token),
        dry_run=args.dry_run,
    )
    if args.dry_run:
        return EXIT_OK

    with requests.Session() as session:
        summary = run(
            candidates,
            fetch=lambda repo_id, etag: fetch_repo(session, repo_id, etag, token=config.token),
            write=bigquery_writer(client, config),
            rate_reserve=args.rate_reserve,
        )
    code = exit_code(summary)
    _log(
        logging.INFO if code == EXIT_OK else logging.ERROR,
        "run finished",
        event="run_summary",
        candidates=summary.candidates,
        attempted=summary.attempted,
        rows=summary.rows_written,
        stopped=summary.stopped,
        rate_remaining=summary.rate_remaining,
        exit_code=code,
        **summary.counts,
    )
    return code


def iter_schema() -> Iterable[tuple[str, str, str]]:
    """Schema as (name, type, mode): the DDL test checks the table matches."""
    return iter(SCHEMA_FIELDS)


if __name__ == "__main__":
    sys.exit(main())
