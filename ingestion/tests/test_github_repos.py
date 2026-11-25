"""Offline tests for the GitHub enrichment job: a fake API, no network, no BigQuery."""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import requests

from ingestion import github_repos as gh
from ingestion.github_repos import Fetch, Outcome, RateLimit, RateLimitExhausted

DDL = Path(__file__).resolve().parents[2] / "warehouse" / "ddl" / "github_repo_metadata.sql"

REPO = {
    "id": 42,
    "full_name": "Octo/Hello",
    "description": "x" * 600,
    "language": "Python",
    "topics": ["Data", "dbt"],
    "license": {"spdx_id": "MIT"},
    "stargazers_count": 10,
    "forks_count": 2,
    "open_issues_count": 1,
    "fork": False,
    "archived": False,
    "created_at": "2020-01-01T00:00:00Z",
    "pushed_at": "2026-10-30T12:00:00Z",
}


class FakeResponse:
    def __init__(self, status: int, body: Any = None, headers: dict[str, str] | None = None):
        self.status_code = status
        self._body = body
        self.headers = {"X-RateLimit-Remaining": "4000", "X-RateLimit-Reset": "1000"}
        self.headers.update(headers or {})
        self.text = "" if body is None else str(body)

    def json(self) -> Any:
        if self._body is None:
            raise ValueError("no body")
        return self._body


class FakeSession:
    """Returns the queued responses (or raises queued exceptions) in order."""

    def __init__(self, *responses: FakeResponse | Exception):
        self.queue = list(responses)
        self.calls: list[dict[str, Any]] = []

    def get(self, url: str, headers: dict[str, str], timeout: Any) -> FakeResponse:
        self.calls.append({"url": url, "headers": headers})
        item = self.queue.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def fetch(session: FakeSession, etag: str | None = None, token: str | None = "t"):
    sleeps: list[float] = []
    result = gh.fetch_repo(session, 42, etag, token=token, sleep=sleeps.append, clock=lambda: 900.0)
    return result, sleeps


# --- pure helpers ---------------------------------------------------------------------------


def test_request_headers_token_and_etag():
    h = gh.request_headers("abc", 'W/"e1"')
    assert h["Authorization"] == "Bearer abc"
    assert h["If-None-Match"] == 'W/"e1"'
    assert h["X-GitHub-Api-Version"] == gh.API_VERSION


def test_request_headers_without_token_or_etag():
    h = gh.request_headers(None, None)
    assert "Authorization" not in h and "If-None-Match" not in h


def test_rate_limit_from_headers_and_missing_values():
    assert gh.rate_limit_from({"X-RateLimit-Remaining": "7", "X-RateLimit-Reset": "99"}) == (
        RateLimit(7, 99)
    )
    assert gh.rate_limit_from({"X-RateLimit-Remaining": "n/a"}) == RateLimit(None, None)


@pytest.mark.parametrize(
    ("headers", "expected"),
    [
        ({"Retry-After": "12"}, 12.0),
        ({"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "930"}, 30.0),
        ({}, 60.0),
    ],
)
def test_secondary_wait(headers, expected):
    assert gh.secondary_wait(headers, now=900.0) == expected


def test_parse_repo_normalises_and_truncates():
    row = gh.parse_repo(REPO)
    assert row["full_name"] == "octo/hello"
    assert row["topics"] == ["data", "dbt"]
    assert row["license_spdx"] == "MIT"
    assert len(row["description"]) == gh.DESCRIPTION_MAX


def test_parse_repo_handles_missing_fields():
    row = gh.parse_repo({"id": 1, "license": None, "topics": None, "description": None})
    assert row["license_spdx"] is None and row["topics"] == [] and row["description"] is None


def test_to_row_has_every_column():
    row = gh.to_row(Fetch(7, Outcome.NOT_FOUND, 404), datetime(2026, 11, 1, tzinfo=UTC))
    assert list(row) == [name for name, _, _ in gh.SCHEMA_FIELDS]
    assert row["status"] == "not_found" and row["topics"] == []


# --- fetch_repo -----------------------------------------------------------------------------


def test_ok_returns_metadata_and_new_etag():
    (result, rate), _ = fetch(FakeSession(FakeResponse(200, REPO, {"ETag": 'W/"new"'})))
    assert result.outcome is Outcome.OK and result.etag == 'W/"new"'
    assert result.data["language"] == "Python" and rate.remaining == 4000


def test_conditional_request_304_keeps_etag():
    session = FakeSession(FakeResponse(304))
    (result, _), _ = fetch(session, etag='W/"old"')
    assert session.calls[0]["headers"]["If-None-Match"] == 'W/"old"'
    assert result.outcome is Outcome.NOT_MODIFIED and result.etag == 'W/"old"'


@pytest.mark.parametrize(
    ("status", "body", "outcome"),
    [
        (404, {"message": "Not Found"}, Outcome.NOT_FOUND),
        (451, {"message": "Unavailable"}, Outcome.BLOCKED),
        (403, {"message": "Repository access blocked"}, Outcome.BLOCKED),
        (422, {"message": "?"}, Outcome.FAILED),
    ],
)
def test_terminal_statuses(status, body, outcome):
    (result, _), sleeps = fetch(FakeSession(FakeResponse(status, body)))
    assert result.outcome is outcome and sleeps == []


def test_primary_limit_ends_the_run():
    resp = FakeResponse(
        403,
        {"message": "API rate limit exceeded"},
        {"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": "5000"},
    )
    with pytest.raises(RateLimitExhausted) as exc:
        fetch(FakeSession(resp))
    assert exc.value.reset_at == 5000


def test_secondary_limit_waits_then_retries():
    limited = FakeResponse(429, {"message": "secondary rate limit"}, {"Retry-After": "5"})
    (result, _), sleeps = fetch(FakeSession(limited, FakeResponse(200, REPO)))
    assert result.outcome is Outcome.OK and sleeps == [5.0]


def test_long_secondary_wait_ends_the_run_instead_of_sleeping():
    limited = FakeResponse(429, {"message": "secondary rate limit"}, {"Retry-After": "600"})
    with pytest.raises(RateLimitExhausted):
        fetch(FakeSession(limited))


def test_server_errors_are_retried_with_backoff():
    session = FakeSession(FakeResponse(502), FakeResponse(503), FakeResponse(200, REPO))
    (result, _), sleeps = fetch(session)
    assert result.outcome is Outcome.OK and len(sleeps) == 2


def test_gives_up_after_max_attempts():
    session = FakeSession(requests.ConnectionError("down"), FakeResponse(500), FakeResponse(500))
    (result, _), sleeps = fetch(session)
    assert result.outcome is Outcome.FAILED and "HTTP 500" in result.error
    assert len(sleeps) == 2  # no sleep after the last attempt


# --- run ------------------------------------------------------------------------------------


def make_fetch(outcomes: dict[int, Outcome | Exception], remaining: int = 4000):
    def _fetch(repo_id: int, etag: str | None) -> tuple[Fetch, RateLimit]:
        item = outcomes[repo_id]
        if isinstance(item, Exception):
            raise item
        return Fetch(repo_id, item, 200), RateLimit(remaining, 0)

    return _fetch


def test_run_writes_all_but_failed_and_batches():
    writes: list[list[dict[str, Any]]] = []
    outcomes = {1: Outcome.OK, 2: Outcome.FAILED, 3: Outcome.NOT_MODIFIED, 4: Outcome.NOT_FOUND}
    summary = gh.run(
        [(i, None) for i in outcomes], fetch=make_fetch(outcomes), write=writes.append, batch_size=2
    )
    assert [len(b) for b in writes] == [2, 1]
    assert [r["repo_id"] for b in writes for r in b] == [1, 3, 4]
    assert summary.rows_written == 3 and summary.counts["failed"] == 1 and summary.stopped is None


def test_run_stops_at_the_rate_reserve():
    writes: list[list[dict[str, Any]]] = []
    outcomes = dict.fromkeys(range(1, 6), Outcome.OK)
    summary = gh.run(
        [(i, None) for i in outcomes],
        fetch=make_fetch(outcomes, remaining=50),
        write=writes.append,
        rate_reserve=100,
    )
    assert summary.attempted == 1 and "reserve" in summary.stopped
    assert summary.rows_written == 1  # rows fetched before the stop are still written


def test_run_stops_cleanly_when_the_limit_is_exhausted():
    writes: list[list[dict[str, Any]]] = []
    outcomes = {1: Outcome.OK, 2: RateLimitExhausted(1234), 3: Outcome.OK}
    summary = gh.run([(i, None) for i in outcomes], fetch=make_fetch(outcomes), write=writes.append)
    assert summary.attempted == 1 and summary.rows_written == 1
    assert "1234" in summary.stopped


@pytest.mark.parametrize(
    ("ok", "failed", "code"), [(100, 5, 0), (100, 11, 0), (100, 12, 1), (10, 6, 1)]
)
def test_exit_code_tolerates_a_few_failures(ok, failed, code):
    summary = gh.Summary()
    summary.counts["ok"], summary.counts["failed"] = ok, failed
    assert gh.exit_code(summary) == code


# --- config and schema ----------------------------------------------------------------------


def test_config_token_from_either_variable():
    base = {"GCP_PROJECT_ID": "p", "GCP_REGION": "r"}
    assert gh.Config.from_env({**base, "GITHUB_TOKEN": "a"}).token == "a"
    assert gh.Config.from_env({**base, "AIRFLOW_VAR_GITHUB_TOKEN_SECRET": "b"}).token == "b"
    assert gh.Config.from_env(base).token is None


def test_config_requires_project_and_region():
    with pytest.raises(gh.ConfigError):
        gh.Config.from_env({"GCP_PROJECT_ID": "p"})


def test_schema_matches_the_ddl():
    columns = re.findall(
        r"^\s+(\w+)\s+(INT64|TIMESTAMP|STRING|BOOL|ARRAY<STRING>)(\s+NOT NULL)?",
        DDL.read_text(),
        re.MULTILINE,
    )
    from_ddl = [
        (
            name,
            "STRING" if kind.startswith("ARRAY") else kind,
            "REPEATED" if kind.startswith("ARRAY") else ("REQUIRED" if not_null else "NULLABLE"),
        )
        for name, kind, not_null in columns
    ]
    assert from_ddl == list(gh.iter_schema())
