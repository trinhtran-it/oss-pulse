import json
from datetime import date

import pytest
import requests

from ingestion import gharchive
from ingestion.gharchive import (
    EXIT_FAILED,
    EXIT_MISSING,
    EXIT_OK,
    EXIT_USAGE,
    HourResult,
    Status,
    TransientError,
    ingest_hour,
    run,
    summarize,
    with_retries,
)
from ingestion.tests.conftest import FakeBucket, FakeResponse, FakeSession

DAY = date(2026, 10, 1)
BODY = b"\x1f\x8b fake gzip payload"
URL15 = "https://example.test/2026-10-01-15.json.gz"
OBJ15 = "raw/gharchive/dt=2026-10-01/hr=15/2026-10-01-15.json.gz"


def _ingest(session, bucket, config, sleeps, **kwargs):
    return ingest_hour(
        DAY, 15, config=config, bucket=bucket, session=session, sleep=sleeps.append, **kwargs
    )


# --- with_retries -------------------------------------------------------------------------


def test_with_retries_succeeds_after_transient_failures():
    outcomes = [TransientError("503"), requests.ConnectionError("reset"), "done"]
    sleeps: list[float] = []

    def fn():
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    assert with_retries(fn, attempts=3, sleep=sleeps.append, delay=lambda n: n * 10) == "done"
    assert sleeps == [10, 20]


def test_with_retries_reraises_when_exhausted():
    calls = []

    def fn():
        calls.append(1)
        raise TransientError("503")

    with pytest.raises(TransientError):
        with_retries(fn, attempts=3, sleep=lambda s: None)
    assert len(calls) == 3


def test_with_retries_does_not_retry_other_errors():
    calls = []

    def fn():
        calls.append(1)
        raise KeyError("bug")

    with pytest.raises(KeyError):
        with_retries(fn, attempts=3, sleep=lambda s: None)
    assert len(calls) == 1


# --- ingest_hour --------------------------------------------------------------------------


def test_ok_streams_bytes_to_partitioned_object(config, bucket, no_sleep):
    session = FakeSession({URL15: [FakeResponse(200, BODY)]})
    r = _ingest(session, bucket, config, no_sleep)

    assert (r.status, r.bytes, r.attempts) == (Status.OK, len(BODY), 1)
    assert r.gcs_uri == f"gs://test-bucket/{OBJ15}"
    assert bucket.objects[OBJ15] == BODY
    name, content_type, kwargs = bucket.uploads[0]
    assert content_type == "application/gzip"
    assert kwargs["retry"] is None  # whole-hour retries only; the stream can't be rewound


def test_raw_stream_is_not_decoded(config, bucket, no_sleep):
    resp = FakeResponse(200, BODY)
    _ingest(FakeSession({URL15: [resp]}), bucket, config, no_sleep)
    assert resp.raw.decode_content is False


def test_404_is_missing_and_not_retried(config, bucket, no_sleep):
    r = _ingest(FakeSession({URL15: [FakeResponse(404)]}), bucket, config, no_sleep)
    assert (r.status, r.attempts, r.error) == (Status.MISSING, 1, None)
    assert no_sleep == [] and bucket.objects == {}


@pytest.mark.parametrize("first", [FakeResponse(503), FakeResponse(429), requests.Timeout("t")])
def test_transient_errors_are_retried(config, bucket, no_sleep, first):
    session = FakeSession({URL15: [first, FakeResponse(500), FakeResponse(200, BODY)]})
    r = _ingest(session, bucket, config, no_sleep)
    assert (r.status, r.attempts) == (Status.OK, 3)
    assert len(no_sleep) == 2 and no_sleep[0] < no_sleep[1] * 2  # backoff applied


def test_fails_after_max_attempts(config, bucket, no_sleep):
    session = FakeSession({URL15: [FakeResponse(500)] * 3})
    r = _ingest(session, bucket, config, no_sleep)
    assert (r.status, r.attempts) == (Status.FAILED, 3)
    assert "HTTP 500" in r.error
    assert bucket.objects == {}


def test_other_4xx_fails_without_retry(config, bucket, no_sleep):
    r = _ingest(FakeSession({URL15: [FakeResponse(403)]}), bucket, config, no_sleep)
    assert (r.status, r.attempts) == (Status.FAILED, 1)
    assert "HTTPError" in r.error


def test_truncated_transfer_is_retried_and_never_left_behind(config, bucket, no_sleep):
    truncated = [FakeResponse(200, BODY, content_length=len(BODY) + 10) for _ in range(3)]
    r = _ingest(FakeSession({URL15: truncated}), bucket, config, no_sleep)
    assert (r.status, r.attempts) == (Status.FAILED, 3)
    assert "size mismatch" in r.error
    assert OBJ15 not in bucket.objects  # truncated upload deleted


def test_skips_existing_object_with_same_size(config, bucket, no_sleep):
    bucket.objects[OBJ15] = BODY
    r = _ingest(FakeSession({URL15: [FakeResponse(200, BODY)]}), bucket, config, no_sleep)
    assert (r.status, r.bytes) == (Status.SKIPPED, len(BODY))
    assert bucket.uploads == []


def test_force_overwrites_existing_object(config, bucket, no_sleep):
    bucket.objects[OBJ15] = BODY
    r = _ingest(
        FakeSession({URL15: [FakeResponse(200, BODY)]}), bucket, config, no_sleep, force=True
    )
    assert r.status is Status.OK
    assert len(bucket.uploads) == 1


def test_overwrites_existing_object_with_different_size(config, bucket, no_sleep):
    bucket.objects[OBJ15] = b"stale partial"
    r = _ingest(FakeSession({URL15: [FakeResponse(200, BODY)]}), bucket, config, no_sleep)
    assert r.status is Status.OK
    assert bucket.objects[OBJ15] == BODY


# --- run / summarize ----------------------------------------------------------------------


def test_run_returns_results_in_hour_order(config, bucket, no_sleep):
    urls = {h: f"https://example.test/2026-10-01-{h}.json.gz" for h in range(4)}
    session = FakeSession(
        {
            urls[0]: [FakeResponse(200, b"a")],
            urls[1]: [FakeResponse(404)],
            urls[2]: [FakeResponse(500)] * 3,
            urls[3]: [FakeResponse(200, b"d")],
        }
    )
    results = run(
        DAY,
        [0, 1, 2, 3],
        config=config,
        bucket=bucket,
        session=session,
        workers=4,
        sleep=no_sleep.append,
    )
    assert [(r.hour, r.status) for r in results] == [
        (0, Status.OK),
        (1, Status.MISSING),
        (2, Status.FAILED),
        (3, Status.OK),
    ]


def _results(*statuses):
    return [HourResult(hour=h, status=s, gcs_uri="gs://b/x") for h, s in enumerate(statuses)]


@pytest.mark.parametrize(
    ("statuses", "allow_missing", "code"),
    [
        ((Status.OK, Status.SKIPPED), False, EXIT_OK),
        ((Status.OK, Status.MISSING), False, EXIT_MISSING),
        ((Status.OK, Status.MISSING), True, EXIT_OK),
        ((Status.OK, Status.FAILED), False, EXIT_FAILED),
        ((Status.MISSING, Status.FAILED), True, EXIT_FAILED),
    ],
)
def test_summarize_exit_codes(statuses, allow_missing, code):
    assert summarize(DAY, _results(*statuses), allow_missing=allow_missing) == code


# --- CLI ----------------------------------------------------------------------------------


def _json_lines(text):
    return [json.loads(line) for line in text.strip().splitlines()]


def test_main_end_to_end_logs_one_line_per_hour_and_summary(monkeypatch, capsys):
    bucket = FakeBucket()
    session = FakeSession(
        {
            "https://example.test/2026-10-01-0.json.gz": [FakeResponse(200, b"x")],
            "https://example.test/2026-10-01-1.json.gz": [FakeResponse(404)],
        }
    )
    monkeypatch.setenv("GCS_BUCKET", "test-bucket")
    monkeypatch.setenv("GHARCHIVE_BASE_URL", "https://example.test")
    monkeypatch.setattr(gharchive, "_make_bucket", lambda cfg: bucket)
    monkeypatch.setattr(gharchive, "_make_session", lambda n: session)

    code = gharchive.main(["--date", "2026-10-01", "--hours", "0-1"])

    lines = _json_lines(capsys.readouterr().out)
    hour_lines = [line for line in lines if line.get("event") == "hour_done"]
    assert {(h["hour"], h["status"]) for h in hour_lines} == {(0, "ok"), (1, "missing")}
    assert all({"bytes", "duration_s", "attempts"} <= h.keys() for h in hour_lines)
    summary = lines[-1]
    assert summary["event"] == "run_summary" and summary["missing_hours"] == [1]
    assert summary["msg"] == "1/2 ok; missing hours [1]"
    assert code == EXIT_MISSING


def test_main_without_bucket_is_usage_error(monkeypatch, capsys):
    monkeypatch.delenv("GCS_BUCKET", raising=False)
    assert gharchive.main(["--date", "2026-10-01"]) == EXIT_USAGE
    assert "GCS_BUCKET is not set" in capsys.readouterr().out


@pytest.mark.parametrize(
    "argv", [["--date", "2026-13-01"], ["--date", "2026-10-01", "--hours", "25"], []]
)
def test_main_rejects_bad_arguments(argv):
    with pytest.raises(SystemExit) as exc:
        gharchive.main(argv)
    assert exc.value.code == EXIT_USAGE
