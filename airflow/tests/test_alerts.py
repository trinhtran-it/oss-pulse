"""Unit tests for the failure-alert callback. No Airflow install needed (Variable is patched)."""

from __future__ import annotations

import json
import urllib.error
from types import SimpleNamespace

import pytest
from oss_pulse_lib import alerts


def _context(exception: BaseException | None = None) -> dict:
    ti = SimpleNamespace(
        dag_id="oss_pulse_daily",
        task_id="spark_flatten",
        try_number=4,
        log_url="http://localhost:8080/log",
    )
    return {"ti": ti, "ds": "2026-10-15", "exception": exception}


class _Response:
    def __init__(self, status: int) -> None:
        self.status = status

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def test_message_names_task_day_attempt_error_and_log():
    text = alerts.build_message(_context(RuntimeError("batch FAILED")))["text"]
    assert "oss_pulse_daily.spark_flatten" in text
    assert "2026-10-15" in text
    assert "Attempt: 4" in text
    assert "RuntimeError: batch FAILED" in text
    assert "<http://localhost:8080/log|Task log>" in text


def test_message_survives_an_almost_empty_context():
    assert "failed for run date *?*" in alerts.build_message({})["text"]


def test_long_errors_are_truncated():
    text = alerts.build_message(_context(ValueError("x" * 5000)))["text"]
    assert len(text) < 600


def test_notify_posts_json_to_the_webhook(monkeypatch):
    sent = {}

    def fake_urlopen(request, timeout):
        sent["url"] = request.full_url
        sent["body"] = json.loads(request.data)
        sent["timeout"] = timeout
        return _Response(200)

    monkeypatch.setattr(alerts, "_webhook_url", lambda: "https://hooks.example/abc")
    monkeypatch.setattr(alerts.urllib.request, "urlopen", fake_urlopen)
    alerts.notify_failure(_context(RuntimeError("boom")))

    assert sent["url"] == "https://hooks.example/abc"
    assert "boom" in sent["body"]["text"]
    assert sent["timeout"] == alerts.TIMEOUT_SECONDS


def test_notify_without_webhook_only_logs(monkeypatch, caplog):
    monkeypatch.setattr(alerts, "_webhook_url", lambda: None)
    monkeypatch.setattr(alerts.urllib.request, "urlopen", pytest.fail)  # must not be called
    alerts.notify_failure(_context(RuntimeError("boom")))
    assert "alert not sent" in caplog.text


def test_notify_skips_failures_that_were_already_alerted(monkeypatch):
    class PipelineFailed(Exception):
        already_alerted = True

    monkeypatch.setattr(alerts, "_webhook_url", lambda: pytest.fail("should not look up"))
    alerts.notify_failure(_context(PipelineFailed("upstream failed")))


@pytest.mark.parametrize(
    "error",
    [urllib.error.URLError("dns"), TimeoutError("slow"), ValueError("bad url")],
)
def test_notify_never_raises_when_the_webhook_breaks(monkeypatch, error):
    def broken(request, timeout):
        raise error

    monkeypatch.setattr(alerts, "_webhook_url", lambda: "https://hooks.example/abc")
    monkeypatch.setattr(alerts.urllib.request, "urlopen", broken)
    alerts.notify_failure(_context(RuntimeError("boom")))  # no exception


def test_notify_never_raises_when_the_variable_lookup_breaks(monkeypatch):
    def broken():
        raise RuntimeError("API server down")

    monkeypatch.setattr(alerts, "_webhook_url", broken)
    alerts.notify_failure(_context(RuntimeError("boom")))  # no exception


def test_non_2xx_response_is_reported_as_not_sent(monkeypatch):
    monkeypatch.setattr(alerts.urllib.request, "urlopen", lambda request, timeout: _Response(500))
    assert alerts.post_to_webhook("https://hooks.example/abc", {"text": "x"}) is False
