"""Failure alerts for oss-pulse DAGs: post a message to a Slack incoming webhook.

Wire it as ``on_failure_callback=notify_failure`` (default_args), so it fires once per task after
its LAST retry fails, not on every attempt.

The webhook URL is a secret. It is read at alert time from the Airflow Variable
``slack_webhook_secret`` (the word "secret" in the key makes Airflow mask it in logs). Set it
outside the repo, e.g. in the git-ignored .env:

    AIRFLOW_VAR_SLACK_WEBHOOK_SECRET=https://hooks.slack.com/services/...

If the Variable is missing, the alert is only logged. Exceptions flagged ``already_alerted``
(record_audit's PipelineFailed) are skipped, so one failed run sends one alert. An alert must
never raise: a broken webhook should not hide the original task failure.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from collections.abc import Mapping
from typing import Any

log = logging.getLogger(__name__)

WEBHOOK_VARIABLE = "slack_webhook_secret"
TIMEOUT_SECONDS = 10


def build_message(context: Mapping[str, Any]) -> dict[str, str]:
    """Slack payload for a failed task, built only from the task context (no I/O)."""
    ti = context.get("ti")
    dag_id = getattr(ti, "dag_id", None) or "?"
    task_id = getattr(ti, "task_id", None) or "?"
    try_number = getattr(ti, "try_number", None)
    log_url = getattr(ti, "log_url", None)
    run_date = context.get("ds") or "?"
    error = context.get("exception")

    lines = [
        f":red_circle: *{dag_id}.{task_id}* failed for run date *{run_date}*",
        f"Attempt: {try_number}" if try_number is not None else None,
        f"Error: `{type(error).__name__}: {str(error)[:300]}`" if error else None,
        f"<{log_url}|Task log>" if log_url else None,
    ]
    return {"text": "\n".join(line for line in lines if line)}


def post_to_webhook(url: str, payload: Mapping[str, Any], timeout: float = TIMEOUT_SECONDS) -> bool:
    """POST a JSON payload. Returns True on HTTP 2xx; logs and returns False on any error."""
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            ok = 200 <= response.status < 300
    except (urllib.error.URLError, TimeoutError, ValueError) as exc:
        log.error("Failure alert could not be sent: %s", exc)
        return False
    if not ok:
        log.error("Failure alert rejected by webhook: HTTP %s", response.status)
    return ok


def _webhook_url() -> str | None:
    from airflow.sdk import Variable  # imported lazily: keeps this module testable without Airflow

    return Variable.get(WEBHOOK_VARIABLE, default=None)


def notify_failure(context: Mapping[str, Any]) -> None:
    """on_failure_callback: send the alert, or just log it when no webhook is configured."""
    if getattr(context.get("exception"), "already_alerted", False):
        log.info("Failure already alerted by an upstream task; not sending a second alert.")
        return
    payload = build_message(context)
    try:
        url = _webhook_url()
    except Exception as exc:  # never let the alert path fail the callback
        log.error("Could not read Variable %s: %s", WEBHOOK_VARIABLE, exc)
        url = None
    if not url:
        log.warning("No %s Variable set; alert not sent: %s", WEBHOOK_VARIABLE, payload["text"])
        return
    if post_to_webhook(url, payload):
        log.info("Failure alert sent to Slack.")
