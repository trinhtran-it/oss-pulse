"""Offline tests for the Wikimedia producer: SSE parsing, record mapping, resume position."""

from __future__ import annotations

import json

import pytest

from streaming import check_windows as cw
from streaming.producer import wiki_producer as wp

EDIT = {
    "type": "edit",
    "wiki": "enwiki",
    "bot": False,
    "minor": True,
    "namespace": 0,
    "user": "Example User",
    "length": {"old": 100, "new": 130},
    "meta": {"id": "e-1", "dt": "2026-11-25T10:00:03Z", "domain": "en.wikipedia.org"},
}


# --- SSE ------------------------------------------------------------------------------------


def test_parse_sse_events_ids_and_comments():
    lines = [
        ": keep-alive comment",
        "event: message",
        'id: [{"offset":1}]',
        'data: {"a": 1}',
        "",
        'data: {"b": 2}',
        "",
    ]
    events = list(wp.parse_sse(lines))
    assert [e.data for e in events] == ['{"a": 1}', '{"b": 2}']
    # the id is kept for the next event until a new one arrives (WHATWG behaviour)
    assert [e.id for e in events] == ['[{"offset":1}]', '[{"offset":1}]']
    assert events[0].event == "message"


def test_parse_sse_multiline_data_crlf_and_no_space():
    lines = ["data:first\r", "data: second", "", "event: x", ""]
    (event,) = list(wp.parse_sse(lines))  # the "event: x" block has no data: not dispatched
    assert event.data == "first\nsecond"


def test_parse_sse_flushes_event_without_trailing_blank_line():
    assert [e.data for e in wp.parse_sse(["data: tail"])] == ["tail"]


# --- records --------------------------------------------------------------------------------


def test_to_record_keeps_only_what_flink_needs():
    record = wp.to_record(EDIT, salt="s")
    assert record == {
        "event_id": "e-1",
        "event_time": "2026-11-25T10:00:03Z",
        "wiki": "enwiki",
        "domain": "en.wikipedia.org",
        "change_type": "edit",
        "is_bot": False,
        "is_minor": True,
        "namespace": 0,
        "editor_hash": wp.editor_hash("Example User", "s"),
        "bytes_delta": 30,
    }
    assert "user" not in json.dumps(record)  # the editor's name never leaves the producer


@pytest.mark.parametrize("change_type", ["log", "categorize", "external"])
def test_to_record_skips_non_edits(change_type):
    assert wp.to_record({**EDIT, "type": change_type}, salt="s") is None


@pytest.mark.parametrize("broken", [{"meta": {}}, {"wiki": None}, {"meta": {"id": "x"}}])
def test_to_record_skips_incomplete_events(broken):
    assert wp.to_record({**EDIT, **broken}, salt="s") is None


def test_new_page_without_old_length():
    record = wp.to_record({**EDIT, "type": "new", "length": {"new": 50}}, salt="s")
    assert record["change_type"] == "new" and record["bytes_delta"] == 50


def test_editor_hash_is_stable_salted_and_not_the_name():
    a, b = wp.editor_hash("Alice", "s1"), wp.editor_hash("Alice", "s1")
    assert a == b and len(a) == 16 and "Alice" not in a
    assert wp.editor_hash("Alice", "s2") != a
    assert wp.editor_hash(None, "s1") is None


# --- resume position ------------------------------------------------------------------------


def test_offset_store_round_trip_and_atomic_write(tmp_path):
    store = wp.OffsetStore(tmp_path / "state" / "last_event_id")
    assert store.load() is None
    store.save('[{"offset":42}]')
    assert store.load() == '[{"offset":42}]'
    assert not list((tmp_path / "state").glob("*.tmp"))  # temp file renamed into place


def test_backoff_grows_and_is_capped():
    assert 0.5 <= wp.backoff(1) <= 2.0
    assert wp.backoff(20, cap=60.0) <= 60.0


def test_stats_report_rate_and_source_lag():
    stats = wp.Stats(received=10, last_event_time="2026-01-01T00:00:00Z")
    out = stats.as_log()
    assert out["received"] == 10 and out["source_lag_s"] > 0


# --- reconciliation -------------------------------------------------------------------------


def test_floor_window_and_recount_dedupes_like_flink():
    from datetime import datetime

    assert cw.floor_window(datetime(2026, 11, 25, 10, 7, 59)) == datetime(2026, 11, 25, 10, 5)
    records = [
        {"event_id": "a", "event_time": "2026-11-25T10:01:00Z"},
        {"event_id": "a", "event_time": "2026-11-25T10:01:00Z"},  # replayed: counted once
        {"event_id": "b", "event_time": "2026-11-25T10:04:59Z"},
        {"event_id": "c", "event_time": "2026-11-25T10:05:00Z"},  # next window
        {"event_id": "d", "event_time": "2026-11-25T09:59:59Z"},  # before the range
    ]
    counts = cw.recount(records, datetime(2026, 11, 25, 10, 0), datetime(2026, 11, 25, 10, 10))
    assert {k: len(v) for k, v in counts.items()} == {
        datetime(2026, 11, 25, 10, 0): 2,
        datetime(2026, 11, 25, 10, 5): 1,
    }
