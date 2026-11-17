from __future__ import annotations

import json
from datetime import date

import pytest

from spark.flatten_events import (
    OUTPUT_COLUMNS,
    Metrics,
    check_quality,
    clean,
    compute_metrics,
    flatten,
    main,
    partition_path,
    read_raw,
)
from spark.tests.conftest import event, write_ndjson_gz

DAY = date(2026, 10, 1)

EVENTS = [
    # new-format PR merge (after ~Oct 2025): action = merged, no pull_request.merged
    event(
        "1",
        "PullRequestEvent",
        "2026-10-01T00:30:00Z",
        org={"id": 7, "login": "acme"},
        payload={"action": "merged", "number": 42, "pull_request": {"id": 9, "number": 42}},
    ),
    event("1", "PullRequestEvent", "2026-10-01T00:30:00Z"),  # duplicate event_id
    # old-format PR merge: closed + pull_request.merged = true
    event(
        "2",
        "PullRequestEvent",
        "2026-10-01T23:59:59Z",
        payload={"action": "closed", "pull_request": {"number": 7, "merged": True}},
    ),
    event(
        "3",
        "PushEvent",
        "2026-10-01T12:00:00Z",
        payload={"ref": "refs/heads/main", "size": 3, "distinct_size": 2, "commits": [{}]},
    ),
    event("4", "WatchEvent", "2026-10-01T05:00:00Z", payload={"action": "started"}),
    event("5", "IssuesEvent", "2026-10-01T06:00:00Z", payload={"issue": {"number": 11}}),
    event("6", "ReleaseEvent", "2026-10-01T07:00:00Z", payload={"release": {"tag_name": "v1"}}),
    event("7", "ForkEvent", "2026-10-01T08:00:00Z", repo={}),  # real upstream quirk
    event("8", "WatchEvent", "2026-09-30T23:59:59Z"),  # previous UTC day: out of range
    event("9", "WatchEvent", "not-a-timestamp"),  # invalid
    {"type": "WatchEvent", "created_at": "2026-10-01T01:00:00Z"},  # invalid: no id
    "{this is not json",  # corrupt
]


@pytest.fixture
def raw_path(tmp_path):
    # hr=XX sub-directories, like the ingestion layout
    write_ndjson_gz(tmp_path / "raw/dt=2026-10-01/hr=00/a.json.gz", EVENTS[:6])
    write_ndjson_gz(tmp_path / "raw/dt=2026-10-01/hr=12/b.json.gz", EVENTS[6:])
    return str(tmp_path / "raw/dt=2026-10-01")


@pytest.fixture
def flat(spark, raw_path):
    return flatten(read_raw(spark, raw_path)).cache()


def _rows(df):
    return {r["event_id"]: r for r in df.collect()}


def test_metrics_count_every_category(flat):
    assert compute_metrics(flat, DAY) == Metrics(
        input_rows=12,
        corrupt_rows=1,
        invalid_rows=2,
        out_of_range_rows=1,
        duplicate_rows=1,
        output_rows=7,
    )


def test_clean_keeps_one_valid_row_per_event_in_column_order(flat):
    out = clean(flat, DAY)
    assert out.columns == OUTPUT_COLUMNS
    assert sorted(_rows(out)) == ["1", "2", "3", "4", "5", "6", "7"]
    assert "hr" not in out.columns  # recursiveFileLookup: no partition column inferred


def test_types_are_explicit(flat):
    types = dict(clean(flat, DAY).dtypes)
    assert types["event_id"] == "string"
    assert types["created_at"] == "timestamp"
    assert types["event_date"] == "date"
    assert (types["actor_id"], types["repo_id"], types["pr_number"]) == ("bigint",) * 3
    assert types["pr_merged"] == "boolean"


def test_created_at_and_derived_columns_are_utc(flat):
    # JVM runs in America/Los_Angeles (see conftest); values must not shift.
    rows = _rows(
        clean(flat, DAY).selectExpr(
            "event_id",
            "date_format(created_at, 'yyyy-MM-dd HH:mm:ss') AS ts",
            "CAST(event_date AS STRING) AS d",
            "event_hour",
        )
    )
    assert (rows["1"]["ts"], rows["1"]["d"], rows["1"]["event_hour"]) == (
        "2026-10-01 00:30:00",
        "2026-10-01",
        0,
    )
    assert (rows["2"]["d"], rows["2"]["event_hour"]) == ("2026-10-01", 23)


def test_payload_fields_flattened_across_formats(flat):
    rows = _rows(clean(flat, DAY))
    new_pr, old_pr, push = rows["1"], rows["2"], rows["3"]
    assert (new_pr.payload_action, new_pr.pr_number, new_pr.pr_merged) == ("merged", 42, None)
    assert (old_pr.payload_action, old_pr.pr_number, old_pr.pr_merged) == ("closed", 7, True)
    assert (push.ref, push.push_size, push.push_distinct_size) == ("refs/heads/main", 3, 2)
    assert rows["5"].issue_number == 11
    assert rows["6"].release_tag == "v1"
    assert rows["4"].payload_action == "started"


def test_entities_repo_owner_and_nulls(flat):
    rows = _rows(clean(flat, DAY))
    assert (rows["1"].repo_owner, rows["1"].repo_name, rows["1"].org_login) == (
        "acme",
        "acme/widgets",
        "acme",
    )
    assert rows["4"].org_login is None  # user-owned repo
    assert (rows["7"].repo_id, rows["7"].repo_owner) == (None, None)  # "repo": {}


def test_payload_json_is_the_raw_object(flat):
    rows = _rows(clean(flat, DAY))
    assert json.loads(rows["3"].payload_json)["commits"] == [{}]  # kept even though not flattened
    assert rows["3"].source_file.endswith("hr=00/a.json.gz")


@pytest.mark.parametrize(
    ("metrics", "fraction", "expected"),
    [
        (Metrics(input_rows=100, corrupt_rows=1, output_rows=99), 0.01, None),
        (Metrics(input_rows=100, corrupt_rows=1, invalid_rows=1, output_rows=98), 0.01, "bad row"),
        (Metrics(), 0.01, "no input rows"),
        (Metrics(input_rows=5, out_of_range_rows=5), 0.01, "no valid rows"),
        (Metrics(input_rows=100, out_of_range_rows=1, output_rows=99), 0.01, None),
        (Metrics(input_rows=100, out_of_range_rows=2, output_rows=98), 0.01, "out-of-range"),
    ],
)
def test_check_quality(metrics, fraction, expected):
    reason = check_quality(metrics, fraction)
    assert reason is None if expected is None else expected in reason


def test_check_quality_out_of_range_threshold_is_separate():
    m = Metrics(input_rows=100, out_of_range_rows=30, output_rows=70)
    assert check_quality(m, max_bad_fraction=0.0, max_out_of_range_fraction=0.5) is None
    assert "time-zone" in check_quality(m, max_bad_fraction=0.5, max_out_of_range_fraction=0.01)


def test_main_aborts_when_wrong_session_time_zone_shifts_dates(spark, tmp_path, capsys):
    """The classic bug: a UTC+7 session puts 17:00-23:59 UTC on the next day."""
    hours = [f"2026-10-01T{h:02d}:30:00Z" for h in range(24)]
    path = write_ndjson_gz(
        tmp_path / "raw/day.json.gz",
        [event(str(i), "WatchEvent", ts) for i, ts in enumerate(hours)],
    )
    out = tmp_path / "processed/events"
    argv = ["--date", "2026-10-01", "--input", str(path), "--output", str(out)]
    try:
        # Simulates a session whose time zone was not forced to UTC. The zone must be set
        # before the DataFrame is built: Spark binds it into to_date()/hour() at analysis time.
        spark.conf.set("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
        shifted = compute_metrics(flatten(read_raw(spark, str(path))), DAY)
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")

    assert shifted.out_of_range_rows == 7  # 17:30 .. 23:30 UTC moved to Oct 2
    assert "out-of-range" in check_quality(shifted, max_bad_fraction=0.01)
    # With the job's UTC setting the same data passes and nothing is lost.
    assert main(argv, spark=spark) == 0
    assert spark.read.parquet(str(out / "dt=2026-10-01")).count() == 24


def test_partition_path():
    assert (
        partition_path("gs://b/processed/events/", DAY) == "gs://b/processed/events/dt=2026-10-01"
    )


def test_main_overwrites_only_its_date(spark, raw_path, tmp_path, capsys):
    out = tmp_path / "processed/events"
    other = out / "dt=2026-09-30"
    other.mkdir(parents=True)
    (other / "keep.parquet").write_text("other date")
    argv = ["--date", "2026-10-01", "--input", raw_path, "--output", str(out)]
    argv += ["--max-bad-fraction", "0.5", "--max-out-of-range-fraction", "0.5"]

    assert main(argv, spark=spark) == 0
    assert main(argv, spark=spark) == 0  # rerun: same partition overwritten, not appended

    assert spark.read.parquet(str(out / "dt=2026-10-01")).count() == 7
    assert (other / "keep.parquet").read_text() == "other date"
    summary = json.loads(capsys.readouterr().out.strip().splitlines()[-1])
    assert summary["event"] == "flatten_summary"
    assert (summary["output_rows"], summary["duplicate_rows"]) == (7, 1)


def test_main_aborts_without_writing_when_quality_gate_fails(spark, raw_path, tmp_path, capsys):
    out = tmp_path / "processed/events"
    argv = ["--date", "2026-10-01", "--input", raw_path, "--output", str(out)]
    assert main(argv, spark=spark) == 1  # 3/12 bad rows > default 1%
    assert not (out / "dt=2026-10-01").exists()
    assert "flatten_aborted" in capsys.readouterr().out
