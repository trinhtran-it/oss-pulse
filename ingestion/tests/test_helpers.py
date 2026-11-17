from datetime import date

import pytest

from ingestion.gharchive import (
    Config,
    ConfigError,
    backoff_delay,
    gcs_object_name,
    parse_hours,
    source_url,
)

DAY = date(2026, 10, 1)


@pytest.mark.parametrize(
    ("hour", "expected"),
    [
        (0, "https://data.gharchive.org/2026-10-01-0.json.gz"),
        (5, "https://data.gharchive.org/2026-10-01-5.json.gz"),
        (15, "https://data.gharchive.org/2026-10-01-15.json.gz"),
    ],
)
def test_source_url_hour_is_not_zero_padded(hour, expected):
    assert source_url(DAY, hour) == expected


def test_source_url_custom_base_url_trailing_slash():
    assert source_url(DAY, 1, "https://mirror.test/") == "https://mirror.test/2026-10-01-1.json.gz"


@pytest.mark.parametrize(
    ("hour", "expected"),
    [
        (15, "raw/gharchive/dt=2026-10-01/hr=15/2026-10-01-15.json.gz"),
        (3, "raw/gharchive/dt=2026-10-01/hr=03/2026-10-01-3.json.gz"),
    ],
)
def test_gcs_object_name_is_hive_partitioned(hour, expected):
    assert gcs_object_name(DAY, hour) == expected


def test_gcs_object_name_normalizes_prefix_slashes():
    assert gcs_object_name(DAY, 0, "/lake/raw/").startswith("lake/raw/dt=2026-10-01/hr=00/")


@pytest.mark.parametrize("hour", [-1, 24])
def test_invalid_hour_rejected(hour):
    with pytest.raises(ValueError):
        source_url(DAY, hour)


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("0-23", list(range(24))),
        ("15", [15]),
        ("0-2,5,22-23", [0, 1, 2, 5, 22, 23]),
        (" 3 - 4 , 1 ", [1, 3, 4]),
        ("5,1-6,5", [1, 2, 3, 4, 5, 6]),  # overlaps de-duplicated, sorted
        ("7-7", [7]),
    ],
)
def test_parse_hours(spec, expected):
    assert parse_hours(spec) == expected


@pytest.mark.parametrize("spec", ["", "24", "5-3", "a", "1-", "-1", "1,,2", "0-24", "1.5"])
def test_parse_hours_rejects_invalid(spec):
    with pytest.raises(ValueError):
        parse_hours(spec)


@pytest.mark.parametrize(
    ("attempt", "low", "high"),
    [(1, 1.0, 2.0), (2, 2.0, 4.0), (3, 4.0, 8.0), (10, 30.0, 60.0)],  # capped at 60s
)
def test_backoff_delay_is_exponential_with_bounded_jitter(attempt, low, high):
    assert backoff_delay(attempt, rand=lambda: 0.0) == low
    assert backoff_delay(attempt, rand=lambda: 1.0) == high


def test_config_from_env():
    cfg = Config.from_env({"GCS_BUCKET": "b", "INGEST_MAX_ATTEMPTS": "5", "GCP_PROJECT_ID": "p"})
    assert (cfg.bucket, cfg.max_attempts, cfg.project, cfg.prefix) == ("b", 5, "p", "raw/gharchive")


@pytest.mark.parametrize(
    "env",
    [
        {},
        {"GCS_BUCKET": "  "},
        {"GCS_BUCKET": "b", "INGEST_MAX_ATTEMPTS": "x"},
        {"GCS_BUCKET": "b", "INGEST_MAX_ATTEMPTS": "0"},
    ],
)
def test_config_from_env_rejects_invalid(env):
    with pytest.raises(ConfigError):
        Config.from_env(env)
