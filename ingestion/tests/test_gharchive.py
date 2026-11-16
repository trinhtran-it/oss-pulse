from datetime import date

from ingestion.gharchive import gcs_blob_path, hourly_urls


def test_hourly_urls_returns_24_files():
    urls = hourly_urls(date(2026, 1, 1))
    assert len(urls) == 24
    assert urls[0].endswith("2026-01-01-0.json.gz")
    assert urls[-1].endswith("2026-01-01-23.json.gz")


def test_gcs_blob_path_is_date_partitioned():
    url = "https://data.gharchive.org/2026-01-01-15.json.gz"
    assert gcs_blob_path(url) == "raw/gharchive/dt=2026-01-01/2026-01-01-15.json.gz"
