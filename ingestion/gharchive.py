"""Download one day of GH Archive hourly files and upload them to GCS."""

from __future__ import annotations

import argparse
import logging
import os
from datetime import date

import requests

BASE_URL = "https://data.gharchive.org"
log = logging.getLogger(__name__)


def hourly_urls(day: date) -> list[str]:
    """Return the 24 GH Archive URLs for a given day."""
    return [f"{BASE_URL}/{day:%Y-%m-%d}-{hour}.json.gz" for hour in range(24)]


def gcs_blob_path(url: str) -> str:
    """Map a GH Archive URL to a GCS object path, partitioned by date."""
    filename = url.rsplit("/", 1)[-1]
    day = filename.rsplit("-", 1)[0]
    return f"raw/gharchive/dt={day}/{filename}"


def download(url: str, timeout: int = 60) -> bytes:
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.content


def upload_to_gcs(bucket_name: str, blob_path: str, data: bytes) -> None:
    from google.cloud import storage

    storage.Client().bucket(bucket_name).blob(blob_path).upload_from_string(data)


def run(day: date, bucket: str) -> None:
    for url in hourly_urls(day):
        blob_path = gcs_blob_path(url)
        log.info("Ingesting %s -> gs://%s/%s", url, bucket, blob_path)
        upload_to_gcs(bucket, blob_path, download(url))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--bucket", default=os.environ.get("GCS_BUCKET"))
    args = parser.parse_args()
    if not args.bucket:
        parser.error("--bucket or GCS_BUCKET is required")
    logging.basicConfig(level=logging.INFO)
    run(args.date, args.bucket)


if __name__ == "__main__":
    main()
