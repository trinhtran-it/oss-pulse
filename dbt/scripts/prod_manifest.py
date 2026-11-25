"""Download or upload dbt's prod manifest.json, the "state" that Slim CI compares against.

The daily Airflow run uploads the manifest after every successful prod `dbt build`
(task publish_dbt_manifest). CI downloads it and builds only what a pull request changed:

    dbt build --select state:modified+ --defer --state <dir>

Usage:
    python dbt/scripts/prod_manifest.py download <dir>   # exit 3 if no prod manifest exists yet
    python dbt/scripts/prod_manifest.py upload <manifest.json>

Location: gs://$GCS_BUCKET/$DBT_PROD_MANIFEST_OBJECT (default dbt/prod/manifest.json).
Auth: Application Default Credentials (your gcloud login, or Workload Identity in CI).
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from google.api_core.exceptions import NotFound
from google.cloud import storage

DEFAULT_OBJECT = "dbt/prod/manifest.json"
EXIT_NOT_FOUND = 3


def _blob() -> storage.Blob:
    bucket = os.environ.get("GCS_BUCKET")
    if not bucket:
        sys.exit("GCS_BUCKET is not set")
    name = os.environ.get("DBT_PROD_MANIFEST_OBJECT", DEFAULT_OBJECT)
    # project=None: the bucket name is global; ADC supplies credentials and quota project.
    return storage.Client(project=os.environ.get("GCP_PROJECT_ID")).bucket(bucket).blob(name)


def download(target_dir: Path) -> int:
    blob = _blob()
    target_dir.mkdir(parents=True, exist_ok=True)
    destination = target_dir / "manifest.json"
    try:
        blob.download_to_filename(destination)
    except NotFound:
        destination.unlink(missing_ok=True)  # the client creates the file before the 404
        print(f"No prod manifest at gs://{blob.bucket.name}/{blob.name}")
        return EXIT_NOT_FOUND
    blob.reload()
    updated = f"{blob.updated:%Y-%m-%d %H:%M} UTC"
    print(f"Downloaded gs://{blob.bucket.name}/{blob.name} (updated {updated})")
    return 0


def upload(manifest: Path) -> int:
    if not manifest.is_file():
        sys.exit(f"{manifest} not found: run `dbt parse --target prod` first")
    blob = _blob()
    blob.upload_from_filename(manifest, content_type="application/json")
    print(f"Uploaded {manifest} to gs://{blob.bucket.name}/{blob.name}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("download", help="fetch the prod manifest into DIR/manifest.json").add_argument(
        "dir", type=Path
    )
    sub.add_parser("upload", help="publish a manifest.json as the prod state").add_argument(
        "manifest", type=Path
    )
    args = parser.parse_args(argv)
    if args.command == "download":
        return download(args.dir)
    return upload(args.manifest)


if __name__ == "__main__":
    sys.exit(main())
