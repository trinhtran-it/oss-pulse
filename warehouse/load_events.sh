#!/usr/bin/env bash
# Load processed events into BigQuery, replacing exactly one daily partition per date.
#
#   warehouse/load_events.sh 2026-10-01 [2026-10-02 ...]
#
# For each date:
#   1. Require gs://$GCS_BUCKET/processed/events/dt=<date>/_SUCCESS. Spark writes it only after a
#      complete write, so a half-written day is never loaded.
#   2. bq load --replace into events$YYYYMMDD. The load job atomically replaces that one partition
#      (other days untouched), so re-running gives the same result. BigQuery rejects the load if
#      any row belongs to another date.
#   3. Report the partition's row count; fail if it is 0.
#
# Env: GCP_PROJECT_ID, GCP_REGION, GCS_BUCKET (required); BQ_DATASET_RAW (default oss_pulse_raw),
#      PROCESSED_PREFIX (default processed/events).
# Exit: 0 all dates loaded; 1 a date failed (stops at the first failure); 2 usage error.

set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID is not set}"
: "${GCP_REGION:?GCP_REGION is not set}"
: "${GCS_BUCKET:?GCS_BUCKET is not set}"
DATASET="${BQ_DATASET_RAW:-oss_pulse_raw}"
PREFIX="${PROCESSED_PREFIX:-processed/events}"
TABLE="${DATASET}.events"

log() { printf '%s [load_events] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }
die() {
  log "ERROR: $*"
  exit 1
}

if [[ $# -eq 0 ]]; then
  echo "usage: $0 YYYY-MM-DD [YYYY-MM-DD ...]" >&2
  exit 2
fi
for day in "$@"; do
  if [[ ! "$day" =~ ^[0-9]{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$ ]]; then
    echo "invalid date '$day', expected YYYY-MM-DD" >&2
    exit 2
  fi
done

bq_() { bq --quiet --headless --project_id="$GCP_PROJECT_ID" --location="$GCP_REGION" "$@"; }

for day in "$@"; do
  src="gs://${GCS_BUCKET}/${PREFIX}/dt=${day}"
  partition="${TABLE}\$${day//-/}"

  gcloud storage objects describe "${src}/_SUCCESS" --project="$GCP_PROJECT_ID" >/dev/null 2>&1 ||
    die "${day}: ${src}/_SUCCESS not found. Spark output missing or incomplete; not loading."

  log "${day}: loading ${src}/*.parquet -> ${partition} (replace)"
  start=$SECONDS
  bq_ load --source_format=PARQUET --replace "$partition" "${src}/*.parquet" ||
    die "${day}: bq load failed"

  rows=$(bq_ show --format=json "$partition" |
    python3 -c 'import json, sys; print(json.load(sys.stdin).get("numRows", "0"))')
  [[ "$rows" -gt 0 ]] || die "${day}: partition is empty after load"
  log "${day}: ok, ${rows} rows in ${partition} ($((SECONDS - start))s)"
done
