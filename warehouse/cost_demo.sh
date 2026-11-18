#!/usr/bin/env bash
# Cost proof: bytes processed by the same question on an unpartitioned copy vs raw.events.
#
#   warehouse/cost_demo.sh COST_DAY DAY [DAY ...]
#   e.g. warehouse/cost_demo.sh 2026-10-02 2026-10-01 2026-10-02 2026-10-03
#
# 1. Loads the same processed Parquet days into <dataset>.events_unpartitioned (no partitioning,
#    no clustering). bq load is free; the copy expires after 7 days.
# 2. Runs the "top starred repos on COST_DAY" query three times, cache disabled:
#      A  unpartitioned copy, no filters
#      B  unpartitioned copy, WITH the date + event_type filters  (filters alone don't prune)
#      C  raw.events (partitioned by day, clustered by event_type), same filters
# 3. Prints bytes processed/billed per query and writes docs/bq_cost_results.json.
#
# Env: GCP_PROJECT_ID, GCP_REGION, GCS_BUCKET; BQ_DATASET_RAW (default oss_pulse_raw).
# Cost: three queries of a few hundred MB at most, within the 1 TiB/month free tier.

set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID is not set}"
: "${GCP_REGION:?GCP_REGION is not set}"
: "${GCS_BUCKET:?GCS_BUCKET is not set}"
DATASET="${BQ_DATASET_RAW:-oss_pulse_raw}"
PREFIX="${PROCESSED_PREFIX:-processed/events}"
OUT_JSON="${COST_RESULTS:-docs/bq_cost_results.json}"

if [[ $# -lt 2 ]]; then
  echo "usage: $0 COST_DAY DAY [DAY ...]" >&2
  exit 2
fi
COST_DAY="$1"
shift

bq_() { bq --quiet --headless --project_id="$GCP_PROJECT_ID" --location="$GCP_REGION" "$@"; }
log() { printf '%s [cost_demo] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

# --- 1. Unpartitioned copy (free load jobs) ---------------------------------------------------
copy="${DATASET}.events_unpartitioned"
mode="--replace"
for day in "$@"; do
  log "loading ${day} into ${copy}"
  bq_ load --source_format=PARQUET "$mode" "$copy" \
    "gs://${GCS_BUCKET}/${PREFIX}/dt=${day}/*.parquet"
  mode="--noreplace"
done
bq_ update --expiration 604800 "$copy" >/dev/null

# --- 2. Same question, three ways -------------------------------------------------------------
query() { # $1 = table, $2 = WHERE clause (may be empty)
  cat <<SQL
SELECT repo_name, COUNT(*) AS stars
FROM \`${GCP_PROJECT_ID}.$1\`
$2
GROUP BY repo_name
ORDER BY stars DESC
LIMIT 10
SQL
}
FILTER="WHERE DATE(created_at) = DATE '${COST_DAY}' AND event_type = 'WatchEvent'"

run() { # $1 = label, $2 = SQL. Prints the job id.
  local job="oss_pulse_cost_${1}_$(date +%s)"
  # stdout discarded: even with --format=none, bq prints a blank line that would corrupt $job.
  bq_ query --use_legacy_sql=false --nouse_cache --job_id="$job" --format=none "$2" >/dev/null
  echo "$job"
}

log "A: ${copy}, no filter"
job_a=$(run a "$(query "$copy" "")")
log "B: ${copy}, date + event_type filter"
job_b=$(run b "$(query "$copy" "$FILTER")")
log "C: ${DATASET}.events (partitioned + clustered), same filter"
job_c=$(run c "$(query "${DATASET}.events" "$FILTER")")

# --- 3. Results -------------------------------------------------------------------------------
stats() { bq_ show --format=json -j "$1"; }
python3 - "$OUT_JSON" "$COST_DAY" "$*" \
  "A" "Unpartitioned, no filter" "$job_a" "$(stats "$job_a")" \
  "B" "Unpartitioned, date + type filter" "$job_b" "$(stats "$job_b")" \
  "C" "Partitioned + clustered, same filter" "$job_c" "$(stats "$job_c")" <<'PY'
import json
import sys

out_path, cost_day, days, *rest = sys.argv[1:]
rows = []
for i in range(0, len(rest), 4):
    key, label, job_id, raw = rest[i : i + 4]
    q = json.loads(raw)["statistics"]["query"]
    rows.append(
        {
            "key": key,
            "label": label,
            "job_id": job_id,
            "bytes_processed": int(q["totalBytesProcessed"]),
            "bytes_billed": int(q["totalBytesBilled"]),
        }
    )

# B and C run identical SQL, so B is the fair baseline. A shows filters add columns to scan.
base = rows[1]["bytes_processed"]
print(f"\nQuestion: top 10 starred repos on {cost_day} (table holds {days})\n")
print(f"{'':2} {'query':38} {'processed':>12} {'billed':>12} {'vs B':>8}")
for r in rows:
    mb_p, mb_b = r["bytes_processed"] / 1e6, r["bytes_billed"] / 1e6
    ratio = r["bytes_processed"] / base if base else 0
    print(f"{r['key']:2} {r['label']:38} {mb_p:>9.1f} MB {mb_b:>9.1f} MB {ratio:>7.1%}")

with open(out_path, "w") as f:
    json.dump({"cost_day": cost_day, "days_loaded": days.split(), "queries": rows}, f, indent=2)
print(f"\nwrote {out_path}")
PY
