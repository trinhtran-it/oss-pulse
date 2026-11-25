#!/usr/bin/env bash
# Key numbers for the README ("At a glance"), recomputed from the real data, as a Markdown table.
#
#   warehouse/metrics.sh [START END]        (default 2026-10-01 2026-10-31)
#   make metrics START=2026-10-01 END=2026-10-31
#
# Sources: raw.events and fct_events (BigQuery), audit.load_log, the raw files in GCS,
# docs/bq_cost_results.json (cost benchmark), `dbt ls` (test count), and Airflow's metadata DB
# for run durations (only if its Postgres container is running: `make up`).
#
# Env: GCP_PROJECT_ID, GCP_REGION, GCS_BUCKET; BQ_DATASET_RAW/MARTS/AUDIT (defaults oss_pulse_*).
# Cost: ~0.5 GB of BigQuery reads per 31 days (created_at and event_id only); listing GCS is free.

set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID is not set}"
: "${GCP_REGION:?GCP_REGION is not set}"
: "${GCS_BUCKET:?GCS_BUCKET is not set}"
RAW="${BQ_DATASET_RAW:-oss_pulse_raw}"
MARTS="${BQ_DATASET_MARTS:-oss_pulse_marts}"
AUDIT="${BQ_DATASET_AUDIT:-oss_pulse_audit}"
START="${1:-2026-10-01}"
END="${2:-2026-10-31}"
DBT="${DBT:-.venv/bin/dbt}"
COMPOSE=(docker compose -f airflow/docker-compose.yaml --env-file .env)

[[ "$START" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ && "$END" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] \
  || { echo "usage: $0 [START END] (YYYY-MM-DD)" >&2; exit 2; }

n() { python3 -c 'import sys; print(f"{int(sys.argv[1]):,}")' "$1"; }  # 30008428 -> 30,008,428

bq_csv() {  # one CSV row, no header
  bq --quiet --headless --project_id="$GCP_PROJECT_ID" --location="$GCP_REGION" \
    query --use_legacy_sql=false --format=csv \
    --parameter="start:DATE:$START" --parameter="end:DATE:$END" "$1" | tail -n 1
}

# Events per day (raw) and the fact table total, which must match.
IFS=, read -r days total avg_day min_day max_day < <(bq_csv "
  with per_day as (
    select date(created_at) as day, count(*) as n
    from \`$RAW.events\`
    where created_at >= timestamp(@start) and created_at < timestamp(date_add(@end, interval 1 day))
    group by day)
  select count(*), sum(n), cast(avg(n) as int64), min(n), max(n) from per_day")
fct_total=$(bq_csv "select count(*) from \`$MARTS.fct_events\` where event_date between @start and @end")

# Days whose latest audit row is a success.
IFS=, read -r audit_ok audit_days < <(bq_csv "
  select countif(status = 'success'), count(*) from (
    select run_date, array_agg(status order by finished_at desc limit 1)[offset(0)] as status
    from \`$AUDIT.load_log\` where run_date between @start and @end group by run_date)")

# Raw gzip bytes per day in GCS.
raw_stats=$(gcloud storage ls -l "gs://$GCS_BUCKET/raw/gharchive/dt=*/**" 2>/dev/null | awk \
  -v s="$START" -v e="$END" '$3 ~ /^gs:/ {
    match($3, /dt=[0-9-]+/); d = substr($3, RSTART + 3, RLENGTH - 3)
    if (d >= s && d <= e) b[d] += $1 }
  END { for (k in b) { n++; t += b[k]; if (b[k] > m) m = b[k] }
        if (n) printf "%.0f MiB/day average, %.0f MiB peak, %.2f GiB total", t/n/2^20, m/2^20, t/2^30
        else printf "n/a" }')

# Cost benchmark (Step 5): unpartitioned with filters (B) vs partitioned + clustered (C).
bytes_saved=$(python3 -c '
import json
q = {x["key"]: x for x in json.load(open("docs/bq_cost_results.json"))["queries"]}
b, c = q["B"]["bytes_processed"], q["C"]["bytes_processed"]
print(f"{1 - c / b:.1%} ({b / 1e6:.1f} MB to {c / 1e6:.1f} MB)")' 2>/dev/null || echo "n/a")

# dbt tests (data tests + unit tests); parsing needs no BigQuery access.
tests="n/a"
if [[ -x "$DBT" ]]; then
  tests=$("$DBT" --quiet ls --project-dir dbt/oss_pulse --profiles-dir dbt/oss_pulse \
    --resource-type test --resource-type unit_test 2>/dev/null | grep -c . || true)
fi

# Run durations from Airflow's metadata DB, if it is up.
runtime="n/a (start Airflow's database: make up)"
if "${COMPOSE[@]}" exec -T postgres pg_isready -U airflow >/dev/null 2>&1; then
  runtime=$("${COMPOSE[@]}" exec -T postgres psql -U airflow -d airflow -At -c "
    select round((percentile_cont(0.5) within group (order by extract(epoch from end_date - start_date)) / 60)::numeric, 1)
           || ' min median wall time over ' || count(*) || ' successful runs'
    from dag_run where dag_id = 'oss_pulse_daily' and state = 'success'
      and logical_date::date between '$START' and '$END'")
fi

check=$([[ "$total" == "$fct_total" ]] && echo "= fct_events" || echo "≠ fct_events ($fct_total)")
cat <<EOF
| Metric ($START … $END) | Value |
|---|---|
| Events loaded | $(n "$total") $check |
| Events per day | $(n "$avg_day") average, $(n "$min_day")–$(n "$max_day") range |
| Days loaded | $audit_ok/$days successful in the audit log ($audit_days with audit rows) |
| Raw data (gzip) | $raw_stats |
| Bytes saved by partitioning + clustering | $bytes_saved |
| dbt tests | $tests |
| End-to-end runtime per day | $runtime |
EOF
