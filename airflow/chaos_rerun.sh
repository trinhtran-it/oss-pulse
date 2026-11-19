#!/usr/bin/env bash
# Chaos test: clear one SUCCESSFUL day of oss_pulse_daily, let Airflow run it again, and prove the
# re-run changed nothing. Row counts for that day must be identical before and after.
#
#   airflow/chaos_rerun.sh 2026-10-15        (or: make chaos-test DAY=2026-10-15)
#
# Compared (BigQuery, prod datasets):
#   raw.events rows and distinct event_ids for the day   <- load_bq_partition (WRITE_TRUNCATE)
#   fct_events rows for the day and the day before       <- dbt_build (insert_overwrite + lookback)
#   agg_repo_daily rows for the day
#
# Needs: Airflow running (make up), bq CLI, and GCP_PROJECT_ID, GCP_REGION, BQ_DATASET_RAW,
#        BQ_DATASET_MARTS, BQ_DATASET_AUDIT in the environment (make loads .env).
# Exit: 0 counts equal; 1 counts differ or the re-run failed; 2 usage error / day not successful.

set -euo pipefail

: "${GCP_PROJECT_ID:?GCP_PROJECT_ID is not set}"
: "${GCP_REGION:?GCP_REGION is not set}"
RAW="${BQ_DATASET_RAW:-oss_pulse_raw}"
MARTS="${BQ_DATASET_MARTS:-oss_pulse_marts}"
AUDIT="${BQ_DATASET_AUDIT:-oss_pulse_audit}"
DAG_ID=oss_pulse_daily
TIMEOUT_MIN="${CHAOS_TIMEOUT_MIN:-60}"

day="${1:-}"
if [[ ! "$day" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
  echo "usage: $0 YYYY-MM-DD" >&2
  exit 2
fi
logical_date="${day}T00:00:00+00:00"

log() { printf '%s [chaos] %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }
compose=(docker compose -f airflow/docker-compose.yaml --env-file .env)
af() { "${compose[@]}" exec -T airflow-scheduler airflow "$@" 2>/dev/null; }
run_state() {
  af dags state "$DAG_ID" "$logical_date" | grep -o -E 'success|failed|running|queued' | tail -1
}
bq_csv() {
  bq --quiet --headless --project_id="$GCP_PROJECT_ID" --location="$GCP_REGION" query \
    --use_legacy_sql=false --format=csv --parameter="day:DATE:${day}" "$1" | tail -n +2
}

counts() {
  bq_csv "
    SELECT 'raw.events rows' AS metric, COUNT(*) AS n
    FROM \`${RAW}.events\`
    WHERE created_at >= TIMESTAMP(@day) AND created_at < TIMESTAMP(DATE_ADD(@day, INTERVAL 1 DAY))
    UNION ALL
    SELECT 'raw.events distinct event_id', COUNT(DISTINCT event_id)
    FROM \`${RAW}.events\`
    WHERE created_at >= TIMESTAMP(@day) AND created_at < TIMESTAMP(DATE_ADD(@day, INTERVAL 1 DAY))
    UNION ALL
    SELECT 'fct_events rows', COUNT(*) FROM \`${MARTS}.fct_events\` WHERE event_date = @day
    UNION ALL
    SELECT 'fct_events rows (day before)', COUNT(*)
    FROM \`${MARTS}.fct_events\` WHERE event_date = DATE_SUB(@day, INTERVAL 1 DAY)
    UNION ALL
    SELECT 'agg_repo_daily rows', COUNT(*) FROM \`${MARTS}.agg_repo_daily\` WHERE event_date = @day
    ORDER BY metric"
}

state=$(run_state || true)
if [[ "$state" != "success" ]]; then
  log "run for ${day} is '${state:-missing}', not success. Pick a finished day."
  exit 2
fi

log "counting ${day} before the re-run"
before=$(counts)

log "clearing every task of the ${day} run"
af tasks clear "$DAG_ID" --start-date "$day" --end-date "$day" --yes >/dev/null
start=$SECONDS

# Wait for the cleared run to go back to running, then to finish.
deadline=$((SECONDS + TIMEOUT_MIN * 60))
seen_active=false
while ((SECONDS < deadline)); do
  sleep 20
  state=$(run_state || true)
  case "$state" in
    running | queued) seen_active=true ;;
    success) $seen_active && break ;;
    failed)
      log "re-run FAILED for ${day}; see the Airflow UI"
      exit 1
      ;;
  esac
done
if [[ "$state" != "success" ]]; then
  log "timed out after ${TIMEOUT_MIN} min (state: ${state:-unknown})"
  exit 1
fi
log "re-run finished in $(((SECONDS - start) / 60)) min"

after=$(counts)

printf '\n%-32s %14s %14s  %s\n' "metric (${day})" before after result
status=0
while IFS=, read -r metric b && IFS=, read -r _ a <&3; do
  if [[ "$b" == "$a" ]]; then result=equal; else result=DIFFERENT; status=1; fi
  printf '%-32s %14s %14s  %s\n' "$metric" "$b" "$a" "$result"
done < <(echo "$before") 3< <(echo "$after")

printf '\naudit.load_log rows for %s:\n' "$day"
bq_csv "SELECT status, rows_loaded, finished_at, dag_run_id FROM \`${AUDIT}.load_log\`
        WHERE run_date = @day ORDER BY finished_at"

if ((status == 0)); then log "PASS: re-running ${day} changed no counts"; else log "FAIL: counts changed"; fi
exit "$status"
