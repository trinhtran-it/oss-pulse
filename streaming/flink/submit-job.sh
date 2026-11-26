#!/usr/bin/env bash
# Submit the Flink SQL job wiki_edits_5min to the session cluster, safely and repeatably:
#  1. If the job is already running, do nothing (so `make stream-up` never starts it twice).
#  2. If a retained checkpoint exists, resume from it: Kafka offsets AND open windows come back
#     together, so a restart neither loses nor double-counts events (exactly-once state).
#     Without it, the job would restart from the committed offsets and lose the open windows.
#  3. Fill in the Postgres credentials from the environment (never stored in the SQL file).
# FRESH_START=1: cancel the running job and start without restoring state, from the consumer
# group's offsets. Needed after changing the query, whose old state no longer fits.
set -euo pipefail

REST="${FLINK_REST:-http://jobmanager:8081}"
JOB_NAME="wiki_edits_5min"
SQL_TEMPLATE="/opt/sql/${JOB_NAME}.sql"
CHECKPOINTS="${CHECKPOINT_DIR:-/flink/checkpoints}"
: "${PG_USER:?PG_USER is not set}"
: "${PG_PASSWORD:?PG_PASSWORD is not set}"

for _ in $(seq 1 60); do
  curl -fsS "${REST}/overview" >/dev/null 2>&1 && break
  sleep 2
done

active="$(curl -fsS "${REST}/jobs/overview" \
  | grep -oE "\"jid\":\"[0-9a-f]+\",\"name\":\"${JOB_NAME}\"[^}]*\"state\":\"(RUNNING|CREATED|INITIALIZING|RESTARTING)\"" \
  | grep -oE '[0-9a-f]{32}' || true)"
if [[ -n "${active}" && "${FRESH_START:-0}" != "1" ]]; then
  echo "submit-job: ${JOB_NAME} is already running, nothing to do"
  exit 0
fi
for jid in ${active}; do
  echo "submit-job: FRESH_START=1, cancelling job ${jid}"
  curl -fsS -X PATCH "${REST}/jobs/${jid}?mode=cancel" >/dev/null
done

sql="$(mktemp --suffix=.sql)"
trap 'rm -f "${sql}"' EXIT
latest="$(find "${CHECKPOINTS}" -mindepth 3 -maxdepth 3 -name _metadata -path '*/chk-*/*' \
  -printf '%T@ %h\n' 2>/dev/null | sort -n | tail -n 1 | cut -d' ' -f2- || true)"
if [[ "${FRESH_START:-0}" == "1" ]]; then
  echo "submit-job: FRESH_START=1, not restoring any checkpoint"
elif [[ -n "${latest}" ]]; then
  echo "submit-job: restoring from ${latest}"
  echo "SET 'execution.state-recovery.path' = 'file://${latest}';" > "${sql}"
else
  echo "submit-job: no retained checkpoint, starting from the committed Kafka offsets"
fi
sed -e "s|\${PG_USER}|${PG_USER}|g" -e "s|\${PG_PASSWORD}|${PG_PASSWORD}|g" "${SQL_TEMPLATE}" >> "${sql}"

# sql-client echoes every statement: mask the password in that output.
/opt/flink/bin/sql-client.sh -f "${sql}" 2>&1 | sed -E "s/('password' = ')[^']*'/\\1***'/"

# sql-client exits 0 even when a statement fails, and a job is briefly RUNNING before its first
# read fails. So wait for a COMPLETED checkpoint: source, operators and state all work.
for _ in $(seq 1 45); do
  jid="$(curl -fsS "${REST}/jobs/overview" \
    | grep -oE "\"jid\":\"[0-9a-f]+\",\"name\":\"${JOB_NAME}\"[^}]*\"state\":\"RUNNING\"" \
    | grep -oE '[0-9a-f]{32}' | head -n 1 || true)"
  if [[ -n "${jid}" ]] && curl -fsS "${REST}/jobs/${jid}/checkpoints" | grep -q '"completed":[1-9]'; then
    echo "submit-job: ${JOB_NAME} is RUNNING and has completed a checkpoint (job ${jid})"
    exit 0
  fi
  sleep 2
done
echo "submit-job: ${JOB_NAME} has no completed checkpoint after 90 s: check the Flink UI" >&2
exit 1
