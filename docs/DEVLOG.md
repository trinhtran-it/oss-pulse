# Dev log

Short entries on things that broke, surprised me, or changed a decision. Newest first.

## 2026-11-19: The dashboard was right, the first drafts were misleading

The Looker Studio report reads two tables built for it (`agg_events_hourly`, 1 MB, and `rpt_top_repos_7d`, 10 rows) instead of `fct_events` (2.4 GB). Each chart refresh is a billed query, and on `fct_events` the same tiles would scan ~1 GB per refresh.

Three review rounds before it was publishable. Each draft showed correct numbers that said the wrong thing:

1. **Default date range.** A date control left on its default shows the *last 28 days*. For a pipeline that stopped at 10-31, every number covered Oct 22–31 only (4.55M events instead of 30.0M), with no error. Fix: a fixed default range.
2. **One line instead of one per category.** Without a category breakdown, the source gap (push/create/delete missing) looked like GitHub activity collapsing. Broken down, only the "code" line drops.
3. **A weekday heatmap from one month.** Fri/Sat looked busiest, but each weekday rests on 4–5 days and daily volume ranges 41k–163k: the busiest days (Oct 2–3) just happened to be a Friday and Saturday. One cell, Fri 16:00, stood far above the rest because of a single burst on Oct 23 (24.5k events in one hour from thousands of accounts, likely delayed events released at once). The heatmap stays, with a note to read hours, not weekdays.

**Lesson.** Check a dashboard against the warehouse (every scorecard and a few cells matched BigQuery exactly). Then ask what a reader would conclude from it, because correct numbers can still lead to a wrong conclusion.

## 2026-11-19: The October backfill found a gap in the source, not in the pipeline

All 31 days loaded and passed every test. But 2026-10-07 had only 162k events, against 1.5–2.1M on the days around it. All 24 hourly files were present. Grouping by event type showed what happened:

| Days | Events/day | Push, Create, Delete | Stars, PRs |
|---|---|---|---|
| Most days to 10-26 | 0.6–2.1M | present (~75% of events) | steady |
| 10-07, 10-15 … 10-17 | 111–163k | **none** | steady |
| 10-18, 10-23 | 377k, 122k | partial | steady |
| 10-27 onward | 81–108k | **none** | steady |

GH Archive's push/create/delete feed was intermittent for three weeks and then stopped. My earlier notes only said it disappeared "somewhere between 10-01 and 11-03".

**Why it matters.** The pipeline was right to load what the source had, but a dashboard would show "pushes = 0 on 10-07" as if nobody pushed that day. None of my tests could catch it. They check keys, relationships and accepted values, not whether a day looks normal.

**What I'd add.** A volume check per event type: warn when a type that existed the day before has zero rows, or when the daily count is below ~20% of the trailing 7-day median. It would run as a dbt test on `agg_repo_daily` or as a check after `load_bq_partition`. On the dashboard, push metrics should be labelled as unreliable for October 2026.

## 2026-11-18: A laptop is not a scheduler (sleep, heartbeats, and a deadlock)

The backfill ran overnight on my Mac. By morning 9 of 31 days were done, and dbt for 10-08 and 10-10 had "failed" four times each. Yet every dbt attempt log ended with `Done. PASS=41 ERROR=0`.

**What happened.** The Mac slept on battery (`caffeinate -s` only blocks sleep on AC power). Containers froze and tasks stopped sending heartbeats for 16–28 minutes. The scheduler logged `Detected a task instance without a heartbeat`, failed the attempt and started a retry. When the frozen dbt woke up and finished, its auth token was already revoked (`403 Invalid auth token`). Other attempts failed on dropped connections (`SSLEOFError`, `BrokenPipe`, `ReadTimeout`). No data was wrong: every step is idempotent, and GCS only publishes an object once its upload completes.

**The real bug it exposed: `depends_on_past` deadlocked.** I re-ran the failed 10-08 run. It went to `queued`, waiting for one of the 2 run slots (`max_active_runs=2`). Both slots were held by 10-09 and 10-10, whose `dbt_build` waited for 10-08's `dbt_build` (`depends_on_past=True`). Nothing could move. I replaced it with `max_active_tis_per_dag=1`: still one dbt build at a time, but without the date order dbt never needed (fact partitions are independent, and the dims keep min/max timestamps and the newest name). After that, re-running an old day works while newer days carry on.

**Lessons.**
- A missed heartbeat looks exactly like a task failure. Production Airflow belongs on always-on infrastructure (Cloud Composer, a VM, Kubernetes), not on a laptop.
- `depends_on_past` together with `max_active_runs > 1` can deadlock when you repair an old day. Use it only when work truly must be done in date order.
- The audit table told the truth: 10-08 shows `failed`, then `success`.

## 2026-11-18: Three Airflow 3 defaults that would have broken the daily run

1. **`schedule="@daily"` is now a cron *trigger*.** Airflow 3 defaults to `create_cron_data_intervals=False`, so the run for `ds=2026-10-01` starts at 10-01 00:00, before any of that day's data exists. A backfill wouldn't notice, but every live run would ask GH Archive for hours not yet published. Fix: `CronDataIntervalTimetable("@daily")`. I only caught it because the scheduler log showed `data_interval start = end`.
2. **`retry_exponential_backoff=True` no longer backs off.** In 3.4 it's a float multiplier, and `True` means 1.0: 5, 5, 5 minutes. Fix: `2.0`, giving 5, 10, 20 minutes.
3. **The project's vCPU quota is 16, and one Dataproc batch needs 12.** With 2 runs active, the second batch failed with `Quota 'CPUS_ALL_REGIONS' exceeded`. Fix: `max_active_tis_per_dag=1` on `spark_flatten` (in 3.4 this also counts deferred tasks), plus short fixed retries, because Dataproc releases the vCPUs a few seconds *after* a batch reports SUCCEEDED.

## 2026-11-18: Breaking dbt on purpose with a duplicate event

**Goal.** Prove that a duplicate row can't silently reach the dashboard.

**What I did.** In my dev dataset (`dbt_trinh`), I copied one existing event back into `fct_events`:

```sql
INSERT INTO dbt_trinh.fct_events
SELECT * FROM dbt_trinh.fct_events
WHERE event_date = '2026-10-01'
  AND event_id = (SELECT MIN(event_id) FROM dbt_trinh.fct_events WHERE event_date = '2026-10-01');
-- event 16146430926 now appears twice
```

Then I ran the normal daily build, `make dbt-build RUN_DATE=2026-10-03`. It rebuilds only 2026-10-02 and 10-03, so the bad partition (10-01) is left as it is, just like a duplicate that slipped in on an earlier day.

**What happened.**

```
28 of 41 FAIL 1 unique_fct_events_event_id ........ [FAIL 1 in 3.69s]
29 of 41 SKIP relation dbt_trinh.agg_repo_daily .... [SKIP]
30 of 41 SKIP test assert_no_negative_counts ....... [SKIP]
... 3 more agg_repo_daily tests skipped
Done. PASS=35 WARN=0 ERROR=1 SKIP=5 TOTAL=41      (exit code 2)
```

- The `unique` test on `fct_events.event_id` failed ("Got 1 result").
- `dbt build` runs each model's tests **before** its children, so `agg_repo_daily`, the table the dashboard reads, was **skipped**. It kept yesterday's correct numbers instead of counting the duplicate.
- The non-zero exit code means Airflow would mark the task failed and alert.

**Fix.** Rebuild the affected day: `make dbt-build RUN_DATE=2026-10-01 DBT_SELECT="fct_events+"`. Because `fct_events` is `insert_overwrite`, the run *replaces* the 2026-10-01 partition from source instead of appending, so the duplicate disappears. All 16 nodes passed, and every day again has rows = distinct `event_id`s (2,073,815 for 10-01).

The lookback window also included 2026-09-30, a day with no data. It caused no error and created no empty partition, which is good to know for days GH Archive skips.

**What I'd say in an interview.**
- Tests are only useful if they *gate* what runs next. `dbt build` (not `dbt run` then `dbt test`) gives that: a failing test stops downstream models.
- The fix was a re-run, not a manual `DELETE`, because the model is idempotent per partition. The same command handles both late data and repairs.
- Limitation: `unique` scans every `event_id` in `fct_events`, which grows forever. At scale, scope it to the rebuilt days with a `where:` config and run a full check weekly.

## 2026-11-18: dbt under-reported incremental cost; copy_partitions cut it by 66%

dbt logged `654.7 MiB processed` for a 2-day incremental `fct_events` run, more than the 481 MiB full 3-day build. `INFORMATION_SCHEMA.JOBS_BY_USER` showed the whole picture: a 333 MB temporary table **plus** a 655 MB `MERGE` = **987 MB**. dbt only reports the last statement.

| Variant | Total bytes processed |
|---|---|
| Dynamic `insert_overwrite` (default) | 987 MB |
| + static `partitions` list | 987 MB (no change: `on_schema_change` still needs the temp table) |
| + `copy_partitions: true` | **333 MB** (partitions replaced by free copy jobs) |

Lesson: check the job history, not the tool's summary line, before claiming a saving.
