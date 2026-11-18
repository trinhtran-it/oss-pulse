# Dev log

Short entries on things that broke, surprised me, or changed a decision. Newest first.

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
