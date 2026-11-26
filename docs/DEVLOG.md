# Dev log

Short entries on things that broke, surprised me, or changed a decision. Newest first.

## 2026-11-26: Streaming: a 14-hour silent crash loop, and a watermark that dropped 42% of the data

Wikimedia EventStreams → Redpanda → Flink SQL (5-minute windows) → Postgres → Grafana. It looked done after the first evening, and it wasn't.

**1. "RUNNING" is not "working".** The submit script checked that the job was RUNNING, and it was, for a few seconds. Then every Kafka fetch failed with `NoClassDefFoundError: com/github/luben/zstd/BufferPool`: my producer compresses with zstd, and Flink's Kafka connector doesn't bundle the codec. The job crash-looped in `RESTARTING` for **14 hours** while every container showed "Up". Fixes:
- add `zstd-jni` (the version kafka-clients 4.2.0 expects, checksum-verified) to the Flink image;
- make the submit script wait for a **completed checkpoint**, which proves the source, the operators and the state all work;
- add a "data age" panel in Grafana: orange after 10 minutes without a new window, red after 30.

(Two smaller missing pieces on the way: the JDBC connector needs OpenLineage jars it doesn't ship, and `sql-client` exits 0 even when a statement fails.)

**2. The watermark dropped 42% of the edits.** After the zstd fix, Flink re-read the topic: 158 windows, 650,935 edits, and 18 windows simply missing between 20:30 and 22:30 UTC. That's when the Mac slept. A recount from the topic showed the events *were* there: on waking, the producer had resumed with `Last-Event-ID` and replayed 4 hours of edits in 3 minutes. But **65% of the replayed events reached their partition more than a minute behind newer ones** (Wikimedia merges several upstream partitions during a replay), so a 1-minute watermark discarded them as late.
- A bigger watermark would fix completeness by making the "live" tile hours stale.
- The fix is **allowed lateness**: windows still fire 1 minute after they end, then fire again whenever late events arrive, for up to 6 hours. The upsert sink turns each re-fire into an UPDATE of the same row.
- Catch: Flink 2.2's window table functions reject late firing ("doesn't support early-fire and late-fire"), so the job uses the legacy `GROUP BY TUMBLE`.

After a fresh start (new consumer group, since a changed query can't restore old state): **184 windows, no gaps, 1,121,916 edits**. `make stream-check` matched all 60 windows of the last 5 hours **exactly**, including the replayed night.

**3. Restarts.**
- Killing the TaskManager: back in 48 s from checkpoint 4.
- `make stream-down` for 2 minutes, then `stream-up`: Flink resumed from the retained checkpoint, and the producer replayed the outage at 189 events/s.
- `COUNT(DISTINCT event_id)` is what makes these replays safe. A duplicate event has the same timestamp, so it lands in the same window and counts once.

**Smaller lessons:**
- `localhost` resolved to IPv6 `::1` while ports were published on `127.0.0.1`; Redpanda now advertises `127.0.0.1`.
- A new Docker volume is owned by root while Flink runs as `flink`; a one-shot `checkpoint-init` service fixes ownership.
- The image entrypoint, not my script, is what writes `FLINK_PROPERTIES` into the config.

**Lesson.** In streaming, "it runs" proves nothing. Only reconciliation against the source showed both the crash loop's effect and the dropped late data, so the recount (`make stream-check`) is part of the project, not a one-off script.

## 2026-11-25: GitHub API enrichment: what the rate limits and ETags actually did

Enrichment adds language and topics for the top 1,000 repos by stars (`GET /repositories/{id}`, by id because names change).

- **Ranked "as of today", it found nothing.** My first default ranked stars over the 30 days up to *yesterday*. The pipeline stopped at 10-31, so that window held no stars. The job now ends the window at the **latest loaded day** (from partition metadata), the same rule the reports use.
- **ETags saved less than I expected.** A forced refresh of the top 50 returned only 3 `304`s: trending repos change their star count all day, so their ETag changes. The quota dropped by exactly the 47 `200`s, which confirms a 304 is free. ETags would pay off on a long tail of quiet repos, not on the top of a trending list. The refresh-after-7-days cursor matters more: an immediate re-run fetched 0 repos.
- **Isolation.** The API returns repos *as they are now*, so this is not a backfillable daily job. It got its own DAG with `catchup=False`. The event DAG's freshness check is now limited to `raw.events`, so a stale enrichment can't fail the data load, and an Airflow pool keeps the two DAGs' dbt builds from running at once.
- **CI vs. 30-day rankings.** `dim_repo_github` → `dim_repo` is a real foreign key in prod, but CI builds `dim_repo` from 2 days. So the test is an error in prod and a warning in CI (162 repos outside the window).
- **A BigQuery gotcha:** `sum(stars) as stars ... having sum(stars) > 0` fails with "aggregations of aggregations". In `HAVING`, `stars` resolves to the alias, so `having stars > 0` is the fix.

Result: 1,000 repos in 8.7 min (982 ok, 18 deleted since October, 0 failed), using a fifth of the hourly quota. The top ~1,000 repos (1% of starred repos) received 33% of October's stars; Python and TypeScript lead.

## 2026-11-25: Repo history (SCD2): renames are common, and Slim CI nearly blocked the PR

**The data.** I expected renames to be rare. In October, 42,422 repos (0.7%) had more than one name, and 233 were renamed *back* (`homelab` → `homeprod` → `homelab`). That ruled out two shortcuts:
- **`dbt snapshot` on `dim_repo`.** It records only what it sees at run time. A backfill of October would have stored one row per repo, and a lost snapshot table could never be rebuilt. `dim_repo_history` is derived from events instead, so a rebuild or a re-run reproduces it byte-for-byte (I compared fingerprints).
- **`group by repo_id, repo_name`.** It would merge both `homelab` periods into one that overlaps `homeprod`. Gaps and islands (a new period whenever the name differs from the previous event) gets it right.

**Ties.** `dim_repo` took the "latest name" with `order by created_at desc`. With two events in the same second it picked at random, and 1 repo of 5.8M disagreed with the new history. Both now order by `created_at`, then `event_id`, and a singular test checks they agree. Deploy note: prod `dim_repo` needs one `--full-refresh` to adopt the tie-break.

**Slim CI would have failed this PR with 26M false failures.** Changing `dim_repo` selects its children, and dbt also flags the relationships test `fct_events -> dim_repo` as modified. With `--defer`, that test compared prod `fct_events` (all of October) with a 2-day CI `dim_repo`. `--indirect-selection cautious` is meant for this, but dbt ignores it for tests that a selector matches directly, and `state:modified+` matches them. The fix is two steps: `dbt ls` lists the changed models, then `dbt build --select <those names> --indirect-selection cautious` runs only tests whose parents were all built in CI. The full build on main still runs the cross-environment tests. I also found that the first version swallowed a failing `dbt ls` (a pipe into `tr`) and reported "no changed models"; `pipefail` fixed that.

Result: 66 dbt tests (3 unit tests). Slim CI builds 32 nodes for this PR, the full CI build 79, and both pass.

## 2026-11-25: Slim CI, and the flag that makes `--defer` trustworthy

A pull request's CI now builds only what changed: `dbt build --select state:modified+ --defer --state <prod manifest>`. Airflow publishes the prod manifest after every successful build. Three things I only learned by testing:

1. **`--defer` alone can read the wrong table.** It falls back to prod only when a parent is *missing* from the CI dataset. Earlier CI runs leave tables in `oss_pulse_ci` for up to a day, so `agg_repo_daily` was built on `oss_pulse_ci.fct_events`. On GitHub that could be another branch's version. `--favor-state` makes dbt read unchanged parents from prod every time. I checked the compiled SQL: `oss_pulse_marts.fct_events` and `oss_pulse_marts.dim_repo`.
2. **Vars rendered in configs count as changes.** With no code change, 4 tests were still "modified", because their `where:` filter renders `var('history_start_date')`, and CI overrides that var to keep the window at 2 days. Vars used *inside macros* are not compared, which is why the models weren't flagged. I kept the 4 tests: they take seconds and are bounded to 2 days.
3. **`--state` is resolved from the dbt project directory,** not from where `make` runs. A relative path gave "no comparison manifest", so the path is absolute now.

Result: editing one aggregate builds 14 nodes instead of 56 (65 s instead of 123 s), and comparing prod with CI shows 0 modified nodes when nothing changed.

## 2026-11-19: Two things CI caught before it even ran in GitHub

1. **`sqlfluff fix` reordered columns.** I had put `exclude_rules` under `[sqlfluff:rules]`, where sqlfluff silently ignores it. So rule ST06 moved every computed column to the end of `stg_events`, `dim_repo` and `agg_events_hourly`. The SQL was still valid, but the incremental tables would no longer match their existing schemas, and the partition copy jobs need an exact match. I reverted, moved the setting to `[sqlfluff]`, and re-ran: the remaining fixes were layout only. Lesson: read an auto-fixer's diff like a colleague's PR.
2. **The `is_bot` unit test depended on CLI vars.** CI builds a 2-day window (`history_start_date: 2026-10-30`), and the unit test's input rows are dated 10-01, so the window filtered them out and the test failed. A unit test should pin its own context, so it now sets its `vars` in `overrides`.

CI's BigQuery build costs ~0.9 GiB per run (126 queries, mostly at the 10 MB minimum) and authenticates through Workload Identity Federation, so no service-account key exists.

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
