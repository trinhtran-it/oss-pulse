# Data notes: GH Archive

Findings from profiling sample hours on 2026-11-12. The numbers come from single hours, so treat them as rough. Re-check them before relying on them.

| Sample hour (UTC) | Events | Size (gz) | Notes |
|---|---|---|---|
| 2025-06-10 15:00 | 149,600 | 85 MB | Full payloads |
| 2026-01-15 15:00 | 146,635 | 39 MB | `PushEvent` and `PullRequestEvent` payloads trimmed |
| 2026-10-01 15:00 (`data/sample/`) | 85,569 | 19 MB | Trimmed payloads; `PushEvent` still present (76%) |
| 2026-11-03 15:00 | 3,530 | 3.6 MB | No `PushEvent` / `CreateEvent` / `DeleteEvent` at all |
| 2026-11-10 03:00 / 15:00 / 20:00 | 3,378 / 3,323 / 3,339 | 3.3–3.8 MB | Same as above |

## Source and format

- **URL pattern:** `https://data.gharchive.org/{YYYY-MM-DD}-{H}.json.gz`, one file per UTC hour. The hour is **not zero-padded** (`2026-01-01-0`, not `-00`).
- **Format:** gzip-compressed NDJSON (one JSON event per line), in the GitHub Events API shape.
- **Timestamps:** `created_at` is ISO-8601 UTC with a `Z` suffix (e.g. `2026-11-10T15:59:59Z`). Each file contains the events created during its UTC hour.
- **Availability:** a file is published shortly after its hour ends. Requesting a future or unpublished hour returns 404.

## Grain and key

- **Grain:** one row = one public GitHub event.
- **Key:** `id`, a **string** of digits. Keep it as `STRING` and don't cast it to an integer, because the key must stay stable if the format changes.
- No duplicate `id`s were found within any single sample hour, but **across the full day 2026-10-01 there were 79 duplicates** in 2,073,894 events (same `id` in two hourly files). Spark de-duplicates on `id` per day; keep a `unique` test in dbt as well.

## Volume

| Period | Per hour | Per day (estimate, ×24) |
|---|---|---|
| Up to ~early Oct 2025 | ~150k events, ~85–110 MB gz | ~3.6M events, ~2–2.5 GB gz |
| ~Oct 2025 to mid-2026 | ~147k events, ~20–40 MB gz | ~3.5M events, ~0.5–0.9 GB gz |
| Now (Nov 2026) | ~3.3–3.5k events, ~3.5 MB gz, ~13 MB raw | **~80–85k events, ~85 MB gz, ~315 MB raw** |

- Volume dropped about **25×** between **2026-10-01** (85.6k events/hour, 76% of them `PushEvent`) and **2026-11-03** (3.5k/hour), mainly because push, create and delete events disappeared.
- **October 2026 backfill (all 31 days, 30.0M events):** the push/create/delete feed was **intermittent before it stopped**. Other event types were steady every day: 4–16k stars and 22–92k PR events.

  | Days | Events/day | Push, Create, Delete |
  |---|---|---|
  | 10-01 … 10-06, 10-08 … 10-14, 10-19 … 10-22, 10-24 … 10-26 | 0.6–2.1M | present (~70–78% of events) |
  | 10-07, 10-15 … 10-17 | 111–163k | **none** |
  | 10-18, 10-23 | 377k, 122k | partial |
  | **10-27 onward** | 81–108k | **none** (also none in the 2026-11-03 sample) |

  Every one of these days had all 24 hourly files, so it's a **source-side gap, not a pipeline error**. The pipeline passed all its tests; nothing currently detects it.
- Some hours are tiny and look **partial** (e.g. `2025-10-09-15` is 479 KB and `2026-09-01-15` is 1.7 MB, against a normal 20–100 MB). See the data-quality section.
- **Implication:** a full 2025 backfill is about 1.3B events. A day of current data fits easily on one machine. Choose the backfill window on purpose: it largely determines both cost and how comparable the metrics are over time.

## Fields to keep

Spark should flatten to these columns, using an **explicit schema** rather than inferring one, because payload shapes differ by event type and era:

| Column | Source path | Type | Notes |
|---|---|---|---|
| `event_id` | `id` | STRING | Primary key |
| `event_type` | `type` | STRING | |
| `created_at` | `created_at` | TIMESTAMP (UTC) | |
| `event_date` | derived from `created_at` | DATE (UTC) | Partition column |
| `actor_id` | `actor.id` | INT64 | |
| `actor_login` | `actor.login` | STRING | Used for bot detection |
| `repo_id` | `repo.id` | INT64 | Use for joins. `repo.name` changes when a repo is renamed |
| `repo_name` | `repo.name` | STRING | `owner/name` |
| `org_id` | `org.id` | INT64 | Nullable |
| `org_login` | `org.login` | STRING | Nullable |
| `is_public` | `public` | BOOL | |
| `action` | `payload.action` | STRING | Nullable; meaning depends on event type |
| `pr_number` | `payload.number` / `payload.pull_request.number` | INT64 | PR events only |
| `pr_merged` | `payload.pull_request.merged` | BOOL | **Before ~Oct 2025 only** |
| `issue_number` | `payload.issue.number` | INT64 | Issue and comment events |
| `push_size` | `payload.size` | INT64 | **Before ~Oct 2025 only** |
| `push_distinct_size` | `payload.distinct_size` | INT64 | **Before ~Oct 2025 only** |
| `ref` | `payload.ref` | STRING | Push, create and delete events |
| `release_tag` | `payload.release.tag_name` | STRING | Release events |
| `payload_json` | `to_json(payload)` | STRING | Raw copy, kept so new fields can be extracted later without re-ingesting |

> Implemented in `spark/flatten_events.py` (explicit `StructType`, session time zone UTC). It also adds `event_hour`, `repo_owner` (`repo_name` before the `/`) and `source_file` (input file path, for lineage).

## Event types and metric rules

| Metric | Rule | Caveats |
|---|---|---|
| Star | `WatchEvent` AND `action = 'started'` | `started` is the only value ever seen. Un-starring produces no event, so stars only ever go up |
| Fork | `ForkEvent` | `action = 'forked'` exists only in newer data, so **don't filter on `action`** |
| PR opened | `PullRequestEvent` AND `action = 'opened'` | Never count every `PullRequestEvent` row: newer data also includes `labeled`, `assigned`, etc. |
| PR merged | **Before ~Oct 2025:** `action = 'closed'` AND `pr_merged = true`. **After:** `action = 'merged'` | After the change, `closed` appears to mean closed without merging (not yet verified) |
| PR review | `PullRequestReviewEvent` AND `action = 'created'` | |
| Issue opened / closed | `IssuesEvent` AND `action IN ('opened', 'closed')` | Exclude `labeled`, `assigned`, etc. |
| Comment | `IssueCommentEvent` AND `action = 'created'` | Covers comments on both issues and PRs |
| Release | `ReleaseEvent` AND `action = 'published'` | |
| Push / commits | `PushEvent`, with commits from `push_size` | Commit counts exist **before ~Oct 2025 only**. **Push events are absent from current data**, so push metrics aren't available for recent dates |
| Active contributors | `COUNT(DISTINCT actor_id)` over the events above, **excluding bots** | |

Keep these rules in one dbt model (`int_events_classified`) instead of repeating them in each mart. Put the event-type-to-category mapping in the `event_types` seed.

## Data-quality issues

**Bots**
- Logins ending in `[bot]` produced **25%** of events in Nov 2026, compared with 17–20% in 2025 and early 2026.
- Add an `is_bot` flag (`actor_login LIKE '%[bot]'`, plus a seed list for bots without the suffix) and exclude bots from contributor and engagement metrics by default.

**Nulls**
- `org` is null for **~57–71%** of events (repos owned by users rather than organizations). This is expected, so `org_*` columns must be nullable.
- Fields extracted from `payload` are null whenever the event type doesn't have them. Use `not_null` tests only on `event_id`, `event_type`, `created_at` and `actor_id`. **Not `repo_id`**: the 2026-10-01 15:00 sample has 1 `ForkEvent` with `"repo": {}` (1 in 85,569). Keep the row, and let marts that group by repo filter `repo_id IS NOT NULL`.

**Missing `PushEvent` fields and payload changes**
- Around early Oct 2025 (when hourly file size fell ~3× with the same event count), `PushEvent` payloads **lost `commits`, `size` and `distinct_size`**. Only `before`, `head`, `push_id`, `ref` and `repository_id` remain.
- In the same change, `PullRequestEvent.payload.pull_request` was cut down to `base, head, id, number, url`. `merged`, `title`, `user`, `additions` and the rest are gone, and a new `merged` action replaces the old `closed + merged` check.
- `PushEvent`, `CreateEvent` and `DeleteEvent` drop out on some October 2026 days and are **gone from 2026-10-27 onward** (see Volume).
- Mitigation: version metric logic by era, keep `payload_json`, and show the eras on the dashboard (a note or reference line) so readers don't mistake an upstream change for a real trend.

**Time zones**
- All timestamps are UTC. Store `created_at` as a UTC `TIMESTAMP` and derive `event_date` in UTC.
- Set `spark.sql.session.timeZone=UTC`. Otherwise `to_date()` uses the cluster's local time zone and events near midnight land on the wrong day.
- Keep Airflow's `default_timezone` as UTC so that `{{ ds }}` matches the file dates.
- Convert to local time only in the dashboard.

**Partial and missing hours**
- Some hours are far smaller than normal (see Volume).
- Add checks: exactly 24 files per day, and each hour's row count compared with the median of the past 7 days (warn below 50%).
- Schedule the daily run after ~01:00 UTC on the following day, so that hour 23 has been published.
