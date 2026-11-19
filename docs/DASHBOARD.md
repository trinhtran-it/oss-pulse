# Dashboard (Looker Studio)

**Live report:** [Looker Studio](https://datastudio.google.com/reporting/b792d5da-dff2-41cb-b080-cb675a64c5ca) · Screenshot: [dashboard.png](dashboard.png)

How the dashboard is built, so it can be rebuilt by hand. Looker Studio has no config-as-code, so this file is the spec.

## Data sources: two small mart tables, nothing else

| Data source | Table | Rows | Used by |
|---|---|---|---|
| `oss-pulse events` | `oss_pulse_marts.agg_events_hourly` | ~18k per month (1 MB) | Scorecards, tiles 1, 2, 4 |
| `oss-pulse top repos` | `oss_pulse_marts.rpt_top_repos_7d` | 10 | Tile 3 |

Both are dbt models ([agg_events_hourly](../dbt/oss_pulse/models/marts/agg_events_hourly.sql), [rpt_top_repos_7d](../dbt/oss_pulse/models/marts/rpt_top_repos_7d.sql)), rebuilt by the daily `dbt_build` task.

**Why not point Looker Studio at `fct_events` or `agg_repo_daily`?** Every chart refresh is a BigQuery query paid by the project. Measured with dry runs on October 2026 data:

| Query behind a tile | On the big marts | On the dashboard tables |
|---|---|---|
| Events by type / day / hour | `fct_events`: 1,067 MB | `agg_events_hourly`: < 1 MB (10 MB minimum billed) |
| Top 10 repos, 7 days | `agg_repo_daily` + `dim_repo` join: ~240 MB | `rpt_top_repos_7d`: 10 rows (10 MB minimum billed) |

The raw dataset is never connected: no business rules (bots, categories, star definition) are applied there.

## Connect

1. [lookerstudio.google.com](https://lookerstudio.google.com) → **Create → Data source → BigQuery**.
2. **My projects** → your project → `oss_pulse_marts` → `agg_events_hourly` → **Connect**.
3. In the field list, check the types:

   | Field | Type | Aggregation |
   |---|---|---|
   | `event_date` | Date (YYYYMMDD) | None |
   | `event_hour` | Number | **None** (it's a dimension) |
   | `weekday`, `event_type`, `event_category` | Text | None |
   | `is_bot` | Boolean | None |
   | `events` | Number | Sum |

4. Add calculated fields (**Add a field**):

   | Name | Formula |
   |---|---|
   | `Stars (humans)` | `SUM(CASE WHEN event_type = "WatchEvent" AND is_bot = false THEN events ELSE 0 END)` |
   | `Bot share` | `SUM(CASE WHEN is_bot = true THEN events ELSE 0 END) / SUM(events)` (type: Percent) |
   | `Avg events per day` | `SUM(events) / COUNT_DISTINCT(event_date)` |

5. **Data credentials: Owner's** (the default). Anyone with the link can view without BigQuery access; queries are billed to your project. **Data freshness: 12 hours** (the default) is fine for a daily pipeline.
6. Repeat for `rpt_top_repos_7d`. Then:
   - Set `window_start` and `window_end` to **Text**. A table with no Date field ignores the date range control, which is what tile 3 needs: it always shows the latest loaded week.
   - Add `Repo` = `HYPERLINK(repo_url, repo_name)`.

## Layout

```
┌──────────────────────────────────────────────────────────────────────────────┐
│ oss-pulse: GitHub public activity                 [Date range ▾] [Bots ▾]     │
│ How to read (text box)                                                       │
├──────────────┬──────────────┬──────────────┬────────────────────────────────┤
│ Events       │ Stars        │ Bot share    │                                │
│ (scorecard)  │ (scorecard)  │ (scorecard)  │                                │
├──────────────┴──────────────┴──────────────┤  3. Top 10 repos by stars      │
│  1. Events by type (bar)                    │     (table)                    │
├─────────────────────────────────────────────┼────────────────────────────────┤
│  2. Daily events by category (time series)  │  4. When humans are active     │
│                                             │     (heatmap)                  │
└─────────────────────────────────────────────┴────────────────────────────────┘
```

Theme: one light theme, no background images. Chart titles go in the chart's **Style → Chart title**, not in separate text boxes, so they move with the chart.

## Controls

| Control | Setup |
|---|---|
| **Date range** | Default: **Custom, Oct 1, 2026 – Oct 31, 2026**. ("Last 7 days" would be empty: the pipeline's `end_date` is 2026-10-31.) Uses `event_date` on the events source. |
| **Bots** (optional) | Drop-down list, control field `is_bot`, label "Bot events". Empty = all events. |

## Tiles

| # | Tile | Chart type | Dimension | Metric | Other settings |
|---|---|---|---|---|---|
| – | Events / Stars / Bot share | Scorecard ×3 | – | `events` / `Stars (humans)` / `Bot share` | Compact numbers |
| 1 | **Events by type** | Bar, horizontal | `event_type` | `events` | Breakdown `event_category` (stacked) to color each bar by category · Sort `events` desc · 16 bars · Axis titles "Events" / "Event type" |
| 2 | **Daily events by category** | Time series | `event_date` | `events` | Breakdown `event_category` · Missing data: "Line to zero" · Y-axis title "Events per day (UTC)" · No second y-axis |
| 3 | **Top 10 repos by stars, latest week** | Table | `stars_rank`, `Repo` | `stars`, `forks`, `prs_merged` | Source `oss-pulse top repos` · Sort `stars_rank` asc · Rows per page 10 · Subtitle "Bots excluded · Oct 25–31, 2026" |
| 4 | **When humans are active (UTC)** | Pivot table with heatmap | Rows `event_hour` (type Number), columns `weekday` | `Avg events per day` (0 decimals) | Filter: `is_bot` = false **and** `event_type` not in PushEvent, CreateEvent, DeleteEvent (see "Source gap") · Sort rows `event_hour` asc, columns `weekday` asc · Totals off · Hours as rows so 24 × 7 fits the page width |

**Tile 4 details:**
- The metric is an average per day, not a sum. October 2026 has five Thursdays, Fridays and Saturdays but only four of the other weekdays, so a sum would make those three days look busier.
- Push, create and delete events are excluded because the source dropped them on some days. Including them would turn the gap into a fake weekday pattern.
- Read the hour pattern, not the weekday ranking. One month gives only 4–5 days per weekday, and human activity ranged from 41k to 163k events a day, so a weekday's row mostly reflects which dates fell on it (Oct 2–3, a Friday and Saturday, were the two busiest days).

## "How to read" text box

> **How to read.** Public GitHub activity from GH Archive, loaded daily by the oss-pulse pipeline. All times are UTC.
> The date range filters every tile except **Top 10 repos**, which always shows the latest loaded week. Stars, the top 10 and the heatmap exclude bots.
> **Source gap:** GH Archive published no push, create or delete events on Oct 7, Oct 15–17 and from Oct 27 onward (partial on Oct 18 and 23). The drops in "code" activity on those days come from the source, not from developers.
> **Heatmap:** with only 4–5 days per weekday, the weekday rows mostly reflect individual busy days (Oct 1–3 were 2–3× busier than mid-month), so read the hour pattern, not the weekday ranking. The Friday 16:00 peak is one burst on Oct 23 (24.5k events in an hour, ~10× normal).

## Share and screenshot

1. **Share → Manage access → General access: "Anyone on the internet with the link can view"**. Viewers see only what the charts show. They can't open the data source or query other tables.
2. Screenshot in **View** mode, Oct 1–31, no control open, with the Google account language set to English (Looker Studio follows it). Save as `docs/dashboard.png` (keep it under ~1 MB). The README already embeds it.
3. Put the report link in the README's Dashboard section.

## Cost

- **dbt, daily:** `agg_events_hourly` ~70 MB (2 days of 5 `fct_events` columns, then free copy jobs). `rpt_top_repos_7d` ~250 MB: 7 days of `agg_repo_daily` plus a `repo_id, repo_name` scan of `dim_repo`.
- **Viewers:** an uncached page view runs about 8 queries (3 scorecards, 4 tiles, the bot drop-down) at the 10 MB minimum each, so ~80 MB. Inside the 1 TB/month free tier, that's about 12,000 uncached views a month. Looker Studio caches results for 12 hours.

## Option: enforce "marts only" with IAM

With owner's credentials, "marts only" is a convention: your account can read every dataset. To enforce it, create a service account with `roles/bigquery.dataViewer` on `oss_pulse_marts` only (plus `roles/bigquery.jobUser` on the project). Use it as the data source credential via the Looker Studio service agent.
- **Pro:** the dashboard physically can't read `raw` or `staging`.
- **Con:** about 30 minutes of IAM setup (Terraform), and the service agent must be granted `iam.serviceAccountTokenCreator`.
- **Cost:** none.

Worth it for a team dashboard; optional for a portfolio.
