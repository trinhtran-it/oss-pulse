-- Streaming sink. Runs once, when the Postgres volume is created.
--
-- Flink writes one row per (5-minute window, wiki) with a JDBC upsert on the primary key, so a
-- window recomputed after a Flink restart overwrites its row instead of adding a second one.
-- written_at is set on the first insert only (Flink's upsert doesn't list it), which makes
-- written_at - window_end the end-to-end latency of a window.

create table if not exists wiki_edits_5min (
    window_start timestamp not null, -- UTC
    window_end timestamp not null, -- UTC, exclusive
    wiki text not null, -- e.g. enwiki, wikidatawiki, commonswiki
    edits bigint not null, -- distinct edits + page creations
    bot_edits bigint not null,
    new_pages bigint not null,
    unique_editors bigint not null, -- distinct editor_hash (names are never stored)
    written_at timestamptz not null default now(),
    primary key (window_start, wiki)
);

create index if not exists wiki_edits_5min_window_end on wiki_edits_5min (window_end);

comment on table wiki_edits_5min is
    'Wikimedia edits per 5-minute event-time window and wiki. Written by the Flink job wiki_edits_5min.';

-- All wikis together, one row per window: what the dashboard's main panel reads.
create or replace view wiki_edits_5min_total as
select
    window_start,
    window_end,
    sum(edits) as edits,
    sum(bot_edits) as bot_edits,
    sum(new_pages) as new_pages,
    round(sum(bot_edits)::numeric / nullif(sum(edits), 0), 4) as bot_share,
    count(*) as wikis,
    min(written_at) as first_written_at
from wiki_edits_5min
group by window_start, window_end;

-- Least privilege: Flink writes, Grafana only reads. Local, localhost-only credentials; override
-- them with STREAM_PG_* variables in .env for anything beyond a laptop.
do $$
begin
    if not exists (select from pg_roles where rolname = 'grafana') then
        create role grafana login password 'grafana';
    end if;
end
$$;
grant usage on schema public to grafana;
grant select on wiki_edits_5min, wiki_edits_5min_total to grafana;
