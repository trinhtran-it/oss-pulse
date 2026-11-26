-- Wikimedia edits per 5-minute event-time window and wiki: Redpanda -> Flink SQL -> Postgres.
-- Submitted by /opt/submit-job.sh, which restores the latest retained checkpoint if there is one.

SET 'pipeline.name' = 'wiki_edits_5min';
SET 'table.local-time-zone' = 'UTC';
SET 'parallelism.default' = '1';
-- Exactly-once state: Kafka offsets and open windows are checkpointed together every 30 s, and
-- kept after a cancel or crash so the next submit resumes from them.
SET 'execution.checkpointing.interval' = '30 s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
-- A topic partition without new data must not hold back the watermark (and every window).
SET 'table.exec.source.idle-timeout' = '30 s';
-- Latency AND completeness: a window is first written when the watermark passes its end
-- (+1 min), then re-written whenever late events for it arrive, for up to 6 hours. A producer
-- that slept replays hours of events out of order; with only the watermark, 65% of them were
-- dropped as late. The upsert sink turns each re-fire into an UPDATE of the same row.
SET 'table.exec.emit.allow-lateness' = '6 h';
SET 'table.exec.emit.late-fire.enabled' = 'true';
SET 'table.exec.emit.late-fire.delay' = '10 s';

CREATE TABLE wiki_changes (
    event_id STRING,
    event_time TIMESTAMP_LTZ(3),
    wiki STRING,
    change_type STRING,
    is_bot BOOLEAN,
    editor_hash STRING,
    -- Events up to 1 minute late still count; a window closes 1 minute after its end.
    WATERMARK FOR event_time AS event_time - INTERVAL '1' MINUTE
) WITH (
    'connector' = 'kafka',
    'topic' = 'wiki.recentchange',
    'properties.bootstrap.servers' = 'redpanda:9092',
    -- versioned: a changed query can't restore old state, so it starts as a new consumer group
    'properties.group.id' = 'flink-wiki-edits-5min-v3',
    'scan.startup.mode' = 'group-offsets',
    'properties.auto.offset.reset' = 'earliest',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE wiki_edits_5min (
    window_start TIMESTAMP(3),
    window_end TIMESTAMP(3),
    wiki STRING,
    edits BIGINT,
    bot_edits BIGINT,
    new_pages BIGINT,
    unique_editors BIGINT,
    PRIMARY KEY (window_start, wiki) NOT ENFORCED  -- upsert: a recomputed window overwrites its row
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/streaming',
    'table-name' = 'wiki_edits_5min',
    'username' = '${PG_USER}',
    'password' = '${PG_PASSWORD}',
    'sink.buffer-flush.interval' = '5 s'
);

-- COUNT(DISTINCT event_id), not COUNT(*): the producer is at-least-once, and a replayed event has
-- the same event_time, so it lands in the same window and is counted once.
-- GROUP BY TUMBLE (a group window), not the TUMBLE table function: in Flink 2.2 only group
-- windows support late firing (table.exec.emit.late-fire.*, set above).
INSERT INTO wiki_edits_5min
SELECT
    CAST(TUMBLE_START(event_time, INTERVAL '5' MINUTE) AS TIMESTAMP(3)) AS window_start,
    CAST(TUMBLE_END(event_time, INTERVAL '5' MINUTE) AS TIMESTAMP(3)) AS window_end,
    wiki,
    COUNT(DISTINCT event_id) AS edits,
    COUNT(DISTINCT CASE WHEN is_bot THEN event_id END) AS bot_edits,
    COUNT(DISTINCT CASE WHEN change_type = 'new' THEN event_id END) AS new_pages,
    COUNT(DISTINCT editor_hash) AS unique_editors
FROM wiki_changes
GROUP BY TUMBLE(event_time, INTERVAL '5' MINUTE), wiki;
