-- raw.github_repo_metadata: one row per GitHub API call for a repository (append-only log).
-- Written by ingestion/github_repos.py; dbt keeps the latest row per repo (stg_github_repos).
-- status: ok (metadata in the row), not_modified (304, unchanged since etag),
--         not_found (404), blocked (451 / access blocked). Failed calls are not written.
-- Small (~1,000 rows a week), so no required partition filter: dbt reads it whole.

CREATE TABLE IF NOT EXISTS github_repo_metadata (
  repo_id           INT64     NOT NULL OPTIONS (description = 'GitHub repository id (stable across renames)'),
  fetched_at        TIMESTAMP NOT NULL OPTIONS (description = 'When the API was called (UTC)'),
  status            STRING    NOT NULL OPTIONS (description = 'ok | not_modified | not_found | blocked'),
  http_status       INT64              OPTIONS (description = 'HTTP status of the call'),
  etag              STRING             OPTIONS (description = 'ETag, sent back as If-None-Match next time'),
  full_name         STRING             OPTIONS (description = 'owner/name at fetch time, lowercase'),
  description       STRING             OPTIONS (description = 'Repository description, first 500 characters'),
  language          STRING             OPTIONS (description = 'Primary language as detected by GitHub'),
  topics            ARRAY<STRING>      OPTIONS (description = 'Repository topics, lowercase'),
  license_spdx      STRING             OPTIONS (description = 'SPDX id of the license, e.g. MIT'),
  stargazers_count  INT64              OPTIONS (description = 'Total stars at fetch time'),
  forks_count       INT64              OPTIONS (description = 'Total forks at fetch time'),
  open_issues_count INT64              OPTIONS (description = 'Open issues + pull requests at fetch time'),
  is_fork           BOOL               OPTIONS (description = 'TRUE if the repository is a fork'),
  is_archived       BOOL               OPTIONS (description = 'TRUE if archived (read-only)'),
  repo_created_at   TIMESTAMP          OPTIONS (description = 'When the repository was created'),
  pushed_at         TIMESTAMP          OPTIONS (description = 'Last push at fetch time')
)
PARTITION BY DATE(fetched_at)
CLUSTER BY repo_id
OPTIONS (description = 'GitHub REST API metadata for top repositories; append-only, latest row wins.');
