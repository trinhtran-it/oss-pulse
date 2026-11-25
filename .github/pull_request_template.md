## What and why

<!-- One or two sentences: what changes, and the reason. Link the issue if there is one. -->

## How it was tested

- [ ] `make ci-local` passes (ruff, sqlfluff, pytest, terraform fmt/validate, gitleaks)
- [ ] dbt changes: `make dbt-ci` or `make dbt-build RUN_DATE=...` passes
- [ ] Spark job changed: `make spark-upload` after merge
- [ ] Terraform changed: `make tf-plan` reviewed; apply after merge

## Data impact

<!-- Tables, columns or row counts that change. Backfill needed? "None" is a fine answer. -->
