# oss-pulse: run `make` (or `make help`) for the list of targets.
# Every setting comes from .env (copy .env.example). Nothing here is specific to one GCP project.

-include .env
export

.DEFAULT_GOAL := help
SHELL := /bin/bash

PY  := .venv/bin/python
TF  := terraform -chdir=terraform

START ?= $(shell date -v-1d +%Y-%m-%d 2>/dev/null || date -d yesterday +%Y-%m-%d)
END   ?= $(START)

# Terraform reads its inputs from .env too, so there is one place to edit.
# A terraform/terraform.tfvars file, if present, overrides these (Terraform precedence).
export TF_VAR_project_id  := $(GCP_PROJECT_ID)
export TF_VAR_region      := $(GCP_REGION)
export TF_VAR_bucket_name := $(GCS_BUCKET)
export TF_VAR_github_repository := $(GITHUB_REPOSITORY)
TF_STATE_PREFIX ?= oss-pulse/dev
TF_BACKEND = $(if $(wildcard terraform/backend.hcl),-backend-config=backend.hcl,\
	-backend-config=bucket=$(TF_STATE_BUCKET) -backend-config=prefix=$(TF_STATE_PREFIX))

.PHONY: help check-env setup test lint sql-lint tf-check secrets-scan ci-local dbt-ci dbt-ci-slim dbt-publish-manifest \
	infra-up infra-down tf-bootstrap tf-init tf-plan tf-apply tf-destroy tf-output \
	up down airflow-build airflow-check airflow-unpause airflow-trigger backfill chaos-test \
	ingest ingest-image ingest-docker spark-upload spark-local spark-dataproc \
	bq-table bq-audit-table bq-load bq-cost-demo bq-cost-chart \
	dbt-deps dbt-debug dbt-build dbt-test dbt-freshness dbt-docs diagrams

help: ## Show this list
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2} \
		/^## / {printf "\n%s\n", substr($$0, 4)}' $(MAKEFILE_LIST)

check-env: ## Fail early if .env is missing or incomplete
	@test -f .env || { echo "Missing .env: cp .env.example .env and fill it in"; exit 1; }
	@for v in GCP_PROJECT_ID GCP_REGION GCS_BUCKET TF_STATE_BUCKET; do \
		val="$${!v}"; \
		if [ -z "$$val" ] || [[ "$$val" == your-* ]]; then echo "Set $$v in .env"; exit 1; fi; \
	done

## Local development
setup: ## Create .venv with all Python deps and install dbt packages
	python3 -m venv .venv
	.venv/bin/pip install --quiet --upgrade pip
	.venv/bin/pip install --quiet -r requirements-dev.txt
	$(MAKE) dbt-deps

test: ## Unit tests: ingestion, Spark (needs Java 17) and Airflow alerts; no cloud access
	.venv/bin/pytest

lint: sql-lint ## ruff (Python) + sqlfluff (dbt SQL)
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .

sql-lint: ## sqlfluff on dbt models and tests (Jinja templater, no BigQuery needed)
	cd dbt/oss_pulse && ../../.venv/bin/sqlfluff lint models tests

tf-check: ## terraform fmt -check + validate (no credentials needed)
	terraform -chdir=terraform fmt -check -recursive
	terraform -chdir=terraform init -backend=false -input=false >/dev/null
	terraform -chdir=terraform validate

GITLEAKS_IMAGE := ghcr.io/gitleaks/gitleaks:v8.30.1
secrets-scan: ## gitleaks over the full git history (Docker)
	docker run --rm -v "$$PWD:/repo:ro" $(GITLEAKS_IMAGE) git /repo --redact --no-banner

ci-local: lint test tf-check secrets-scan ## Every CI check that needs no cloud access

diagrams: ## Render docs/*.png from docs/diagrams/*.dot (needs Graphviz)
	dot -Tpng -Gdpi=150 docs/diagrams/architecture.dot -o docs/architecture.png
	dot -Tpng -Gdpi=150 docs/diagrams/data_model.dot -o docs/data_model.png

## Cloud infrastructure (Terraform + BigQuery tables + Spark job)
infra-up: check-env tf-bootstrap tf-init ## Create or update everything in GCP (asks before applying)
	$(TF) apply
	$(MAKE) bq-table bq-audit-table spark-upload

infra-down: check-env ## Destroy everything Terraform created (asks first; keeps the state bucket)
	$(TF) destroy

# The versioned remote-state bucket is created outside Terraform, so `destroy` can't delete it.
tf-bootstrap: check-env ## Create the Terraform state bucket (skipped if it exists)
	@if gcloud storage buckets describe gs://$(TF_STATE_BUCKET) --project=$(GCP_PROJECT_ID) >/dev/null 2>&1; then \
		echo "State bucket gs://$(TF_STATE_BUCKET) exists"; \
	else \
		gcloud storage buckets create gs://$(TF_STATE_BUCKET) --project=$(GCP_PROJECT_ID) \
			--location=$(GCP_REGION) --uniform-bucket-level-access --public-access-prevention && \
		gcloud storage buckets update gs://$(TF_STATE_BUCKET) --versioning; \
	fi

tf-init:
	$(TF) init -input=false $(TF_BACKEND)

tf-plan: ## Show what infra-up would change
	$(TF) plan -out=tfplan

tf-apply:
	$(TF) apply tfplan

tf-destroy:
	$(TF) destroy

tf-output: ## Bucket URL, dataset IDs, service account email
	$(TF) output

## Orchestration (Airflow 3 in Docker)
# UI: http://localhost:8080 (airflow / airflow). The DAG starts paused.
COMPOSE := docker compose -f airflow/docker-compose.yaml --env-file .env
AIRFLOW := $(COMPOSE) exec -T airflow-scheduler airflow
DAY     ?= $(START)

airflow-build: ## Build the Airflow image (official 3.4 + ingestion deps + dbt)
	$(COMPOSE) build

# dbt_packages/ is mounted from the host, so install packages first.
up: check-env dbt-deps ## Start Airflow locally and wait until it is healthy
	$(COMPOSE) up -d --wait

down: ## Stop Airflow (keeps its database volume)
	$(COMPOSE) down

# Fails if any DAG has an import error (parsed inside the real Airflow image).
airflow-check: ## Check the DAG imports cleanly inside the running image
	$(AIRFLOW) dags reserialize
	$(AIRFLOW) dags list-import-errors -o plain | tee /dev/stderr | grep -q "No data found"
	$(AIRFLOW) dags list -o plain | grep oss_pulse_daily

airflow-unpause: ## Unpause the DAG: catchup creates one run per day from OSS_PULSE_START_DATE
	$(AIRFLOW) dags unpause oss_pulse_daily

airflow-trigger: ## Trigger one run now: DAY=YYYY-MM-DD
	$(AIRFLOW) dags trigger oss_pulse_daily --logical-date $(DAY)T00:00:00+00:00

# --reprocess-behavior failed: create missing days AND re-run failed ones.
backfill: ## (Re)run days START=YYYY-MM-DD END=YYYY-MM-DD (DAG must be unpaused)
	$(AIRFLOW) backfill create --dag-id oss_pulse_daily --from-date $(START) --to-date $(END) \
		--reprocess-behavior failed

chaos-test: ## Clear a finished day, re-run it, compare counts: DAY=YYYY-MM-DD
	airflow/chaos_rerun.sh $(DAY)

## Pipeline steps one by one (what the DAG runs)
# make ingest DATE=2026-10-01 [HOURS=0-23] [ARGS="--force"]
DATE  ?= $(START)
HOURS ?= 0-23

ingest: ## GH Archive -> GCS raw for DATE=YYYY-MM-DD (skips hours already uploaded)
	$(PY) -m ingestion.gharchive --date $(DATE) --hours $(HOURS) $(ARGS)

ingest-image:
	docker build -f ingestion/Dockerfile -t oss-pulse-ingestion .

# Same job in the container, using your ADC login (read-only mount).
ingest-docker: ingest-image
	docker run --rm -e GCS_BUCKET -e GCP_PROJECT_ID \
		-v $(HOME)/.config/gcloud:/home/app/.config/gcloud:ro \
		oss-pulse-ingestion --date $(DATE) --hours $(HOURS) $(ARGS)

# make spark-local    DATE=2026-10-01   # local files (data/sample) -> data/processed/events
# make spark-dataproc DATE=2026-10-01   # gs:// raw -> gs:// processed, on Dataproc Serverless
LOCAL_INPUT      ?= data/sample/$(DATE)-*.json.gz
DATAPROC_SA      ?= oss-pulse-pipeline@$(GCP_PROJECT_ID).iam.gserviceaccount.com
DATAPROC_SUBNET  ?= oss-pulse-subnet
# Runtime 2.2 = Spark 3.5 (verified 2026-11-17); local tests also run Spark 4.x.
DATAPROC_RUNTIME ?= 2.2

# The DAG runs gs://$GCS_BUCKET/code/spark/flatten_events.py: re-upload after changing the job.
spark-upload: ## Upload the Spark job the DAG runs (after changing spark/flatten_events.py)
	gcloud storage cp spark/flatten_events.py gs://$(GCS_BUCKET)/code/spark/flatten_events.py

spark-local:
	$(PY) spark/flatten_events.py --date $(DATE) \
		--input '$(LOCAL_INPUT)' --output data/processed/events

# Smallest Serverless shape: driver + 2 executors, 4 cores each, capped (no autoscaling up).
spark-dataproc: ## Raw -> Parquet for DATE on Dataproc Serverless
	gcloud dataproc batches submit pyspark spark/flatten_events.py \
		--project=$(GCP_PROJECT_ID) --region=$(GCP_REGION) \
		--batch=flatten-$(DATE)-$(shell date +%s) \
		--service-account=$(DATAPROC_SA) --subnet=$(DATAPROC_SUBNET) \
		--deps-bucket=gs://$(GCS_BUCKET) --ttl=1h \
		--labels=project=oss-pulse,step=flatten $(if $(DATAPROC_RUNTIME),--version=$(DATAPROC_RUNTIME)) \
		--properties=spark.executor.instances=2,spark.dynamicAllocation.maxExecutors=2 \
		-- --date $(DATE) \
		--input gs://$(GCS_BUCKET)/raw/gharchive/dt=$(DATE)/ \
		--output gs://$(GCS_BUCKET)/processed/events

# make bq-load DATES="2026-10-01 2026-10-02"     # replace those daily partitions
# make bq-cost-demo DATES="2026-10-01 2026-10-02 2026-10-03" COST_DAY=2026-10-02
DATES    ?= $(DATE)
COST_DAY ?= $(lastword $(DATES))
BQ       := bq --quiet --headless --project_id=$(GCP_PROJECT_ID) --location=$(GCP_REGION)

bq-table: ## Create raw.events (idempotent DDL)
	$(BQ) --dataset_id=$(GCP_PROJECT_ID):$(BQ_DATASET_RAW) query --use_legacy_sql=false \
		< warehouse/ddl/events.sql

bq-audit-table: ## Create audit.load_log (idempotent DDL)
	$(BQ) --dataset_id=$(GCP_PROJECT_ID):$(BQ_DATASET_AUDIT) query --use_legacy_sql=false \
		< warehouse/ddl/load_log.sql

bq-load: ## Parquet -> raw.events partitions: DATES="YYYY-MM-DD ..."
	warehouse/load_events.sh $(DATES)

bq-cost-demo:
	warehouse/cost_demo.sh $(COST_DAY) $(DATES)
	$(PY) warehouse/plot_cost.py

bq-cost-chart:
	$(PY) warehouse/plot_cost.py

## dbt
# make dbt-build                                      # dev target (DBT_DEV_DATASET), all history
# make dbt-build DBT_TARGET=prod RUN_DATE=2026-10-03  # incremental: rebuild run_date (+lookback)
DBT_TARGET ?= dev
RUN_DATE   ?=
DBT_SELECT ?=
DBT        := .venv/bin/dbt
DBT_FLAGS   = --project-dir dbt/oss_pulse --profiles-dir dbt/oss_pulse --target $(DBT_TARGET)
DBT_VARS    = $(if $(RUN_DATE),--vars '{run_date: $(RUN_DATE)}')
DBT_SEL     = $(if $(DBT_SELECT),--select $(DBT_SELECT))

dbt-deps: ## Install dbt packages (dbt_utils)
	$(DBT) deps $(DBT_FLAGS)

dbt-debug: ## Check the dbt connection to BigQuery
	$(DBT) debug $(DBT_FLAGS)

dbt-build: ## Models + tests [DBT_TARGET=dev|prod] [RUN_DATE=...] [DBT_SELECT=...] [DBT_EXTRA=--full-refresh]
	$(DBT) build $(DBT_FLAGS) $(DBT_VARS) $(DBT_SEL) $(DBT_EXTRA)

dbt-test:
	$(DBT) test $(DBT_FLAGS) $(DBT_VARS) $(DBT_SEL)

dbt-freshness: ## Source freshness of raw.events (warn 26 h, error 50 h)
	$(DBT) source freshness $(DBT_FLAGS)

# What CI runs against BigQuery: a 2-day full build, then the daily incremental path on top.
CI_VARS := {history_start_date: 2026-10-30, run_date: 2026-10-31}
dbt-ci: ## The CI dbt builds, in the oss_pulse_ci dataset (needs the CI Terraform resources)
	$(DBT) build --project-dir dbt/oss_pulse --profiles-dir dbt/oss_pulse --target ci \
		--full-refresh --vars '$(CI_VARS)'
	$(DBT) build --project-dir dbt/oss_pulse --profiles-dir dbt/oss_pulse --target ci \
		--vars '$(CI_VARS)'

# Slim CI: build only what changed vs. prod (state:modified+), read everything else from prod
# (--defer). Falls back to the full dbt-ci build when no prod manifest has been published yet.
PROD_STATE := dbt/oss_pulse/target/prod-state
# --favor-state: always read unchanged parents from prod, even if an older CI run left a copy in
# oss_pulse_ci (it could be another branch's version). --state must be absolute.
# What a pull request builds, in two steps:
#  1. `dbt ls` lists changed models/seeds and everything downstream (state:modified+);
#  2. `dbt build` selects those by name with `--indirect-selection cautious`, so a test runs only
#     if ALL its parents were built here. A selector like state:modified+ also picks tests of
#     unchanged models, e.g. relationships fct_events -> dim_repo when dim_repo changes: prod
#     fct_events (all history) vs. a 2-day CI dim_repo = 26M false failures. (cautious is ignored
#     for tests matched directly, so the names must come from step 1.) Mixed tests still run in
#     the full build on main. Singular and unit tests that changed themselves are added directly.
# --defer --favor-state: read unchanged parents from prod, never from an older CI run's leftovers.
STATE_FLAGS = --state $(CURDIR)/$(PROD_STATE)
CI_FLAGS    = --project-dir dbt/oss_pulse --profiles-dir dbt/oss_pulse --target ci
dbt-ci-slim: ## CI dbt builds, only for models changed vs. prod (falls back to dbt-ci)
	@set -o pipefail; rc=0; $(PY) dbt/scripts/prod_manifest.py download $(PROD_STATE) || rc=$$?; \
	if [ $$rc -eq 3 ]; then echo "No prod manifest yet: full CI build"; $(MAKE) --no-print-directory dbt-ci; exit; fi; \
	if [ $$rc -ne 0 ]; then exit $$rc; fi; \
	models=$$($(DBT) --quiet ls $(CI_FLAGS) $(STATE_FLAGS) --select state:modified+ \
		--resource-type model --resource-type seed --output name | tr '\n' ' ') || exit $$?; \
	echo "Slim CI, changed models and children: $${models:-none}"; \
	for mode in --full-refresh ""; do \
		$(DBT) build $(CI_FLAGS) $$mode --vars '$(CI_VARS)' $(STATE_FLAGS) --defer --favor-state \
			--indirect-selection cautious \
			--select $$models state:modified,test_type:singular state:modified,resource_type:unit_test \
			|| exit $$?; \
	done

# Airflow publishes the manifest after every prod build (publish_dbt_manifest). Use this once,
# before the first Airflow run, or to reset Slim CI's baseline to the current code.
dbt-publish-manifest: ## Upload the prod dbt manifest (Slim CI's baseline) from this checkout
	$(DBT) parse --project-dir dbt/oss_pulse --profiles-dir dbt/oss_pulse --target prod --target-path target/prod-publish
	$(PY) dbt/scripts/prod_manifest.py upload dbt/oss_pulse/target/prod-publish/manifest.json

dbt-docs: ## dbt docs as one static HTML file (target/static_index.html)
	$(DBT) docs generate $(DBT_FLAGS) --static
