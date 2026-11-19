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
TF_STATE_PREFIX ?= oss-pulse/dev
TF_BACKEND = $(if $(wildcard terraform/backend.hcl),-backend-config=backend.hcl,\
	-backend-config=bucket=$(TF_STATE_BUCKET) -backend-config=prefix=$(TF_STATE_PREFIX))

.PHONY: help check-env setup test lint \
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

lint: ## ruff check + format check
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .

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

dbt-build: ## Models + tests [DBT_TARGET=dev|prod] [RUN_DATE=YYYY-MM-DD] [DBT_SELECT=...]
	$(DBT) build $(DBT_FLAGS) $(DBT_VARS) $(DBT_SEL)

dbt-test:
	$(DBT) test $(DBT_FLAGS) $(DBT_VARS) $(DBT_SEL)

dbt-freshness: ## Source freshness of raw.events (warn 26 h, error 50 h)
	$(DBT) source freshness $(DBT_FLAGS)

dbt-docs: ## dbt docs as one static HTML file (target/static_index.html)
	$(DBT) docs generate $(DBT_FLAGS) --static
