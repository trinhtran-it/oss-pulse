.PHONY: setup up down backfill test lint ingest ingest-image ingest-docker spark-local spark-dataproc bq-table bq-load bq-cost-demo bq-cost-chart dbt-deps dbt-debug dbt-build dbt-test dbt-freshness dbt-docs tf-bootstrap tf-init tf-plan tf-apply tf-destroy tf-output

# Load local settings (GCP_PROJECT_ID, GCP_REGION, TF_STATE_BUCKET, ...) if present.
-include .env
export

TF := terraform -chdir=terraform

START ?= $(shell date -v-1d +%Y-%m-%d 2>/dev/null || date -d yesterday +%Y-%m-%d)
END   ?= $(START)

setup:
	python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt

up:
	docker compose -f airflow/docker-compose.yml up -d

down:
	docker compose -f airflow/docker-compose.yml down

backfill:
	docker compose -f airflow/docker-compose.yml exec airflow-scheduler \
		airflow dags backfill oss_pulse_daily -s $(START) -e $(END)

test:
	.venv/bin/pytest

lint:
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .

# --- Ingestion (GH Archive -> GCS raw) ------------------------------------------
# make ingest DATE=2026-10-01 [HOURS=0-23] [ARGS="--force"]
DATE  ?= $(START)
HOURS ?= 0-23

ingest:
	.venv/bin/python -m ingestion.gharchive --date $(DATE) --hours $(HOURS) $(ARGS)

ingest-image:
	docker build -f ingestion/Dockerfile -t oss-pulse-ingestion .

# Same job in the container, using your ADC login (read-only mount).
ingest-docker: ingest-image
	docker run --rm -e GCS_BUCKET -e GCP_PROJECT_ID \
		-v $(HOME)/.config/gcloud:/home/app/.config/gcloud:ro \
		oss-pulse-ingestion --date $(DATE) --hours $(HOURS) $(ARGS)

# --- Spark: raw -> processed/events (Parquet) ------------------------------------
# make spark-local    DATE=2026-10-01   # local files (data/sample) -> data/processed/events
# make spark-dataproc DATE=2026-10-01   # gs:// raw -> gs:// processed, on Dataproc Serverless
LOCAL_INPUT      ?= data/sample/$(DATE)-*.json.gz
DATAPROC_SA      ?= oss-pulse-pipeline@$(GCP_PROJECT_ID).iam.gserviceaccount.com
DATAPROC_SUBNET  ?= oss-pulse-subnet
# Runtime 2.2 = Spark 3.5 (verified 2026-11-17); local tests also run Spark 4.x.
DATAPROC_RUNTIME ?= 2.2

spark-local:
	.venv/bin/python spark/flatten_events.py --date $(DATE) \
		--input '$(LOCAL_INPUT)' --output data/processed/events

# Smallest Serverless shape: driver + 2 executors, 4 cores each, capped (no autoscaling up).
spark-dataproc:
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

# --- BigQuery: processed/events -> raw.events ----------------------------------
# make bq-table                                  # create raw.events (idempotent DDL)
# make bq-load DATES="2026-10-01 2026-10-02"     # replace those daily partitions
# make bq-cost-demo DATES="2026-10-01 2026-10-02 2026-10-03" COST_DAY=2026-10-02
DATES    ?= $(DATE)
COST_DAY ?= $(lastword $(DATES))
BQ       := bq --quiet --headless --project_id=$(GCP_PROJECT_ID) --location=$(GCP_REGION)

bq-table:
	$(BQ) --dataset_id=$(GCP_PROJECT_ID):$(BQ_DATASET_RAW) query --use_legacy_sql=false \
		< warehouse/ddl/events.sql

bq-load:
	warehouse/load_events.sh $(DATES)

bq-cost-demo:
	warehouse/cost_demo.sh $(COST_DAY) $(DATES)
	.venv/bin/python warehouse/plot_cost.py

bq-cost-chart:
	.venv/bin/python warehouse/plot_cost.py

# --- dbt ----------------------------------------------------------------------------
# make dbt-build                                  # dev target (DBT_DEV_DATASET), all history
# make dbt-build DBT_TARGET=prod RUN_DATE=2026-10-03  # incremental: rebuild run_date (+lookback)
DBT_TARGET ?= dev
RUN_DATE   ?=
DBT_SELECT ?=
DBT        := .venv/bin/dbt
DBT_FLAGS   = --project-dir dbt/oss_pulse --profiles-dir dbt/oss_pulse --target $(DBT_TARGET)
DBT_VARS    = $(if $(RUN_DATE),--vars '{run_date: $(RUN_DATE)}')
DBT_SEL     = $(if $(DBT_SELECT),--select $(DBT_SELECT))

dbt-deps:
	$(DBT) deps $(DBT_FLAGS)

dbt-debug:
	$(DBT) debug $(DBT_FLAGS)

dbt-build:
	$(DBT) build $(DBT_FLAGS) $(DBT_VARS) $(DBT_SEL)

dbt-test:
	$(DBT) test $(DBT_FLAGS) $(DBT_VARS) $(DBT_SEL)

dbt-freshness:
	$(DBT) source freshness $(DBT_FLAGS)

dbt-docs:
	$(DBT) docs generate $(DBT_FLAGS) --static

# --- Terraform -----------------------------------------------------------------

# One-time: create the versioned remote-state bucket (kept outside Terraform on purpose).
tf-bootstrap:
	@test -n "$(GCP_PROJECT_ID)" -a -n "$(TF_STATE_BUCKET)" || (echo "Set GCP_PROJECT_ID and TF_STATE_BUCKET in .env" && exit 1)
	gcloud storage buckets create gs://$(TF_STATE_BUCKET) --project=$(GCP_PROJECT_ID) \
		--location=$(GCP_REGION) --uniform-bucket-level-access --public-access-prevention
	gcloud storage buckets update gs://$(TF_STATE_BUCKET) --versioning

tf-init:
	$(TF) init -backend-config=backend.hcl

tf-plan:
	$(TF) plan -out=tfplan

tf-apply:
	$(TF) apply tfplan

tf-destroy:
	$(TF) destroy

tf-output:
	$(TF) output
