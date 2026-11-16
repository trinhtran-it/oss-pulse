.PHONY: setup up down backfill test lint tf-bootstrap tf-init tf-plan tf-apply tf-destroy tf-output

# Load local settings (GCP_PROJECT_ID, GCP_REGION, TF_STATE_BUCKET, ...) if present.
-include .env
export

TF := terraform -chdir=terraform

START ?= $(shell date -v-1d +%Y-%m-%d 2>/dev/null || date -d yesterday +%Y-%m-%d)
END   ?= $(START)

setup:
	python -m venv .venv && .venv/bin/pip install -r requirements.txt

up:
	docker compose -f airflow/docker-compose.yml up -d

down:
	docker compose -f airflow/docker-compose.yml down

backfill:
	docker compose -f airflow/docker-compose.yml exec airflow-scheduler \
		airflow dags backfill oss_pulse_daily -s $(START) -e $(END)

test:
	.venv/bin/pytest ingestion/tests

lint:
	.venv/bin/ruff check .
	.venv/bin/ruff format --check .

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
