.PHONY: setup up down backfill test lint

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
