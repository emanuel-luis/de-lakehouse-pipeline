# One entry point for local runs and CI. Needs bash + make (Linux, macOS or WSL) and uv.
SHELL := bash
.SHELLFLAGS := -eu -o pipefail -c
.DEFAULT_GOAL := help

# Optional overrides from .env (see .env.example); exported to uv, the CLI and dbt. GNU make
# gives a value set in .env priority over the shell environment; `make VAR=value` beats both.
-include .env
export

# Absolute paths: dbt and DuckDB resolve relative paths against different directories.
export LAKEHOUSE_DATA_DIR ?= $(abspath data)
export LAKEHOUSE_DUCKDB_PATH ?= $(LAKEHOUSE_DATA_DIR)/warehouse/lakehouse.duckdb
export DO_NOT_TRACK ?= 1

DATA_SOURCE ?= synthetic
UV_RUN := uv run --locked $(if $(filter kaggle,$(DATA_SOURCE)),--extra kaggle,)
DBT_FLAGS := --project-dir dbt --profiles-dir dbt

# Throwaway venv for the DAG integrity test, outside uv.lock (Airflow's pins would clash with it).
AIRFLOW_VENV ?= $(HOME)/.venvs/de-lakehouse-airflow
AIRFLOW_CONSTRAINTS ?= https://raw.githubusercontent.com/apache/airflow/constraints-3.3.2/constraints-3.12.txt

.PHONY: help install lint fmt test test-fast source ingest dbt-deps dbt-build summary demo cdc-demo clean up down airflow-manifest dag-test

help: ## list the targets
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

install: ## create the virtualenv from uv.lock
	uv sync --locked

lint: ## what the CI lint job runs
	uv run --locked ruff check
	uv run --locked ruff format --check
	uv run --locked mypy

fmt: ## format and auto-fix
	uv run --locked ruff format
	uv run --locked ruff check --fix

test: ## every test, including any marked slow
	uv run --locked pytest -m "slow or not slow"

test-fast: ## the default selection (no dbt runs), with the coverage gate
	uv run --locked pytest

source: ## write the source CSVs (DATA_SOURCE=synthetic|kaggle)
	$(UV_RUN) lakehouse source

ingest: ## validate source CSVs into typed landing Parquet + ingest log
	$(UV_RUN) lakehouse ingest

dbt-deps: ## install dbt packages from package-lock.yml
	$(UV_RUN) dbt deps $(DBT_FLAGS)

dbt-build: ## build and test every dbt model (DuckDB)
	mkdir -p "$$(dirname "$(LAKEHOUSE_DUCKDB_PATH)")"
	$(UV_RUN) dbt build $(DBT_FLAGS)

summary: ## print the last ingest run and mart row counts
	$(UV_RUN) lakehouse summary

demo: ## end to end: source -> landing -> dbt build -> summary
	$(MAKE) --no-print-directory source
	$(MAKE) --no-print-directory ingest
	$(MAKE) --no-print-directory dbt-deps
	$(MAKE) --no-print-directory dbt-build
	$(MAKE) --no-print-directory summary

cdc-demo: ## demo, then a simulated source change loaded incrementally, then an empty increment
	$(MAKE) --no-print-directory demo
	$(UV_RUN) lakehouse cdc simulate
	$(UV_RUN) lakehouse ingest --mode incremental
	$(MAKE) --no-print-directory dbt-build
	$(UV_RUN) lakehouse summary --watermarks
	$(UV_RUN) lakehouse ingest --mode incremental
	$(MAKE) --no-print-directory dbt-build
	$(MAKE) --no-print-directory summary

up: ## start the Airflow stack (creates missing JWT secret and Fernet key in .env)
	@touch .env
	@[ -z "$$(tail -c1 .env)" ] || echo >> .env
	@grep -q '^AIRFLOW__API_AUTH__JWT_SECRET=' .env || echo "AIRFLOW__API_AUTH__JWT_SECRET=$$(openssl rand -hex 32)" >> .env
	@grep -q '^AIRFLOW__CORE__FERNET_KEY=' .env || echo "AIRFLOW__CORE__FERNET_KEY=$$(openssl rand -base64 32 | tr '+/' '-_')" >> .env
	docker compose up -d --build --wait

down: ## stop the Airflow stack and delete its volumes
	docker compose down -v

airflow-manifest: ## dbt deps + parse: writes dbt/target/manifest.json for the DAG
	$(UV_RUN) dbt deps $(DBT_FLAGS)
	$(UV_RUN) dbt parse $(DBT_FLAGS)

dag-test: airflow-manifest ## DAG integrity test in a throwaway Airflow 3.3.2 + Cosmos venv
	uv venv --quiet --python 3.12 --allow-existing "$(AIRFLOW_VENV)"
	uv pip install --quiet --python "$(AIRFLOW_VENV)/bin/python" --constraint "$(AIRFLOW_CONSTRAINTS)" \
		apache-airflow==3.3.2 apache-airflow-providers-standard astronomer-cosmos==1.15.1 pytest
	AIRFLOW_HOME="$$(mktemp -d)" AIRFLOW__CORE__LOAD_EXAMPLES=False LAKEHOUSE_DBT_PROJECT_DIR="$(abspath dbt)" \
		"$(AIRFLOW_VENV)/bin/python" -m pytest -c /dev/null --rootdir airflow -q airflow/tests

clean: ## delete generated data under ./data and dbt build output
	find data -mindepth 1 -maxdepth 1 ! -name .gitkeep -exec rm -rf {} +
	rm -rf dbt/target dbt/logs dbt/dbt_packages
