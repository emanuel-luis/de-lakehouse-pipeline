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

.PHONY: help install lint fmt test test-fast source ingest dbt-deps dbt-build summary demo clean

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

clean: ## delete generated data under ./data and dbt build output
	find data -mindepth 1 -maxdepth 1 ! -name .gitkeep -exec rm -rf {} +
	rm -rf dbt/target dbt/logs dbt/dbt_packages
