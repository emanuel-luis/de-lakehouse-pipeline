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

# Diagrams: docs/diagrams/<name>.d2 rendered to <name>.svg (committed). One pinned D2 everywhere.
# Layout engine, themes and padding live in docs/diagrams/_style.d2 (d2-config), so d2 runs in a
# clean environment: D2_* and SCALE variables would override that config. --scale 1 is not a
# d2-config key; it writes the natural width and height, so GitHub never upscales a small diagram.
D2_VERSION := 0.9.0
# sha256 of d2-v$(D2_VERSION)-linux-amd64.tar.gz, from the release's SHA256SUMS
D2_SHA256 := 5669ddc46b99e942cc96078f4a4e36d5e62103348f4c05179ede27802fdd87a9
D2_HOME ?= $(HOME)/.local/share/d2/v$(D2_VERSION)
D2 ?= $(D2_HOME)/bin/d2
D2_RUN = env -i PATH="$$PATH" HOME="$$HOME" "$(D2)" --scale 1
DIAGRAMS := $(filter-out docs/diagrams/_%,$(wildcard docs/diagrams/*.d2))

.PHONY: help install lint fmt test test-fast source ingest dbt-deps dbt-build summary demo cdc-demo clean up down airflow-manifest dag-test site d2-install diagrams diagrams-check diagrams-watch

help: ## list the targets
	@grep -hE '^[a-zA-Z0-9_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-16s %s\n", $$1, $$2}'

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
	@grep -q '^AIRFLOW__API_AUTH__JWT_SECRET=' .env || { v="$$(openssl rand -hex 32)"; \
		[ -n "$$v" ] || { echo "openssl failed: no JWT secret written" >&2; exit 1; }; \
		echo "AIRFLOW__API_AUTH__JWT_SECRET=$$v" >> .env; }
	@grep -q '^AIRFLOW__CORE__FERNET_KEY=' .env || { v="$$(openssl rand -base64 32 | tr '+/' '-_')"; \
		[ -n "$$v" ] || { echo "openssl failed: no Fernet key written" >&2; exit 1; }; \
		echo "AIRFLOW__CORE__FERNET_KEY=$$v" >> .env; }
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

site: ## build site/ (dbt docs, Elementary report, landing page); run make demo or cdc-demo first
	rm -rf site
	mkdir -p site/dbt site/elementary
	$(UV_RUN) dbt docs generate --static $(DBT_FLAGS)
	cp dbt/target/static_index.html site/dbt/index.html
	uv run --locked --group site edr report --project-dir dbt --profiles-dir dbt --config-dir dbt/edr \
		--file-path "$(abspath site/elementary/index.html)" --open-browser false
	cp docs/site/index.html site/index.html

d2-install: ## install the pinned D2 (Linux x86_64) into D2_HOME, checksum verified
	@if [ -x "$(D2_HOME)/bin/d2" ]; then echo "D2 v$(D2_VERSION) already in $(D2_HOME)"; exit 0; fi; \
	[ "$$(uname -sm)" = "Linux x86_64" ] || { echo "d2-install covers Linux x86_64 only;" \
		"install D2 v$(D2_VERSION) yourself and pass D2=/path/to/d2" >&2; exit 1; }; \
	tmp="$$(mktemp -d)"; trap 'rm -rf "$$tmp"' EXIT; \
	curl -fsSLo "$$tmp/d2.tar.gz" \
		"https://github.com/terrastruct/d2/releases/download/v$(D2_VERSION)/d2-v$(D2_VERSION)-linux-amd64.tar.gz"; \
	echo "$(D2_SHA256)  $$tmp/d2.tar.gz" | sha256sum --check --strict; \
	tar -xzf "$$tmp/d2.tar.gz" -C "$$tmp"; \
	mkdir -p "$(D2_HOME)/bin"; \
	install -m 0755 "$$tmp/d2-v$(D2_VERSION)/bin/d2" "$(D2_HOME)/bin/d2"; \
	"$(D2_HOME)/bin/d2" --version

diagrams: ## render docs/diagrams/*.d2 to SVG with the pinned D2 (run make d2-install once)
	@v="$$($(D2_RUN) --version 2>/dev/null || true)"; [ "$$v" = "v$(D2_VERSION)" ] || { \
		echo "need D2 v$(D2_VERSION) at $(D2), found '$${v:-nothing}': run make d2-install" >&2; exit 1; }
	@for f in $(DIAGRAMS); do $(D2_RUN) "$$f" "$${f%.d2}.svg"; done

diagrams-check: diagrams ## render, then fail if a committed SVG is stale, missing or has no source
	@shopt -s nullglob; for s in docs/diagrams/*.svg; do \
		[ -f "$${s%.svg}.d2" ] || { echo "$$s has no .d2 source" >&2; exit 1; }; done
	@changed="$$(git status --porcelain --untracked-files=all -- 'docs/diagrams/*.svg')"; \
	if [ -n "$$changed" ]; then echo "$$changed" >&2; \
		echo "rendered SVGs differ from the committed ones: run make diagrams and commit" >&2; exit 1; fi

diagrams-watch: ## live preview in the browser while editing: make diagrams-watch NAME=architecture
	@# NAME only from the command line: WSL exports NAME (the hostname) into every shell.
	@[ "$(origin NAME)" = "command line" ] && [ -f "docs/diagrams/$(NAME).d2" ] || { \
		echo "usage: make diagrams-watch NAME=<one of: $(basename $(notdir $(DIAGRAMS)))>" >&2; exit 1; }
	$(D2_RUN) --watch --browser "$${BROWSER:-0}" docs/diagrams/$(NAME).d2 docs/diagrams/$(NAME).svg

clean: ## delete generated data under ./data and dbt build output
	find data -mindepth 1 -maxdepth 1 ! -name .gitkeep -exec rm -rf {} +
	rm -rf dbt/target dbt/logs dbt/dbt_packages
