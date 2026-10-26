# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `lakehouse source --if-missing`: writes the source only when it does not exist, so repeated orchestrated runs keep it.
- Airflow image (`airflow/Dockerfile`, Airflow 3.3.2, Cosmos 1.15.1) with the DAG, the dbt project, the dbt manifest and the pipeline virtualenv baked in.
- Docker compose stack with Postgres, scheduler, API server and DAG processor on LocalExecutor, plus `make up`, `make down` and `make airflow-manifest`.
- DAG `lakehouse_pipeline` (hourly): source, CDC simulation and incremental ingest as bash tasks, then the dbt project as a Cosmos task group; every Cosmos task runs in the one-slot `duckdb_writer` pool. Tests with several parents (relationships between models, reconciliation of sources with the ingest log) run as their own tasks once every parent is ready.
- DAG integrity test and `make dag-test`, which run in a throwaway Airflow venv outside `uv.lock`.
- CI: a `dags` job in `ci.yml` and a `compose` workflow that starts the stack, checks for DAG import errors, runs `airflow dags test` and then one scheduler-driven run.
- Dependabot updates for the `docker` (the uv build image) and `docker-compose` ecosystems. The Airflow base image is bumped by hand, together with the `apache-airflow` pin and the constraints URL.
- Polars ingestion with a typed contract per Olist table: reject handling with a reason, a reject-ratio threshold, atomic Parquet writes, and an ingest log with run_id, UTC timestamps, row counts and rejects.
- Deterministic synthetic data generator with the Olist schema and deliberate dirt (default source), and an opt-in Kaggle source for the real dataset.
- `lakehouse` CLI with `source`, `ingest` and `summary` subcommands.
- dbt project on DuckDB: staging views for orders, order items and customers; marts `fct_orders` and `dim_customers` with enforced contracts and column descriptions.
- dbt tests for keys, accepted values, relationships and ranges, plus a custom `reconciles_with_ingest_log` test.
- Makefile with `make demo` (source, ingest, dbt build, summary), lint, test and clean targets.
- dbt staging models and sources for products, sellers, reviews, payments, geolocation and category translation, each reconciled with the ingest log.
- dbt bronze layer: one deduplicated table per source table (latest batch wins), with geolocation collapsed to one row per zip prefix.
- dbt intermediate views `int_order_items_enriched` and `int_review_enriched`.
- Marts `fct_order_items`, `dim_products`, `dim_sellers` and `dim_dates`; `fct_orders` and `dim_customers` now read bronze and intermediate and gain `order_date_key`, `distinct_products`, `distinct_sellers`, `total_payment_value`, `avg_review_score` and `total_spent`.
- Singular test `assert_orders_without_items_are_unfulfilled` and range tests that bound order timestamps by a fixed source window.
- GitHub Actions: CI (lint, tests with an 80% coverage gate, full pipeline run on synthetic data) and a security workflow.
- `.env.example`, README, CLAUDE.md, implementation plan and decision log.
- `lakehouse cdc simulate`: moves orders one lifecycle step and adds new orders with their customers, items and payments, on a simulated clock taken from the data.
- `lakehouse ingest --mode incremental`: lands only the orders past the last watermark from the ingest log as a batch file; a run with nothing new writes no file and keeps the watermark.
- `lakehouse summary --watermarks`, and the ingest mode in the summary.
- Incremental `bronze_orders` (`delete+insert` on `order_id`), emptied and rebuilt when a newer full load of orders is in the ingest log.
- `make cdc-demo`, and a slow end-to-end CDC test that runs dbt as a subprocess; CI runs both.

### Changed

- The Airflow metadata database moves from PostgreSQL 17.11 to 18.6 (supported by Airflow 3.3.2), with its volume mounted at `/var/lib/postgresql` as the 18 image expects. `make down` deletes the volume, so there is nothing to migrate.
- DuckDB moves from 1.5 to 2.0 (`duckdb>=2.0,<3`). A warehouse file written by 2.x cannot be opened by 1.x; rebuild it with `make clean demo`.
