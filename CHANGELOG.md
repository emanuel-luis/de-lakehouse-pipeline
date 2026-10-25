# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog 1.1.0](https://keepachangelog.com/en/1.1.0/), and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Polars ingestion with a typed contract per Olist table: reject handling with a reason, a reject-ratio threshold, atomic Parquet writes, and an ingest log with run_id, UTC timestamps, row counts and rejects.
- Deterministic synthetic data generator with the Olist schema and deliberate dirt (default source), and an opt-in Kaggle source for the real dataset.
- `lakehouse` CLI with `source`, `ingest` and `summary` subcommands.
- dbt project on DuckDB: staging views for orders, order items and customers; marts `fct_orders` and `dim_customers` with enforced contracts and column descriptions.
- dbt tests for keys, accepted values, relationships and ranges, plus a custom `reconciles_with_ingest_log` test.
- Makefile with `make demo` (source, ingest, dbt build, summary), lint, test and clean targets.
- GitHub Actions: CI (lint, tests with an 80% coverage gate, full pipeline run on synthetic data) and a security workflow.
- `.env.example`, README, CLAUDE.md, implementation plan and decision log.
