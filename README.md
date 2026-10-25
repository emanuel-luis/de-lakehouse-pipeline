# de-lakehouse-pipeline

An end-to-end ELT pipeline on open e-commerce data: Polars ingestion with typed data contracts, then a dbt staging, bronze, intermediate and marts build on DuckDB, reproducible with one command and no credentials.

[![ci](https://github.com/emanuel-luis/de-lakehouse-pipeline/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/emanuel-luis/de-lakehouse-pipeline/actions/workflows/ci.yml)
![license](https://img.shields.io/badge/license-MIT-green)
![python](https://img.shields.io/badge/python-3.12-blue)
![dbt-core](https://img.shields.io/badge/dbt--core-1.12-orange)
![duckdb](https://img.shields.io/badge/DuckDB-2.x-yellow)

## What this demonstrates

- Typed data contracts at the landing boundary: every source table has a schema, bad rows are split out with a reason instead of failing the load or slipping through, and a table whose reject ratio is too high fails loudly.
- Idempotent, atomic, re-runnable steps: files are written to a temp path and swapped in with `os.replace`, and re-running the pipeline gives identical data.
- Run metadata: each run records a run_id, UTC timestamps, row counts and rejects per table in an ingest log, and a dbt test reconciles the landing data against that log.
- A four-layer dbt build on DuckDB (staging, bronze, intermediate, marts) over all nine Olist tables: deduplicated bronze tables, enriched views, star-schema marts with enforced model contracts, a description on every mart column, and tests on keys, ranges and relationships.
- Change data capture on a source with no update column: a simulator changes the source, the ingest lands only the rows past a watermark kept in the ingest log, and `bronze_orders` merges them incrementally.
- One-command reproducible demo, plus CI that runs lint, tests, the full pipeline and the CDC demo with no credentials.

## Architecture

```mermaid
flowchart LR
  A["Source CSVs<br/>synthetic generator or Kaggle"] --> B["lakehouse ingest<br/>Polars contracts"]
  B --> C[("Landing Parquet")]
  B --> D[("Rejects Parquet")]
  B --> E[("Ingest log")]
  C --> F["dbt staging<br/>views"]
  E -. reconciled by a dbt test .-> F
  F --> G["dbt bronze<br/>deduplicated tables"]
  K["lakehouse cdc simulate"] -. changes .-> A
  E -. watermark .-> B
  G --> I["dbt intermediate<br/>enriched views"]
  G --> J["dbt marts<br/>tables, enforced contracts"]
  I --> J
  J --> H[("DuckDB warehouse")]
```

Python owns source to landing and never writes DuckDB. dbt is the only DuckDB writer: its sources read the landing Parquet directly, so there is one writer and no lock contention.

| Layer | Location | Format | Owner | Contract and checks |
| --- | --- | --- | --- | --- |
| Source | `data/source/*.csv` | CSV | generator or Kaggle download | Olist file and column names |
| Landing | `data/landing/<table>/` | Parquet | `lakehouse ingest` | Typed contract per table, required columns, `_batch_id`, `_extracted_at`, `_source_file`; `full.parquet` plus one `batch-<run_id>.parquet` per incremental orders load |
| Rejects | `data/rejects/<table>/` | Parquet | `lakehouse ingest` | Source columns as text plus `_reject_reason` and `_batch_id` |
| Ingest log | `data/_meta/ingest_log/` | Parquet | `lakehouse ingest` | One row per table and run; reconciled with landing by a dbt test |
| Staging | schema `staging` | DuckDB views | dbt | Renames and cleaning only, one model per landing table; not_null, accepted_values and ranges (keys are tested in bronze, since landing keeps duplicates) |
| Bronze | schema `bronze` | DuckDB tables | dbt | One deterministic row per natural key (latest `_batch_id` wins); `bronze_orders` is incremental; geolocation collapsed to one row per zip prefix; unique, not_null, relationships |
| Intermediate | schema `intermediate` | DuckDB views | dbt | Joins and the translated product category, at a declared grain; grain tests |
| Marts | schema `marts` | DuckDB tables | dbt | Star schema with enforced contracts, column descriptions, key, range and relationship tests |

### Marts

| Model | Grain |
| --- | --- |
| `fct_orders` | one row per order |
| `fct_order_items` | one row per order line (order, item number) |
| `dim_customers` | one row per `customer_unique_id` |
| `dim_products` | one row per product, with orders, revenue and average review score |
| `dim_sellers` | one row per seller, with orders, items sold, revenue and average review score |
| `dim_dates` | one row per calendar day between the first and last order date |

Both facts carry foreign keys to the dimensions, checked by relationship tests. Products with no category, or a category missing from the translation table, get the category `unknown`.

## Quickstart

Prerequisites: [uv](https://docs.astral.sh/uv/), `make` and bash (Linux, macOS, or WSL on Windows).

```bash
git clone https://github.com/emanuel-luis/de-lakehouse-pipeline && cd de-lakehouse-pipeline
make demo
```

`make demo` writes synthetic source CSVs, ingests them into landing Parquet, installs the dbt packages, runs `dbt build` (models and tests), and prints the last ingest run with the mart row counts. Everything lands under `data/` (gitignored); the warehouse is `data/warehouse/lakehouse.duckdb`.

Query the marts with a read-only connection (dbt holds the write lock while it runs):

```bash
duckdb -readonly data/warehouse/lakehouse.duckdb "select order_status, count(*) from marts.fct_orders group by 1"
# or, without the DuckDB CLI:
uv run python -c "import duckdb; c = duckdb.connect('data/warehouse/lakehouse.duckdb', read_only=True); print(c.sql('select * from marts.dim_customers limit 5'))"
```

### Real data

```bash
cp .env.example .env        # then uncomment and set DATA_SOURCE=kaggle and KAGGLE_API_TOKEN
make demo
```

Values in `.env` take precedence over shell variables for `make` targets. For a one-off run, pass the variable on the command line instead: `make demo DATA_SOURCE=kaggle` (with `KAGGLE_API_TOKEN` exported).

The [Olist Brazilian e-commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) is downloaded at runtime from Kaggle under its own license (CC BY-NC-SA 4.0 per the Kaggle page). It is never committed to this repository. The MIT license covers the code only.

The default synthetic data has the same file and column names and referential integrity, from a fixed seed, with deliberate dirt: duplicated and jittered geolocation rows, orders without items, junk rows with a null key, non-numeric prices, repeated review ids, products without a category or missing from the translation table, mixed-case cities with trailing spaces, and zip prefixes without zero padding.

## Run metadata and idempotency

- run_id has the form `YYYYMMDDTHHMMSSffffffZ` (UTC, sorts lexically) and doubles as the `_batch_id` stamped on every landed row.
- The ingest log has one row per table and run: `run_id, data_source, table_name, mode, status, started_at, finished_at, rows_in, rows_out, rows_rejected, watermark_column, watermark_from, watermark_to, landing_file, error`. The invariant is `rows_in = rows_out + rows_rejected`.
- Landing and reject files are written to a temp file in the same directory and swapped in with `os.replace`, so a failed write never leaves a partial file.
- Commit order: the landing file first, the log row after. A crash in between leaves data without a log row, never a log row without data.
- Re-running ingestion replaces the landing files with the same content, so a second `make demo` gives identical data. The dbt test `reconciles_with_ingest_log` fails the build if landing and log disagree.

## Change data capture

Olist has no update column, so landing derives `order_updated_at` as the latest lifecycle timestamp of an order, and that is the watermark column.

```bash
make cdc-demo
```

`make cdc-demo` runs `make demo`, then:

1. `lakehouse cdc simulate` changes the source CSVs in place. Up to 50 orders that have items move one lifecycle step (created to approved, approved, invoiced or processing to shipped, shipped to delivered), each setting exactly the matching timestamp, and 20 new `created` orders arrive with a new customer, 1 to 3 items on existing products and sellers, and one credit card payment for the total. The simulated clock is one hour after the latest lifecycle timestamp in the data, never the wall clock, so the same source and seed always give the same files. Options: `--changes`, `--new-orders`, `--seed`.
2. `lakehouse ingest --mode incremental` reads the last watermark of `orders` from the ingest log (the highest `watermark_to` since the latest successful full load) and lands only the rows whose `order_updated_at` is later, or cannot be computed so validation still sees them, as `batch-<run_id>.parquet`. The log row records `watermark_from` and `watermark_to`. Without a previous watermark the table is loaded in full; the other tables have no watermark column and are always reloaded in full.
3. `dbt build` merges the new batch into `bronze_orders` (`delete+insert` on `order_id`, only batches newer than the table). When the ingest log shows a full load of orders newer than the table, the table is emptied first and rebuilt from landing, so a regenerated source never leaves stale orders behind.
4. `lakehouse summary --watermarks` shows the run, the current watermark and the mart counts: `fct_orders` gains exactly the new orders.
5. A second incremental ingest finds no rows past the watermark: it writes no file, keeps the watermark and logs a successful run with 0 rows. The next `dbt build` leaves the marts unchanged.

Known limits of this design:

- No hard deletes: a row removed from the source is never removed downstream.
- A change that sets no lifecycle timestamp (for example a status fix with no new date) does not move `order_updated_at` and is not captured.
- The strict `>` comparison assumes the source clock never goes backwards; a late write stamped at or before the watermark is missed.

## Tech stack

Python 3.12 · Polars · Pydantic · DuckDB · dbt-core 1.12 · dbt-duckdb · dbt_utils · uv · ruff · mypy · pytest · Hypothesis · GitHub Actions

## Project structure

```
src/de_lakehouse_pipeline/   ingestion: settings, contracts, sources, landing, run log, CLI
dbt/                         dbt project: staging, bronze, intermediate, marts, generic and singular tests, profiles
tests/                       pytest suite (hermetic: each test runs in its own temp dir)
docs/                        implementation plan and decision log
.github/workflows/           ci.yml, security.yml
Makefile                     single entry point for local runs and CI
```

## Development

| Target | What it does |
| --- | --- |
| `make help` | list the targets |
| `make install` | create the virtualenv from uv.lock |
| `make lint` | ruff check, ruff format --check, mypy (what the CI lint job runs) |
| `make fmt` | format and auto-fix |
| `make test-fast` | default test selection (no dbt runs) with the coverage gate |
| `make test` | every test, including the `slow` end-to-end CDC test that runs dbt (needs `make dbt-deps`) |
| `make source` | write the source CSVs (`DATA_SOURCE=synthetic` or `kaggle`) |
| `make ingest` | validate source CSVs into typed landing Parquet plus the ingest log |
| `make dbt-deps` | install dbt packages from package-lock.yml |
| `make dbt-build` | build and test every dbt model |
| `make summary` | print the last ingest run and mart row counts |
| `make demo` | source, ingest, dbt-deps, dbt-build, summary |
| `make cdc-demo` | demo, then simulate source changes, ingest incrementally, build, and repeat with an empty increment |
| `make clean` | delete generated data and dbt build output |

CI runs three jobs: lint (ruff and mypy, strict on `src`), test (pytest with an 80% coverage gate), and pipeline (`make demo`, `make cdc-demo` and the slow end-to-end test on synthetic data). Configuration is documented in `.env.example`.

## Roadmap

This repository ships in stacked pull requests, one per phase. The first three are implemented.

- PR1, minimum shippable: ingestion with contracts, dbt staging and marts, tests, CI, `make demo`.
- PR2, full medallion: staging, bronze, intermediate and marts for all nine tables.
- PR3, CDC simulation: an incremental load driven by a watermark in the ingest log, and an incremental `bronze_orders`. This is what you are reading.
- PR4, upcoming: orchestration with Airflow 3 and Astronomer Cosmos.
- PR5, upcoming: Elementary observability and a static docs site.

Design details: [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md). Decision log: [docs/DECISIONS.md](docs/DECISIONS.md).

## License

MIT. See [LICENSE](LICENSE).
