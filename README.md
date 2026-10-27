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
- Orchestration with Airflow 3 and Astronomer Cosmos: an hourly DAG runs source, CDC simulation and incremental ingest, then Elementary's tables, every project dbt model and test as its own Airflow task, and the Elementary report, in a local docker compose stack. A one-slot pool serializes the tasks against DuckDB's single writer.
- Data observability with Elementary: schema change checks on every bronze table and a row count anomaly check on bronze_orders, reported next to the dbt docs in a static site that CI publishes to GitHub Pages.
- One-command reproducible demo, plus CI that runs lint, tests, the full pipeline, the CDC demo, the DAG integrity test and the site build with no credentials.

## Architecture

![Architecture: an operator runs the pipeline with make; GitHub Actions runs it on synthetic data and smoke-tests the Airflow 3 compose stack, whose hourly DAG runs the same lakehouse CLI and dbt; dbt is the only writer of the DuckDB warehouse, and the site with the dbt docs and the Elementary report is published to GitHub Pages](docs/diagrams/architecture.svg)

![Data flow: source CSVs pass the typed contracts of lakehouse ingest into landing Parquet, rejects and the ingest log, which also holds the CDC watermark; dbt reads landing into staging, bronze, intermediate and marts, with the tests of each layer beside it; Elementary stores the results, and the dbt docs and the Elementary report make up the GitHub Pages site](docs/diagrams/data-flow.svg)

Diagram sources: [architecture.d2](docs/diagrams/architecture.d2) and [data-flow.d2](docs/diagrams/data-flow.d2) (D2, rendered to SVG; see [Development](#development)).

Python owns source to landing and never writes DuckDB. dbt is the only DuckDB writer: its sources read the landing Parquet directly, so there is one writer and no lock contention. The Airflow DAG runs the same commands in order (see Orchestration); it adds scheduling and per-task visibility, not new logic.

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

![Marts star schema, keys only: fct_order_items (primary key order_id and order_item_id) references fct_orders, dim_products, dim_sellers, dim_customers and dim_dates; fct_orders references dim_customers and dim_dates](docs/diagrams/data-model.svg)

Source: [data-model.d2](docs/diagrams/data-model.d2). Every mart column, with its type and description, is in the dbt docs on the published site.

## Quickstart

Prerequisites: [uv](https://docs.astral.sh/uv/), `make` and bash (Linux, macOS, or WSL on Windows).

```bash
git clone https://github.com/emanuel-luis/de-lakehouse-pipeline && cd de-lakehouse-pipeline
make demo
```

`make demo` writes synthetic source CSVs, ingests them into landing Parquet, installs the dbt packages, runs `dbt build` (models and tests), and prints the last ingest run with the mart row counts. Everything lands under `data/` (gitignored); the warehouse is `data/warehouse/lakehouse.duckdb`.

![Sequence of make demo: make calls lakehouse source, which writes the source CSVs; lakehouse ingest, which writes landing, rejects and then the ingest log rows; dbt deps and dbt build, which read landing and build every layer in DuckDB; and lakehouse summary, which reads the last run and the mart counts through a read-only connection](docs/diagrams/user-flows-make-demo.svg)

Source: [user-flows-make-demo.d2](docs/diagrams/user-flows-make-demo.d2).

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

![Sequence of the CDC run: the simulator changes the source CSVs; the incremental ingest reads the last orders watermark from the ingest log, lands only the orders past it as a batch file and logs the new watermark; dbt build checks the log for a newer full load, reads the new batches and merges them into bronze_orders; a second incremental run finds 0 rows and leaves the marts unchanged](docs/diagrams/user-flows-cdc-run.svg)

Source: [user-flows-cdc-run.d2](docs/diagrams/user-flows-cdc-run.d2).

Known limits of this design:

- No hard deletes: a row removed from the source is never removed downstream.
- A change that sets no lifecycle timestamp (for example a status fix with no new date) does not move `order_updated_at` and is not captured.
- The strict `>` comparison assumes the source clock never goes backwards; a late write stamped at or before the watermark is missed.

## Orchestration

Airflow 3.3.2 runs the pipeline on an hourly schedule, with dbt orchestrated by [Astronomer Cosmos](https://astronomer.github.io/astronomer-cosmos/) 1.15.1. Prerequisite: Docker with compose.

```bash
make up      # builds the image, creates the secrets in .env, starts the stack
```

The UI is at http://localhost:8080. It uses Airflow's simple auth manager with every user as admin and no login: local use only, so compose publishes the port on 127.0.0.1 only. `make up` appends a JWT secret and a Fernet key to the gitignored `.env` when they are missing and never prints them.

The DAG `lakehouse_pipeline` (`@hourly`, one active run at a time, no catchup) runs, in order:

1. `lakehouse source --if-missing`: writes the synthetic source only when it does not exist yet.
2. `lakehouse cdc simulate`: changes the source.
3. `lakehouse ingest --mode incremental`: lands the rows past the watermark.
4. `elementary_models`: `dbt run --select elementary` builds the package's own tables. It runs before the dbt tasks because Elementary's on-run-end hook, which stores results, and its tests read those tables.
5. A Cosmos task group built from the dbt manifest: one Airflow task per project model and one for its tests. The Elementary package's own models are excluded from it (`exclude=["package:elementary"]`); the project's Elementary tests on bronze still run inside the per-model test tasks. A test that reads two models, such as a relationships test between a fact and a dimension, gets its own task that waits for both, as `dbt build` would. The tests that reconcile each landing table with the ingest log read only sources, so they run as their own tasks right after `ingest`. Elementary's on-run-end hook runs in every one of these dbt calls and stores their results, which adds a few seconds to each. The dbt artifacts (models, tests, sources) are uploaded once per run by `elementary_models`; the group's tasks skip that upload.
6. `edr_report`: `edr report` reads the results and writes the report to `$LAKEHOUSE_DATA_DIR/site/elementary/index.html` inside the data volume. Nothing publishes it from the stack; the published site is built by CI (see Observability).

![Sequence of a scheduled run: the scheduler starts an hourly run and records it in PostgreSQL, runs the source, cdc_simulate and ingest tasks through the lakehouse CLI against the lakehouse-data volume, then elementary_models, the Cosmos dbt tasks one at a time in the duckdb_writer pool, and edr_report, which writes the Elementary report](docs/diagrams/user-flows-airflow-run.svg)

Source: [user-flows-airflow-run.d2](docs/diagrams/user-flows-airflow-run.d2).

Like any Airflow DAG, it starts paused. Unpause it in the UI, trigger a run there, or use the CLI:

```bash
docker compose exec scheduler airflow dags unpause lakehouse_pipeline
docker compose exec scheduler airflow dags trigger lakehouse_pipeline
```

Every Cosmos task, models and tests alike, runs its own dbt process, and each one opens DuckDB for writing. DuckDB allows one writer, so the stack creates a pool named `duckdb_writer` with 1 slot and every Cosmos task uses it, as do `elementary_models` and `edr_report`, which also open DuckDB through dbt. The three Python tasks need no pool because Python never writes DuckDB.

The DAG, the dbt project, the manifest (written by `dbt parse` at build time) and the pipeline virtualenv are baked into the image; there are no bind mounts, so a DAG edit needs another `make up`, which rebuilds the image. Warehouse and landing data live in the named volume `lakehouse-data`.

```bash
make dag-test   # DAG integrity test: dbt parse, then DagBag import checks in a throwaway Airflow + Cosmos venv
make down       # stops the stack and deletes its volumes, including the warehouse
```

## Observability

The [Elementary](https://docs.elementary-data.com/) dbt package (0.26.1) is installed with the project. Its models and test results live in the DuckDB schema `elementary`; nothing else is written there.

- `elementary.schema_changes` on every bronze table, severity error: a dropped, added or retyped column fails the build.
- `elementary.volume_anomalies` on `bronze_orders`, severity warn. Bronze has no timestamp column, so it compares the table's row count between runs. It needs history to compare against, so a fresh database has nothing to flag; it warns instead of failing for that reason, and `make cdc-demo` adds runs.
- Anonymous usage tracking is off for dbt (project flag) and for the edr CLI (`dbt/edr/config.yml`).

The site is built from the current warehouse, so run `make demo` or `make cdc-demo` first:

```bash
make site   # writes site/index.html, site/dbt/index.html and site/elementary/index.html
```

`make site` runs `dbt docs generate --static`, then the edr CLI from the `site` dependency group (`uv sync --locked --group site` installs it; it is not part of a default sync), then copies the landing page from `docs/site/index.html`. Open `site/index.html` in a browser.

CI builds the same site from synthetic data on every run and, on pushes to main, publishes it to GitHub Pages: https://emanuel-luis.github.io/de-lakehouse-pipeline/ (live after the first merge to main). The architecture notes are in [docs/architecture.md](docs/architecture.md).

![dbt docs page for the fct_orders model, showing its details, description and columns](docs/img/dbt-docs.png)

![Elementary report dashboard with test results, tables health and monitored tables](docs/img/elementary-report.png)

Screenshots of the dbt docs (fct_orders model page) and the Elementary report dashboard, built from the synthetic data after `make cdc-demo`.

## Tech stack

Python 3.12 · Polars · Pydantic · DuckDB · dbt-core 1.12 · dbt-duckdb · dbt_utils · Elementary · Airflow 3.3 · Astronomer Cosmos · Docker Compose · uv · ruff · mypy · pytest · Hypothesis · GitHub Actions

## Project structure

```
src/de_lakehouse_pipeline/   ingestion: settings, contracts, sources, landing, run log, CLI
dbt/                         dbt project: staging, bronze, intermediate, marts, generic and singular tests, profiles, Elementary config
tests/                       pytest suite (hermetic: each test runs in its own temp dir)
airflow/                     Dockerfile, the lakehouse_pipeline DAG and its integrity test
docker-compose.yml          local Airflow stack: Postgres, scheduler, API server, DAG processor
docs/                        implementation plan, decision log, architecture notes
docs/diagrams/               D2 diagram sources, the shared _style.d2 and the rendered SVGs
docs/site/                   landing page for the published site
.github/workflows/           ci.yml, compose.yml, security.yml
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
| `make up` | start the Airflow stack (creates missing secrets in `.env`) |
| `make down` | stop the stack and delete its volumes |
| `make airflow-manifest` | dbt deps and parse: writes `dbt/target/manifest.json` for the DAG |
| `make dag-test` | DAG integrity test in a throwaway Airflow 3.3.2 + Cosmos venv |
| `make site` | build `site/`: dbt docs, Elementary report, landing page (run `make demo` first; needs `uv sync --locked --group site` once) |
| `make clean` | delete generated data and dbt build output |
| `make d2-install` | install the pinned D2 (Linux x86_64) under `~/.local/share/d2`, checksum verified |
| `make diagrams` | render every `docs/diagrams/*.d2` to SVG |
| `make diagrams-check` | render, then fail if a committed SVG is stale, missing or has no source (what the CI diagrams job runs) |
| `make diagrams-watch NAME=<name>` | live preview of one diagram in the browser while you edit it |

### Diagrams

The diagrams are [D2](https://d2lang.com) sources in `docs/diagrams/`, rendered to SVG next to them and committed, so GitHub shows them without a build. Every diagram imports `_style.d2`, which pins the layout engine (ELK), the light and dark themes and the colors per element kind; the SVGs follow the reader's light or dark mode. D2 is pinned to v0.9.0 in the Makefile.

1. `make d2-install` once (Linux or WSL; elsewhere install D2 v0.9.0 yourself and pass `D2=/path/to/d2`).
2. Edit a `.d2` file and run `make diagrams-watch NAME=data-flow`: open the URL it prints, and the page reloads on every save. Stop it with Ctrl+C.
3. `make diagrams`, then commit the `.d2` and the `.svg` together.

The `d2.exe` of the same version on Windows renders byte-identical SVGs, so `d2 --scale 1 docs/diagrams/<name>.d2` works there too.

CI runs lint (ruff and mypy, strict on `src`), test (pytest with an 80% coverage gate), pipeline (`make demo`, `make cdc-demo` and the slow end-to-end test on synthetic data), dags (`make dag-test`), diagrams (`make diagrams-check`, plus the rendered SVGs as an artifact) and site (`make cdc-demo`, `make site`, upload of the Pages artifact). A deploy-pages job publishes the artifact on pushes to main only. A separate compose workflow builds the image, starts the stack, checks that the DAG has no import errors, runs `airflow dags test lakehouse_pipeline`, then unpauses the DAG and waits for one run driven by the scheduler; on pull requests it runs only when `airflow/`, `dbt/`, `src/`, `uv.lock` or the compose file change, and it also runs on pushes to main, weekly and on demand. Configuration is documented in `.env.example`.

## Roadmap

This repository ships in stacked pull requests, one per phase. All five are implemented.

- PR1, minimum shippable: ingestion with contracts, dbt staging and marts, tests, CI, `make demo`.
- PR2, full medallion: staging, bronze, intermediate and marts for all nine tables.
- PR3, CDC simulation: an incremental load driven by a watermark in the ingest log, and an incremental `bronze_orders`.
- PR4, orchestration: Airflow 3 and Astronomer Cosmos in a docker compose stack, with the DAG integrity test and a compose smoke workflow.
- PR5, observability: Elementary tests and a static site (dbt docs, Elementary report) published to GitHub Pages. This is what you are reading.

Design details: [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md). Decision log: [docs/DECISIONS.md](docs/DECISIONS.md).

## License

MIT. See [LICENSE](LICENSE).
