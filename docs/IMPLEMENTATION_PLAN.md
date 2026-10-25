# Implementation plan

Five stacked PRs, one per phase. This file is the contract between the Python ingestion side and
the dbt side; decisions it relies on are logged in [`DECISIONS.md`](DECISIONS.md).

## Design in one paragraph

Python owns **source → landing**: it reads the source CSVs, enforces a typed contract per table,
splits rejects out with a reason, and writes Parquet files atomically plus one ingest-log row per
table and run. Python never writes the DuckDB warehouse. **dbt is the only DuckDB writer**: its
sources read the landing Parquet through dbt-duckdb `external_location`, and it builds
staging → bronze → intermediate → marts. One writer avoids DuckDB's single-writer lock under
Airflow, and the CDC watermark lives where the ingest commit is recorded (the ingest log), so
there is no separate `_watermarks` table.

Options rejected for the landing boundary: Python loading DuckDB `raw` tables plus a `_watermarks`
table (two DuckDB writers, Python owning DDL); dbt reading the CSVs directly (no contract or
reject boundary, no extraction watermark).

## Deviations from the 2026-05 planning spec

1. Default data source is a deterministic synthetic generator; Kaggle is opt-in (DECISIONS 7).
   DuckDB only, no Databricks/Delta profile (2). No Streamlit (3). Current versions (6).
2. `scripts/*.py` become subcommands of the `lakehouse` CLI entry point.
3. Landing is typed Parquet behind contracts, with rejects and a run log.
4. No `_watermarks` table, no `pipeline_config.json`, no `transform_cdc_to_hot` macro: the
   contract registry is the metadata, and a macro with one caller is not worth it.
5. Olist has no update column, so landing derives
   `order_updated_at = greatest(non-null lifecycle timestamps)`. The CDC simulator mutates the
   source CSVs, as an operational system would.
6. `bronze_orders` uses `delete+insert`, not `merge`.
7. One dbt target `dev`, paths from env. CI uses a file database, not `:memory:`, because several
   dbt and edr invocations share it.
8. `generate_schema_name` ships in PR1 so schema names never change later.
9. Nine source tables, not eight (`product_category_name_translation` is included).
10. Official `apache/airflow` slim image with docker compose, not the Astro CLI or Runtime.

## Data flow and layer contracts

```
data/source/*.csv                              source system: 9 Olist-named CSVs (generator or Kaggle); PR3 simulator mutates it
  -> lakehouse ingest                          Polars: read as Utf8 -> cast per contract -> split valid / rejects
data/landing/<table>/full.parquet              full mode (atomic replace; deletes batch-*.parquet)
data/landing/<table>/batch-<run_id>.parquet    incremental mode (PR3, orders only)
data/rejects/<table>/batch-<run_id>.parquet    all source columns as Utf8 + _reject_reason + _batch_id
data/_meta/ingest_log/run-<run_id>.parquet     one row per (run, table)
  -> dbt (only DuckDB writer)                  data/warehouse/lakehouse.duckdb
     schemas: staging (views) -> bronze (PR2) -> intermediate (PR2) -> marts -> elementary (PR5)
```

- `run_id` = `YYYYMMDDTHHMMSSffffffZ` (UTC, sorts lexically); it is also the `_batch_id`.
- Commit order: landing file via tmp + `os.replace`, then the log row. A crash in between leaves an
  orphan landing file with no log row; the watermark has not moved, so the next run extracts those
  rows again and bronze dedup absorbs them (at-least-once into landing, exactly-once in bronze).
- Landing tables (source file `olist_<x>_dataset.csv` unless noted): `orders`, `order_items`,
  `customers`, `products`, `sellers`, `reviews` (`order_reviews`), `payments` (`order_payments`),
  `geolocation`, `category_translation` (`product_category_name_translation.csv`).
- Every landing file carries `_batch_id VARCHAR`, `_extracted_at TIMESTAMPTZ`,
  `_source_file VARCHAR`.
- Reject rules: a required column is null; a non-empty value fails its cast. If the rejected ratio
  for a table exceeds `MAX_REJECT_RATIO` (default 0.01) the table's status is `failed`, no landing
  file is written and the CLI exits 1. A missing source column raises `SchemaMismatchError` for the
  table. Extra columns are dropped with a warning. A BOM is stripped from header names.
- Duplicate primary keys are not rejected: landing stays faithful, bronze dedups.

## PR1 — minimum shippable (`feat/minimum-shippable`)

### Task A1 — Python core (`src/de_lakehouse_pipeline/`, `tests/`)

| Path | Responsibility |
| --- | --- |
| `settings.py` | `Settings(BaseSettings)`, `env_file=".env"`, `extra="ignore"`. Fields (env alias): `data_source: Literal["synthetic","kaggle"] = "synthetic"` (`DATA_SOURCE`), `data_dir: Path = Path("data")` (`LAKEHOUSE_DATA_DIR`), `duckdb_path: Path \| None` (`LAKEHOUSE_DUCKDB_PATH`, default `data_dir/warehouse/lakehouse.duckdb`), `synthetic_orders: int = 5000, ge=200` (`SYNTHETIC_ORDERS`), `synthetic_seed: int = 42` (`SYNTHETIC_SEED`), `max_reject_ratio: float = 0.01` (`MAX_REJECT_RATIO`). Properties `source_dir`, `landing_dir`, `rejects_dir`, `ingest_log_dir`, `warehouse_path` (resolved); `ensure_dirs()`. No work at import time. |
| `contracts.py` | `@dataclass(frozen=True, slots=True) TableContract(name, source_file, columns: Mapping[str, pl.DataType], primary_key: tuple[str, ...], required: frozenset[str], derived: Mapping[str, pl.Expr] = {}, incremental_column: str \| None = None)`; `CONTRACTS: Final[Mapping[str, TableContract]]` for all 9 tables; `METADATA_COLUMNS`. |
| `io.py` | `atomic_write_parquet(df, path)`: tmp file in the same dir, `os.replace`, tmp removed on error. |
| `sources/__init__.py` | `prepare_source(settings) -> dict[str, Path]`, dispatch on `data_source`. |
| `sources/synthetic.py` | `generate(dest, *, orders, seed) -> dict[str, Path]`. Stdlib `random.Random(seed)` only; every value built as a string; never reads the wall clock (window 2017-01-01..2018-08-31); each CSV written atomically. |
| `sources/_vocab.py` | Constants: 27 UFs, cities, ~20 categories with translations (one untranslated on purpose), statuses, payment types. |
| `sources/kaggle.py` | `fetch(dest) -> dict[str, Path]`. Imports `kagglehub` inside the function. `KaggleUnavailableError` if the extra is missing or `KAGGLE_API_TOKEN` is unset (presence check only, never log the value). `dataset_download("olistbr/brazilian-ecommerce")`, copy the 9 CSVs. |
| `landing.py` | `read_source_csv(path, contract)` (all Utf8, `""` → null, header check); `validate(raw, contract) -> ValidationResult(valid, rejects)`; `write_landing(df, landing_dir, contract, batch_id, mode: Literal["full","incremental"]) -> Path`; `write_rejects(...) -> Path \| None`; `SchemaMismatchError`. |
| `runlog.py` | `TableRun` dataclass, `INGEST_LOG_SCHEMA`, `write_run_log(records, log_dir, run_id) -> Path`, `read_run_log(log_dir) -> pl.DataFrame`. |
| `ingest.py` | `new_run_id(now) -> str`; `ingest(settings, tables=None, *, mode="full", clock=utc_now) -> list[TableRun]`; the run log is written in `finally` with each table's status. |
| `summary.py` | `summarize(settings) -> str`: last run's log rows plus mart row counts via `duckdb.connect(path, read_only=True)`, closed at once. |
| `cli.py` | Subcommands, no logic: `source [--orders N --seed S]`, `ingest [--tables ...]`, `summary`. Exit 1 if any table failed. Stdlib logging with `run_id` in the format. |

Ingest-log columns: `run_id, data_source, table_name, mode, status ('success'|'failed'),
started_at, finished_at (TIMESTAMPTZ), rows_in, rows_out, rows_rejected (BIGINT),
watermark_column, watermark_from, watermark_to (nullable), landing_file, error (nullable)`.
Invariant: `rows_in = rows_out + rows_rejected`.

Generator dirt (deterministic, proportional to scale; a rejected row never leaves a valid row
orphaned): exact and jittered geolocation duplicates per zip; ~1% of orders without items, all
`canceled`/`unavailable`/`created`; junk rows with a null PK in orders and customers; a few
`order_items` with a non-numeric `price`; `review_id` repeated across orders; products with a null
category and one category missing from the translation table; cities with mixed case and trailing
spaces; zip prefixes without zero padding.

Tests (fast): `test_synthetic.py` (same seed → byte-identical sha256, other seed differs, headers
match contract order, referential integrity, every kind of dirt present); `test_contracts.py`
(PK ⊆ required ⊆ columns, the 9 file names); `test_landing.py` (reject reasons table-driven,
missing column raises, extra column dropped, failed atomic write keeps the old file and leaves no
tmp); a Hypothesis property that `validate` conserves row counts; `test_ingest.py` (tiny run in
`tmp_path`, log counts add up, re-run gives identical data columns, reject ratio fails the table
with no landing file); `test_runlog.py` (round trip); `test_cli.py` (subcommands).

### Task A2 — Makefile, CI, docs

Makefile: `-include .env` + `export`; `##` help on every target;
`DBT := uv run dbt --project-dir dbt --profiles-dir dbt`;
`export LAKEHOUSE_DATA_DIR ?= $(abspath data)`. Targets: `help` (default), `install`
(`uv sync --locked`), `lint` (ruff check, ruff format --check, mypy — exactly the CI lint job),
`fmt`, `test` (all tests incl. slow), `test-fast` (default selection), `source`, `ingest`,
`dbt-deps`, `dbt-build`, `summary`, `demo` (source + ingest + dbt-deps + dbt-build + summary),
`clean` (`data/*` except `.gitkeep`, `dbt/target`).

`.github/workflows/ci.yml`: push to main, pull_request, workflow_dispatch, cron `0 6 * * 1`;
top-level `permissions: contents: read`; concurrency per workflow/event/ref, cancel in progress on
PRs. Every job: checkout `3d3c42e5aac5ba805825da76410c181273ba90b1 # v7.0.1` with
`persist-credentials: false`, setup-uv `07a047e608db84b3d8c06cd9a7c7dcc9c355c18e # v10.3.2` with
`python-version: "3.12"` and cache, `uv sync --locked`, `timeout-minutes`. Jobs: `lint` (ruff with
`--output-format=github`, format check, mypy); `test` (`needs: lint`, pytest with coverage gate);
`pipeline` (`needs: lint`, `LAKEHOUSE_DATA_DIR=${{ runner.temp }}/data`, `make demo`, then
upload-artifact `cf430e030ddbb5b0abf93d22962f4752f3646cd9 # v7.0.2` with `if: always()` for
`dbt/target/run_results.json` and `${{ runner.temp }}/data/_meta`).

`.github/workflows/security.yml`: `pip-audit` on `uv export --frozen --all-extras --all-groups
--no-emit-project` (Python 3.12) and `zizmor` on the workflows; weekly cron.

Also: `.env.example` (every assignment commented out: values in `.env` override the shell for make), `README.md`,
`CHANGELOG.md`, repo `CLAUDE.md` (Commands, Repo map, Invariants, Conventions).

### Task B — dbt (`dbt/`)

| Path | Responsibility |
| --- | --- |
| `dbt_project.yml` | name/profile `de_lakehouse_pipeline`; `flags: {send_anonymous_usage_stats: false}`; `staging: +schema: staging, +materialized: view`; `marts: +schema: marts, +materialized: table, +contract: {enforced: true}`. |
| `profiles.yml` | target `dev`: `type: duckdb`, `path: "{{ env_var('LAKEHOUSE_DUCKDB_PATH', '../data/warehouse/lakehouse.duckdb') }}"`, `threads: 4`. |
| `packages.yml` + `package-lock.yml` | `dbt-labs/dbt_utils` 1.4.1. |
| `macros/generate_schema_name.sql` | Use the custom schema as-is. |
| `models/staging/_stg__sources.yml` | Source `olist` with `external_location` = `read_parquet('<LAKEHOUSE_DATA_DIR>/landing/{name}/*.parquet', union_by_name = true)`; tables orders, order_items, customers. Source `ingest` with table `ingest_log` over `_meta/ingest_log/*.parquet`. `reconciles_with_ingest_log` data test on each olist table. |
| `stg_olist__orders.sql`, `stg_olist__order_items.sql`, `stg_olist__customers.sql` | Rename and clean only: lower(status), trim/lower(city), upper(state), `lpad(zip, 5, '0')`. Keep `_batch_id`, `_extracted_at`. |
| `_stg__models.yml` | orders: PK unique/not_null, `accepted_values` on the 8 Olist statuses. items: `dbt_utils.unique_combination_of_columns(order_id, order_item_id)`, relationships to orders. customers: PK unique/not_null. |
| `tests/generic/test_reconciles_with_ingest_log.sql` | Source `count(*)` equals `sum(rows_out)` over successful log rows for that table since the latest full batch. |
| `models/marts/fct_orders.sql` | One row per order: `order_id, customer_id, customer_unique_id` (FK to `dim_customers`; fixes the spec's grain mismatch), `order_status`, the 5 Olist timestamps, `total_items bigint`, `total_amount decimal(18,2)`, `total_freight decimal(18,2)` (explicit casts, 0 for orders without items), `delivery_days integer`, `is_late_delivery boolean`. |
| `models/marts/dim_customers.sql` | One row per `customer_unique_id`: zip, city, state from the latest order; `first_order_at`, `last_order_at`, `total_orders bigint`. |
| `_marts__models.yml` | `data_type` + description on every column; PK tests; relationships `fct_orders.customer_unique_id → dim_customers`; `dbt_utils.accepted_range(min_value=0)` on amounts. |

### Hand-off contract A ↔ B

- Env vars `LAKEHOUSE_DATA_DIR`, `LAKEHOUSE_DUCKDB_PATH`; the Makefile always exports absolute
  paths.
- Landing glob `<data>/landing/<table>/*.parquet`.
- Types (Polars → DuckDB): Utf8→VARCHAR, Int64→BIGINT, Float64→DOUBLE, Decimal(12,2)→DECIMAL(12,2),
  Datetime("us")→TIMESTAMP (naive source local time), Datetime("us","UTC")→TIMESTAMPTZ.
- orders: `order_id, customer_id, order_status` VARCHAR; `order_purchase_timestamp,
  order_approved_at, order_delivered_carrier_date, order_delivered_customer_date,
  order_estimated_delivery_date` TIMESTAMP; derived `order_updated_at` TIMESTAMP. PK `order_id`;
  required `order_id, customer_id, order_status, order_purchase_timestamp`.
- order_items: `order_id`, `order_item_id` BIGINT, `product_id`, `seller_id`,
  `shipping_limit_date` TIMESTAMP, `price`, `freight_value` DECIMAL(12,2). PK
  (`order_id`, `order_item_id`).
- customers: all VARCHAR (zip stays a string). PK `customer_id`.

### Verification

```
uv sync --locked && make lint && make test        # exit 0
make demo && make demo                            # exit 0 twice; identical mart counts
git ls-files data                                 # only data/.gitkeep
```

CI green on lint, test, pipeline, pip-audit and zizmor. Kaggle is a manual check:
`make demo DATA_SOURCE=kaggle`.

## PR2 — full medallion (dbt only)

- Staging for products, sellers, reviews, payments, geolocation, category_translation.
- `bronze_*` for the 9 tables (`table`): `row_number()` over the PK ordered by `_batch_id desc`
  (orders also by `order_updated_at desc`); `bronze_geolocation` collapses to one row per zip
  prefix (avg lat/lng, most frequent city/state); reviews keyed on (`review_id`, `order_id`); each
  keeps `_batch_id`, `_extracted_at`, `_source_file`.
- Intermediate: `int_order_items_enriched` (one row per item with product, translated category —
  `'unknown'` when untranslated — and seller), `int_review_enriched`.
- Marts: `fct_order_items`, `dim_products`, `dim_sellers`, `dim_dates` (`generate_series` over the
  order date range); `fct_orders` and `dim_customers` repointed to bronze/intermediate. All marts
  keep `contract: enforced`.
- Singular test `assert_orders_without_items_are_unfulfilled`. Future-date and positive-amount
  checks use `dbt_utils.accepted_range` / `expression_is_true`, not custom tests.
- Risk: contract `data_type` mismatches on aggregates; cast explicitly.

## PR3 — CDC simulation

- `cdc.py`: `simulate(source_dir, *, changes, new_orders, seed) -> SimulationResult`. The simulated
  clock steps from the source's max timestamp, one hour per batch, never the wall clock.
  Transitions that each set a timestamp (created→approved, approved→shipped, shipped→delivered);
  new orders come with their customers and items so referential integrity holds; CSVs rewritten
  atomically.
- `runlog.last_watermark(log_dir, table) -> datetime | None`.
- `ingest(..., mode="incremental")`: tables with `incremental_column` (orders,
  `order_updated_at`) filter `> watermark_from`; no previous watermark → full load; zero rows →
  no file, `status=success`, watermark unchanged. Other tables load in full mode.
- CLI: `cdc simulate`, `ingest --mode incremental`, `summary --watermarks`.
- `bronze_orders` incremental: `delete+insert`, `unique_key='order_id'`, `on_schema_change='fail'`,
  filter `_batch_id > (select coalesce(max(_batch_id), '') from {{ this }})`, dedup inside the
  slice. The uniqueness test on `stg_olist__orders` moves to `bronze_orders` (staging becomes a
  change log).
- `make cdc-demo`: demo → summary → simulate (50 changes, 20 new) → incremental ingest →
  `dbt build` → summary (+20 orders, changed statuses, new watermark) → incremental ingest again
  (0 rows) → `dbt build` → identical summary.
- `tests/test_cdc_e2e.py` (`slow`, `dbtRunner` in `tmp_path`) asserts each claim; CI pipeline job
  adds `make cdc-demo` and `pytest -m slow`.
- Documented limits: no hard deletes; a change that sets no timestamp is invisible; `>` assumes a
  monotonic source clock.

## PR4 — orchestration

- `airflow/Dockerfile`, multi-stage on `apache/airflow:slim-3.3.2-python3.12`: the builder creates
  `/opt/lakehouse-venv` from `uv export --frozen --no-dev` plus the project wheel and bakes
  `manifest.json` with `dbt deps` + `dbt parse`; the final stage installs
  `astronomer-cosmos==1.15.1` under the 3.3.2 / Python 3.12 constraints, copies the venv and
  `dbt/`, runs as `USER airflow`.
- `docker-compose.yml` (project name `de-lakehouse-pipeline`): `postgres` (pinned, healthcheck),
  `airflow-init` (`db migrate`, `pools set duckdb_writer 1`), `api-server` (8080, health
  `/api/v2/monitor/health`), `scheduler`, `dag-processor`; LocalExecutor; simple auth manager with
  all-admins for local use; JWT secret and Fernet key from `.env` (generated by `make up` if
  missing); named volume `lakehouse-data` at `/opt/airflow/data`. No triggerer (nothing defers).
- DAG `lakehouse_pipeline` (`max_active_runs=1`, `catchup=False`): bash tasks `lakehouse source
  --if-missing` → `cdc simulate` → `ingest --mode incremental` → Cosmos `DbtTaskGroup`
  (`ExecutionMode.LOCAL`, `dbt_executable_path=/opt/lakehouse-venv/bin/dbt`,
  `LoadMode.DBT_MANIFEST`, the repo `profiles.yml`, no dbt deps install at runtime,
  `operator_args={"pool": "duckdb_writer"}`). The 1-slot pool serializes per-model dbt processes
  against DuckDB's single writer.
- CI `dags` job: throwaway venv with Airflow 3.3.2 + Cosmos under constraints (outside `uv.lock`),
  `dbt parse` for the manifest, `DagBag` with no import errors and the expected task ids.
- `.github/workflows/compose.yml`: PRs touching `airflow/**`, `dbt/**`, `src/**`, `uv.lock` or the
  compose file, plus main, weekly and dispatch: `docker compose up -d --wait`,
  `airflow dags list-import-errors` empty, `airflow dags test lakehouse_pipeline`; 30 min timeout.
- Makefile: `up`, `down`, `airflow-manifest`.

## PR5 — observability and static site

- `elementary-data/elementary` 0.26.1 in `packages.yml` (`elementary: +schema: elementary`); an
  `elementary` profile on the same file; `elementary.schema_changes` on bronze,
  `volume_anomalies` on `bronze_orders` (history from `cdc-demo`).
- edr CLI in dependency group `site` (`elementary-data[dbt1-duckdb]==0.26.1`); if it does not
  resolve next to Polars 2 / Pydantic, run it through `uvx --from ...` and document the audit gap.
- `make site`: `dbt docs generate --static` → `site/dbt/index.html`;
  `edr report ... --file-path site/elementary/index.html`; a hand-written `site/index.html`.
- Cosmos: `RenderConfig(exclude=["package:elementary"])` plus one `dbt run --select elementary`
  task and one `edr report` task.
- `ci.yml` gains `site` (`needs: [test, pipeline]`, builds from synthetic data on every event) and
  `deploy-pages` (`needs: site`, only on push to `main`, `permissions: {pages: write,
  id-token: write}`, `environment: github-pages`) with configure-pages
  `45bfe0192ca1faeb007ade9deae92b16b8254a0d # v6.0.0`, upload-pages-artifact
  `fc324d3547104276b827a68afc52ff2a11cc49c9 # v5.0.0`, deploy-pages
  `368f82528645a54fb793d4d04e342629a3f51346 # v5.0.1`. The owner enables Pages (source: GitHub
  Actions) before this merges.
- README: real badges and screenshots; `docs/architecture.md`.

## Risks

1. `env_var` inside a source `external_location` and the `{name}` substitution are unverified on
   dbt 1.12 / dbt-duckdb 1.11: check with `dbt compile` first; fallback `var('landing_dir')`.
2. dbt working-directory handling for relative defaults: the Makefile and CI always pass absolute
   paths.
3. Git Bash `make` on Windows yields `/d/...` paths native DuckDB cannot open: the supported route
   is Linux, macOS or WSL; the `lakehouse` CLI itself is portable.
4. Polars 2.0 API drift (`infer_schema`, Decimal, `str.to_datetime(strict=False)`): unit tests
   pin the behaviour.
5. Any open DuckDB handle blocks dbt; `summary` connects read-only and closes at once.
6. Elementary on DuckDB and its CLI's dependency set: spike at the start of PR5.
7. Airflow 3 slim image: standard provider presence and `DagBag` parsing without a metadata DB;
   verified in the `dags` job.
8. Kaggle file drift (BOM, nulls): manual run only; rejects surface it.
9. DuckDB 2.x stays blocked until dbt-duckdb supports it (DECISIONS 6).
