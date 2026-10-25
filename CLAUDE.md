# CLAUDE.md

Instructions for AI agents working in this repository. Public portfolio project on open and synthetic data. Design: `docs/IMPLEMENTATION_PLAN.md`; decisions: `docs/DECISIONS.md`.

## Commands

Run everything through `make` (needs bash, make and uv: Linux, macOS or WSL).

- `make install`: `uv sync --locked`.
- `make lint`: ruff check, ruff format --check, mypy. Exactly what the CI lint job runs.
- `make fmt`: ruff format and `ruff check --fix`.
- `make test-fast`: default pytest selection (no `slow`), with the coverage gate.
- `make test`: every test, including the `slow` end-to-end CDC test (runs dbt as a subprocess; skipped without `dbt/dbt_packages`).
- `make source` / `make ingest` / `make summary`: `lakehouse source|ingest|summary`. Also `lakehouse cdc simulate [--changes N --new-orders N --seed S]`, `lakehouse ingest --mode incremental`, `lakehouse summary --watermarks`.
- `make dbt-deps` / `make dbt-build`: dbt packages and build + test.
- `make demo`: source, ingest, dbt-deps, dbt-build, summary. Must pass twice in a row.
- `make cdc-demo`: demo, then cdc simulate, incremental ingest, dbt build, summary with watermarks, an empty incremental ingest, dbt build, summary. Must pass twice in a row.
- `make clean`: deletes generated data under `data/` and dbt build output.
- `make demo DATA_SOURCE=kaggle`: real Olist data; needs `KAGGLE_API_TOKEN` (in `.env` or exported). Values in `.env` override shell variables for make (GNU make precedence), command-line variables override both; that is why every assignment in `.env.example` is commented out.

Calling dbt directly: `uv run dbt <cmd> --project-dir dbt --profiles-dir dbt`.

Env vars read by Python and dbt: `LAKEHOUSE_DATA_DIR`, `LAKEHOUSE_DUCKDB_PATH`. Always absolute paths: dbt and DuckDB resolve relative paths against different directories. The Makefile exports absolute defaults. Other variables are documented in `.env.example`.

## Repo map

- `src/de_lakehouse_pipeline/`: ingestion package. `settings.py` (env config), `contracts.py` (typed contract per table), `sources/` (synthetic generator, Kaggle fetch), `landing.py` (read, validate, reject split, watermark slice, atomic writes), `runlog.py` (run log, `last_watermark`) and `ingest.py` (orchestration, full and incremental modes), `cdc.py` (source change simulator), `summary.py`, `cli.py` (entry point `lakehouse`, no logic).
- `dbt/`: `models/staging` (views, `stg_olist__*`, one per landing table), `models/bronze` (tables, `bronze_*`, deduplicated; `bronze_orders` incremental), `models/intermediate` (views, `int_*`), `models/marts` (`fct_orders`, `fct_order_items`, `dim_customers`, `dim_products`, `dim_sellers`, `dim_dates`), `macros/generate_schema_name.sql`, `tests/generic/` (`reconciles_with_ingest_log`), `tests/singular/`, `profiles.yml` (single target `dev`).
- `tests/`: pytest suite; `test_cdc_e2e.py` is the `slow` dbt round trip.
- `data/`: generated output (source, landing, rejects, `_meta`, warehouse). Only `.gitkeep` is tracked.
- `.github/workflows/`: `ci.yml` (lint, test, pipeline), `security.yml`.

## Invariants

- dbt is the only process that writes DuckDB. Python reads DuckDB only through `duckdb.connect(path, read_only=True)` and closes it at once; an open handle blocks dbt.
- Landing, reject and source writes are atomic (temp file in the same directory, then `os.replace`). The ingest log row is written after the data it describes.
- `rows_in = rows_out + rows_rejected` in every log row.
- The synthetic generator and the CDC simulator never read the wall clock and are byte-deterministic per seed (stdlib `random.Random`; the simulator seeds with the seed and its data-derived clock).
- The CDC watermark lives only in the ingest log. `bronze_orders` empties itself when a successful full load of orders is newer than its latest `_batch_id` (DECISIONS 12).
- Rejects never orphan valid rows: a rejected parent must not leave valid children behind in the generated data.
- `data/` is never committed. No Olist rows are committed.
- Never print or log secret values (`KAGGLE_API_TOKEN`: check presence only).
- Tests are hermetic: an autouse fixture clears the pipeline's env vars and runs each test in its own temp dir, so `make test` never touches `data/`.
- Public repo: no employer names, employer code or internal identifiers. Everything is built on open or synthetic data.

## Conventions

- Python 3.12. Ruff line length 100 with the BLE rule set (a broad `except` needs `# noqa: BLE001 - reason`), DTZ for timezone-aware datetimes. mypy strict on `src`. pytest runs with `--strict-markers`; end-to-end tests carry the `slow` marker. Coverage gate: 80% on `src`, on the fast selection.
- dbt naming: `stg_olist__<table>`, `bronze_*`, `int_*`, `fct_*`, `dim_*`. Layers other than staging use CTEs, lowercase SQL and explicit column lists. Staging renames and cleans only.
- Marts have enforced contracts (`data_type` on every column) and a description on every column. Cast aggregates explicitly to match the contract.
- Bronze is deterministic: no wall-clock columns, and every dedup or tie-break has a total order, so re-runs give identical data. Uniqueness is asserted in bronze, not in staging (landing keeps duplicates). Time-bound data tests use the `source_window_max` var, never the current date.
- dbt 1.12: generic test arguments go under `arguments:`. dbt-duckdb `external_location` goes under `config:`.
- Keep `uv.lock` in sync (`uv lock`); CI uses `--locked`. DuckDB is on 2.x (DECISIONS 13): a warehouse file written by 2.x is not readable by 1.x, so rebuild it with `make clean demo` after a downgrade. DuckDB 2 relations (`con.sql(...)`) have no `fetchone()` or `fetchmany()`, and mypy does not flag it; fetch from `con.execute(...)` instead.
- Conventional Commits in English. One PR per phase (PR1 to PR3 are in the code; PR4 and PR5 are in the plan, not in the code).
- Do not document features that are not implemented as if they were. Docs written for readers stay in English.
