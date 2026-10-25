"""Hourly pipeline: source CSVs -> CDC simulation -> incremental ingest -> dbt build (Cosmos).

Paths come from the environment so the same file runs in the image and in the test venv.
"""

import os
from datetime import UTC, datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG
from cosmos import (
    DbtTaskGroup,
    ExecutionConfig,
    ExecutionMode,
    InvocationMode,
    LoadMode,
    ProfileConfig,
    ProjectConfig,
    RenderConfig,
)

DBT_PROJECT_DIR = Path(os.environ.get("LAKEHOUSE_DBT_PROJECT_DIR", "/opt/airflow/dbt"))
DBT_MANIFEST = Path(
    os.environ.get("LAKEHOUSE_DBT_MANIFEST", str(DBT_PROJECT_DIR / "target" / "manifest.json"))
)
DBT_BIN = os.environ.get("LAKEHOUSE_DBT_BIN", "/opt/lakehouse-venv/bin/dbt")
LAKEHOUSE_BIN = os.environ.get("LAKEHOUSE_BIN", "/opt/lakehouse-venv/bin/lakehouse")
POOL = "duckdb_writer"

with DAG(
    dag_id="lakehouse_pipeline",
    schedule="@hourly",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "dbt", "duckdb"],
) as dag:
    # Python never writes DuckDB, so these three need no pool slot.
    source = BashOperator(task_id="source", bash_command=f"{LAKEHOUSE_BIN} source --if-missing")
    cdc_simulate = BashOperator(
        task_id="cdc_simulate", bash_command=f"{LAKEHOUSE_BIN} cdc simulate"
    )
    ingest = BashOperator(
        task_id="ingest", bash_command=f"{LAKEHOUSE_BIN} ingest --mode incremental"
    )

    # Every dbt process opens DuckDB for writing, so the 1-slot pool covers every Cosmos task.
    dbt = DbtTaskGroup(
        group_id="dbt",
        project_config=ProjectConfig(
            dbt_project_path=DBT_PROJECT_DIR, manifest_path=DBT_MANIFEST, install_dbt_deps=False
        ),
        profile_config=ProfileConfig(
            profile_name="de_lakehouse_pipeline",
            target_name="dev",
            profiles_yml_filepath=DBT_PROJECT_DIR / "profiles.yml",
        ),
        # A test with several parents (relationships between marts) runs as its own task after
        # all of them, as in `dbt build`; otherwise it would run with the first parent built and
        # fail on a table that does not exist yet.
        render_config=RenderConfig(
            load_method=LoadMode.DBT_MANIFEST, should_detach_multiple_parents_tests=True
        ),
        execution_config=ExecutionConfig(
            execution_mode=ExecutionMode.LOCAL,
            invocation_mode=InvocationMode.SUBPROCESS,
            dbt_executable_path=DBT_BIN,
        ),
        operator_args={"pool": POOL, "append_env": True},
    )

    source >> cdc_simulate >> ingest >> dbt
