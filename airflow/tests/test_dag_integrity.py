"""DAG integrity: run with `make dag-test` (needs Airflow + Cosmos and a dbt manifest)."""

from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS = Path(__file__).resolve().parents[1] / "dags"
DAG_ID = "lakehouse_pipeline"
# dbt processes outside Cosmos; the Python tasks never write DuckDB and stay in the default pool
WRITERS = {"elementary_models", "edr_report"}


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS))


def test_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}


def test_dag_settings(dagbag: DagBag) -> None:
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None
    assert dag.schedule == "@hourly"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_expected_tasks_and_pool(dagbag: DagBag) -> None:
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None
    ids = set(dag.task_ids)
    assert {"source", "cdc_simulate", "ingest"} <= ids
    cosmos = [t for t in dag.tasks if t.task_id.startswith("dbt.")]
    assert any(t.task_id == "dbt.fct_orders.run" for t in cosmos), sorted(ids)
    assert any(t.task_id.endswith(".test") or ".test" in t.task_id for t in cosmos)
    assert {t.pool for t in cosmos} == {"duckdb_writer"}
    assert {t.pool for t in dag.tasks if t not in cosmos} == {"default_pool", "duckdb_writer"}
    for t in dag.tasks:
        assert (t.pool == "duckdb_writer") == (t in cosmos or t.task_id in WRITERS), t.task_id


def test_multi_parent_tests_run_after_every_parent(dagbag: DagBag) -> None:
    """A relationships test reads two models, so it runs once both are built, as in dbt build."""
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None
    detached = [t for t in dag.tasks if t.task_id.startswith("dbt.relationships_")]
    assert detached, "relationships tests should be rendered as their own tasks"
    for task in detached:
        parents = {u for u in task.upstream_task_ids if u.startswith("dbt.")}
        assert len(parents) >= 2, (task.task_id, sorted(parents))
    test_name = (
        "relationships_fct_orders_customer_unique_id__customer_unique_id__ref_dim_customers_"
    )
    task = dag.get_task(f"dbt.{test_name}_test")
    assert {"dbt.fct_orders.test", "dbt.dim_customers.test"} <= task.upstream_task_ids
    # and the per-model test tasks leave it out
    assert test_name in str(dag.get_task("dbt.dim_customers.test").exclude).split()


def test_elementary_tasks_order_and_pool(dagbag: DagBag) -> None:
    """Elementary's tables are built before the dbt group and the report is built after it."""
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None
    assert {"elementary_models", "edr_report"} <= set(dag.task_ids)
    assert dag.get_task("elementary_models").pool == "duckdb_writer"
    assert dag.get_task("edr_report").pool == "duckdb_writer"
    assert "ingest" in dag.get_task("elementary_models").upstream_task_ids
    cosmos = [t for t in dag.tasks if t.task_id.startswith("dbt.")]
    assert cosmos
    roots = [t for t in cosmos if not {u for u in t.upstream_task_ids if u.startswith("dbt.")}]
    assert roots
    for task in roots:
        assert "elementary_models" in task.upstream_task_ids, task.task_id
    leaves = [t for t in cosmos if not {d for d in t.downstream_task_ids if d.startswith("dbt.")}]
    for task in leaves:
        assert "edr_report" in task.downstream_task_ids, task.task_id


def test_artifacts_upload_once_per_run(dagbag: DagBag) -> None:
    """Only elementary_models uploads dbt artifacts; every Cosmos task skips it, not run results."""
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None
    cosmos = [t for t in dag.tasks if t.task_id.startswith("dbt.")]
    assert cosmos
    for task in cosmos:
        assert task.vars == {"disable_dbt_artifacts_autoupload": True}, task.task_id
    command = dag.get_task("elementary_models").bash_command
    assert "disable_" not in command and "--vars" not in command


def test_no_task_comes_from_the_elementary_package(dagbag: DagBag) -> None:
    """Cosmos task ids carry the dbt node name; package models are `elementary.*` models, which
    the project does not define, so none of the package's model names may appear."""
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None
    names = ("dbt_models", "dbt_run_results", "elementary_test_results", "schema_columns_snapshot")
    for task_id in dag.task_ids:
        assert not any(task_id.startswith(f"dbt.{n}.") for n in names), task_id
    # the project's own Elementary tests on bronze run inside the per-model test task (`dbt ls --exclude package:elementary` keeps them: their package is the project)
    assert "dbt.bronze_orders.test" in dag.task_ids


def test_source_reconciliation_tests_run_after_ingest(dagbag: DagBag) -> None:
    dag = dagbag.dags.get(DAG_ID)
    assert dag is not None
    recon = [t for t in dag.tasks if t.task_id.startswith("dbt.source_reconciles_with_ingest_log")]
    assert len(recon) == 9, [t.task_id for t in recon]
    for task in recon:
        assert "elementary_models" in task.upstream_task_ids
        assert "ingest" in dag.get_task("elementary_models").upstream_task_ids
