"""DAG integrity: run with `make dag-test` (needs Airflow + Cosmos and a dbt manifest)."""

from pathlib import Path

import pytest
from airflow.models import DagBag

DAGS = Path(__file__).resolve().parents[1] / "dags"
DAG_ID = "lakehouse_pipeline"


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
    assert {t.pool for t in dag.tasks if t not in cosmos} == {"default_pool"}


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
