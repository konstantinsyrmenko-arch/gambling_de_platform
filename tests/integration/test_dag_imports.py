from __future__ import annotations

import pytest
from airflow.models import DagBag

pytestmark = pytest.mark.integration


def test_all_dags_import_without_errors():
    dag_bag = DagBag(
        dag_folder="/opt/airflow/dags",
        include_examples=False,
    )

    assert dag_bag.import_errors == {}


def test_end_to_end_pipeline_dependency_graph():
    dag_bag = DagBag(
        dag_folder="/opt/airflow/dags",
        include_examples=False,
    )
    dag = dag_bag.get_dag("gaming_etl_pipeline")

    assert dag is not None
    assert dag.schedule == "0 1 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.render_template_as_native_obj is True
    assert dag.params["lookback_days"] == 0
    assert set(dag.task_ids) == {
        "load_providers",
        "load_players",
        "load_exchange_rates",
        "load_games",
        "load_deposits",
        "load_withdrawals",
        "load_game_transactions",
        "build_analytics_outputs",
    }
    for task in dag.tasks:
        assert task.trigger_run_id.startswith(
            "orchestrated__{{ dag.dag_id }}__"
        )
        assert task.retries == 2

    assert dag.get_task("load_providers").downstream_task_ids == {
        "load_games",
    }
    expected_fact_tasks = {
        "load_deposits",
        "load_withdrawals",
        "load_game_transactions",
    }
    for task_id in ("load_players", "load_exchange_rates", "load_games"):
        assert dag.get_task(task_id).downstream_task_ids == (expected_fact_tasks)
    for task_id in expected_fact_tasks:
        assert dag.get_task(task_id).downstream_task_ids == {
            "build_analytics_outputs",
        }
