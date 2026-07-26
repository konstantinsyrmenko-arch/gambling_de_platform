from __future__ import annotations

from datetime import datetime

from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)
from airflow.sdk import DAG, Param, cross_downstream
from src.config import DAG_DEFAULT_ARGS


def _trigger(
    *,
    task_id: str,
    dag_id: str,
    conf: dict[str, str] | None = None,
) -> TriggerDagRunOperator:
    return TriggerDagRunOperator(
        task_id=task_id,
        trigger_dag_id=dag_id,
        trigger_run_id=(
            f"orchestrated__{{{{ dag.dag_id }}}}__{dag_id}__"
            "{{ run_id }}"
        ),
        conf=conf,
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True,
    )


LOAD_PERIOD = {
    "period_from": "{{ params.period_from }}",
    "period_to": "{{ params.period_to }}",
    "lookback_days": "{{ params.lookback_days }}",
}
REPORT_PERIOD = {
    "date_from": "{{ params.period_from }}",
    "date_to": "{{ params.period_to }}",
    "lookback_days": "{{ params.lookback_days }}",
}


with DAG(
    dag_id="gaming_etl_pipeline",
    description="Load dictionaries and facts, then rebuild analytics",
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args=DAG_DEFAULT_ARGS,
    params={
        "lookback_days": Param(
            default=0,
            type="integer",
            minimum=0,
            description="Days through today to reload; 0 means all data",
        ),
        "period_from": Param(
            default="",
            type=["null", "string"],
            description="Inclusive period start, YYYY-MM-DD",
        ),
        "period_to": Param(
            default="",
            type=["null", "string"],
            description="Inclusive period end, YYYY-MM-DD",
        ),
    },
    tags=["etl", "end-to-end", "gaming"],
) as gaming_etl_pipeline:
    providers = _trigger(
        task_id="load_providers",
        dag_id="load_providers_to_postgres",
    )
    players = _trigger(
        task_id="load_players",
        dag_id="load_players_to_postgres",
    )
    exchange_rates = _trigger(
        task_id="load_exchange_rates",
        dag_id="load_exchange_rates_to_postgres",
        conf=LOAD_PERIOD,
    )
    games = _trigger(
        task_id="load_games",
        dag_id="load_games_to_postgres",
    )

    deposits = _trigger(
        task_id="load_deposits",
        dag_id="load_deposits_to_clickhouse",
        conf=LOAD_PERIOD,
    )
    withdrawals = _trigger(
        task_id="load_withdrawals",
        dag_id="load_withdrawals_to_clickhouse",
        conf=LOAD_PERIOD,
    )
    game_transactions = _trigger(
        task_id="load_game_transactions",
        dag_id="load_game_transactions_to_clickhouse",
        conf=LOAD_PERIOD,
    )
    analytics = _trigger(
        task_id="build_analytics_outputs",
        dag_id="build_analytics_outputs",
        conf=REPORT_PERIOD,
    )

    providers >> games
    cross_downstream(
        [players, exchange_rates, games],
        [deposits, withdrawals, game_transactions],
    )
    [deposits, withdrawals, game_transactions] >> analytics
