from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task
from src.config import DAG_DEFAULT_ARGS
from src.loaders.clickhouse.game_transactions import (
    load_game_transactions,
)

LOGGER = logging.getLogger(__name__)

CSV_FILE = Path(__file__).parent / "files" / "games.csv"


@dag(
    dag_id="load_game_transactions_to_clickhouse",
    description="Load game transactions from CSV to ClickHouse",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
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
            title="Period start",
        ),
        "period_to": Param(
            default="",
            type=["null", "string"],
            title="Period end",
        ),
    },
    tags=["clickhouse", "game-transactions"],
)
def load_game_transactions_to_clickhouse():
    @task(do_xcom_push=False)
    def load_file() -> None:
        context = get_current_context()
        params = context["params"]

        result = load_game_transactions(
            file_path=CSV_FILE,
            period_from=params.get("period_from"),
            period_to=params.get("period_to"),
            lookback_days=params.get("lookback_days"),
        )

        LOGGER.info("Game transactions loaded: %s", result)

    load_file()


load_game_transactions_to_clickhouse()
