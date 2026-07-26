from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from airflow.sdk import Param, dag, get_current_context, task
from src.config import DAG_DEFAULT_ARGS
from src.loaders.clickhouse.withdrawals import (
    load_withdrawals,
)

LOGGER = logging.getLogger(__name__)

CSV_FILE = Path(__file__).parent / "files" / "withdrawals.csv"


@dag(
    dag_id="load_withdrawals_to_clickhouse",
    description="Load withdrawals from CSV to ClickHouse",
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
    tags=["clickhouse", "withdrawals"],
)
def load_withdrawals_to_clickhouse():
    @task(do_xcom_push=False)
    def load_file() -> None:
        context = get_current_context()
        params = context["params"]

        result = load_withdrawals(
            file_path=CSV_FILE,
            period_from=params.get("period_from"),
            period_to=params.get("period_to"),
            lookback_days=params.get("lookback_days"),
        )

        LOGGER.info("Withdrawals loaded: %s", result)

    load_file()


load_withdrawals_to_clickhouse()
